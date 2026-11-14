"""Durable task handoffs with exclusive leases and explicit recovery."""

import asyncio
import re
import secrets
import sqlite3
import time
import uuid
from contextlib import closing, contextmanager
from pathlib import Path

from .ipc import ProtocolError, RequestError
from .models import INTEGER, TEXT
from .service import fail, serve, service_args
from .toolbox import METHODS, ToolService, tool

TOOLS = {
    "submit": tool(
        "Queue a task for an agent. Retries using the same sender and key return the original task.",
        {"sender": TEXT, "agent": TEXT, "key": TEXT, "task": TEXT},
    ),
    "list_tasks": tool(
        "List tasks newest first. Optionally filter by agent, status or cursor.",
        optional={"agent": TEXT, "status": TEXT, "before": INTEGER, "limit": INTEGER},
    ),
    "get": tool("Read a task and any recorded result.", {"id": TEXT}),
    "review": tool(
        "Resolve a task that may have been interrupted. Choose requeue or cancel after checking its effects.",
        {"id": TEXT, "action": TEXT, "note": TEXT, "expected_revision": INTEGER},
    ),
}
# Dispatcher operations stay available over IPC without being advertised to models.
DISPATCH = {
    "claim": tool("Claim a queued task", {"agent": TEXT, "lease_seconds": INTEGER}),
    "renew": tool(
        "Renew an active lease", {"id": TEXT, "token": TEXT, "lease_seconds": INTEGER}
    ),
    "finish": tool(
        "Record the result of an active lease",
        {"id": TEXT, "token": TEXT, "status": TEXT, "result": TEXT},
    ),
}
STATES = ("queued", "running", "done", "review", "cancelled")


def label(value, field):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", value):
        raise fail(
            "invalid_label",
            f"{field} must use 1–96 letters, digits, dots, underscores, or hyphens",
        )
    return value


def content(value, field, limit):
    if not isinstance(value, str) or not value.strip() or len(value.encode()) > limit:
        raise fail("invalid_content", f"{field} requires 1–{limit} UTF-8 bytes")
    return value


def number(value, lo, hi, field):
    if type(value) is not int or not lo <= value <= hi:
        raise fail("invalid_number", f"{field} must be between {lo} and {hi}")
    return value


class Mailbox:
    def __init__(self, path, clock=time.time):
        self.path = Path(path).expanduser().resolve()
        self.clock = clock
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.path.touch(mode=0o600, exist_ok=True)
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("PRAGMA journal_mode = WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS tasks (
                    seq INTEGER PRIMARY KEY,
                    id TEXT NOT NULL UNIQUE, sender TEXT NOT NULL, agent TEXT NOT NULL,
                    key TEXT NOT NULL, task TEXT NOT NULL, status TEXT NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 1, attempts INTEGER NOT NULL DEFAULT 0,
                    token TEXT, lease_until REAL, result TEXT, review_note TEXT,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL,
                    UNIQUE(sender, key)
                );
                CREATE INDEX IF NOT EXISTS task_pick ON tasks(agent, status, seq);
                CREATE TABLE IF NOT EXISTS completions (
                    task_id TEXT NOT NULL REFERENCES tasks(id),
                    token TEXT NOT NULL, status TEXT NOT NULL, result TEXT NOT NULL,
                    PRIMARY KEY(task_id, token)
                );
            """)

    @contextmanager
    def transaction(self):
        with closing(sqlite3.connect(self.path, timeout=3)) as db, db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN IMMEDIATE")
            now = self.clock()
            db.execute(
                """
                UPDATE tasks
                   SET status='review', token=NULL, lease_until=NULL, revision=revision+1,
                       review_note='Lease expired. Verify effects before retrying.', updated_at=?
                 WHERE status='running' AND lease_until<=?
            """,
                (now, now),
            )
            # Preserve lease expiry even when a method rejects a stale request.
            db.execute("SAVEPOINT operation")
            try:
                yield db, now
            except RequestError:
                db.execute("ROLLBACK TO operation")
                db.execute("RELEASE operation")
                db.commit()
                raise

    @staticmethod
    def public(row):
        if row is None:
            return None
        result = dict(row)
        result.pop("token")
        return result

    def request(self, method, args):
        if method not in TOOLS and method not in DISPATCH:
            raise fail("unknown_tool", "Unknown mailbox method")
        with self.transaction() as (db, now):
            return getattr(self, method)(db, now, **args)

    def submit(self, db, now, sender, agent, key, task):
        sender, agent, key = (
            label(value, name)
            for name, value in (("sender", sender), ("agent", agent), ("key", key))
        )
        task = content(task, "task", 4000)
        row = db.execute(
            "SELECT * FROM tasks WHERE sender=? AND key=?", (sender, key)
        ).fetchone()
        if row:
            if (row["agent"], row["task"]) != (agent, task):
                raise fail(
                    "key_conflict",
                    "This sender and key already identify a different task",
                )
            return self.public(row)
        identifier = uuid.uuid4().hex
        db.execute(
            """
            INSERT INTO tasks(id, sender, agent, key, task, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)
        """,
            (identifier, sender, agent, key, task, now, now),
        )
        return self.get(db, now, identifier)

    def get(self, db, _now, id):
        row = db.execute("SELECT * FROM tasks WHERE id=?", (id,)).fetchone()
        if row is None:
            raise fail("unknown_task", "Task does not exist")
        return self.public(row)

    def list_tasks(self, db, _now, agent=None, status=None, before=None, limit=20):
        if agent is not None:
            label(agent, "agent")
        if status is not None and status not in STATES:
            raise fail("invalid_status", "Unknown task status")
        limit = number(limit, 1, 50, "limit")
        before = (
            number(before, 1, 2**63 - 1, "before") if before is not None else 2**63 - 1
        )
        rows = db.execute(
            """
            SELECT seq, id, sender, agent, key, status, revision, attempts, created_at, updated_at
              FROM tasks
             WHERE seq < ? AND (? IS NULL OR agent=?) AND (? IS NULL OR status=?)
             ORDER BY seq DESC LIMIT ?
        """,
            (before, agent, agent, status, status, limit + 1),
        ).fetchall()
        return {
            "tasks": [dict(row) for row in rows[:limit]],
            "before": rows[limit - 1]["seq"] if len(rows) > limit else None,
        }

    def claim(self, db, now, agent, lease_seconds):
        agent = label(agent, "agent")
        duration = number(lease_seconds, 1, 600, "lease_seconds")
        row = db.execute(
            """
            SELECT id FROM tasks WHERE agent=? AND status='queued' ORDER BY seq LIMIT 1
        """,
            (agent,),
        ).fetchone()
        if row is None:
            return {"task": None}
        token = secrets.token_urlsafe(32)
        db.execute(
            """
            UPDATE tasks SET status='running', attempts=attempts+1, token=?, lease_until=?,
                             revision=revision+1, updated_at=?, review_note=NULL
            WHERE id=?
        """,
            (token, now + duration, now, row["id"]),
        )
        return {"task": self.get(db, now, row["id"]), "token": token}

    def renew(self, db, now, id, token, lease_seconds):
        duration = number(lease_seconds, 1, 600, "lease_seconds")
        until = now + duration
        changed = db.execute(
            """
            UPDATE tasks SET lease_until=? WHERE id=? AND token=? AND status='running'
        """,
            (until, id, token),
        ).rowcount
        if not changed:
            raise fail("stale_lease", "Task is no longer owned by that lease")
        return {"lease_until": until}

    def finish(self, db, now, id, token, status, result):
        if status not in ("done", "review", "queued"):
            raise fail("invalid_status", "Choose done, review, or queued")
        result = content(result, "result", 12000)
        previous = db.execute(
            "SELECT status, result FROM completions WHERE task_id=? AND token=?",
            (id, token),
        ).fetchone()
        if previous:
            if (previous["status"], previous["result"]) != (status, result):
                raise fail(
                    "finish_conflict", "This lease already recorded a different result"
                )
            return self.get(db, now, id)
        changed = db.execute(
            """
            UPDATE tasks SET status=?, token=NULL, lease_until=NULL, result=?,
                             revision=revision+1, updated_at=?
            WHERE id=? AND token=? AND status='running'
        """,
            (status, result, now, id, token),
        ).rowcount
        if not changed:
            raise fail("stale_lease", "Task is no longer owned by that lease")
        db.execute(
            "INSERT INTO completions VALUES (?, ?, ?, ?)", (id, token, status, result)
        )
        return self.get(db, now, id)

    def review(self, db, now, id, action, note, expected_revision):
        if action not in ("requeue", "cancel"):
            raise fail("invalid_action", "Choose requeue or cancel")
        note = content(note, "review note", 2000)
        expected_revision = number(expected_revision, 1, 2**63 - 1, "expected_revision")
        status = "queued" if action == "requeue" else "cancelled"
        changed = db.execute(
            """
            UPDATE tasks SET status=?, review_note=?, revision=revision+1, updated_at=?
            WHERE id=? AND status='review' AND revision=?
        """,
            (status, note, now, id, expected_revision),
        ).rowcount
        if not changed:
            raise fail(
                "revision_conflict",
                "Task state or revision changed. Read it before resolving.",
            )
        return self.get(db, now, id)


class MailboxService(ToolService):
    def __init__(self, mailbox):
        async def invoke(name, arguments):
            return await asyncio.to_thread(mailbox.request, name, arguments)

        super().__init__(TOOLS, invoke)

    async def handle(self, name, params, emit, stream):
        if name == "invoke" and params.get("name") in DISPATCH:
            return {"value": await self.invoke(params["name"], params["arguments"])}
        return await super().handle(name, params, emit, stream)


def main():
    parser = service_args(__doc__, "tools.mailbox")
    parser.add_argument(
        "--state", required=True, type=Path, help="SQLite mailbox database"
    )
    args = parser.parse_args()
    try:
        worker = MailboxService(Mailbox(args.state))
        asyncio.run(serve(args.socket, args.name, args.replace, METHODS, worker.handle))
    except KeyboardInterrupt:
        return 130
    except (RequestError, ProtocolError, EOFError, OSError, sqlite3.Error) as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
