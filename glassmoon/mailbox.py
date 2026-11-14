"""Durable task handoffs with exclusive leases and explicit recovery."""

import asyncio
import re
import secrets
import sqlite3
import time
import uuid
from contextlib import closing, contextmanager
from pathlib import Path

from .ipc import ProtocolError, RequestError, call
from .models import INTEGER, TEXT, encoded_size, result_text
from .service import fail, serve, service_args
from .toolbox import METHODS, ToolService, tool

TOOLS = {
    "submit": tool(
        "Queue a task for an agent. Retries using the same sender and key return the original task.",
        {"sender": TEXT, "agent": TEXT, "key": TEXT, "task": TEXT},
        {
            "recovery_check": TEXT | {
                "description": "Optional check to perform after interruption. Leave empty if there is no check.",
            },
            "parent": TEXT | {"description": "ID of the task that delegated this work, if any."},
        },
    ),
    "list_tasks": tool(
        "List tasks newest first. Optionally filter by agent, status or cursor.",
        optional={"agent": TEXT, "status": TEXT, "parent": TEXT, "before": INTEGER, "limit": INTEGER},
    ),
    "get": tool("Read a task and any recorded result.", {"id": TEXT}),
    "timeline": tool(
        "Read paginated task state changes and tool events, newest first.",
        {"id": TEXT}, {"before": INTEGER},
    ),
    "review": tool(
        "Resolve an interrupted task after checking its effects, or cancel an unclaimed task.",
        {"id": TEXT, "action": TEXT, "note": TEXT, "expected_revision": INTEGER},
    ),
    "reconcile": tool(
        "Recover a completed agent receipt for a task awaiting review", {"id": TEXT}
    ),
}
# Dispatcher operations stay available over IPC without being advertised to models.
DISPATCH = {
    "progress": tool(
        "Record one tool lifecycle event under the active lease",
        {"id": TEXT, "token": TEXT, "index": INTEGER, "name": TEXT, "phase": TEXT},
    ),
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
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("PRAGMA journal_mode = WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS tasks (
                    seq INTEGER PRIMARY KEY,
                    id TEXT NOT NULL UNIQUE, sender TEXT NOT NULL, agent TEXT NOT NULL,
                    key TEXT NOT NULL, task TEXT NOT NULL, status TEXT NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 1, attempts INTEGER NOT NULL DEFAULT 0,
                    token TEXT, lease_until REAL, result TEXT, review_note TEXT,
                    recovery_check TEXT, parent TEXT REFERENCES tasks(id),
                    created_at REAL NOT NULL, updated_at REAL NOT NULL,
                    UNIQUE(sender, key)
                );
                CREATE INDEX IF NOT EXISTS task_pick ON tasks(agent, status, seq);
                CREATE TABLE IF NOT EXISTS completions (
                    task_id TEXT NOT NULL REFERENCES tasks(id),
                    token TEXT NOT NULL, status TEXT NOT NULL, result TEXT NOT NULL,
                    PRIMARY KEY(task_id, token)
                );
                CREATE TABLE IF NOT EXISTS task_progress (
                    task_id TEXT NOT NULL REFERENCES tasks(id),
                    attempt INTEGER NOT NULL, event_index INTEGER NOT NULL,
                    name TEXT NOT NULL, phase TEXT NOT NULL, observed_at REAL NOT NULL,
                    PRIMARY KEY(task_id, attempt, event_index)
                );
                CREATE TABLE IF NOT EXISTS task_events (
                    seq INTEGER PRIMARY KEY,
                    task_id TEXT NOT NULL REFERENCES tasks(id),
                    attempt INTEGER NOT NULL, kind TEXT NOT NULL,
                    name TEXT, phase TEXT, observed_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS task_history ON task_events(task_id, seq);
            """)
            db.execute("BEGIN IMMEDIATE")
            fields = {field[1] for field in db.execute("PRAGMA table_info(tasks)")}
            if "recovery_check" not in fields:
                db.execute("ALTER TABLE tasks ADD COLUMN recovery_check TEXT")
            if "parent" not in fields:
                db.execute("ALTER TABLE tasks ADD COLUMN parent TEXT REFERENCES tasks(id)")
            db.execute("CREATE INDEX IF NOT EXISTS task_children ON tasks(parent, seq)")

    @contextmanager
    def transaction(self):
        with closing(sqlite3.connect(self.path, timeout=3)) as db, db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN IMMEDIATE")
            now = self.clock()
            expired = db.execute(
                """
                UPDATE tasks
                   SET status='review', token=NULL, lease_until=NULL, revision=revision+1,
                       review_note='Lease expired. Verify effects before retrying.', updated_at=?
                 WHERE status='running' AND lease_until<=?
                RETURNING id, attempts
            """,
                (now, now),
            ).fetchall()
            for task in expired:
                self.event(db, now, task["id"], task["attempts"], "lease_expired")
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

    @staticmethod
    def event(db, now, identifier, attempt, kind, name=None, phase=None):
        db.execute(
            "INSERT INTO task_events(task_id, attempt, kind, name, phase, observed_at) VALUES (?, ?, ?, ?, ?, ?)",
            (identifier, attempt, kind, name, phase, now),
        )

    def timeline(self, db, _now, id, before=None):
        if not db.execute("SELECT 1 FROM tasks WHERE id=?", (id,)).fetchone():
            raise fail("unknown_task", "Task does not exist")
        before = 2**63 - 1 if before is None else number(before, 1, 2**63 - 1, "before")
        rows = db.execute(
            """SELECT seq, attempt, kind, name, phase, observed_at FROM task_events
               WHERE task_id=? AND seq<? ORDER BY seq DESC LIMIT 21""",
            (id, before),
        ).fetchall()
        return {
            "events": [dict(row) for row in rows[:20]],
            "before": rows[19]["seq"] if len(rows) > 20 else None,
        }

    def submit(self, db, now, sender, agent, key, task, recovery_check=None, parent=None):
        sender, agent, key = (
            label(value, name)
            for name, value in (("sender", sender), ("agent", agent), ("key", key))
        )
        task = content(task, "task", 4000)
        if isinstance(recovery_check, str) and not recovery_check.strip():
            recovery_check = None
        if recovery_check is not None:
            recovery_check = content(recovery_check, "recovery check", 1200)
        if isinstance(parent, str) and not parent.strip():
            parent = None
        if parent is not None:
            parent = label(parent, "parent task")
            if not db.execute("SELECT 1 FROM tasks WHERE id=?", (parent,)).fetchone():
                raise fail("unknown_parent", "The parent task does not exist")
        row = db.execute(
            "SELECT * FROM tasks WHERE sender=? AND key=?", (sender, key)
        ).fetchone()
        if row:
            if (row["agent"], row["task"], row["recovery_check"], row["parent"]) != (
                agent, task, recovery_check, parent,
            ):
                raise fail(
                    "key_conflict",
                    "This sender and key already identify a different task",
                )
            return self.get(db, now, row["id"])
        identifier = uuid.uuid4().hex
        db.execute(
            """
            INSERT INTO tasks(id, sender, agent, key, task, recovery_check, parent, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)
        """,
            (identifier, sender, agent, key, task, recovery_check, parent, now, now),
        )
        self.event(db, now, identifier, 0, "submitted")
        return self.get(db, now, identifier)

    def get(self, db, _now, id):
        row = db.execute("SELECT * FROM tasks WHERE id=?", (id,)).fetchone()
        if row is None:
            raise fail("unknown_task", "Task does not exist")
        task = self.public(row)
        events = db.execute(
            """
            SELECT event_index, name, phase, observed_at FROM task_progress
             WHERE task_id=? AND attempt=? ORDER BY event_index
        """,
            (id, row["attempts"]),
        ).fetchall()
        task["progress"] = [dict(event) for event in events]
        if encoded_size(task) > 60 * 1024 and task["result"]:
            original = task["result"]
            task["result_truncated"] = True
            lower, upper = 0, len(original)
            while lower < upper:
                middle = (lower + upper + 1) // 2
                task["result"] = original[:middle]
                if encoded_size(task) <= 60 * 1024:
                    lower = middle
                else:
                    upper = middle - 1
            task["result"] = original[:lower]
        return task

    def progress(self, db, now, id, token, index, name, phase):
        index = number(index, 1, 64, "event index")
        name = label(name, "tool name")
        if phase not in ("started", "finished", "failed"):
            raise fail("invalid_phase", "Expected a tool lifecycle phase")
        task = db.execute(
            "SELECT attempts FROM tasks WHERE id=? AND token=? AND status='running'",
            (id, token),
        ).fetchone()
        if not task:
            raise fail("stale_lease", "Task is no longer owned by that lease")
        previous = db.execute(
            """
            SELECT name, phase FROM task_progress
            WHERE task_id=? AND attempt=? AND event_index=?
        """,
            (id, task["attempts"], index),
        ).fetchone()
        if previous is not None:
            if (previous["name"], previous["phase"]) != (name, phase):
                raise fail(
                    "progress_conflict",
                    "This event index already identifies a different tool event",
                )
            return {"recorded": True}
        db.execute(
            "INSERT INTO task_progress VALUES (?, ?, ?, ?, ?, ?)",
            (id, task["attempts"], index, name, phase, now),
        )
        self.event(db, now, id, task["attempts"], "tool", name, phase)
        return {"recorded": True}

    def list_tasks(self, db, _now, agent=None, status=None, parent=None, before=None, limit=20):
        if agent is not None:
            label(agent, "agent")
        if status is not None and status not in STATES:
            raise fail("invalid_status", "Unknown task status")
        if parent is not None:
            label(parent, "parent task")
        limit = number(limit, 1, 50, "limit")
        before = (
            number(before, 1, 2**63 - 1, "before") if before is not None else 2**63 - 1
        )
        rows = db.execute(
            """
            SELECT seq, id, sender, agent, key, status, revision, attempts, parent, created_at, updated_at
              FROM tasks
             WHERE seq < ? AND (? IS NULL OR agent=?) AND (? IS NULL OR status=?)
                   AND (? IS NULL OR parent=?)
             ORDER BY seq DESC LIMIT ?
        """,
            (before, agent, agent, status, status, parent, parent, limit + 1),
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
            SELECT id, attempts FROM tasks WHERE agent=? AND status='queued' ORDER BY seq LIMIT 1
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
        self.event(db, now, row["id"], row["attempts"] + 1, "claimed")
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
        task = db.execute(
            """
            UPDATE tasks SET status=?, token=NULL, lease_until=NULL, result=?,
                             revision=revision+1, updated_at=?
            WHERE id=? AND token=? AND status='running'
            RETURNING attempts
        """,
            (status, result, now, id, token),
        ).fetchone()
        if task is None:
            raise fail("stale_lease", "Task is no longer owned by that lease")
        db.execute(
            "INSERT INTO completions VALUES (?, ?, ?, ?)", (id, token, status, result)
        )
        self.event(db, now, id, task["attempts"], {
            "done": "reply_recorded", "review": "review_required", "queued": "delivery_deferred",
        }[status])
        return self.get(db, now, id)

    def review(self, db, now, id, action, note, expected_revision):
        if action not in ("requeue", "cancel", "verified"):
            raise fail("invalid_action", "Choose requeue, cancel, or verified")
        note = content(note, "review note", 2000)
        expected_revision = number(expected_revision, 1, 2**63 - 1, "expected_revision")
        status = {"requeue": "queued", "cancel": "cancelled", "verified": "done"}[
            action
        ]
        task = db.execute(
            """
            UPDATE tasks SET status=?, review_note=?, result=COALESCE(?, result),
                             revision=revision+1, updated_at=?
            WHERE id=? AND revision=?
              AND (status='review' OR (status='queued' AND attempts=0 AND ?='cancel'))
            RETURNING attempts
        """,
            (
                status,
                note,
                note if action == "verified" else None,
                now,
                id,
                expected_revision,
                action,
            ),
        ).fetchone()
        if task is None:
            raise fail(
                "revision_conflict",
                "Task state or revision changed. Read it before resolving.",
            )
        self.event(db, now, id, task["attempts"], {
            "requeue": "requeued", "cancel": "cancelled", "verified": "verified",
        }[action])
        return self.get(db, now, id)

    def record_receipt(self, db, now, id, expected_revision, attempt, result):
        result = content(result, "result", 12000)
        changed = db.execute(
            """
            UPDATE tasks SET status='done', result=?, revision=revision+1,
                             review_note='Recovered from the saved agent receipt.', updated_at=?
            WHERE id=? AND status='review' AND revision=? AND attempts=?
        """,
            (result, now, id, expected_revision, attempt),
        ).rowcount
        if not changed:
            raise fail(
                "revision_conflict", "Task state changed while checking its receipt"
            )
        self.event(db, now, id, attempt, "receipt_recovered")
        return self.get(db, now, id)

    def settle_receipt(self, identifier, revision, attempt, result):
        with self.transaction() as (db, now):
            return self.record_receipt(db, now, identifier, revision, attempt, result)


class MailboxService(ToolService):
    def __init__(self, mailbox, socket):
        async def invoke(name, arguments):
            if name == "reconcile":
                return await self.reconcile(arguments["id"])
            return await asyncio.to_thread(mailbox.request, name, arguments)

        super().__init__(TOOLS, invoke)
        self.socket = socket
        self.mailbox = mailbox

    async def reconcile(self, identifier):
        task = await self.invoke("get", {"id": identifier})
        if task["status"] == "done":
            return task
        if task["status"] != "review":
            raise fail("not_in_review", "Only a task awaiting review can be reconciled")
        try:
            async with asyncio.timeout(5):
                receipt = await call(
                    self.socket,
                    f"agent.{task['agent']}",
                    "task_receipt",
                    {"id": identifier, "attempt": task["attempts"]},
                )
        except (RequestError, ProtocolError, EOFError, OSError, TimeoutError):
            raise fail(
                "receipt_unavailable", "Could not check the recipient's receipt"
            ) from None
        if receipt["status"] != "done":
            raise fail(
                "receipt_incomplete",
                "No completed receipt for this attempt. Verify its effects.",
            )
        if receipt["reply"].get("outcome") == "review":
            raise fail("receipt_needs_review", "The saved reply asks you to verify task effects")
        result = (
            result_text(receipt["reply"]["message"]["content"]).strip()
            or "Agent returned an empty reply."
        )
        return await asyncio.to_thread(
            self.mailbox.settle_receipt,
            identifier,
            task["revision"],
            task["attempts"],
            result.encode()[:12000].decode(errors="ignore"),
        )

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
        worker = MailboxService(Mailbox(args.state), args.socket)
        asyncio.run(
            serve(
                args.socket,
                args.name,
                args.replace,
                METHODS,
                worker.handle,
                reconnect=True,
            )
        )
    except KeyboardInterrupt:
        return 130
    except (RequestError, ProtocolError, EOFError, OSError, sqlite3.Error) as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
