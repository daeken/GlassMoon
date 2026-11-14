"""Durable task handoffs with exclusive leases and explicit recovery."""

import asyncio
import re
import secrets
import sqlite3
import time
import uuid
from contextlib import closing, contextmanager
from pathlib import Path

from .evidence import identifier as evidence_identifier
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
            "evidence": {
                "type": "array", "maxItems": 6,
                "description": "Published references to include when first delivering this task.",
                "items": {
                    "type": "object", "properties": {"owner": TEXT, "key": TEXT},
                    "required": ["owner", "key"], "additionalProperties": False,
                },
            },
        },
    ),
    "list_tasks": tool(
        "List tasks newest first. Optionally filter by agent, status or cursor.",
        optional={"agent": TEXT, "status": TEXT, "parent": TEXT, "before": INTEGER, "limit": INTEGER},
    ),
    "get": tool("Read a task and any recorded result.", {"id": TEXT}),
    "handoff": tool(
        "Summarize recently active or unresolved tasks for an operator taking over.",
        optional={"limit": INTEGER},
    ),
    "timeline": tool(
        "Read paginated task state changes and tool events, newest first.",
        {"id": TEXT}, {"before": INTEGER},
    ),
    "attach_evidence": tool(
        "Attach a reference to published evidence to a task.",
        {"id": TEXT, "owner": TEXT, "key": TEXT},
        {"attempt": INTEGER | {"description": "The attempt that produced the evidence. Omit when unknown."}},
    ),
    "review": tool(
        "Resolve an interrupted task after checking its effects, or cancel an unclaimed task.",
        {"id": TEXT, "action": TEXT, "note": TEXT, "expected_revision": INTEGER},
    ),
    "reconcile": tool(
        "Recover a saved agent reply for a task awaiting review",
        {"id": TEXT}, {"attempt": INTEGER},
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
COMPLETION_SQL = """
    CASE WHEN tasks.status='done' THEN (
        SELECT CASE kind
            WHEN 'verified' THEN 'operator_verified'
            WHEN 'reply_recorded' THEN 'agent_reply'
            WHEN 'receipt_recovered' THEN 'recovered_reply'
        END FROM task_events
        WHERE task_id=tasks.id AND kind IN ('verified', 'reply_recorded', 'receipt_recovered')
        ORDER BY seq DESC LIMIT 1
    ) END AS completion_source
"""


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
                    recovery_check TEXT, parent TEXT REFERENCES tasks(id), available_at REAL NOT NULL DEFAULT 0,
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
                CREATE TABLE IF NOT EXISTS task_evidence (
                    task_id TEXT NOT NULL REFERENCES tasks(id), owner TEXT NOT NULL,
                    key TEXT NOT NULL, attempt INTEGER, at_submission INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(task_id, owner, key)
                );
            """)
            db.execute("BEGIN IMMEDIATE")
            fields = {field[1] for field in db.execute("PRAGMA table_info(tasks)")}
            if "recovery_check" not in fields:
                db.execute("ALTER TABLE tasks ADD COLUMN recovery_check TEXT")
            if "parent" not in fields:
                db.execute("ALTER TABLE tasks ADD COLUMN parent TEXT REFERENCES tasks(id)")
            if "available_at" not in fields:
                db.execute("ALTER TABLE tasks ADD COLUMN available_at REAL NOT NULL DEFAULT 0")
            evidence_fields = {field[1] for field in db.execute("PRAGMA table_info(task_evidence)")}
            if "attempt" not in evidence_fields:
                db.execute("ALTER TABLE task_evidence ADD COLUMN attempt INTEGER")
            if "at_submission" not in evidence_fields:
                db.execute("ALTER TABLE task_evidence ADD COLUMN at_submission INTEGER NOT NULL DEFAULT 0")
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

    def attach_evidence(self, db, now, id, owner, key, attempt=None):
        return self.link_evidence(db, now, id, owner, key, attempt)

    def link_evidence(self, db, now, id, owner, key, attempt=None, *, at_submission=False):
        if not evidence_identifier(owner) or not evidence_identifier(key):
            raise fail("invalid_evidence_key", "Use a simple owner and key")
        row = db.execute("SELECT attempts FROM tasks WHERE id=?", (id,)).fetchone()
        if row is None:
            raise fail("unknown_task", "Task does not exist")
        if attempt is not None:
            number(attempt, 1, row["attempts"], "attempt")
        fields = (id, owner, key)
        previous = db.execute(
            "SELECT attempt FROM task_evidence WHERE task_id=? AND owner=? AND key=?", fields
        ).fetchone()
        if previous is None:
            count = db.execute("SELECT COUNT(*) FROM task_evidence WHERE task_id=?", (id,)).fetchone()[0]
            if count >= 12:
                raise fail("evidence_limit", "A task can link at most 12 evidence references")
            db.execute(
                "INSERT INTO task_evidence(task_id, owner, key, attempt, at_submission) VALUES (?, ?, ?, ?, ?)",
                (*fields, attempt, at_submission),
            )
            self.event(db, now, id, attempt or 0, "evidence_attached", f"{owner}/{key}")
        elif attempt is None:
            attempt = previous["attempt"]
        elif attempt != previous["attempt"]:
            raise fail("evidence_conflict", "This task already links the evidence with a different attempt")
        result = {"id": id, "owner": owner, "key": key}
        if attempt is not None:
            result["attempt"] = attempt
        return result

    def submit(self, db, now, sender, agent, key, task, recovery_check=None, parent=None, evidence=None):
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
        references = set()
        if evidence is not None:
            if not isinstance(evidence, list) or len(evidence) > 6:
                raise fail("invalid_evidence", "Supply up to six evidence references")
            for entry in evidence:
                if (
                    not isinstance(entry, dict) or entry.keys() != {"owner", "key"}
                    or not evidence_identifier(entry["owner"]) or not evidence_identifier(entry["key"])
                ):
                    raise fail("invalid_evidence", "Each evidence reference needs an owner and key")
                references.add((entry["owner"], entry["key"]))
        row = db.execute(
            "SELECT * FROM tasks WHERE sender=? AND key=?", (sender, key)
        ).fetchone()
        if row:
            original_references = {
                tuple(ref) for ref in db.execute(
                    "SELECT owner, key FROM task_evidence WHERE task_id=? AND at_submission=1", (row["id"],)
                )
            }
            if (row["agent"], row["task"], row["recovery_check"], row["parent"]) != (
                agent, task, recovery_check, parent,
            ) or original_references != references:
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
        for owner, evidence_key in sorted(references):
            self.link_evidence(db, now, identifier, owner, evidence_key, at_submission=True)
        return self.get(db, now, identifier)

    def get(self, db, _now, id):
        row = db.execute(f"SELECT tasks.*, {COMPLETION_SQL} FROM tasks WHERE id=?", (id,)).fetchone()
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
        refs = db.execute(
            "SELECT owner, key, attempt FROM task_evidence WHERE task_id=? ORDER BY owner, key", (id,)
        ).fetchall()
        task["evidence"] = [
            {key: value for key, value in dict(ref).items() if value is not None}
            for ref in refs
        ]
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
            f"""
            SELECT seq, id, sender, agent, key, status, revision, attempts, parent, created_at, updated_at,
                   {COMPLETION_SQL}
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

    def handoff(self, db, now, limit=8):
        limit = number(limit, 1, 10, "limit")
        rows = db.execute(f"""
            SELECT id, sender, agent, key, status, revision, attempts, lease_until, available_at,
                   CASE WHEN status='done' THEN result END AS finished_result,
                   EXISTS(SELECT 1 FROM task_evidence WHERE task_id=tasks.id) AS has_evidence,
                   recovery_check IS NOT NULL AS has_recovery_check,
                   COALESCE((SELECT observed_at FROM task_events WHERE task_id=tasks.id
                             ORDER BY seq DESC LIMIT 1), updated_at) AS last_activity,
                   latest_progress.name AS last_tool, latest_progress.phase AS last_tool_phase,
                   {COMPLETION_SQL}
            FROM tasks LEFT JOIN task_progress AS latest_progress
              ON latest_progress.task_id=tasks.id AND latest_progress.attempt=tasks.attempts
             AND latest_progress.event_index=(
                SELECT MAX(event_index) FROM task_progress WHERE task_id=tasks.id AND attempt=tasks.attempts
             )
            WHERE status IN ('running','review','queued','done')
            ORDER BY CASE status WHEN 'review' THEN 0 WHEN 'running' THEN 1
                                WHEN 'queued' THEN 2 ELSE 3 END,
                     last_activity DESC, seq DESC
            LIMIT ?
        """, (limit + 1,)).fetchall()
        tasks = []
        for row in rows[:limit]:
            task = dict(row)
            result = task.pop("finished_result")
            if result:
                task["result_excerpt"] = result[:240]
                task["result_excerpt_truncated"] = len(result) > 240
            tasks.append(task)
        return {"observed_at": now, "tasks": tasks, "more": len(rows) > limit}

    def claim(self, db, now, agent, lease_seconds):
        agent = label(agent, "agent")
        duration = number(lease_seconds, 1, 600, "lease_seconds")
        row = db.execute(
            """
            SELECT id, attempts FROM tasks
            WHERE agent=? AND status='queued' AND available_at<=? ORDER BY seq LIMIT 1
        """,
            (agent, now),
        ).fetchone()
        if row is None:
            return {"task": None}
        token = secrets.token_urlsafe(32)
        db.execute(
            """
            UPDATE tasks SET status='running', attempts=attempts+1, token=?, lease_until=?,
                             revision=revision+1, updated_at=?
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
                             revision=revision+1, updated_at=?,
                             review_note=CASE WHEN ?='queued' THEN review_note ELSE NULL END,
                             available_at=CASE WHEN ?='queued' THEN ? + (1 << MIN(attempts, 5)) ELSE 0 END
            WHERE id=? AND token=? AND status='running'
            RETURNING attempts
        """,
            (status, result, now, status, status, now, id, token),
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
            UPDATE tasks SET status=?, review_note=?, result=COALESCE(?, result), available_at=0,
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

    def record_receipt(self, db, now, id, expected_revision, attempt, result, needs_review):
        result = content(result, "result", 12000)
        status = "review" if needs_review else "done"
        note = (
            "Recovered a partial agent reply. Verify effects before retrying."
            if needs_review else "Recovered from the saved agent receipt."
        )
        changed = db.execute(
            """
            UPDATE tasks SET status=?, result=?, revision=revision+1, review_note=?, updated_at=?
            WHERE id=? AND status='review' AND revision=? AND attempts=?
        """,
            (status, result, note, now, id, expected_revision, attempt),
        ).rowcount
        if not changed:
            raise fail(
                "revision_conflict", "Task state changed while checking its receipt"
            )
        self.event(db, now, id, attempt, "partial_receipt_recovered" if needs_review else "receipt_recovered")
        return self.get(db, now, id)

    def settle_receipt(self, identifier, revision, attempt, result, needs_review=False):
        with self.transaction() as (db, now):
            return self.record_receipt(db, now, identifier, revision, attempt, result, needs_review)


class MailboxService(ToolService):
    def __init__(self, mailbox, socket):
        async def invoke(name, arguments):
            if name == "reconcile":
                return await self.reconcile(arguments["id"], arguments.get("attempt"))
            return await asyncio.to_thread(mailbox.request, name, arguments)

        super().__init__(TOOLS, invoke)
        self.socket = socket
        self.mailbox = mailbox

    async def reconcile(self, identifier, attempt=None):
        task = await self.invoke("get", {"id": identifier})
        if attempt is not None and task["attempts"] != attempt:
            raise fail("attempt_changed", "A newer task attempt has already started")
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
        needs_review = receipt["reply"].get("outcome") == "review"
        result = (
            result_text(receipt["reply"]["message"]["content"]).strip()
            or "Agent returned an empty reply."
        )
        result = result.encode()[:12000].decode(errors="ignore")
        if needs_review and task["result"] == result:
            return task
        return await asyncio.to_thread(
            self.mailbox.settle_receipt,
            identifier,
            task["revision"],
            task["attempts"],
            result,
            needs_review,
        )

    async def handle(self, name, params, emit, stream):
        if name == "invoke" and params.get("name") in DISPATCH:
            return {"value": await self.invoke(params["name"], params["arguments"])}
        return await super().handle(name, params, emit, stream)


def main():
    parser = service_args(__doc__, "tools.mailbox")
    parser.add_argument("--staged", action="store_true", help="Wait for a separately probed activation")
    parser.add_argument(
        "--state", required=True, type=Path, help="SQLite mailbox database"
    )
    args = parser.parse_args()
    if args.staged and args.replace is not None:
        parser.error("--staged and --replace cannot be used together")
    try:
        worker = MailboxService(Mailbox(args.state), args.socket)
        asyncio.run(
            serve(
                args.socket,
                args.name,
                args.replace,
                METHODS,
                worker.handle,
                staged=args.staged,
                reconnect=True,
                maintainer=args.maintainer,
            )
        )
    except KeyboardInterrupt:
        return 130
    except (RequestError, ProtocolError, EOFError, OSError, sqlite3.Error) as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
