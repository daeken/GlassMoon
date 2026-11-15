"""Provider-neutral model descriptors, conversation messages, and transcripts."""

import fcntl
import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path

from .functions import TOOL_FIELDS
from .service import fail, method

TEXT = {"type": "string"}
INTEGER = {"type": "integer"}
BOOLEAN = {"type": "boolean"}
ARRAY = {"type": "array"}
OBJECT = {"type": "object"}
MESSAGE = {
    "type": "object",
    "properties": {
        "role": {"enum": ["developer", "user", "assistant"]},
        "content": {"oneOf": [TEXT, {"type": "array", "items": OBJECT}]},
    },
    "required": ["role", "content"],
}
CAPABILITY = {
    "type": "object",
    "properties": {
        "id": TEXT,
        "input_modalities": {"type": "array", "items": TEXT},
        "output_modalities": {"type": "array", "items": TEXT},
        "context_tokens": {"type": "integer", "minimum": 1},
        "max_output_tokens": {"type": "integer", "minimum": 1},
        "streaming": BOOLEAN,
        "tool_calls": BOOLEAN,
        "tool_images": BOOLEAN,
    },
    "required": [
        "id",
        "input_modalities",
        "output_modalities",
        "context_tokens",
        "streaming",
    ],
}
GENERATE = method(
    "Generate a reply from a transcript",
    {"model": TEXT, "messages": {"type": "array", "items": MESSAGE}},
    {"output_modalities": ARRAY, **TOOL_FIELDS},
)
CATALOG = method(
    "List model capabilities",
    result={
        "type": "object",
        "properties": {
            "models": {"type": "array", "items": CAPABILITY},
        },
    },
)


def modalities(values):
    if (
        not isinstance(values, list)
        or not values
        or any(
            not isinstance(value, str) or not value or len(value) > 40
            for value in values
        )
    ):
        raise fail("invalid_modalities", "Supply a nonempty array of modality names")
    return set(values)


def capabilities(model):
    if not isinstance(model.get("id"), str) or not model["id"]:
        raise fail("invalid_model", "Model ID is required")
    modalities(model["input_modalities"])
    modalities(model["output_modalities"])
    for key in ("context_tokens", "max_output_tokens"):
        required = key == "context_tokens" or key in model
        if required and (type(model.get(key)) is not int or model[key] < 1):
            raise fail("invalid_model", f"{key} must be a positive integer")
    if type(model.get("streaming")) is not bool:
        raise fail("invalid_model", "streaming must be a boolean")
    for key in ("tool_calls", "tool_images"):
        if key in model and type(model[key]) is not bool:
            raise fail("invalid_model", f"{key} must be a boolean")
    return model


def parts(content):
    if isinstance(content, str):
        content = [{"type": "text", "text": content}]
    if not isinstance(content, list) or not content:
        raise fail("invalid_message", "Content needs text or an array of content parts")
    for part in content:
        if (
            not isinstance(part, dict)
            or not isinstance(part.get("type"), str)
            or not part["type"]
        ):
            raise fail("invalid_message", "Content parts require a modality type")
        if part["type"] == "text" and not isinstance(part.get("text"), str):
            raise fail("invalid_message", "Text parts require a text string")
    return content


def message(value):
    role = value["role"]
    if role not in ("developer", "user", "assistant"):
        raise fail("invalid_role", "Expected developer, user, or assistant")
    return {"role": role, "content": parts(value["content"])}


def input_modalities(messages):
    return {part["type"] for item in messages for part in parts(item["content"])}


def result_text(content):
    return "".join(
        part["text"] if part["type"] == "text" else f"[{part['type']}]"
        for part in parts(content)
    )


def encoded_size(value):
    return len(
        json.dumps(
            value, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode()
    )


def within_reply_limit(value):
    if encoded_size(value) > 60 * 1024:
        raise fail("reply_too_large", "Reply exceeds the GlassMoon frame limit")
    return value


class Transcripts:
    def __init__(self, path):
        path = Path(path).expanduser()
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd)
        self.path = path.resolve()
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS conversations (
                id TEXT PRIMARY KEY, model TEXT, revision INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS compactions (
                conversation TEXT PRIMARY KEY REFERENCES conversations(id),
                through_revision INTEGER NOT NULL, summary TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS compaction_checkpoints (
                conversation TEXT NOT NULL REFERENCES conversations(id),
                through_revision INTEGER NOT NULL, source_from INTEGER NOT NULL,
                snapshot_revision INTEGER, summary TEXT NOT NULL,
                PRIMARY KEY (conversation, through_revision)
            );
            INSERT OR IGNORE INTO compaction_checkpoints
                (conversation, through_revision, source_from, snapshot_revision, summary)
                SELECT conversation, through_revision, 1, NULL, summary FROM compactions
                WHERE through_revision > 0 AND NOT EXISTS (
                    SELECT 1 FROM compaction_checkpoints
                    WHERE compaction_checkpoints.conversation = compactions.conversation
                );
            CREATE TABLE IF NOT EXISTS messages (
                conversation TEXT NOT NULL REFERENCES conversations(id),
                position INTEGER NOT NULL, value TEXT NOT NULL,
                PRIMARY KEY (conversation, position)
            );
            CREATE TABLE IF NOT EXISTS task_receipts (
                conversation TEXT NOT NULL REFERENCES conversations(id),
                task_id TEXT NOT NULL, attempt INTEGER NOT NULL,
                digest TEXT NOT NULL, reply TEXT,
                PRIMARY KEY (conversation, task_id, attempt)
            );
            CREATE TABLE IF NOT EXISTS tool_policies (
                conversation TEXT PRIMARY KEY REFERENCES conversations(id),
                revision INTEGER NOT NULL, overrides TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS agent_tool_overrides (
                conversation TEXT PRIMARY KEY REFERENCES conversations(id), overrides TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS model_preferences (
                conversation TEXT PRIMARY KEY REFERENCES conversations(id),
                revision INTEGER NOT NULL, mode TEXT NOT NULL, provider TEXT, model TEXT
            );
            CREATE TABLE IF NOT EXISTS agent_last_model (
                conversation TEXT PRIMARY KEY REFERENCES conversations(id),
                revision INTEGER NOT NULL, provider TEXT NOT NULL, model TEXT NOT NULL,
                generation INTEGER
            );
            CREATE TABLE IF NOT EXISTS agent_tool_results (
                id INTEGER PRIMARY KEY, conversation TEXT NOT NULL REFERENCES conversations(id),
                run TEXT NOT NULL, call_id TEXT NOT NULL, name TEXT NOT NULL,
                arguments TEXT NOT NULL, output TEXT,
                UNIQUE (conversation, run, call_id)
            );
            CREATE TABLE IF NOT EXISTS agent_tool_elisions (
                result INTEGER NOT NULL REFERENCES agent_tool_results(id),
                reason TEXT NOT NULL CHECK (reason IN ('explicit', 'age', 'budget')),
                PRIMARY KEY (result, reason)
            );
            CREATE TABLE IF NOT EXISTS agent_tool_checkpoints (
                conversation TEXT NOT NULL REFERENCES conversations(id), run TEXT NOT NULL,
                source_from INTEGER NOT NULL REFERENCES agent_tool_results(id),
                through_result INTEGER NOT NULL REFERENCES agent_tool_results(id),
                summary TEXT NOT NULL,
                PRIMARY KEY (conversation, run, through_result)
            );
        """)

    def close(self):
        self.db.close()

    @contextmanager
    def agent_turn(self):
        fd = os.open(f"{self.path}.turn", os.O_CREAT | os.O_RDWR, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise fail("agent_busy", "This agent is already replying") from None
            yield
        finally:
            os.close(fd)

    def create(self, model=None, identifier=None):
        identifier = identifier or uuid.uuid4().hex
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO conversations (id, model) VALUES (?, ?)",
                (identifier, model),
            )
        return identifier

    def read(self, identifier):
        rows = self.db.execute(
            """SELECT c.model, c.revision, m.value
               FROM conversations c LEFT JOIN messages m ON c.id = m.conversation
               WHERE c.id = ? ORDER BY m.position""",
            (identifier,),
        ).fetchall()
        if not rows:
            raise fail("unknown_conversation", "Conversation does not exist")
        model, revision, _ = rows[0]
        return (
            model,
            revision,
            [json.loads(row[2]) for row in rows if row[2] is not None],
        )

    def revision(self, identifier):
        row = self.db.execute("SELECT revision FROM conversations WHERE id=?", (identifier,)).fetchone()
        if row is None:
            raise fail("unknown_conversation", "Conversation does not exist")
        return row[0]

    def memory(self, identifier):
        row = self.db.execute(
            """SELECT c.revision, COALESCE(k.through_revision, 0), COALESCE(k.summary, '')
               FROM conversations c LEFT JOIN compactions k ON k.conversation=c.id
               WHERE c.id=?""",
            (identifier,),
        ).fetchone()
        if row is None:
            raise fail("unknown_conversation", "Conversation does not exist")
        revision, through, summary = row
        rows = self.db.execute(
            """SELECT source_from, through_revision, snapshot_revision FROM compaction_checkpoints
               WHERE conversation=? AND through_revision<=? ORDER BY through_revision DESC LIMIT 8""",
            (identifier, through),
        )
        return {
            "revision": revision, "through_revision": through, "summary": summary,
            "checkpoints": [
                {"source_from": start, "through_revision": end, "snapshot_revision": snapshot}
                for start, end, snapshot in rows
            ],
        }

    def context(self, identifier):
        snapshot = self.db.execute(
            """SELECT c.revision, COALESCE(k.through_revision, 0), COALESCE(k.summary, ''), m.value
               FROM conversations c
               LEFT JOIN compactions k ON k.conversation = c.id
               LEFT JOIN messages m ON m.conversation = c.id AND m.position >= COALESCE(k.through_revision, 0)
               WHERE c.id = ? ORDER BY m.position""",
            (identifier,),
        ).fetchall()
        if not snapshot:
            raise fail("unknown_conversation", "Conversation does not exist")
        revision, through, summary, _ = snapshot[0]
        return (
            revision,
            through,
            summary,
            [json.loads(row[3]) for row in snapshot if row[3] is not None],
        )

    def compact(self, identifier, revision, previous, through, summary):
        if not 0 <= previous < through <= revision or (through - previous) % 2:
            raise fail("invalid_compaction_range", "Compact a contiguous range of complete turns")
        with self.db:
            changed = self.db.execute(
                """INSERT INTO compactions (conversation, through_revision, summary)
                   SELECT id, ?, ? FROM conversations WHERE id = ? AND revision = ?
                   ON CONFLICT(conversation) DO UPDATE SET
                       through_revision = excluded.through_revision, summary = excluded.summary
                   WHERE compactions.through_revision = ?""",
                (through, summary, identifier, revision, previous),
            ).rowcount
            if not changed:
                raise fail(
                    "conversation_changed", "Conversation changed during compaction"
                )
            self.db.execute(
                """INSERT INTO compaction_checkpoints
                   (conversation, through_revision, source_from, snapshot_revision, summary)
                   VALUES (?, ?, ?, ?, ?)""",
                (identifier, through, previous + 1, revision, summary),
            )

    def history(self, identifier, before=None):
        conversation = self.db.execute(
            "SELECT model, revision FROM conversations WHERE id=?", (identifier,)
        ).fetchone()
        if conversation is None:
            raise fail("unknown_conversation", "Conversation does not exist")
        model, revision = conversation
        end = revision if before is None else min(max(0, before), revision)
        page = []
        size = 0
        rows = self.db.execute(
            "SELECT value FROM messages WHERE conversation=? AND position<? ORDER BY position DESC LIMIT 200",
            (identifier, end),
        )
        for (value,) in rows:
            item = json.loads(value)
            encoded = encoded_size(item) + 1
            if page and size + encoded > 60 * 1024:
                break
            page.append(item)
            size += encoded
        return {
            "conversation": identifier,
            "model": model,
            "revision": revision,
            "messages": page[::-1],
            "before": end - len(page) or None,
        }

    def find_history(self, identifier, query, before=None):
        if not isinstance(query, str) or not query.strip() or len(query) > 200:
            raise fail("invalid_query", "Use a search phrase of up to 200 characters")
        if before is not None and (type(before) is not int or before < 1):
            raise fail("invalid_cursor", "Use the previous search page's cursor")
        revision = self.revision(identifier)
        end = revision if before is None else min(revision, before)
        rows = self.db.execute(
            """SELECT position, value FROM messages
               WHERE conversation=? AND position<? ORDER BY position DESC LIMIT 1001""",
            (identifier, end),
        )
        result = []
        cursor = None
        needle = query.casefold()
        for scanned, (position, value) in enumerate(rows):
            if scanned == 1000:
                cursor = position + 1
                break
            item = json.loads(value)
            text = result_text(item["content"])
            index = text.casefold().find(needle)
            if index >= 0:
                if not text.isascii():
                    # Unicode folding can expand characters, so map the match back to the display text.
                    offset = 0
                    for original, char in enumerate(text):
                        offset += len(char.casefold())
                        if offset > index:
                            index = original
                            break
                start = max(0, index - 80)
                snippet = text[start:index + len(query) + 80]
                if encoded_size(snippet) > 2500:
                    snippet = snippet[:340]
                result.append({"position": position, "role": item["role"], "snippet": snippet})
            if len(result) == 20:
                cursor = position or None
                break
        return {"matches": result, "before": cursor}

    def task_receipt(self, identifier, task_id, attempt):
        row = self.db.execute(
            "SELECT digest, reply FROM task_receipts WHERE conversation=? AND task_id=? AND attempt=?",
            (identifier, task_id, attempt),
        ).fetchone()
        if row is None:
            return None
        digest, reply = row
        return {
            "digest": digest,
            "reply": json.loads(reply) if reply is not None else None,
        }

    def begin_task(self, identifier, task_id, attempt, digest):
        with self.db:
            self.db.execute(
                "INSERT INTO task_receipts (conversation, task_id, attempt, digest) VALUES (?, ?, ?, ?)",
                (identifier, task_id, attempt, digest),
            )

    def record_tool_calls(self, identifier, run, calls):
        references = {}
        with self.db:
            for item in calls:
                cursor = self.db.execute(
                    """INSERT INTO agent_tool_results (conversation, run, call_id, name, arguments)
                       VALUES (?, ?, ?, ?, ?) ON CONFLICT (conversation, run, call_id) DO NOTHING""",
                    (identifier, run, item["id"], item["name"], item["arguments"]),
                )
                if cursor.rowcount != 1:
                    raise fail("duplicate_tool_call", "The provider reused a tool call ID")
                references[item["id"]] = cursor.lastrowid
        return references

    def record_tool_result(self, reference, output):
        with self.db:
            self.db.execute(
                "UPDATE agent_tool_results SET output=? WHERE id=?",
                (output if isinstance(output, str) else json.dumps(output, ensure_ascii=False), reference),
            )

    def latest_tool_result(self, identifier, *, excluding):
        row = self.db.execute(
            """SELECT id FROM agent_tool_results
               WHERE conversation=? AND output IS NOT NULL AND name<>? ORDER BY id DESC LIMIT 1""",
            (identifier, excluding),
        ).fetchone()
        if row is None:
            raise fail("unknown_result", "This agent has no completed tool result")
        return row[0]

    def record_tool_elisions(self, identifier, references, reason):
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            for reference in references:
                if not self.db.execute(
                    "SELECT 1 FROM agent_tool_results WHERE conversation=? AND id=? AND output IS NOT NULL",
                    (identifier, reference),
                ).fetchone():
                    raise fail("unknown_result", f"Result #{reference} is absent or incomplete")
                self.db.execute(
                    "INSERT OR IGNORE INTO agent_tool_elisions (result, reason) VALUES (?, ?)",
                    (reference, reason),
                )

    def record_tool_checkpoint(self, identifier, run, source_from, through, summary):
        with self.db:
            self.db.execute(
                """INSERT INTO agent_tool_checkpoints VALUES (?, ?, ?, ?, ?)""",
                (identifier, run, source_from, through, summary),
            )

    def tool_checkpoint(self, identifier, reference):
        if type(reference) is not int or not 1 <= reference < 2**63:
            raise fail("invalid_result_id", "Expected a positive tool result ID")
        row = self.db.execute(
            """SELECT checkpoint.source_from, checkpoint.through_result, checkpoint.summary
               FROM agent_tool_results AS result LEFT JOIN agent_tool_checkpoints AS checkpoint
                 ON checkpoint.conversation=result.conversation AND checkpoint.run=result.run
                 AND result.id BETWEEN checkpoint.source_from AND checkpoint.through_result
               WHERE result.conversation=? AND result.id=?""",
            (identifier, reference),
        ).fetchone()
        if row is None:
            raise fail("unknown_result", "This agent has no result with that ID")
        start, through, summary = row
        if through is None:
            return {"id": reference, "status": "uncompacted"}
        return {
            "id": reference, "status": "compacted", "source_from": start,
            "through_result": through, "summary": summary,
        }

    def tool_result(self, identifier, reference, offset):
        # Limit each page before it is placed in an IPC response or a model request.
        page_size = 2048
        if type(reference) is not int or not 1 <= reference < 2**63:
            raise fail("invalid_result_id", "Expected a positive tool result ID")
        if type(offset) is not int or not 0 <= offset < 2**63 - page_size:
            raise fail("invalid_offset", "Expected a nonnegative character offset")
        row = self.db.execute(
            """SELECT name, substr(output, ?, ?) FROM agent_tool_results
               WHERE conversation=? AND id=?""",
            (offset + 1, page_size + 1, identifier, reference),
        ).fetchone()
        if row is None:
            raise fail("unknown_result", "This agent has no result with that ID")
        name, output = row
        if output is None:
            return {"id": reference, "tool": name, "status": "unconfirmed", "next_offset": None}
        page = {
            "id": reference, "tool": name, "status": "recorded", "offset": offset,
            "text": output[:page_size],
            "next_offset": offset + page_size if len(output) > page_size else None,
        }
        elisions = self.db.execute(
            "SELECT reason FROM agent_tool_elisions WHERE result=? ORDER BY reason", (reference,),
        )
        reasons = [reason for (reason,) in elisions]
        if reasons:
            page["elisions"] = reasons
        return page

    def tool_policy(self, identifier):
        row = self.db.execute(
            """SELECT t.revision, t.overrides, COALESCE(a.overrides, '{}')
               FROM tool_policies t LEFT JOIN agent_tool_overrides a ON a.conversation=t.conversation
               WHERE t.conversation=?""",
            (identifier,),
        ).fetchone()
        return (row[0], json.loads(row[1]), json.loads(row[2])) if row else (0, {}, {})

    def model_preference(self, identifier):
        row = self.db.execute(
            "SELECT revision, mode, provider, model FROM model_preferences WHERE conversation=?",
            (identifier,),
        ).fetchone()
        return row if row else (0, "default", None, None)

    def last_model(self, identifier):
        row = self.db.execute(
            "SELECT revision, provider, model, generation FROM agent_last_model WHERE conversation=?",
            (identifier,),
        ).fetchone()
        return dict(zip(("revision", "provider", "model", "generation"), row, strict=True)) if row else None

    def set_model_preference(self, identifier, expected, mode, provider, model):
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            revision, *values = self.model_preference(identifier)
            if revision != expected:
                raise fail("model_preference_changed", "Read the current preference before updating it")
            if values == [mode, provider, model]:
                return revision, mode, provider, model
            saved = (revision + 1, mode, provider, model)
            self.db.execute(
                """INSERT INTO model_preferences VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(conversation) DO UPDATE SET revision=excluded.revision,
                       mode=excluded.mode, provider=excluded.provider, model=excluded.model""",
                (identifier, *saved),
            )
            return saved

    def set_tool_policy(self, identifier, expected, service, mode, *, agent_requested=False):
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            revision, operator, agent = self.tool_policy(identifier)
            if revision != expected:
                raise fail("policy_changed", "Read the current tool policy before updating it")
            if agent_requested and service in operator:
                raise fail("tool_policy_denied", "An explicit operator setting controls this service")
            overrides = agent if agent_requested else operator
            if overrides.get(service, "default") == mode and (agent_requested or service not in agent):
                return revision, operator, agent
            if not agent_requested:
                agent.pop(service, None)
            if mode == "default":
                overrides.pop(service, None)
            else:
                if len(overrides) >= 32 and service not in overrides:
                    raise fail("policy_limit", "Clear a tool override before setting another")
                overrides[service] = mode
            revision += 1
            self.db.execute(
                """INSERT INTO tool_policies VALUES (?, ?, ?)
                   ON CONFLICT(conversation) DO UPDATE SET
                       revision=excluded.revision, overrides=excluded.overrides""",
                (identifier, revision, json.dumps(operator)),
            )
            self.db.execute(
                """INSERT INTO agent_tool_overrides VALUES (?, ?)
                   ON CONFLICT(conversation) DO UPDATE SET overrides=excluded.overrides""",
                (identifier, json.dumps(agent)),
            )
            return revision, operator, agent

    def append(self, identifier, revision, *messages, receipt=None, selected_model=None):
        with self.db:
            if not self.db.execute(
                "UPDATE conversations SET revision = revision + ? WHERE id = ? AND revision = ?",
                (len(messages), identifier, revision),
            ).rowcount:
                raise fail(
                    "conversation_changed",
                    "The conversation changed during this request",
                )
            self.db.executemany(
                "INSERT INTO messages VALUES (?, ?, ?)",
                [
                    (
                        identifier,
                        index,
                        json.dumps(item, ensure_ascii=False, allow_nan=False),
                    )
                    for index, item in enumerate(messages, revision)
                ],
            )
            if selected_model is not None:
                self.db.execute(
                    """INSERT INTO agent_last_model VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT(conversation) DO UPDATE SET revision=excluded.revision,
                           provider=excluded.provider, model=excluded.model, generation=excluded.generation""",
                    (identifier, revision + len(messages), *selected_model),
                )
            if receipt is not None:
                task_id, attempt, reply = receipt
                changed = self.db.execute(
                    """UPDATE task_receipts SET reply=?
                       WHERE conversation=? AND task_id=? AND attempt=? AND reply IS NULL""",
                    (
                        json.dumps(reply, ensure_ascii=False, allow_nan=False),
                        identifier,
                        task_id,
                        attempt,
                    ),
                ).rowcount
                if not changed:
                    raise fail(
                        "receipt_changed", "The task receipt changed while running"
                    )
        return revision + len(messages)
