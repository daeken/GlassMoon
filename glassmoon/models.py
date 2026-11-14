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
    if "tool_calls" in model and type(model["tool_calls"]) is not bool:
        raise fail("invalid_model", "tool_calls must be a boolean")
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
            CREATE TABLE IF NOT EXISTS messages (
                conversation TEXT NOT NULL REFERENCES conversations(id),
                position INTEGER NOT NULL, value TEXT NOT NULL,
                PRIMARY KEY (conversation, position)
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

    def history(self, identifier, before=None):
        model, revision, messages = self.read(identifier)
        end = revision if before is None else min(max(0, before), revision)
        page = []
        size = 0
        for item in reversed(messages[:end]):
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

    def append(self, identifier, revision, *messages):
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
        return revision + len(messages)
