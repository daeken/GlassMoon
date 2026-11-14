"""Store explicitly shared browser observations in a local evidence catalog."""

import asyncio
import base64
import binascii
import hashlib
import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path

from .functions import image_part
from .ipc import ProtocolError, RequestError
from .models import OBJECT, TEXT, encoded_size
from .service import fail, serve, service_args
from .toolbox import METHODS, ToolService, call_tool, tool

TOOLS = {
    "publish": tool(
        "Save an explicit reproduction summary and bounded metadata for another agent.",
        {"owner": TEXT, "key": TEXT, "summary": TEXT},
        {"metadata": OBJECT},
    ),
    "read": tool("Read a shared reproduction summary.", {"owner": TEXT, "key": TEXT}),
    "image": tool(
        "Read the explicitly shared screenshot for this evidence.",
        {"owner": TEXT, "key": TEXT},
    ),
}
TOOLS["image"]["returns_image"] = True


def identifier(value):
    return (
        isinstance(value, str)
        and 1 <= len(value) <= 80
        and value.isascii()
        and all(char.isalnum() or char in "-_." for char in value)
    )


def screenshot_bytes(image):
    if not image_part(image):
        raise fail("invalid_image", "Use a bounded JPEG or PNG screenshot tool result")
    header, payload = image["url"].split(",", 1)
    try:
        raw = base64.b64decode(payload, validate=True)
    except binascii.Error:
        raise fail("invalid_image", "Screenshot has invalid base64 data") from None
    magic = b"\xff\xd8" if header == "data:image/jpeg;base64" else b"\x89PNG\r\n\x1a\n"
    if not raw.startswith(magic):
        raise fail("invalid_image", "Screenshot bytes do not match the declared format")
    return raw


def screenshot_digest(image):
    return hashlib.sha256(screenshot_bytes(image)).hexdigest()


async def export_image(socket, service, owner, key, destination):
    evidence = await call_tool(socket, service, "image", {"owner": owner, "key": key})
    raw = screenshot_bytes(evidence["image"])
    if hashlib.sha256(raw).hexdigest() != evidence["image_sha256"]:
        raise fail("image_integrity", "Screenshot does not match the saved digest")
    path = Path(destination).expanduser().absolute()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(raw)
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    return path


class Evidence:
    def __init__(self, path):
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.path.touch(mode=0o600, exist_ok=True)
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS evidence (
                    owner TEXT NOT NULL, key TEXT NOT NULL, summary TEXT NOT NULL,
                    metadata TEXT NOT NULL, PRIMARY KEY(owner, key)
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS evidence_image (
                    owner TEXT NOT NULL, key TEXT NOT NULL, url TEXT NOT NULL,
                    digest TEXT NOT NULL, PRIMARY KEY(owner, key)
                )
            """)

    def request(self, name, args):
        owner, key = args["owner"], args["key"]
        if not identifier(owner) or not identifier(key):
            raise fail("invalid_evidence_key", "Use a simple owner and key")
        with closing(sqlite3.connect(self.path, timeout=3)) as db, db:
            if name == "publish":
                summary, metadata = args["summary"], args.get("metadata", {})
                if (
                    not isinstance(summary, str)
                    or not 1 <= len(summary.encode()) <= 6000
                ):
                    raise fail(
                        "invalid_summary", "Supply 1–6000 UTF-8 bytes of summary"
                    )
                if not isinstance(metadata, dict) or encoded_size(metadata) > 4000:
                    raise fail(
                        "invalid_metadata",
                        "Evidence metadata requires a bounded object",
                    )
                payload = json.dumps(metadata, ensure_ascii=False, sort_keys=True)
                image = args.get("image")
                digest = screenshot_digest(image) if image is not None else None
                inserted = db.execute(
                    "INSERT OR IGNORE INTO evidence VALUES (?, ?, ?, ?)",
                    (owner, key, summary, payload),
                ).rowcount
                if inserted and image is not None:
                    db.execute(
                        "INSERT OR IGNORE INTO evidence_image VALUES (?, ?, ?, ?)",
                        (owner, key, image["url"], digest),
                    )
            row = db.execute(
                "SELECT summary, metadata FROM evidence WHERE owner=? AND key=?",
                (owner, key),
            ).fetchone()
            if row is None:
                raise fail(
                    "evidence_missing", "No evidence was published under that key"
                )
            if name == "publish" and row != (summary, payload):
                raise fail(
                    "evidence_conflict", "This key already contains different evidence"
                )
            stored = db.execute(
                "SELECT url, digest FROM evidence_image WHERE owner=? AND key=?",
                (owner, key),
            ).fetchone()
            if (
                name == "publish"
                and image is not None
                and (not stored or stored[1] != digest)
            ):
                raise fail(
                    "evidence_conflict",
                    "This key already contains a different screenshot",
                )
            result = {
                "owner": owner,
                "key": key,
                "summary": row[0],
                "metadata": json.loads(row[1]),
                "image_sha256": stored[1] if stored else None,
            }
            if name == "image":
                if not stored:
                    raise fail("image_missing", "This evidence has no screenshot")
                image = {"type": "image", "url": stored[0]}
                if screenshot_digest(image) != stored[1]:
                    raise fail(
                        "image_integrity", "Stored screenshot does not match its digest"
                    )
                result["image"] = image
            return result

    async def invoke(self, name, args):
        return await asyncio.to_thread(self.request, name, args)


class EvidenceService(ToolService):
    async def handle(self, name, params, emit, stream):
        if name == "invoke" and params.get("name") == "publish_image":
            args = params["arguments"]
            if not isinstance(args, dict) or not args.get("image"):
                raise fail("invalid_image", "An explicit screenshot is required")
            return {"value": await self.invoke("publish", args)}
        if (
            name == "invoke"
            and params.get("name") == "publish"
            and "image" in params.get("arguments", {})
        ):
            raise fail("invalid_image", "Use publish_image to share a screenshot")
        return await super().handle(name, params, emit, stream)


def main():
    parser = service_args(__doc__, "tools.evidence")
    parser.add_argument("--state", required=True, type=Path)
    args = parser.parse_args()
    try:
        evidence = Evidence(args.state)
        worker = EvidenceService(TOOLS, evidence.invoke)
        asyncio.run(
            serve(
                args.socket,
                args.name,
                args.replace,
                METHODS,
                worker.handle,
                reconnect=True,
                maintainer=args.maintainer,
            )
        )
    except KeyboardInterrupt:
        return 130
    except (OSError, sqlite3.Error, ProtocolError, RequestError, EOFError) as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
