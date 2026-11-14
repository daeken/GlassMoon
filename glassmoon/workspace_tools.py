"""Tools a GlassMoon agent can use in an operator-selected workspace."""

import asyncio
import fcntl
import hashlib
import heapq
import json
import os
import signal
import stat
import sys
import tempfile
from pathlib import Path

from .ipc import RequestError
from .models import INTEGER, TEXT
from .service import fail, method

FILE_LIMIT = 2 * 1024 * 1024
OUTPUT_LIMIT = 4000
EVAL_TIMEOUT = 15

TOOLS = {
    "list_files": method(
        "List one workspace directory or a page of its entries. Use . for the workspace root.",
        {"path": TEXT},
        {"after": TEXT},
    ),
    "read_file": method(
        "Read a slice of a UTF-8 workspace file and return its current SHA-256 revision",
        {"path": TEXT},
        {"offset": INTEGER},
    ),
    "find_in_file": method(
        "Find literal, case-sensitive text in one UTF-8 workspace file. "
        "Returns matching line numbers, character columns, and excerpts. To continue, pass next as after_line and include revision.",
        {"path": TEXT, "query": TEXT},
        {"after_line": INTEGER, "revision": TEXT},
    ),
    "write_file": method(
        "Create a UTF-8 file or replace it at the revision returned by read_file. "
        "Use an empty revision when creating a file. Its parent must exist.",
        {"path": TEXT, "text": TEXT, "revision": TEXT},
    ),
    "edit_file": method(
        "Replace exactly one occurrence of old text in a UTF-8 workspace file. "
        "Use the revision returned by read_file.",
        {"path": TEXT, "old": TEXT, "new": TEXT, "revision": TEXT},
    ),
    "edit_file_batch": method(
        "Replace several exact strings in one file at the given SHA-256 revision. All edits must match before anything is written.",
        {
            "path": TEXT, "revision": TEXT,
            "edits": {
                "type": "array", "minItems": 1, "maxItems": 8,
                "items": {
                    "type": "object", "properties": {"old": TEXT, "new": TEXT},
                    "required": ["old", "new"], "additionalProperties": False,
                },
            },
        },
    ),
    "python_eval": method(
        "Evaluate a raw Python expression or execute Python statements in a fresh process. "
        "Print values to return them when running statements. The current directory is the workspace.",
        {"code": TEXT},
    ),
}

PYTHON = """
import ast
import sys

sys.path.insert(0, sys.argv[1])
source = sys.stdin.read()
scope = {"__name__": "__main__"}
try:
    parsed = ast.parse(source, mode="eval")
except SyntaxError:
    exec(compile(source, "<glassmoon>", "exec"), scope)
else:
    value = eval(compile(parsed, "<glassmoon>", "eval"), scope)
    if value is not None:
        print(repr(value))
"""


class Workspace:
    def __init__(self, root, python=False, socket=None, *, read_only=False):
        self.root = Path(root).expanduser().resolve(strict=True)
        if not self.root.is_dir():
            raise ValueError("The workspace must be a directory")
        if read_only and python:
            raise ValueError("Raw Python requires a writable workspace")
        self.socket = socket
        self.names = (
            ["list_files", "read_file", "find_in_file"] if read_only
            else [name for name in TOOLS if name != "python_eval" or python]
        )
        self.definitions = [
            {
                "name": name,
                "description": TOOLS[name]["description"],
                "parameters": TOOLS[name]["params_schema"],
            }
            for name in self.names
        ]

    def path(self, path, *, allow_root=False):
        if not isinstance(path, str) or not path:
            raise fail("invalid_path", "Supply a workspace-relative file path")
        relative = Path(path)
        if relative.is_absolute() or ".." in relative.parts:
            raise fail("invalid_path", "Use a workspace-relative path")
        destination = self.root / relative
        resolved = destination.resolve()
        if (
            not resolved.is_relative_to(self.root)
            or resolved != destination
            or (resolved == self.root and not allow_root)
        ):
            raise fail(
                "invalid_path", "Choose a file inside the workspace without symlinks"
            )
        if not resolved.parent.is_dir():
            raise fail("missing_directory", "The parent directory does not exist")
        return resolved

    def list_files(self, directory, after):
        if after and ("/" in after or "\0" in after or len(after) > 255):
            raise fail("invalid_cursor", "Use the next cursor returned by list_files")
        try:
            fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        except NotADirectoryError:
            raise fail("invalid_directory", "Choose a directory in the workspace") from None
        try:
            with os.scandir(fd) as entries:
                names = heapq.nsmallest(
                    31,
                    (
                        (entry.name, entry)
                        for entry in entries
                        if entry.name > after
                        and not entry.name.startswith(".")
                        and not any(0xD800 <= ord(char) <= 0xDFFF for char in entry.name)
                    ),
                )
                page = []
                for name, entry in names[:30]:
                    kind = "other"
                    if entry.is_dir(follow_symlinks=False):
                        kind = "directory"
                    elif entry.is_file(follow_symlinks=False):
                        kind = "file"
                    elif entry.is_symlink():
                        kind = "symlink"
                    page.append({"name": name, "kind": kind})
        finally:
            os.close(fd)
        return {"entries": page, "next": page[-1]["name"] if len(names) > len(page) else None}

    def read(self, path):
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as file:
            mode = os.fstat(file.fileno()).st_mode
            if not stat.S_ISREG(mode):
                raise fail("invalid_file", "Choose a regular UTF-8 file")
            data = file.read(FILE_LIMIT + 1)
        if len(data) > FILE_LIMIT:
            raise fail("file_too_large", "File exceeds the workspace tool size limit")
        return (
            data.decode("utf-8"),
            hashlib.sha256(data).hexdigest(),
            stat.S_IMODE(mode),
        )

    def find_in_file(self, path, query, after_line=0, revision=None):
        if not query or len(query) > 160 or "\n" in query or "\r" in query:
            raise fail("invalid_query", "Use 1–160 characters on one line")
        if after_line < 0:
            raise fail("invalid_cursor", "Use the next line cursor returned by find_in_file")
        text, current_revision, _ = self.read(path)
        if revision is not None and revision != current_revision:
            raise fail("revision_changed", "The file changed. Restart the search.")
        matches = []
        for number, line in enumerate(text.split("\n"), 1):
            if number <= after_line:
                continue
            position = line.find(query)
            if position == -1:
                continue
            if len(matches) == 12:
                return {"matches": matches, "revision": current_revision, "next": matches[-1]["line"]}
            start = max(0, position - 40)
            end = min(len(line), position + max(90, len(query) + 40))
            excerpt = ("…" if start else "") + line[start:end] + ("…" if end < len(line) else "")
            matches.append({"line": number, "column": position + 1, "excerpt": excerpt})
        return {"matches": matches, "revision": current_revision, "next": None}

    def update(self, name, params, path):
        revision = params["revision"]
        if name == "write_file" and revision == "":
            # Exclusive creation prevents overwriting a file added after a model's read.
            data = params["text"].encode()
            if len(data) > FILE_LIMIT:
                raise fail(
                    "file_too_large", "File exceeds the workspace tool size limit"
                )
            try:
                with path.open("xb") as file:
                    file.write(data)
            except FileExistsError:
                raise fail(
                    "revision_changed", "The file already exists. Read it first."
                ) from None
            return {"revision": hashlib.sha256(data).hexdigest()}

        text, current, mode = self.read(path)
        if current != revision:
            raise fail("revision_changed", "The file changed. Read it before editing.")
        if name in ("edit_file", "edit_file_batch"):
            edits = (
                params["edits"] if name == "edit_file_batch"
                else [{"old": params["old"], "new": params["new"]}]
            )
            if not 1 <= len(edits) <= 8:
                raise fail("invalid_arguments", "Supply one to eight edits")
            for index, edit in enumerate(edits, start=1):
                if (
                    not isinstance(edit, dict) or set(edit) != {"old", "new"}
                    or not all(isinstance(value, str) for value in edit.values())
                ):
                    raise fail("invalid_arguments", "Every edit needs old and new text")
                old = edit["old"]
                if not old or text.count(old) != 1:
                    message = (
                        "The old text must occur exactly once" if name == "edit_file"
                        else f"Old text for edit {index} must occur exactly once"
                    )
                    raise fail("edit_conflict", message)
                text = text.replace(old, edit["new"], 1)
        else:
            text = params["text"]
        data = text.encode()
        if len(data) > FILE_LIMIT:
            raise fail("file_too_large", "File exceeds the workspace tool size limit")
        fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".glassmoon-edit-")
        try:
            with os.fdopen(fd, "wb") as file:
                file.write(data)
                os.fchmod(file.fileno(), mode)
            if self.read(path)[1] != revision:
                raise fail(
                    "revision_changed", "The file changed. Read it before editing."
                )
            os.replace(temporary, path)
        finally:
            Path(temporary).unlink(missing_ok=True)
        return {"revision": hashlib.sha256(data).hexdigest()}

    async def evaluate(self, code):
        if not isinstance(code, str) or len(code.encode()) > 12_000:
            raise fail("invalid_code", "Supply a Python string of at most 12000 bytes")
        environment = {
            key: os.environ[key]
            for key in ("PATH", "LANG", "LC_ALL")
            if key in os.environ
        }
        if self.socket is not None:
            environment["GLASSMOON_SOCKET"] = str(self.socket)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            "-u",
            "-c",
            PYTHON,
            str(Path(__file__).resolve().parent.parent),
            cwd=self.root,
            env=environment,
            start_new_session=True,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        output = bytearray()
        truncated = False

        async def drain():
            nonlocal truncated
            while chunk := await process.stdout.read(4096):
                remaining = OUTPUT_LIMIT - len(output)
                output.extend(chunk[:remaining])
                truncated |= len(chunk) > remaining

        async def send():
            try:
                process.stdin.write(code.encode())
                await process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                process.stdin.close()

        timed_out = False
        try:
            async with asyncio.timeout(EVAL_TIMEOUT):
                await asyncio.gather(send(), drain(), process.wait())
        except TimeoutError:
            timed_out = True
        finally:
            # Descendants may keep the output pipe open after the interpreter exits.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            await process.wait()
        return {
            "exit_code": process.returncode,
            "output": output.decode(errors="replace"),
            "truncated": truncated,
            "timed_out": timed_out,
        }

    async def execute(self, tool_call):
        name = tool_call["name"]
        try:
            if name not in self.names:
                raise fail("unknown_tool", "This agent has no tool by that name")
            params = json.loads(tool_call["arguments"])
            schema = TOOLS[name]["params_schema"]
            if (
                not isinstance(params, dict)
                or not set(schema["required"])
                <= params.keys()
                <= schema["properties"].keys()
                or any(
                    type(value) is not {
                        "integer": int, "string": str, "array": list,
                    }[schema["properties"][key]["type"]]
                    for key, value in params.items()
                )
            ):
                raise fail(
                    "invalid_arguments", "Arguments do not match the tool schema"
                )
            if name == "python_eval":
                return await self.evaluate(params["code"])
            if name == "list_files":
                directory = self.path(params["path"], allow_root=True)
                return self.list_files(directory, params.get("after", ""))
            path = self.path(params["path"])
            if name == "find_in_file":
                return self.find_in_file(path, params["query"], params.get("after_line", 0), params.get("revision"))
            if name == "read_file":
                text, revision, _ = self.read(path)
                offset = params.get("offset", 0)
                if not 0 <= offset <= len(text):
                    raise fail("invalid_offset", "Offset must be within the file")
                end = min(offset + 3000, len(text))
                return {
                    "text": text[offset:end],
                    "revision": revision,
                    "next_offset": end if end < len(text) else None,
                }
            lock = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    raise fail(
                        "workspace_busy", "Another workspace edit is in progress"
                    ) from None
                return self.update(name, params, path)
            finally:
                os.close(lock)
        except RequestError as error:
            return {"error": error.code, "message": str(error)}
        except FileNotFoundError:
            return {"error": "file_not_found", "message": "The file does not exist"}
        except UnicodeDecodeError:
            return {"error": "invalid_file", "message": "The file is not valid UTF-8"}
        except (OSError, ValueError, RuntimeError):
            return {
                "error": "tool_failed",
                "message": "Could not perform the workspace operation",
            }
