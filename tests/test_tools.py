import asyncio
import json
import os
import tempfile
import unittest
from contextlib import AsyncExitStack
from pathlib import Path
from unittest.mock import patch

import httpx

from glassmoon.agent import METHODS as AGENT_METHODS
from glassmoon.agent import Agent
from glassmoon.chat_provider import PROVIDERS, ChatProvider
from glassmoon.ipc import call
from glassmoon.models import CATALOG, GENERATE, Transcripts
from glassmoon.provider_router import METHODS as ROUTER_METHODS
from glassmoon.provider_router import Router
from glassmoon.service import serve
from glassmoon.workspace_tools import Workspace

BROKER = Path(os.environ.get("GLASSMOON_BROKER", "target/debug/glassmoon"))


def tool(name, **params):
    return {"name": name, "arguments": json.dumps(params)}


def sse(*items):
    return (
        "".join(f"data: {json.dumps(item)}\n\n" for item in items) + "data: [DONE]\n\n"
    )


class ToolTests(unittest.IsolatedAsyncioTestCase):
    async def test_read_only_workspace_lists_and_reads_but_refuses_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory, "note.txt")
            file.write_text("original")
            workspace = Workspace(directory, read_only=True)
            self.assertEqual([item["name"] for item in workspace.definitions], ["list_files", "read_file"])
            self.assertNotIn("error", await workspace.execute(tool("list_files", path=".")))
            read = await workspace.execute(tool("read_file", path=file.name))
            self.assertEqual(read["text"], "original")
            for name in ("write_file", "edit_file", "edit_file_batch", "python_eval"):
                self.assertEqual((await workspace.execute(tool(name)))["error"], "unknown_tool")
            self.assertEqual(file.read_text(), "original")
            with self.assertRaises(ValueError):
                Workspace(directory, python=True, read_only=True)

    async def test_workspace_batch_edit_rolls_back_failed_match(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory, "note.txt")
            file.write_text("first\nsecond\n")
            workspace = Workspace(directory)
            read = await workspace.execute(tool("read_file", path="note.txt"))
            fields = {"path": "note.txt", "revision": read["revision"]}
            first = {"old": "first", "new": "one"}
            failed = await workspace.execute(tool(
                "edit_file_batch", **fields, edits=[first, {"old": "absent", "new": "two"}],
            ))
            self.assertEqual(failed["error"], "edit_conflict")
            self.assertEqual(file.read_text(), "first\nsecond\n")
            edited = await workspace.execute(tool(
                "edit_file_batch", **fields, edits=[first, {"old": "second", "new": "two"}],
            ))
            self.assertIn("revision", edited)
            self.assertEqual(file.read_text(), "one\ntwo\n")

    async def test_workspace_edits_check_revisions_and_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = Workspace(root)
            target = root / "note.txt"
            create = await workspace.execute(
                tool("write_file", path=target.name, text="moon", revision="")
            )
            self.assertNotIn("error", create)
            read = await workspace.execute(tool("read_file", path=target.name))
            self.assertEqual(read["revision"], create["revision"])
            self.assertEqual(read["text"], "moon")

            edited = await workspace.execute(
                tool(
                    "edit_file",
                    path=target.name,
                    old="moon",
                    new="glass",
                    revision=read["revision"],
                )
            )
            self.assertEqual(target.read_text(), "glass")
            stale = await workspace.execute(
                tool(
                    "write_file",
                    path=target.name,
                    text="stale",
                    revision=read["revision"],
                )
            )
            self.assertEqual(stale["error"], "revision_changed")
            self.assertNotIn("error", edited)

            (root / "link").symlink_to(target)
            (root / "nested").mkdir()
            (root / ".hidden").write_text("fixture")
            for index in range(31):
                (root / f"item-{index:02}.txt").touch()
            page = await workspace.execute(tool("list_files", path="."))
            second = await workspace.execute(tool("list_files", path=".", after=page["next"]))
            names = {entry["name"]: entry["kind"] for entry in page["entries"] + second["entries"]}
            self.assertEqual(len(names), 34)
            self.assertEqual(names["nested"], "directory")
            self.assertEqual(names["link"], "symlink")
            self.assertNotIn(".hidden", names)
            self.assertIsNone(second["next"])
            self.assertEqual((await workspace.execute(tool("list_files", path="link")))["error"], "invalid_path")
            for path in ("link", "../note.txt", str(target)):
                result = await workspace.execute(tool("read_file", path=path))
                self.assertEqual(result["error"], "invalid_path")
            self.assertEqual(
                (await workspace.execute(tool("python_eval", code="1+1")))["error"],
                "unknown_tool",
            )

    async def test_python_exec_expression_and_timeout(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Workspace(directory, python=True)
            result = await workspace.execute(tool("python_eval", code="6 * 7"))
            self.assertEqual(result["output"].strip(), "42")
            result = await workspace.execute(
                tool(
                    "python_eval",
                    code="from pathlib import Path\nPath('made.txt').write_text('done')",
                )
            )
            self.assertEqual(result["exit_code"], 0)
            self.assertEqual(Path(directory, "made.txt").read_text(), "done")
            with patch("glassmoon.workspace_tools.EVAL_TIMEOUT", 0.2):
                run = await workspace.execute(
                    tool("python_eval", code="while True: pass")
                )
            self.assertTrue(run["timed_out"])

    async def test_agent_replays_tools_through_router(self):
        async with AsyncExitStack() as stack, asyncio.timeout(12):
            folder = Path(
                stack.enter_context(
                    tempfile.TemporaryDirectory(prefix="gm-", dir="/tmp")
                )
            )
            socket = folder / "ipc"
            broker = await asyncio.create_subprocess_exec(BROKER, socket)

            async def stop_broker():
                broker.terminate()
                await asyncio.wait_for(broker.wait(), 5)

            stack.push_async_callback(stop_broker)
            while not socket.is_socket():
                self.assertIsNone(broker.returncode)
                await asyncio.sleep(0.01)

            bodies = []
            provider = PROVIDERS["zai"]

            def upstream(request):
                self.assertEqual(str(request.url), provider.url)
                body = json.loads(request.content)
                bodies.append(body)
                if len(bodies) == 1:
                    first = {
                        "choices": [
                            {
                                "index": 0,
                                "delta": {
                                    "reasoning_content": "planning",
                                    "tool_calls": [
                                        {
                                            "index": 0,
                                            "id": "exec",
                                            "function": {
                                                "name": "python_eval",
                                                "arguments": '{"code":',
                                            },
                                        }
                                    ],
                                },
                            }
                        ]
                    }
                    last = {
                        "choices": [
                            {
                                "index": 0,
                                "finish_reason": "tool_calls",
                                "delta": {
                                    "tool_calls": [
                                        {
                                            "index": 0,
                                            "function": {"arguments": '"6*7"}'},
                                        }
                                    ],
                                },
                            }
                        ]
                    }
                    return httpx.Response(200, text=sse(first, last))
                if len(bodies) == 2:
                    self.assertEqual(
                        body["messages"][-2]["reasoning_content"], "planning"
                    )
                    self.assertIn(
                        "42", json.loads(body["messages"][-1]["content"])["output"]
                    )
                    return httpx.Response(
                        200,
                        text=sse(
                            {
                                "choices": [
                                    {
                                        "index": 0,
                                        "finish_reason": "tool_calls",
                                        "delta": {
                                            "tool_calls": [
                                                {
                                                    "index": 0,
                                                    "id": "write",
                                                    "function": {
                                                        "name": "write_file",
                                                        "arguments": {
                                                            "path": "result.txt",
                                                            "text": "42",
                                                            "revision": "",
                                                        },
                                                    },
                                                }
                                            ]
                                        },
                                    }
                                ]
                            }
                        ),
                    )
                self.assertIn("revision", json.loads(body["messages"][-1]["content"]))
                return httpx.Response(
                    200,
                    text=sse(
                        {
                            "choices": [
                                {
                                    "index": 0,
                                    "finish_reason": "stop",
                                    "delta": {"content": "Saved 42."},
                                }
                            ]
                        }
                    ),
                )

            client = await stack.enter_async_context(
                httpx.AsyncClient(transport=httpx.MockTransport(upstream))
            )
            adapter = ChatProvider(provider, client, "test-key", provider.models)
            transcripts = Transcripts(folder / "state")
            stack.callback(transcripts.close)
            agent = Agent(
                socket,
                "moon",
                "models",
                {"provider": "zai", "model": "glm-5.3"},
                "Use the tools.",
                transcripts,
                Workspace(folder, python=True),
            )
            for service, methods, handler, kind in [
                (
                    "zai",
                    {"models": CATALOG, "generate": GENERATE},
                    adapter.handle,
                    "model_provider",
                ),
                ("models", ROUTER_METHODS, Router(socket).handle, None),
                ("agent.moon", AGENT_METHODS, agent.handle, None),
            ]:
                ready = asyncio.Event()
                worker = asyncio.create_task(
                    serve(
                        socket,
                        service,
                        None,
                        methods,
                        handler,
                        kind=kind,
                        on_ready=ready.set,
                    )
                )

                async def stop(task):
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

                stack.push_async_callback(stop, worker)
                await ready.wait()

            chunks = []

            async def emit(chunk):
                chunks.append(chunk)

            answer = await call(
                socket,
                "agent.moon",
                "chat",
                {"text": "Calculate and save"},
                on_chunk=emit,
            )
            self.assertEqual((folder / "result.txt").read_text(), "42")
            self.assertEqual(answer["message"]["content"][0]["text"], "Saved 42.")
            self.assertEqual(answer["provider"], "zai")
            self.assertEqual(
                [item.get("name") for item in chunks if item.get("phase") == "started"],
                ["python_eval", "write_file"],
            )
            self.assertEqual(chunks[-1], {"type": "text", "text": "Saved 42."})
            self.assertEqual(transcripts.history("moon")["revision"], 2)

    async def test_deepseek_buffered_image_request(self):
        provider = PROVIDERS["deepseek"]

        def upstream(request):
            self.assertEqual(str(request.url), provider.url)
            self.assertEqual(request.headers["Authorization"], "Bearer deepseek-test")
            body = json.loads(request.content)
            self.assertFalse(body["stream"])
            self.assertEqual(body["messages"][0]["content"][0]["type"], "image_url")
            return httpx.Response(
                200,
                json={
                    "choices": [
                        {
                            "index": 0,
                            "finish_reason": "stop",
                            "message": {"content": "an image"},
                        }
                    ]
                },
            )

        async def emit(_):
            self.fail("Buffered response emitted a chunk")

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            adapter = ChatProvider(provider, client, "deepseek-test", provider.models)
            answer = await adapter.handle(
                "generate",
                {
                    "model": "deepseek-flash",
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {"type": "image", "url": "https://example.org/image"},
                            ],
                        }
                    ],
                },
                emit,
                False,
            )
            self.assertEqual(answer["message"]["content"], "an image")
