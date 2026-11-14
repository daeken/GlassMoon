import asyncio
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from glassmoon.ipc import call, connect
from glassmoon.toolbox import call_tool


class StackTests(unittest.IsolatedAsyncioTestCase):
    async def test_dispatcher_discovers_agent_registered_after_start(self):
        with TemporaryDirectory(prefix="gm-discover-", dir="/tmp") as root:
            state = Path(root) / "state"
            processes = []

            async def start(*args):
                process = await asyncio.create_subprocess_exec(
                    sys.executable, "-m", *args,
                    stdout=asyncio.subprocess.PIPE,
                )
                processes.append(process)
                return process

            try:
                async with asyncio.timeout(15):
                    offline = await start("glassmoon.stack", "examples/offline-stack.toml", "--state", state)
                    socket = json.loads(await offline.stdout.readline())["socket"]
                    task = await call_tool(socket, "tools.mailbox", "submit", {
                        "sender": "test", "agent": "late", "key": "registration", "task": "A local task",
                    })
                    dispatcher = await start("glassmoon.dispatcher", "--socket", socket, "--discover", "--interval", "0.1")
                    await asyncio.sleep(0.25)
                    queued = await call_tool(socket, "tools.mailbox", "get", {"id": task["id"]})
                    self.assertEqual((queued["status"], queued["attempts"]), ("queued", 0))
                    late = await start("glassmoon.agent", "--socket", socket, "--name", "late", "--state", state / "late.db")
                    self.assertIn(b"agent.late active", await late.stdout.readline())
                    delivered = json.loads(await dispatcher.stdout.readline())
                    self.assertEqual((delivered["id"], delivered["status"]), (task["id"], "done"))
            finally:
                for process in reversed(processes):
                    if process.returncode is None:
                        process.terminate()
                        await asyncio.wait_for(process.wait(), 10)

    async def test_independent_offline_stacks_do_not_share_tasks(self):
        with TemporaryDirectory(prefix="gm-stack-", dir="/tmp") as root:
            processes = []

            async def start(state):
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m",
                    "glassmoon.stack",
                    "examples/offline-stack.toml",
                    "--state",
                    state,
                    stdout=asyncio.subprocess.PIPE,
                )
                processes.append(process)
                line = await process.stdout.readline()
                self.assertTrue(line)
                ready = json.loads(line)
                self.assertEqual(ready["event"], "ready")
                return process, ready["socket"]

            try:
                async with asyncio.timeout(12):
                    first, a = await start(Path(root) / "a")
                    _, b = await start(Path(root) / "b")
                    self.assertNotEqual(a, b)
                    await call_tool(
                        a,
                        "tools.mailbox",
                        "submit",
                        {
                            "sender": "test",
                            "agent": "moon",
                            "key": "isolation",
                            "task": "local only",
                        },
                    )
                    self.assertEqual(
                        (await call_tool(b, "tools.mailbox", "list_tasks", {}))[
                            "tasks"
                        ],
                        [],
                    )
                    first.terminate()
                    self.assertEqual(await first.wait(), 0)
                    self.assertFalse(Path(a).exists())
                    async with connect(b) as wire:
                        self.assertEqual(
                            len(
                                (await wire.request("list", "list"))["result"][
                                    "services"
                                ]
                            ),
                            4,
                        )
                    response = await call(
                        b, "agent.moon", "chat", {"text": "still live"}
                    )
                    self.assertEqual(
                        response["message"]["content"][0]["text"], "Echo: still live"
                    )
            finally:
                for process in reversed(processes):
                    if process.returncode is None:
                        process.terminate()
                        await asyncio.wait_for(process.wait(), 10)
