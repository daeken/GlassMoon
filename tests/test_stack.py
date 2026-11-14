import asyncio
import json
import os
import signal
import socket
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from glassmoon.agent import METHODS
from glassmoon.ipc import call, connect
from glassmoon.stack import manifest
from glassmoon.toolbox import call_tool


class StackTests(unittest.IsolatedAsyncioTestCase):
    async def test_dispatcher_term_finishes_admitted_task_without_claiming_next(self):
        with TemporaryDirectory(prefix="gm-dispatch-drain-", dir="/tmp") as root:
            stack = await asyncio.create_subprocess_exec(sys.executable, "-m", "glassmoon.stack",
                "examples/offline-stack.toml", "--state", Path(root) / "state", stdout=asyncio.subprocess.PIPE)
            dispatcher = None
            try:
                async with asyncio.timeout(10):
                    s = json.loads(await stack.stdout.readline())["socket"]
                    async with connect(s) as worker:
                        await worker.request("register", "register", name="agent.fixture", interface_version=1, methods=METHODS)
                        await worker.request("promote", "promote", expected_active=None)
                        body = {"sender": "test", "agent": "fixture", "task": "Local task"}
                        first = await call_tool(s, "tools.mailbox", "submit", body | {"key": "first"})
                        second = await call_tool(s, "tools.mailbox", "submit", body | {"key": "second"})
                        dispatcher = await asyncio.create_subprocess_exec(sys.executable, "-m", "glassmoon.dispatcher",
                            "--socket", s, "--agent", "fixture", "--interval", "0.1")
                        work = await worker.receive()
                        self.assertEqual(work["params"]["id"], first["id"])
                        dispatcher.terminate()
                        await asyncio.sleep(0.05)
                        await worker.send("complete", call_id=work["call_id"], status="ok",
                            result={"message": {"role": "assistant", "content": "Saved"}})
                        self.assertEqual(await dispatcher.wait(), 0)
                        for task, status in ((first, "done"), (second, "queued")):
                            self.assertEqual((await call_tool(s, "tools.mailbox", "get", {"id": task["id"]}))["status"], status)
            finally:
                if dispatcher is not None and dispatcher.returncode is None:
                    dispatcher.kill()
                    await dispatcher.wait()
                if stack.returncode is None:
                    stack.terminate()
                    await stack.wait()

    def test_manifest_rejects_invalid_command_and_environment_types_before_launch(self):
        with TemporaryDirectory() as folder:
            path = Path(folder, "stack.toml")
            for error, content in (
                ("broker command", "broker = [17]\n"),
                ("worker needs a command", 'command = ["", "arg"]\n'),
                ("service must be a nonempty string", 'command = ["worker"]\nservice = []\n'),
                ("environment values", 'command = ["worker"]\nenv = { KEY = 10 }\n'),
                ("pass_env must be an array", 'command = ["worker"]\npass_env = "KEY"\n'),
            ):
                with self.subTest(error=error):
                    if not content.startswith("broker"):
                        content = 'broker = ["broker"]\n[[worker]]\nname = "worker"\n' + content
                    path.write_text(content)
                    with self.assertRaisesRegex(ValueError, error):
                        manifest(path)

    async def test_stack_timeout_names_failed_readiness_probe(self):
        with TemporaryDirectory(prefix="gm-stack-probe-", dir="/tmp") as root:
            folder = Path(root)
            manifest = folder / "stack.toml"
            content = Path("examples/offline-stack.toml").read_text()
            manifest.write_text(content.replace('expect = { agent = "moon" }', 'expect = { agent = "other" }'))
            process = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "glassmoon.stack", manifest,
                "--state", folder / "state", "--timeout", "2",
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                async with asyncio.timeout(10):
                    _, detail = await process.communicate()
                self.assertEqual(process.returncode, 1)
                self.assertIn(b"agent.moon was not ready", detail)
                self.assertIn(b"info probe returned unexpected data", detail)
            finally:
                if process.returncode is None:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), 5)

    async def test_stack_restarts_broker_and_preserves_agent_state(self):
        with TemporaryDirectory(prefix="gm-stack-recovery-", dir="/tmp") as root:
            process = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "glassmoon.stack", "examples/offline-stack.toml",
                "--state", Path(root) / "state", "--restart-broker",
                stdout=asyncio.subprocess.PIPE,
            )
            try:
                async with asyncio.timeout(12):
                    ready = json.loads(await process.stdout.readline())
                    socket_path = ready["socket"]
                    status = await asyncio.create_subprocess_exec(
                        sys.executable, "-m", "glassmoon.stack", "examples/offline-stack.toml",
                        "--state", Path(root) / "state", "--status", stdout=asyncio.subprocess.PIPE,
                    )
                    status_output, _ = await status.communicate()
                    self.assertEqual(status.returncode, 0)
                    snapshot = json.loads(status_output)
                    self.assertEqual(snapshot["socket"], socket_path)
                    self.assertEqual(snapshot["missing"], [])
                    self.assertEqual(len(snapshot["services"]), 4)
                    await call(socket_path, "agent.moon", "chat", {"text": "Before broker exit"})
                    os.kill(ready["broker_pid"], signal.SIGKILL)
                    exited = json.loads(await process.stdout.readline())
                    recovered = json.loads(await process.stdout.readline())
                    self.assertEqual((exited["event"], exited["process"]), ("exited", "broker"))
                    self.assertEqual(recovered["event"], "broker_recovered")
                    self.assertNotEqual(ready["broker_pid"], recovered["broker_pid"])
                    history = await call(socket_path, "agent.moon", "history", {})
                    self.assertEqual(history["revision"], 2)
                    reply = await call(socket_path, "agent.moon", "chat", {"text": "After broker exit"})
                    self.assertIn("After broker exit", str(reply["message"]))
            finally:
                if process.returncode is None:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), 5)

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

    async def test_manifest_dispatcher_delivers_queued_work_while_stack_runs(self):
        with TemporaryDirectory(prefix="gm-stack-dispatch-", dir="/tmp") as root:
            root = Path(root)
            manifest_path = root / "stack.toml"
            definition = Path("examples/offline-stack.toml").read_text()
            dispatcher = 'dispatcher = ["{python}", "-m", "glassmoon.dispatcher", "--socket", "{socket}", "--discover", "--interval", "0.1"]\n'
            manifest_path.write_text(dispatcher + definition)
            process = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "glassmoon.stack", manifest_path,
                "--state", root / "state", stdout=asyncio.subprocess.PIPE,
            )
            try:
                async with asyncio.timeout(10):
                    socket_path = json.loads(await process.stdout.readline())["socket"]
                    task = await call_tool(socket_path, "tools.mailbox", "submit", {
                        "sender": "operator", "agent": "moon", "key": "verify", "task": "Offline manifest delivery",
                    })
                    while True:
                        found = await call_tool(socket_path, "tools.mailbox", "get", {"id": task["id"]})
                        if found["status"] == "done":
                            break
                        await asyncio.sleep(.1)
                    self.assertEqual(found["attempts"], 1)
                    self.assertIn("Offline manifest delivery", found["result"])
            finally:
                if process.returncode is None:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), 5)

    async def test_independent_offline_stacks_do_not_share_tasks(self):
        with TemporaryDirectory(prefix="gm-stack-", dir="/tmp") as root:
            processes = []

            async def start(state, *options):
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m",
                    "glassmoon.stack",
                    "examples/offline-stack.toml",
                    "--state",
                    state,
                    *options,
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
                    duplicate = await asyncio.create_subprocess_exec(
                        sys.executable, "-m", "glassmoon.stack", "examples/offline-stack.toml",
                        "--state", Path(root) / "a", "--resume", stderr=asyncio.subprocess.PIPE,
                    )
                    processes.append(duplicate)
                    _, detail = await duplicate.communicate()
                    self.assertEqual(duplicate.returncode, 1)
                    self.assertIn(b"already in use", detail)
                    task = await call_tool(
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
                    await call(a, "agent.moon", "chat", {"text": "before resume"})
                    self.assertEqual(
                        (await call_tool(b, "tools.mailbox", "list_tasks", {}))[
                            "tasks"
                        ],
                        [],
                    )
                    first.terminate()
                    self.assertEqual(await first.wait(), 0)
                    self.assertFalse(Path(a).exists())
                    # Reproduce the stale pathname a broker leaves after an abrupt exit.
                    with socket.socket(socket.AF_UNIX) as stale:
                        stale.bind(a)
                        stale.listen()
                    _, restored = await start(Path(root) / "a", "--resume")
                    self.assertEqual(restored, a)
                    self.assertEqual(
                        (await call_tool(restored, "tools.mailbox", "get", {"id": task["id"]}))["status"],
                        "queued",
                    )
                    history = await call(restored, "agent.moon", "history", {})
                    self.assertEqual(history["revision"], 2)
                    self.assertIn("before resume", json.dumps(history["messages"]))
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
