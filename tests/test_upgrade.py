"""End-to-end regressions for routing across a live service replacement."""

import asyncio
import json
import os
import sys
import unittest
from contextlib import AsyncExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch

from glassmoon.agent import Agent
from glassmoon.echo import METHODS
from glassmoon.ipc import ProtocolError, RequestError, call, connect
from glassmoon.models import Transcripts
from glassmoon.release import release
from glassmoon.service import serve
from glassmoon.toolbox import METHODS as TOOL_METHODS
from glassmoon.toolbox import ToolService, tool
from glassmoon.tui import Terminal

BROKER = Path(os.environ.get("GLASSMOON_BROKER", "target/debug/glassmoon"))


class UpgradeTests(unittest.IsolatedAsyncioTestCase):
    async def test_exact_call_refuses_broker_without_feature(self):
        async with AsyncExitStack() as stack, asyncio.timeout(5):
            folder = Path(stack.enter_context(TemporaryDirectory(prefix="gm-old-", dir="/tmp")))
            socket = folder / "ipc"
            received = []
            done = asyncio.Event()

            async def old_broker(reader, writer):
                try:
                    request = json.loads(await reader.readline())
                    writer.write(json.dumps({
                        "type": "response", "id": request["id"], "status": "ok",
                        "result": {"protocol": 1},
                    }).encode() + b"\n")
                    await writer.drain()
                    received.append(await reader.readline())
                finally:
                    writer.close()
                    await writer.wait_closed()
                    done.set()

            await stack.enter_async_context(await asyncio.start_unix_server(old_broker, path=socket))
            with self.assertRaises(RequestError) as error:
                await call(socket, "echo", "echo", {"value": "staged only"}, generation=42)
            self.assertEqual(error.exception.code, "unsupported_generation_calls")
            await done.wait()
            done.clear()
            cli = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "glassmoon", "--socket", socket,
                "call", "echo", "echo", "--generation", "42",
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await cli.communicate()
            self.assertEqual(cli.returncode, 1)
            self.assertIn(b"unsupported_generation_calls", stderr)
            await done.wait()
            self.assertEqual(received, [b"", b""])

    async def test_exact_call_rejects_a_mismatched_reply(self):
        async with AsyncExitStack() as stack, asyncio.timeout(5):
            folder = Path(stack.enter_context(TemporaryDirectory(prefix="gm-origin-", dir="/tmp")))
            socket = folder / "ipc"

            async def wrong_route(reader, writer):
                try:
                    for generation, result in (
                        (None, {"protocol": 1, "features": ["generation_calls"]}),
                        (41, {"value": "wrong worker"}),
                    ):
                        request = json.loads(await reader.readline())
                        message = {"type": "response", "id": request["id"], "status": "ok", "result": result}
                        if generation is not None:
                            message["generation"] = generation
                        writer.write(json.dumps(message).encode() + b"\n")
                        await writer.drain()
                finally:
                    writer.close()
                    await writer.wait_closed()

            await stack.enter_async_context(await asyncio.start_unix_server(wrong_route, path=socket))
            with self.assertRaises(ProtocolError):
                await call(socket, "echo", "echo", {"value": "staged only"}, generation=42)

    async def test_supervisor_preserves_worker_through_broker_outage(self):
        async with AsyncExitStack() as stack, asyncio.timeout(15):
            folder = Path(stack.enter_context(TemporaryDirectory(prefix="gm-supervise-", dir="/tmp")))
            socket = folder / "ipc"

            async def start_broker():
                process = await asyncio.create_subprocess_exec(BROKER, socket)
                stack.push_async_callback(self.stop_process, process)
                while not socket.is_socket():
                    self.assertIsNone(process.returncode)
                    await asyncio.sleep(0.01)
                return process

            broker = await start_broker()
            supervisor = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "glassmoon.supervisor", "--socket", socket,
                "--service", "moon.text", "--expect", '{"status":"ok"}',
                "--interval", "0.05", "--timeout", "1",
                "--failures", "1", "--restarts", "0", "--",
                sys.executable, "-m", "examples.moon_service", "--socket", socket,
                stdout=asyncio.subprocess.PIPE,
            )
            stack.push_async_callback(self.stop_process, supervisor)

            async def expect(event):
                while line := await supervisor.stdout.readline():
                    if f'"event": "{event}"'.encode() in line:
                        return
                    self.assertNotIn(b'"event": "unhealthy"', line)
                self.fail(f"Missing supervisor event {event}")

            await expect("healthy")
            await self.stop_process(broker)
            await expect("broker_unreachable")
            await start_broker()
            await expect("broker_reconnected")
            for _ in range(20):
                try:
                    result = await call(socket, "moon.text", "health", {})
                    self.assertEqual(result["status"], "ok")
                    break
                except RequestError:
                    await asyncio.sleep(0.1)
            else:
                self.fail("Worker did not re-register")
            self.assertIsNone(supervisor.returncode)


    async def start_broker(self, stack, drain_ms):
        directory = stack.enter_context(TemporaryDirectory(prefix="glassmoon-", dir="/tmp"))
        socket = Path(directory, "broker.sock")
        broker = await asyncio.create_subprocess_exec(BROKER, socket, str(drain_ms))
        stack.push_async_callback(self.stop_process, broker)
        while not socket.is_socket():
            self.assertIsNone(broker.returncode)
            await asyncio.sleep(0.01)
        return socket

    async def stop_process(self, process):
        if process.returncode is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            await asyncio.wait_for(process.wait(), 5)

    async def register_old(self, stack, socket):
        old = await stack.enter_async_context(connect(socket))
        response = await old.request(
            "register", "register", name="echo", interface_version=1, methods=METHODS
        )
        generation = response["result"]["generation"]
        await old.request("promote", "promote", expected_active=None)
        return old, generation

    async def start_replacement(self, stack, socket, previous):
        service = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "glassmoon.echo", "--socket", socket,
            "--version", "v2", "--replace", str(previous),
            stdout=asyncio.subprocess.PIPE,
        )
        stack.push_async_callback(self.stop_process, service)
        ready = (await service.stdout.readline()).decode()
        self.assertIn("echo v2 active as generation ", ready)
        return int(ready.split()[-1])

    async def call(self, client, request_id, value, version=1):
        await client.send(
            "call", id=request_id, service="echo", interface_version=version,
            method="echo", params={"value": value},
        )

    async def test_new_calls_switch_while_old_calls_drain(self):
        async with AsyncExitStack() as stack, asyncio.timeout(10):
            socket = await self.start_broker(stack, 5000)
            old, previous = await self.register_old(stack, socket)
            client = await stack.enter_async_context(connect(socket))
            watcher = await stack.enter_async_context(connect(socket))
            snapshot = (await watcher.request("watch", "watch"))["result"]
            self.assertEqual(snapshot["services"][0]["generation"], previous)
            self.assertEqual(snapshot["services"][0]["state"], "active")

            await self.call(client, "old", "before")
            invocation = await old.receive()
            self.assertEqual(invocation["params"], {"value": "before"})

            latest = await self.start_replacement(stack, socket, previous)
            self.assertEqual((await old.receive())["type"], "draining")
            for event, generation in [
                ("registered", latest), ("promoted", latest), ("draining", previous)
            ]:
                update = await watcher.receive()
                self.assertEqual(update["event"], event)
                self.assertEqual(update["service"]["generation"], generation)
                if event == "draining":
                    self.assertGreater(update["service"]["drain_remaining_ms"], 0)
                    self.assertLessEqual(update["service"]["drain_remaining_ms"], 5000)

            await self.call(client, "new", "after")
            reply = await client.receive()
            self.assertEqual(reply["id"], "new")
            self.assertEqual(reply["generation"], latest)
            self.assertEqual(reply["result"], {"value": "after", "version": "v2"})

            await old.send(
                "complete", call_id=invocation["call_id"], status="ok",
                result={"value": "before", "version": "v1"},
            )
            reply = await client.receive()
            self.assertEqual(reply["id"], "old")
            self.assertEqual(reply["generation"], previous)
            self.assertEqual(reply["result"], {"value": "before", "version": "v1"})
            self.assertEqual(await old.receive(), {"type": "retire", "reason": "drained"})
            retired = await watcher.receive()
            self.assertEqual(retired["event"], "retired")
            self.assertEqual(retired["service"]["generation"], previous)
            self.assertEqual(retired["service"]["reason"], "drained")

    async def test_drain_timeout_does_not_disrupt_new_generation(self):
        async with AsyncExitStack() as stack, asyncio.timeout(10):
            socket = await self.start_broker(stack, 100)
            old, previous = await self.register_old(stack, socket)
            client = await stack.enter_async_context(connect(socket))
            await self.call(client, "old", "before")
            invocation = await old.receive()

            latest = await self.start_replacement(stack, socket, previous)
            self.assertEqual((await old.receive())["type"], "draining")
            self.assertEqual(await old.receive(), {"type": "retire", "reason": "deadline"})
            reply = await client.receive()
            self.assertEqual(reply["id"], "old")
            self.assertEqual(reply["generation"], previous)
            self.assertEqual(reply["error"]["code"], "drain_timeout")

            await old.send("complete", call_id=invocation["call_id"], status="ok", result=None)
            await self.call(client, "new", "after")
            reply = await client.receive()
            self.assertEqual(reply["id"], "new")
            self.assertEqual(reply["generation"], latest)

    async def test_stale_promotion_and_interface_mismatch(self):
        async with AsyncExitStack() as stack, asyncio.timeout(10):
            socket = await self.start_broker(stack, 5000)
            _, previous = await self.register_old(stack, socket)
            staged = await stack.enter_async_context(connect(socket))
            registration = await staged.request(
                "register", "register", name="echo", interface_version=2,
                methods=METHODS, maintainer="moon",
            )
            latest = registration["result"]["generation"]
            await staged.send("promote", id="stale", expected_active=None)
            self.assertEqual((await staged.receive())["error"]["code"], "active_changed")
            await staged.request("promote", "promote", expected_active=previous)
            client = await stack.enter_async_context(connect(socket))
            await self.call(client, "old-version", "call")
            self.assertEqual((await client.receive())["error"]["code"], "interface_mismatch")
            description = await client.request("describe", "describe", generation=latest)
            self.assertEqual(description["result"]["interface_version"], 2)
            self.assertEqual(description["result"]["maintainer"], "moon")

            terminal = Terminal(None, socket)
            terminal.services = {0: {"name": "watch snapshot", "generation": 0}}
            await terminal.list_services()
            self.assertIn(f"echo #{latest}", terminal.windows["status"][-1])
            self.assertEqual(list(terminal.services), [0])
            await terminal.show_maintainer("echo")
            self.assertIn(f"echo #{latest}  maintainer moon", terminal.windows["status"][-1])
            queue = AsyncMock()
            with patch.object(terminal, "queue_task", queue):
                terminal.input = f"/maintain echo#{previous} Stale handoff"
                terminal.submit()
                await asyncio.gather(*terminal.tasks)
                queue.assert_not_awaited()
                terminal.input = f"/maintain echo#{latest} Inspect the promoted worker"
                terminal.submit()
                await asyncio.gather(*terminal.tasks)
            arguments = queue.await_args.args
            self.assertEqual(arguments[0], "moon")
            self.assertIn(str(latest), arguments[1])
            self.assertIn("Inspect the promoted worker", arguments[1])

    async def test_discard_retires_only_a_staged_worker(self):
        async with AsyncExitStack() as stack, asyncio.timeout(10):
            socket = await self.start_broker(stack, 5000)
            active, previous = await self.register_old(stack, socket)
            staged = await stack.enter_async_context(connect(socket))
            registration = await staged.request(
                "register", "candidate", name="echo", interface_version=1, methods=METHODS,
            )
            candidate = registration["result"]["generation"]
            client = await stack.enter_async_context(connect(socket))
            with self.assertRaises(RequestError) as rejected:
                await client.request("discard", "active", generation=previous)
            self.assertEqual(rejected.exception.code, "not_staged")
            call_task = asyncio.create_task(call(socket, "echo", "echo", {"value": "check"}, generation=candidate))
            invoke = await staged.receive()
            self.assertEqual(invoke["method"], "echo")
            discarded = await client.request("discard", "discard", generation=candidate)
            self.assertEqual(discarded["result"], {"discarded": candidate})
            with self.assertRaises(RequestError) as interrupted:
                await call_task
            self.assertEqual(interrupted.exception.code, "generation_discarded")
            self.assertEqual(await staged.receive(), {"type": "retire", "reason": "discarded"})
            await self.call(client, "still-active", "current")
            self.assertEqual((await active.receive())["params"], {"value": "current"})

    async def test_pre_promotion_probe_targets_only_staged_generation(self):
        async with AsyncExitStack() as stack, asyncio.timeout(10):
            socket = await self.start_broker(stack, 5000)
            old, previous = await self.register_old(stack, socket)
            version = "bad"

            async def handler(_name, _params, _emit, _stream):
                return {"version": version}

            ready = asyncio.Event()
            candidate = asyncio.create_task(serve(
                socket, "echo", None, METHODS, handler, staged=True, on_ready=ready.set,
            ))

            async def stop_candidate():
                candidate.cancel()
                await asyncio.gather(candidate, return_exceptions=True)

            stack.push_async_callback(stop_candidate)
            await ready.wait()
            client = await stack.enter_async_context(connect(socket))
            rows = (await client.request("list", "list"))["result"]["services"]
            generation = next(item["generation"] for item in rows if item["state"] == "staged")
            with self.assertRaises(RequestError) as failure:
                await release(socket, generation, "echo", {"value": "test"}, {"version": "v2"})
            self.assertEqual(failure.exception.code, "probe_mismatch")
            await self.call(client, "still-old", "check")
            received = await old.receive()
            self.assertEqual(received["params"], {"value": "check"})

            version = "v2"
            with self.assertRaises(RequestError) as stale_release:
                await release(
                    socket, generation, "echo", {}, {"version": "v2"}, expected_active=0,
                )
            self.assertEqual(stale_release.exception.code, "active_changed")
            checked = await release(
                socket, generation, "echo", {"value": "test"}, {"version": "v2"},
                check_only=True, expected_active=previous,
            )
            self.assertTrue(checked["probe_passed"])
            self.assertEqual(checked["generation"], generation)
            self.assertGreater(checked["checked_at"], 0)
            async with connect(socket) as observer:
                snapshot = (await observer.request("list", "after-probe"))["result"]["services"]
            self.assertEqual(
                {row["generation"]: row["state"] for row in snapshot},
                {previous: "active", generation: "staged"},
            )
            promoted = await release(socket, generation, "echo", {"value": "test"}, {"version": "v2"})
            self.assertEqual((promoted["previous"], promoted["generation"]), (previous, generation))
            self.assertEqual(await call(socket, "echo", "echo", {"value": 1}), {"version": "v2"})
            with self.assertRaises(RequestError) as draining:
                await call(socket, "echo", "echo", {"value": 1}, generation=previous)
            self.assertEqual(draining.exception.code, "generation_unavailable")
            await old.send("complete", call_id=received["call_id"], status="ok", result={"version": "v1"})
            self.assertEqual((await client.receive())["result"], {"version": "v1"})

    async def test_active_and_promoted_workers_reconnect_after_broker_restart(self):
        async with AsyncExitStack() as stack, asyncio.timeout(12):
            directory = stack.enter_context(TemporaryDirectory(prefix="gm-reconnect-", dir="/tmp"))
            socket = Path(directory, "ipc")

            async def broker():
                process = await asyncio.create_subprocess_exec(BROKER, socket)
                stack.push_async_callback(self.stop_process, process)
                while not socket.is_socket():
                    self.assertIsNone(process.returncode)
                    await asyncio.sleep(.01)
                return process

            first = await broker()
            ready = asyncio.Event()
            starts = 0

            def on_ready():
                nonlocal starts
                starts += 1
                ready.set()

            async def handler(_name, _params, _emit, _stream):
                return {"starts": starts}

            worker = asyncio.create_task(serve(
                socket, "echo", None, METHODS, handler, on_ready=on_ready, reconnect=True,
            ))

            async def stop_worker():
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)

            stack.push_async_callback(stop_worker)
            await ready.wait()
            self.assertEqual(await call(socket, "echo", "echo", {}), {"starts": 1})
            staged_ready = asyncio.Event()
            staged_worker = asyncio.create_task(serve(
                socket, "staged.echo", None, METHODS, handler,
                on_ready=staged_ready.set, reconnect=True, staged=True,
            ))

            async def stop_staged():
                staged_worker.cancel()
                await asyncio.gather(staged_worker, return_exceptions=True)

            stack.push_async_callback(stop_staged)
            await staged_ready.wait()
            async with connect(socket) as wire:
                before = (await wire.request("list", "list"))["result"]["services"]
            generation = next(row["generation"] for row in before if row["name"] == "echo")
            staged_generation = next(
                row["generation"] for row in before if row["name"] == "staged.echo"
            )
            await release(socket, staged_generation, "echo", {}, {"starts": 1})
            self.assertEqual(await call(socket, "staged.echo", "echo", {}), {"starts": 1})
            pending_ready = asyncio.Event()
            pending = asyncio.create_task(serve(
                socket, "pending.echo", None, METHODS, handler,
                on_ready=pending_ready.set, reconnect=True, staged=True,
            ))

            async def stop_pending():
                pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)

            stack.push_async_callback(stop_pending)
            await pending_ready.wait()
            ready.clear()
            staged_ready.clear()
            await self.stop_process(first)
            await broker()
            with self.assertRaises((EOFError, OSError)):
                await pending
            await ready.wait()
            await staged_ready.wait()
            self.assertEqual(await call(socket, "echo", "echo", {}), {"starts": 2})
            self.assertEqual(await call(socket, "staged.echo", "echo", {}), {"starts": 2})
            with self.assertRaises(RequestError) as stale:
                await call(socket, "echo", "echo", {}, generation=generation)
            self.assertEqual(stale.exception.code, "generation_unavailable")
            self.assertFalse(worker.done())

    async def test_agent_rejects_changed_tool_generation_until_next_turn(self):
        async with AsyncExitStack() as stack, asyncio.timeout(10):
            socket = await self.start_broker(stack, 5000)
            store = Transcripts(socket.with_name("agent.db"))
            stack.callback(store.close)
            agent = Agent(socket, "moon", "models", {}, "", store, tool_services=["tools.fixture"])
            calls = []

            async def start(label, previous=None):
                async def invoke(name, args):
                    calls.append((label, name, args))
                    return label

                ready = asyncio.Event()
                service = ToolService({"note": tool(f"Note operation {label}")}, invoke)
                worker = asyncio.create_task(serve(
                    socket, "tools.fixture", previous, TOOL_METHODS, service.handle,
                    on_ready=ready.set,
                ))

                async def stop():
                    worker.cancel()
                    await asyncio.gather(worker, return_exceptions=True)

                stack.push_async_callback(stop)
                await ready.wait()

            await start("first")
            original = (await agent.handle("tool_status", {}, None, False))["services"]
            tool_request = {
                "provider": "demo", "model": "test",
                "message": {"role": "assistant", "content": ""},
                "tool_calls": [{"id": "first", "name": "tools_fixture__note", "arguments": "{}"}],
            }
            changed = False

            async def router(_socket, _service, _method, params, **_kwargs):
                nonlocal changed
                if "tool_history" in params:
                    seen.append(params["tool_history"][0]["outputs"]["first"])
                    return {"message": {"role": "assistant", "content": "Read the tool result"}}
                if not changed:
                    await start("replacement", original["tools.fixture"]["generation"])
                    changed = True
                return tool_request

            seen = []
            with patch("glassmoon.agent.call", AsyncMock(side_effect=router)):
                await agent.handle("chat", {"text": "Try the note operation"}, None, False)
                self.assertEqual(calls, [])
                self.assertIn('"error": "tool_changed"', seen[0])
                await agent.handle("chat", {"text": "Try again with the current tool"}, None, False)
            self.assertEqual(calls, [("replacement", "note", {})])
            self.assertIn('"value": "replacement"', seen[1])


if __name__ == "__main__":
    unittest.main()
