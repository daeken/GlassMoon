"""End-to-end regressions for routing across a live service replacement."""

import asyncio
import os
import sys
import unittest
from contextlib import AsyncExitStack
from pathlib import Path
from tempfile import TemporaryDirectory

from glassmoon.echo import METHODS
from glassmoon.ipc import connect

BROKER = Path(os.environ.get("GLASSMOON_BROKER", "target/debug/glassmoon"))


class UpgradeTests(unittest.IsolatedAsyncioTestCase):
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
                "register", "register", name="echo", interface_version=2, methods=METHODS
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


if __name__ == "__main__":
    unittest.main()
