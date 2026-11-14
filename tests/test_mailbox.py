import asyncio
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from unittest.mock import AsyncMock, patch

from glassmoon.agent import Agent
from glassmoon.dispatcher import Dispatch
from glassmoon.ipc import RequestError
from glassmoon.mailbox import Mailbox, MailboxService
from glassmoon.models import Transcripts

TASK = {"sender": "moon", "agent": "tide", "key": "check", "task": "Check the fixture."}


class MailboxTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "mail.db"
        self.now = 100
        self.box = Mailbox(self.path, clock=lambda: self.now)

    def rpc(self, method, **fields):
        return self.box.request(method, fields)

    def test_dedupe_exclusive_leases_and_review(self):
        task = self.rpc("submit", **TASK)
        self.assertEqual(self.rpc("submit", **TASK)["id"], task["id"])
        with self.assertRaises(RequestError) as conflict:
            self.rpc("submit", **(TASK | {"task": "Different work"}))
        self.assertEqual(conflict.exception.code, "key_conflict")

        barrier = Barrier(2)

        def claim():
            barrier.wait(timeout=3)
            return self.rpc("claim", agent="tide", lease_seconds=10)

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(claim) for _ in range(2)]
            claims = [future.result(timeout=5) for future in futures]
        valid = [item for item in claims if item["task"] is not None]
        self.assertEqual(len(valid), 1)
        first = valid[0]
        self.assertNotIn("token", self.rpc("get", id=task["id"]))
        self.now += 5
        self.rpc("renew", id=task["id"], token=first["token"], lease_seconds=10)
        self.now += 11
        self.box = Mailbox(self.path, clock=lambda: self.now)
        with self.assertRaises(RequestError) as stale:
            self.rpc(
                "finish",
                id=task["id"],
                token=first["token"],
                status="done",
                result="stale",
            )
        self.assertEqual(stale.exception.code, "stale_lease")
        expired = self.rpc("get", id=task["id"])
        self.assertEqual(expired["status"], "review")
        self.assertIsNone(self.rpc("claim", agent="tide", lease_seconds=10)["task"])
        self.rpc(
            "review",
            id=task["id"],
            action="requeue",
            note="Verified no effect",
            expected_revision=expired["revision"],
        )
        second = self.rpc("claim", agent="tide", lease_seconds=10)
        self.assertNotEqual(first["token"], second["token"])
        done = {
            "id": task["id"],
            "token": second["token"],
            "status": "done",
            "result": "Verified fixture",
        }
        self.assertEqual(self.rpc("finish", **done), self.rpc("finish", **done))
        self.assertEqual(self.rpc("get", id=task["id"])["attempts"], 2)

    async def test_dispatcher_records_reply_and_does_not_replay_it(self):
        task = self.rpc("submit", **TASK)
        dispatcher = Dispatch("", "tools.mailbox", "tide")

        async def route(method, fields):
            return self.rpc(method, **fields)

        reply = AsyncMock(
            return_value={"message": {"role": "assistant", "content": "Ready"}}
        )
        with (
            patch.object(dispatcher, "online", AsyncMock(return_value=True)),
            patch.object(dispatcher, "rpc", side_effect=route),
            patch("glassmoon.dispatcher.call", reply),
        ):
            done = await dispatcher.deliver()
            self.assertEqual(done["result"], "Ready")
            self.assertIn(task["id"], reply.await_args.args[3]["text"])
            self.assertIsNone(await dispatcher.deliver())
        reply.assert_awaited_once()

    async def test_cancelled_delivery_requires_review(self):
        task = self.rpc("submit", **TASK)
        dispatcher = Dispatch("", "tools.mailbox", "tide")
        started = asyncio.Event()

        async def route(method, fields):
            return self.rpc(method, **fields)

        async def waiting(*_):
            started.set()
            await asyncio.Event().wait()

        with (
            patch.object(dispatcher, "online", AsyncMock(return_value=True)),
            patch.object(dispatcher, "rpc", side_effect=route),
            patch("glassmoon.dispatcher.call", waiting),
        ):
            work = asyncio.create_task(dispatcher.deliver())
            await asyncio.wait_for(started.wait(), 5)
            work.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await work
        self.now += 31
        self.assertEqual(self.rpc("get", id=task["id"])["status"], "review")
        self.assertIsNone(self.rpc("claim", agent="tide", lease_seconds=10)["task"])

    async def test_agent_receipt_survives_lost_ack_and_reconciles_mailbox(self):
        task = self.rpc("submit", **TASK)
        self.rpc("claim", agent="tide", lease_seconds=5)
        params = {"id": task["id"], "attempt": 1, "text": "Verify the fixture."}

        async def emit(_):
            pass

        database = self.path.with_name("agent.sqlite3")
        store = Transcripts(database)
        agent = Agent("", "tide", "", {}, "", store)
        generation = AsyncMock(
            return_value={"message": {"role": "assistant", "content": "Saved once"}}
        )
        with patch.object(agent, "generate", generation):
            first = await agent.handle("run_task", params, emit, False)
        store.close()

        self.now += 6
        self.assertEqual(self.rpc("get", id=task["id"])["status"], "review")
        store = Transcripts(database)
        self.addCleanup(store.close)
        agent = Agent("", "tide", "", {}, "", store)
        with patch.object(agent, "generate", generation):
            self.assertEqual(await agent.handle("run_task", params, emit, False), first)
        generation.assert_awaited_once()
        self.assertEqual(store.history("tide")["revision"], 2)

        async def lookup(_socket, _service, method, payload):
            return await agent.handle(method, payload, emit, False)

        worker = MailboxService(self.box, "socket")
        with patch("glassmoon.mailbox.call", side_effect=lookup):
            result = await worker.reconcile(task["id"])
        self.assertEqual(
            (result["status"], result["result"], result["attempts"]),
            ("done", "Saved once", 1),
        )

    async def test_started_receipt_does_not_replay_an_interrupted_attempt(self):
        task = self.rpc("submit", **TASK)
        params = {"id": task["id"], "attempt": 1, "text": "Start the fixture."}
        store = Transcripts(self.path.with_name("agent.sqlite3"))
        self.addCleanup(store.close)
        agent = Agent("", "tide", "", {}, "", store)

        async def emit(_):
            pass

        with (
            patch.object(agent, "generate", AsyncMock(side_effect=OSError)),
            self.assertRaises(OSError),
        ):
            await agent.handle("run_task", params, emit, False)
        with self.assertRaises(RequestError) as caught:
            await agent.handle("run_task", params, emit, False)
        self.assertEqual(caught.exception.code, "task_uncertain")
        self.assertEqual(store.history("tide")["revision"], 0)

    async def test_dispatcher_preserves_ambiguity_of_admitted_errors(self):
        task = self.rpc("submit", **TASK)
        dispatcher = Dispatch("", "tools.mailbox", "tide")

        async def route(method, fields):
            return self.rpc(method, **fields)

        async def agent(_socket, _service, method, _params):
            if method == "task_receipt":
                return {"status": "started"}
            raise RequestError(
                {"code": "unavailable", "message": "Tool stopped"}, generation=3
            )

        with (
            patch.object(dispatcher, "online", AsyncMock(return_value=True)),
            patch.object(dispatcher, "rpc", side_effect=route),
            patch("glassmoon.dispatcher.call", side_effect=agent),
            patch("glassmoon.dispatcher.asyncio.sleep", AsyncMock()),
        ):
            result = await dispatcher.deliver()
        self.assertEqual(result["status"], "review")
        self.assertEqual(self.rpc("get", id=task["id"])["attempts"], 1)

    async def test_dispatcher_recovers_completed_receipt_before_expiry(self):
        task = self.rpc("submit", **TASK)
        dispatcher = Dispatch("", "tools.mailbox", "tide")
        methods = []

        async def route(method, fields):
            return self.rpc(method, **fields)

        async def agent(_socket, _service, method, _params):
            methods.append(method)
            if method == "run_task":
                raise EOFError
            return {"status": "done", "reply": {"message": {"content": "Saved once"}}}

        with (
            patch.object(dispatcher, "online", AsyncMock(return_value=True)),
            patch.object(dispatcher, "rpc", side_effect=route),
            patch("glassmoon.dispatcher.call", side_effect=agent),
        ):
            done = await dispatcher.deliver()
        self.assertEqual(methods, ["run_task", "task_receipt"])
        self.assertEqual(
            (done["id"], done["status"], done["result"]),
            (task["id"], "done", "Saved once"),
        )
