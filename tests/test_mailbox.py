import asyncio
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from threading import Barrier
from unittest.mock import AsyncMock, patch

from examples.handoff_fixture import HandoffFixture
from glassmoon.agent import Agent
from glassmoon.dispatcher import Dispatch
from glassmoon.ipc import RequestError
from glassmoon.mailbox import Mailbox, MailboxService
from glassmoon.models import Transcripts, encoded_size
from glassmoon.tui import Terminal

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
        check = "Look up the fixture receipt by effect key."
        task = self.rpc("submit", **TASK, recovery_check=check)
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
            self.assertIn(check, reply.await_args.args[3]["text"])
            self.assertIsNone(await dispatcher.deliver())
        reply.assert_awaited_once()

    async def test_cancelled_delivery_requires_review(self):
        task = self.rpc("submit", **TASK)
        dispatcher = Dispatch("", "tools.mailbox", "tide")
        started = asyncio.Event()

        async def route(method, fields):
            return self.rpc(method, **fields)

        async def waiting(*_, **_options):
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

        async def agent(_socket, _service, method, _params, **_options):
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

        async def agent(_socket, _service, method, _params, **_options):
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

    async def test_saved_partial_reply_keeps_task_in_review(self):
        task = self.rpc("submit", **TASK)
        dispatcher = Dispatch("", "tools.mailbox", "tide")
        reply = {"message": {"content": "Tool effects need review"}, "outcome": "review"}

        async def route(method, fields):
            return self.rpc(method, **fields)

        with (
            patch.object(dispatcher, "online", AsyncMock(return_value=True)),
            patch.object(dispatcher, "rpc", side_effect=route),
            patch("glassmoon.dispatcher.call", AsyncMock(return_value=reply)),
        ):
            row = await dispatcher.deliver()
        self.assertEqual((row["status"], row["result"]), ("review", "Tool effects need review"))
        worker = MailboxService(self.box, "socket")
        with (
            patch("glassmoon.mailbox.call", AsyncMock(return_value={"status": "done", "reply": reply})),
            self.assertRaises(RequestError) as rejected,
        ):
            await worker.reconcile(task["id"])
        self.assertEqual(rejected.exception.code, "receipt_needs_review")

    async def test_tool_progress_records_metadata_and_survives_review(self):
        task = self.rpc("submit", **TASK)
        dispatcher = Dispatch("", "tools.mailbox", "tide")

        async def route(method, fields):
            return self.rpc(method, **fields)

        async def agent(_socket, _service, method, _params, **options):
            if method == "task_receipt":
                return {"status": "started"}
            await options["on_chunk"](
                {
                    "type": "tool",
                    "phase": "started",
                    "name": "save_note",
                    "arguments": "synthetic-secret-payload",
                }
            )
            raise EOFError

        with (
            patch.object(dispatcher, "online", AsyncMock(return_value=True)),
            patch.object(dispatcher, "rpc", side_effect=route),
            patch("glassmoon.dispatcher.call", side_effect=agent),
        ):
            result = await dispatcher.deliver()
        self.assertEqual(result["status"], "review")
        reopened = Mailbox(self.path, clock=lambda: self.now).request(
            "get", {"id": task["id"]}
        )
        self.assertEqual(
            reopened["progress"],
            [
                {
                    "event_index": 1,
                    "name": "save_note",
                    "phase": "started",
                    "observed_at": 100.0,
                },
            ],
        )

    def test_verified_external_effect_closes_review_without_replay(self):
        task = self.rpc("submit", **TASK)
        self.rpc("claim", agent="tide", lease_seconds=5)
        fixture_path = self.path.with_name("fixture.sqlite")
        with HandoffFixture(fixture_path) as fixture:
            expected = fixture.apply(task["id"], "Saved before losing the reply")[
                "receipt"
            ]
        self.now += 6
        review = self.rpc("get", id=task["id"])
        self.assertEqual(review["status"], "review")
        with HandoffFixture(fixture_path) as fixture:
            evidence = fixture.lookup(task["id"])
            self.assertEqual(evidence, expected)
            self.assertEqual(fixture.count(), 1)
        result = self.rpc(
            "review",
            id=task["id"],
            expected_revision=review["revision"],
            action="verified",
            note=f"Confirmed fixture receipt {evidence['receipt_id']}",
        )
        self.assertEqual((result["status"], result["attempts"]), ("done", 1))
        self.assertEqual(result["result"], "Confirmed fixture receipt 1")
        self.assertIsNone(self.rpc("claim", agent="tide", lease_seconds=5)["task"])

    def test_recovery_check_survives_migration_and_expiry(self):
        legacy = self.rpc("submit", **TASK)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("ALTER TABLE tasks DROP COLUMN recovery_check")
        self.box = Mailbox(self.path, clock=lambda: self.now)
        self.assertEqual(self.rpc("submit", **TASK)["id"], legacy["id"])
        self.assertIsNone(self.rpc("get", id=legacy["id"])["recovery_check"])
        self.rpc("claim", agent="tide", lease_seconds=2)

        args = TASK | {"key": "note", "recovery_check": "Look up effect key note."}
        task = self.rpc("submit", **args)
        self.assertEqual(self.rpc("submit", **args)["id"], task["id"])
        with self.assertRaises(RequestError) as conflict:
            self.rpc("submit", **(args | {"recovery_check": "Altered check."}))
        self.assertEqual(conflict.exception.code, "key_conflict")
        self.rpc("claim", agent="tide", lease_seconds=5)
        self.now += 6
        self.box = Mailbox(self.path, clock=lambda: self.now)
        expired = self.rpc("get", id=task["id"])
        self.assertEqual(expired["status"], "review")
        self.assertEqual(expired["recovery_check"], args["recovery_check"])

    def test_timeline_pages_attempts_and_records_verification_distinctly(self):
        task = self.rpc("submit", **TASK)
        identifier = task["id"]
        self.rpc("submit", **TASK)
        lease = self.rpc("claim", agent="tide", lease_seconds=1)
        for index in range(1, 21):
            event = {
                "id": identifier, "token": lease["token"], "index": index,
                "name": "save_note", "phase": "failed" if index == 20 else "started",
            }
            self.rpc("progress", **event)
            self.rpc("progress", **event)
        self.now += 2
        review = self.rpc("get", id=identifier)
        self.rpc(
            "review", id=identifier, expected_revision=review["revision"],
            action="requeue", note="Confirmed fixture is ready for a repeat attempt",
        )
        self.rpc("claim", agent="tide", lease_seconds=1)
        self.now += 2
        review = self.rpc("get", id=identifier)
        self.rpc(
            "review", id=identifier, expected_revision=review["revision"],
            action="verified", note="Confirmed local receipt exactly once",
        )
        self.box = Mailbox(self.path, clock=lambda: self.now)
        newer = self.rpc("timeline", id=identifier)
        older = self.rpc("timeline", id=identifier, before=newer["before"])
        self.assertIsNone(older["before"])
        events = list(reversed(newer["events"] + older["events"]))
        expected = (
            [(0, "submitted"), (1, "claimed")] + [(1, "tool")] * 20
            + [(1, "lease_expired"), (1, "requeued"), (2, "claimed"),
               (2, "lease_expired"), (2, "verified")]
        )
        self.assertEqual([(event["attempt"], event["kind"]) for event in events], expected)
        self.assertEqual(events[21]["phase"], "failed")
        self.assertNotIn("result", events[-1])

    def test_get_keeps_escape_heavy_results_within_a_reply(self):
        values = TASK | {"task": "A" + "\0" * 3999, "recovery_check": "A" + "\0" * 1199}
        task = self.rpc("submit", **values)
        lease = self.rpc("claim", agent="tide", lease_seconds=5)
        for index in range(1, 65):
            self.rpc(
                "progress", id=task["id"], token=lease["token"], index=index,
                name="x" * 96, phase="finished",
            )
        result = "A" + "\0" * 11999
        reply = self.rpc("finish", id=task["id"], token=lease["token"], status="done", result=result)
        self.assertTrue(reply["result_truncated"])
        self.assertLessEqual(encoded_size(reply), 60 * 1024)
        self.assertEqual(len(reply["progress"]), 64)
        self.assertTrue(result.startswith(reply["result"]))
        self.assertEqual(self.rpc("get", id=task["id"]), reply)
        self.assertEqual(self.rpc("submit", **values), reply)

    def test_delegated_tasks_link_to_a_real_parent(self):
        parent = self.rpc("submit", **TASK)
        self.assertEqual(self.rpc("submit", **TASK, parent="", recovery_check=" "), parent)
        other = self.rpc("submit", **(TASK | {"key": "other"}))
        args = TASK | {"sender": "tide", "agent": "finch", "key": "child"}
        with self.assertRaises(RequestError) as rejected:
            self.rpc("submit", **args, parent="missing")
        self.assertEqual(rejected.exception.code, "unknown_parent")
        child = self.rpc("submit", **args, parent=parent["id"])
        self.assertEqual(child["parent"], parent["id"])
        self.assertEqual(self.rpc("submit", **args, parent=parent["id"]), child)
        with self.assertRaises(RequestError) as conflict:
            self.rpc("submit", **args, parent=other["id"])
        self.assertEqual(conflict.exception.code, "key_conflict")
        self.box = Mailbox(self.path, clock=lambda: self.now)
        self.assertEqual(
            [row["id"] for row in self.rpc("list_tasks", parent=parent["id"])["tasks"]],
            [child["id"]],
        )

    async def test_terminal_paginates_the_same_filtered_review_queue(self):
        parent = self.rpc("submit", **TASK)
        ids = []
        for index in range(23):
            args = TASK | {"key": f"review-{index}", "agent": "finch"}
            task = self.rpc("submit", **args, parent=parent["id"])
            ids.append(task["id"])
            self.rpc("claim", agent="finch", lease_seconds=1)
        self.now += 2
        self.rpc("submit", **(TASK | {"key": "still-queued"}), parent=parent["id"])

        async def route(_socket, _service, method, fields):
            return self.rpc(method, **fields)

        terminal = Terminal(None, "unused")
        with patch("glassmoon.tui.call_tool", side_effect=route):
            await terminal.review_queue(parent["id"])
            await asyncio.gather(terminal.more_tasks(), terminal.more_tasks())
        observed = [line.split()[0] for line in terminal.windows["status"] if "outcome unknown" in line]
        self.assertEqual(observed, list(reversed(ids)))

    def test_cancel_unclaimed_task_without_disrupting_an_admitted_worker(self):
        task = self.rpc("submit", **TASK)
        options = {
            "id": task["id"], "expected_revision": task["revision"],
            "action": "cancel", "note": "No longer needed",
        }
        result = self.rpc("review", **options)
        self.assertEqual((result["status"], result["attempts"]), ("cancelled", 0))
        self.assertIsNone(self.rpc("claim", agent="tide", lease_seconds=10)["task"])
        task = self.rpc("submit", **(TASK | {"key": "next"}))
        running = self.rpc("claim", agent="tide", lease_seconds=10)["task"]
        with self.assertRaises(RequestError) as blocked:
            self.rpc("review", **(options | {"id": task["id"], "expected_revision": running["revision"]}))
        self.assertEqual(blocked.exception.code, "revision_conflict")
        self.assertEqual(self.rpc("get", id=task["id"])["status"], "running")

    def test_deferred_delivery_yields_to_other_ready_work(self):
        task = self.rpc("submit", **TASK)
        unrelated = self.rpc("submit", **(TASK | {"key": "other-ready"}))
        first = self.rpc("claim", agent="tide", lease_seconds=5)
        deferred = self.rpc("finish", id=task["id"], token=first["token"], status="queued", result="Agent busy")
        self.assertGreater(deferred["available_at"], self.now)
        second = self.rpc("claim", agent="tide", lease_seconds=5)
        self.assertEqual(second["task"]["id"], unrelated["id"])
        self.assertIsNone(self.rpc("claim", agent="tide", lease_seconds=5)["task"])
        self.now = deferred["available_at"]
        again = self.rpc("claim", agent="tide", lease_seconds=5)
        self.assertEqual((again["task"]["id"], again["task"]["attempts"]), (task["id"], 2))
