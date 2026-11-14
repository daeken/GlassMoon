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

    async def test_dispatcher_includes_parent_and_linked_evidence_in_delivery(self):
        parent = self.rpc("submit", **(TASK | {"agent": "moon", "key": "parent"}))
        child = self.rpc("submit", **TASK, parent=parent["id"])
        self.rpc("attach_evidence", id=child["id"], owner="moon", key="original-screenshot")
        dispatcher = Dispatch("", "tools.mailbox", "tide")

        async def route(method, fields):
            return self.rpc(method, **fields)

        reply = AsyncMock(return_value={"message": {"role": "assistant", "content": "Ready"}})
        with patch.object(dispatcher, "rpc", side_effect=route), patch("glassmoon.dispatcher.call", reply):
            outcome = await dispatcher.deliver(check_online=False)
        self.assertEqual(outcome["status"], "done")
        prompt = reply.await_args.args[3]["text"]
        self.assertIn(parent["id"], prompt)
        self.assertIn("moon/original-screenshot", prompt)

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

    async def test_dispatcher_can_reconcile_if_finish_arrives_after_expiry(self):
        task = self.rpc("submit", **TASK)
        dispatcher = Dispatch("", "tools.mailbox", "tide")
        reply = {"message": {"role": "assistant", "content": "Saved once"}}

        async def agent(*_args, **_kwargs):
            self.now += 31
            return reply

        async def route(method, fields):
            if method == "reconcile":
                worker = MailboxService(self.box, "socket")
                with patch("glassmoon.mailbox.call", AsyncMock(return_value={"status": "done", "reply": reply})):
                    return await worker.reconcile(fields["id"], fields["attempt"])
            return self.rpc(method, **fields)

        generation = AsyncMock(side_effect=agent)
        with (
            patch.object(dispatcher, "online", AsyncMock(return_value=True)),
            patch.object(dispatcher, "rpc", side_effect=route),
            patch("glassmoon.dispatcher.call", generation),
        ):
            result = await dispatcher.deliver()
        self.assertEqual((result["id"], result["status"], result["attempts"]), (task["id"], "done", 1))
        generation.assert_awaited_once()
        worker = MailboxService(self.box, "socket")
        with self.assertRaises(RequestError) as mismatch:
            await worker.reconcile(task["id"], 2)
        self.assertEqual(mismatch.exception.code, "attempt_changed")

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
        with patch("glassmoon.mailbox.call", AsyncMock(return_value={"status": "done", "reply": reply})):
            self.assertEqual(await worker.reconcile(task["id"]), row)

    async def test_dispatcher_recovers_late_partial_receipt_without_resolving_review(self):
        task = self.rpc("submit", **TASK)
        dispatcher = Dispatch("", "tools.mailbox", "tide")
        reply = {"message": {"content": "Check whether the note was saved"}, "outcome": "review"}

        async def agent(*_args, **_options):
            self.now += 31
            return reply

        async def route(method, fields):
            if method == "reconcile":
                worker = MailboxService(self.box, "socket")
                with patch("glassmoon.mailbox.call", AsyncMock(return_value={"status": "done", "reply": reply})):
                    recovered = await worker.reconcile(fields["id"], fields["attempt"])
                    self.assertEqual(await worker.reconcile(fields["id"], fields["attempt"]), recovered)
                    return recovered
            return self.rpc(method, **fields)

        with (
            patch.object(dispatcher, "online", AsyncMock(return_value=True)),
            patch.object(dispatcher, "rpc", side_effect=route),
            patch("glassmoon.dispatcher.call", AsyncMock(side_effect=agent)) as run_task,
        ):
            recovered = await dispatcher.deliver()
        run_task.assert_awaited_once()
        self.assertEqual((recovered["status"], recovered["attempts"]), ("review", 1))
        self.assertEqual(recovered["result"], reply["message"]["content"])
        timeline = self.rpc("timeline", id=task["id"])["events"]
        self.assertEqual([event["kind"] for event in timeline[:2]], ["partial_receipt_recovered", "lease_expired"])

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

    def test_task_rows_distinguish_agent_reply_recovery_and_operator_verification(self):
        expected = {}
        for key in ("reply", "recovered", "verified"):
            task = self.rpc("submit", **(TASK | {"key": key}))
            lease = self.rpc("claim", agent="tide", lease_seconds=1)
            if key == "reply":
                done = self.rpc("finish", id=task["id"], token=lease["token"], status="done", result="Reply")
                expected[key] = "agent_reply"
            else:
                self.now += 2
                review = self.rpc("get", id=task["id"])
                self.assertIsNone(review["completion_source"])
                if key == "recovered":
                    done = self.box.settle_receipt(task["id"], review["revision"], 1, "Recovered reply")
                    expected[key] = "recovered_reply"
                else:
                    done = self.rpc("review", id=task["id"], action="verified", note="Checked effect", expected_revision=review["revision"])
                    expected[key] = "operator_verified"
            self.assertEqual(done["completion_source"], expected[key])
        listing = self.rpc("list_tasks", status="done")["tasks"]
        self.assertEqual({row["key"]: row["completion_source"] for row in listing}, expected)

    def test_handoff_reports_expired_and_active_work_with_durable_activity(self):
        interrupted = self.rpc("submit", **TASK, recovery_check="Read the fixture receipt")
        self.rpc("claim", agent="tide", lease_seconds=1)
        active = self.rpc("submit", **(TASK | {"agent": "moon", "key": "active"}))
        claimed = self.rpc("claim", agent="moon", lease_seconds=20)
        self.now += 3
        self.rpc("progress", id=active["id"], token=claimed["token"], index=1, name="fixture", phase="started")
        report = self.rpc("handoff", limit=2)
        review, running = report["tasks"]
        self.assertEqual((review["id"], review["status"], review["has_recovery_check"]), (interrupted["id"], "review", 1))
        self.assertEqual((running["id"], running["status"], running["last_activity"]), (active["id"], "running", 103))
        self.assertEqual((running["last_tool"], running["last_tool_phase"]), ("fixture", "started"))
        self.assertEqual(running["lease_until"], 120)
        self.assertFalse(report["more"])
        self.assertTrue(self.rpc("handoff", limit=1)["more"])

    def test_handoff_includes_a_bounded_excerpt_for_completed_work(self):
        evidence = [{"owner": "moon", "key": key} for key in ("one", "two", "three")]
        task = self.rpc("submit", **TASK, evidence=evidence)
        lease = self.rpc("claim", agent="tide", lease_seconds=30)
        result = "Start of the report. " + "detail " * 50
        self.rpc("finish", id=task["id"], token=lease["token"], status="done", result=result)
        row = self.rpc("handoff")["tasks"][0]
        self.assertEqual(row["result_excerpt"], result[:240])
        self.assertTrue(row["result_excerpt_truncated"])
        self.assertTrue(row["more_evidence"])
        self.assertEqual([ref["key"] for ref in row["evidence"]], ["one", "three"])
        self.assertEqual(self.rpc("get", id=row["id"])["result"], result)

    def test_handoff_surfaces_saved_partial_reply_in_review(self):
        task = self.rpc("submit", **TASK)
        lease = self.rpc("claim", agent="tide", lease_seconds=30)
        reply = "Check whether the click produced a saved note."
        self.rpc("finish", id=task["id"], token=lease["token"], status="review", result=reply)
        row = self.rpc("handoff")["tasks"][0]
        self.assertEqual((row["status"], row["result_excerpt"]), ("review", reply))
        self.assertFalse(row["result_excerpt_truncated"])

    def test_get_keeps_escape_heavy_results_within_a_reply(self):
        values = TASK | {"task": "A" + "\0" * 3999, "recovery_check": "A" + "\0" * 1199}
        task = self.rpc("submit", **values)
        lease = self.rpc("claim", agent="tide", lease_seconds=5)
        for index in range(1, 73):
            self.rpc(
                "progress", id=task["id"], token=lease["token"], index=index,
                name="x" * 96, phase="finished",
            )
        result = "A" + "\0" * 11999
        reply = self.rpc("finish", id=task["id"], token=lease["token"], status="done", result=result)
        self.assertTrue(reply["result_truncated"])
        self.assertLessEqual(encoded_size(reply), 60 * 1024)
        self.assertEqual(len(reply["progress"]), 64)
        self.assertEqual(reply["progress"][0]["event_index"], 9)
        self.assertEqual(reply["progress"][-1]["event_index"], 72)
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

    async def test_terminal_task_detail_shows_assignee_and_current_lease(self):
        task = self.rpc("submit", **TASK)
        self.rpc("claim", agent="tide", lease_seconds=5)
        terminal = Terminal(None, "unused")
        with patch("glassmoon.tui.call_tool", AsyncMock(return_value=self.rpc("get", id=task["id"]))):
            await terminal.show_task(task["id"])
        lines = list(terminal.windows["status"])
        self.assertTrue(any("moon → tide" in line and "attempts 1" in line for line in lines))
        self.assertTrue(any("Current lease ends" in line for line in lines))

    async def test_terminal_watch_reports_changes_and_stops_at_review(self):
        states = [
            {"revision": 1, "attempts": 0, "status": "queued"},
            {"revision": 1, "attempts": 0, "status": "queued"},
            {"revision": 2, "attempts": 1, "status": "running", "progress": [
                {"event_index": 1, "name": "fixture", "phase": "started"},
            ]},
            {"revision": 3, "attempts": 1, "status": "review"},
        ]
        terminal = Terminal(None, "unused")
        with (
            patch("glassmoon.tui.call_tool", AsyncMock(side_effect=states)),
            patch("glassmoon.tui.asyncio.sleep", AsyncMock()),
        ):
            terminal.toggle_task_watch("fixture")
            await asyncio.gather(*terminal.tasks)
        lines = list(terminal.windows["status"])
        self.assertEqual(sum("queued" in line for line in lines), 1)
        self.assertTrue(any("fixture  started" in line for line in lines))
        self.assertTrue(any("review" in line for line in lines))
        self.assertEqual(terminal.watched_tasks, {})

    async def test_terminal_watch_recovers_after_mailbox_disconnect(self):
        running = {
            "revision": 2, "attempts": 1, "status": "running", "progress": [
                {"event_index": 1, "name": "fixture", "phase": "started"},
            ],
        }
        unavailable = RequestError({"code": "unavailable", "message": "No active mailbox"})
        disconnected = RequestError({"code": "service_disconnected", "message": "Mailbox retired"}, generation=5)
        responses = [running, disconnected, EOFError(), unavailable, running, running | {
            "revision": 3, "status": "done",
        }]
        terminal = Terminal(None, "unused")
        with (
            patch("glassmoon.tui.call_tool", AsyncMock(side_effect=responses)),
            patch("glassmoon.tui.asyncio.sleep", AsyncMock()) as sleep,
        ):
            await terminal.watch_task("fixture")
        self.assertEqual([item.args[0] for item in sleep.await_args_list], [1, 1, 2, 4, 1])
        lines = list(terminal.windows["status"])
        self.assertEqual(sum("waiting" in line for line in lines), 1)
        self.assertEqual(sum("reconnected" in line for line in lines), 1)
        self.assertEqual(sum("started" in line for line in lines), 1)
        self.assertTrue(any("done" in line for line in lines))

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

    async def test_requeue_note_reaches_agent_after_pre_admission_deferral(self):
        task = self.rpc("submit", **TASK)
        self.rpc("claim", agent="tide", lease_seconds=1)
        self.now += 2
        review = self.rpc("get", id=task["id"])
        note = "The note already exists. Check the receipt before doing anything else."
        self.rpc("review", id=task["id"], action="requeue", note=note, expected_revision=review["revision"])
        dispatcher = Dispatch("", "tools.mailbox", "tide")

        async def route(method, fields):
            return self.rpc(method, **fields)

        busy = RequestError({"code": "agent_busy", "message": "Agent already in a turn"})
        reply = {"message": {"content": "I confirmed the existing receipt"}}
        with (
            patch.object(dispatcher, "rpc", side_effect=route),
            patch("glassmoon.dispatcher.call", AsyncMock(side_effect=[busy, reply])) as run_task,
        ):
            deferred = await dispatcher.deliver(check_online=False)
            self.now = deferred["available_at"]
            done = await dispatcher.deliver(check_online=False)
        self.assertIn(note, run_task.await_args.args[3]["text"])
        self.assertEqual((done["status"], done["review_note"]), ("done", None))

    def test_linked_evidence_is_idempotent_and_contains_no_image(self):
        task = self.rpc("submit", **TASK)
        fields = {"id": task["id"], "owner": "moon", "key": "screenshot"}
        self.assertEqual(self.rpc("attach_evidence", **fields), fields)
        self.assertEqual(self.rpc("attach_evidence", **fields), fields)
        found = self.rpc("get", id=task["id"])
        self.assertEqual(found["evidence"], [{"owner": "moon", "key": "screenshot"}])
        self.assertEqual(found["revision"], task["revision"])
        events = self.rpc("timeline", id=task["id"])["events"]
        self.assertEqual([event["kind"] for event in events], ["evidence_attached", "submitted"])
        self.assertEqual(events[0]["name"], "moon/screenshot")

    def test_submitted_evidence_is_available_at_first_claim_and_part_of_dedupe(self):
        references = [{"owner": "moon", "key": "first"}, {"owner": "moon", "key": "second"}]
        task = self.rpc("submit", **TASK, evidence=references)
        self.assertEqual(task["evidence"], references)
        self.assertEqual(self.rpc("submit", **TASK, evidence=references[::-1]), task)
        rejected = TASK | {"key": "invalid"}
        with self.assertRaises(RequestError):
            self.rpc("submit", **rejected, evidence=[references[0], {"owner": ["bad"], "key": "second"}])
        self.assertEqual([row["id"] for row in self.rpc("list_tasks")["tasks"]], [task["id"]])
        lease = self.rpc("claim", agent="tide", lease_seconds=3)
        self.assertEqual(lease["task"]["evidence"], references)
        self.rpc("attach_evidence", id=task["id"], owner="moon", key="third", attempt=1)
        self.assertEqual(self.rpc("submit", **TASK, evidence=references)["id"], task["id"])
        with self.assertRaises(RequestError) as conflict:
            self.rpc("submit", **TASK, evidence=references[:1])
        self.assertEqual(conflict.exception.code, "key_conflict")

    def test_evidence_can_name_the_attempt_that_produced_it(self):
        task = self.rpc("submit", **TASK)
        self.rpc("claim", agent="tide", lease_seconds=1)
        self.now += 2
        review = self.rpc("get", id=task["id"])
        self.rpc("review", id=task["id"], action="requeue", note="Retry the fixture", expected_revision=review["revision"])
        self.rpc("claim", agent="tide", lease_seconds=1)
        fields = {"id": task["id"], "owner": "tide", "key": "before-retry"}
        attached = self.rpc("attach_evidence", **fields, attempt=1)
        self.assertEqual(self.rpc("attach_evidence", **fields), attached)
        with self.assertRaises(RequestError) as conflict:
            self.rpc("attach_evidence", **fields, attempt=2)
        self.assertEqual(conflict.exception.code, "evidence_conflict")
        self.box = Mailbox(self.path, clock=lambda: self.now)
        row = self.rpc("get", id=task["id"])
        self.assertEqual((row["attempts"], row["evidence"][0]["attempt"]), (2, 1))
        event = self.rpc("timeline", id=task["id"])["events"][0]
        self.assertEqual((event["kind"], event["attempt"]), ("evidence_attached", 1))

    async def test_terminal_reads_evidence_before_linking_a_task_attempt(self):
        terminal = Terminal(None, "ipc")
        evidence = {"owner": "moon", "key": "screenshot"}
        fields = evidence | {"id": "task", "attempt": 2}
        reader = AsyncMock(side_effect=[{"summary": "Fixture"}, fields])
        with patch("glassmoon.tui.call_tool", reader):
            await terminal.link_evidence("task moon screenshot 2")
        self.assertEqual([call.args for call in reader.await_args_list], [
            ("ipc", "tools.evidence", "read", evidence),
            ("ipc", "tools.mailbox", "attach_evidence", fields),
        ])
        self.assertIn("(attempt 2)", terminal.windows["status"][-1])
        rejected = AsyncMock(side_effect=RequestError({"code": "evidence_missing", "message": "Missing"}))
        with patch("glassmoon.tui.call_tool", rejected), self.assertRaises(RequestError):
            await terminal.link_evidence("task moon missing")
        rejected.assert_awaited_once()

    async def test_terminal_queues_published_evidence_with_initial_task(self):
        terminal = Terminal(None, "ipc")
        ref = {"owner": "moon", "key": "screenshot"}
        reader = AsyncMock(side_effect=[{"summary": "Fixture"}, {"id": "task"}])
        with patch("glassmoon.tui.call_tool", reader):
            await terminal.queue_evidence_task("agent.tide moon screenshot | Compare the observation")
        read, submit = [call.args for call in reader.await_args_list]
        self.assertEqual(read, ("ipc", "tools.evidence", "read", ref))
        self.assertEqual(submit[:3], ("ipc", "tools.mailbox", "submit"))
        self.assertEqual(submit[3]["agent"], "tide")
        self.assertEqual(submit[3]["evidence"], [ref])
        self.assertEqual(submit[3]["task"], "Compare the observation")
