"""Deterministic, local-only handoff receipt tests."""

import concurrent.futures
import tempfile
import threading
import unittest
from pathlib import Path

from examples.handoff_fixture import HandoffFixture


class HandoffFixtureTests(unittest.TestCase):
    def setUp(self):
        # Keep all disposable artifacts inside the workspace.
        self.temp = tempfile.TemporaryDirectory(prefix=".handoff-test-", dir=Path.cwd())
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "notes.sqlite"

    def test_success_replay_and_conflict(self):
        with HandoffFixture(self.path) as fixture:
            first = fixture.apply("task-1/attempt-1", "reviewed locally")
            self.assertEqual(first["status"], "success")
            self.assertFalse(first["replayed"])
            replay = fixture.apply("task-1/attempt-1", "reviewed locally")
            self.assertEqual(replay["receipt"], first["receipt"])
            self.assertTrue(replay["replayed"])
            conflict = fixture.apply("task-1/attempt-1", "different note")
            self.assertEqual(conflict["status"], "conflict")
            self.assertEqual(conflict["receipt"], first["receipt"])
            self.assertEqual(fixture.count(), 1)

    def test_receipt_survives_reopening_and_lost_ack(self):
        with HandoffFixture(self.path) as fixture:
            self.assertIsNone(fixture.lookup("lost-ack"))
            original = fixture.apply("lost-ack", "committed before acknowledgment")
        # Discard caller state, as if acknowledgment was lost after commit.
        with HandoffFixture(self.path) as fixture:
            self.assertEqual(fixture.lookup("lost-ack"), original["receipt"])
            self.assertEqual(
                fixture.apply("lost-ack", "committed before acknowledgment"),
                dict(original, replayed=True),
            )
            self.assertEqual(fixture.apply("lost-ack", "changed")["status"], "conflict")
            self.assertEqual(fixture.count(), 1)

    def test_concurrent_workers_store_only_once(self):
        with HandoffFixture(self.path):
            pass
        barrier = threading.Barrier(2)

        def worker():
            with HandoffFixture(self.path) as fixture:
                barrier.wait(timeout=3)
                return fixture.apply("shared", "same note")

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(worker) for _ in range(2)]
            results = [f.result(timeout=8) for f in futures]
        self.assertEqual(sorted(r["replayed"] for r in results), [False, True])
        self.assertEqual(results[0]["receipt"], results[1]["receipt"])
        with HandoffFixture(self.path) as fixture:
            self.assertEqual(fixture.count(), 1)

    def test_validation_and_unicode_byte_bounds(self):
        with HandoffFixture(self.path) as fixture:
            for key, note in [
                ("", "x"),
                ("k" * 129, "x"),
                ("k", "é" * 1025),
                ("k", None),
            ]:
                with self.assertRaises(ValueError):
                    fixture.apply(key, note)
            self.assertEqual(fixture.count(), 0)
            self.assertEqual(fixture.apply("é" * 64, "é" * 1024)["status"], "success")

    def test_distinct_keys_have_distinct_receipts(self):
        with HandoffFixture(self.path) as fixture:
            a = fixture.apply("a", "same")["receipt"]
            b = fixture.apply("b", "same")["receipt"]
            self.assertNotEqual(a["receipt_id"], b["receipt_id"])
            self.assertEqual(fixture.count(), 2)


if __name__ == "__main__":
    unittest.main()
