"""Regression tests for durable conversation checkpoint ranges."""

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from glassmoon.compaction import compact, prefix
from glassmoon.ipc import RequestError
from glassmoon.models import Transcripts


def turn(index):
    return [
        {"role": "user", "content": f"User {index}: remember this exact wording."},
        {"role": "assistant", "content": f"Assistant {index}: recorded."},
    ]


class CompactionProvenanceTests(unittest.IsolatedAsyncioTestCase):
    async def test_repeated_compaction_preserves_sources_and_recent_messages(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "agent.db")
            transcript = [message for index in range(6) for message in turn(index)]
            requests = []

            async def generate(messages):
                requests.append(messages)
                return {"message": {"role": "assistant", "content": f"Checkpoint {len(requests)}"}}

            with (
                closing(Transcripts(path)) as store,
                patch.dict("os.environ", {
                    "GLASSMOON_COMPACTION_TRIGGER_BYTES": "1",
                    "GLASSMOON_COMPACTION_RETAIN_TURNS": "2",
                }),
            ):
                store.create(identifier="moon")
                store.append("moon", 0, *transcript[:8])
                end, _, tail = await compact(store, "moon", *store.context("moon"), generate)
                self.assertEqual((end, tail), (4, transcript[4:8]))
                store.append("moon", 8, *transcript[8:])
                end, _, tail = await compact(store, "moon", *store.context("moon"), generate)
                self.assertEqual((end, tail), (8, transcript[8:]))
                self.assertEqual(requests[0][1:-1], transcript[:4])
                self.assertIn("Checkpoint 1", requests[1][1]["content"])
                self.assertEqual(requests[1][2:-1], transcript[4:8])
                with self.assertRaises(RequestError) as conflict:
                    store.compact("moon", 12, 4, 10, "Stale")
                self.assertEqual(conflict.exception.code, "conversation_changed")

            with closing(Transcripts(path)) as store:
                self.assertEqual(store.history("moon")["messages"], transcript)
                self.assertEqual(store.memory("moon"), {
                    "revision": 12, "through_revision": 8, "summary": "Checkpoint 2",
                    "checkpoints": [
                        {"source_from": 5, "through_revision": 8, "snapshot_revision": 12},
                        {"source_from": 1, "through_revision": 4, "snapshot_revision": 8},
                    ],
                })
                self.assertEqual(store.db.execute(
                    "SELECT summary FROM compaction_checkpoints ORDER BY through_revision"
                ).fetchall(), [("Checkpoint 1",), ("Checkpoint 2",)])

    def test_legacy_summary_imports_once_without_inventing_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "legacy.db")
            with closing(sqlite3.connect(path)) as db:
                db.executescript("""
                    CREATE TABLE conversations (id TEXT PRIMARY KEY, model TEXT, revision INTEGER NOT NULL);
                    CREATE TABLE compactions (conversation TEXT PRIMARY KEY, through_revision INTEGER NOT NULL, summary TEXT NOT NULL);
                    INSERT INTO conversations VALUES ('moon', NULL, 4);
                    INSERT INTO compactions VALUES ('moon', 2, 'Legacy summary');
                """)
            for _ in range(2):
                with closing(Transcripts(path)) as store:
                    state = store.memory("moon")
                    self.assertEqual(state["summary"], "Legacy summary")
                    self.assertEqual(state["checkpoints"], [
                        {"source_from": 1, "through_revision": 2, "snapshot_revision": None},
                    ])

    def test_forced_checkpoint_respects_configured_tail_and_complete_turns(self):
        transcript = [message for index in range(4) for message in turn(index)]
        with patch.dict("os.environ", {
            "GLASSMOON_COMPACTION_TRIGGER_BYTES": "26000",
            "GLASSMOON_COMPACTION_RETAIN_TURNS": "1",
        }):
            self.assertEqual(prefix(transcript, ""), [])
            self.assertEqual(prefix(transcript + [turn(4)[0]], "", force=True), transcript[:6])
            self.assertEqual(prefix([transcript[0], transcript[0], *transcript[2:]], "", force=True), [])
