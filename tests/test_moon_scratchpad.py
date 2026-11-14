"""Focused scratchpad tests; all persistence stays in a disposable repo directory."""

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from examples import moon_scratchpad as pad
from glassmoon.toolbox import load


class ScratchpadTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(
            prefix=".scratchpad-test-", dir=Path.cwd()
        )
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / "notes.db")
        self.env = patch.dict(os.environ, {"GLASSMOON_SCRATCHPAD_PATH": self.path})
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_create_update_conflict_and_missing(self):
        self.assertFalse(pad.get_note("missing")["found"])
        self.assertEqual(pad.put_note("task", "first", 0)["revision"], 1)
        self.assertEqual(pad.put_note("task", "lost", 0)["error"], "revision_conflict")
        self.assertEqual(pad.get_note("task")["text"], "first")
        self.assertEqual(pad.put_note("task", "second", 1)["revision"], 2)

    def test_fresh_worker_process_reads_persistence(self):
        pad.put_note("decision", "Keep Unicode: é", 0)
        code = "from examples.moon_scratchpad import get_note, put_note; import json; print(json.dumps(get_note('decision'))); print(json.dumps(put_note('decision', 'updated', 1)))"
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
        read, write = map(json.loads, result.stdout.splitlines())
        self.assertEqual(read["text"], "Keep Unicode: é")
        self.assertEqual(write["revision"], 2)
        self.assertEqual(pad.get_note("decision")["text"], "updated")

    def test_concurrent_cas(self):
        pad.put_note("race", "original", 0)
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(
                pool.map(lambda i: pad.put_note("race", str(i), 1), range(4))
            )
        self.assertEqual(sum(r["ok"] for r in results), 1)
        self.assertEqual(pad.get_note("race")["revision"], 2)

    def test_listing_pages(self):
        for key in ["c", "a", "b"]:
            pad.put_note(key, "data", 0)
        first = pad.list_notes(limit=2)
        self.assertEqual([n["key"] for n in first["notes"]], ["a", "b"])
        last = pad.list_notes(after=first["next_after"], limit=2)
        self.assertEqual(
            last, {"notes": [{"key": "c", "revision": 1}], "next_after": None}
        )
        self.assertEqual(pad.list_notes(after="z")["notes"], [])

    def test_validation_and_utf8_boundaries(self):
        pad.put_note("unicode", "é" * 1024, 0)
        for action in [
            lambda: pad.put_note("a", "é" * 1025, 0),
            lambda: pad.put_note("a", 1, 0),
            lambda: pad.put_note("a", "x", True),
            lambda: pad.put_note("a", "x", -1),
            lambda: pad.get_note("../escape"),
            lambda: pad.list_notes(limit=True),
            lambda: pad.list_notes(limit=101),
            lambda: pad.list_notes(after="bad/"),
            lambda: pad.put_note("a", "\ud800", 0),
        ]:
            with self.assertRaises((ValueError, TypeError)):
                action()
        for location in ["", "relative.db"]:
            with (
                patch.dict(os.environ, {"GLASSMOON_SCRATCHPAD_PATH": location}),
                self.assertRaises(ValueError),
            ):
                pad.get_note("a")

    def test_toolbox_reload_and_dispatch(self):
        async def exercise():
            module = Path("examples/moon_scratchpad.py")
            first = load(module)
            declarations = await first.handle("tools", {}, None, False)
            self.assertEqual(len(declarations["tools"]), 3)
            await first.handle(
                "invoke",
                {
                    "name": "put_note",
                    "arguments": {
                        "key": "handoff",
                        "text": "durable",
                        "expected_revision": 0,
                    },
                },
                None,
                False,
            )
            replacement = load(module)
            reply = await replacement.handle(
                "invoke",
                {"name": "get_note", "arguments": {"key": "handoff"}},
                None,
                False,
            )
            self.assertEqual(reply["value"]["text"], "durable")

        asyncio.run(exercise())


if __name__ == "__main__":
    unittest.main()
