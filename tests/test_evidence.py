import base64
import hashlib
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import AsyncMock, patch

from glassmoon.evidence import Evidence, export_image
from glassmoon.ipc import RequestError


def jpeg(payload):
    return {
        "type": "image",
        "url": "data:image/jpeg;base64," + base64.b64encode(payload).decode(),
    }


class EvidenceTests(unittest.TestCase):
    def test_image_stays_out_of_summary_and_integrity_is_checked(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "visual.sqlite3"
            ref = {"owner": "moon", "key": "review"}
            publish = ref | {
                "summary": "Saved is visible.",
                "metadata": {"engine": "webkit"},
            }
            content = b"\xff\xd8fixture"
            worker = Evidence(path)
            saved = worker.request("publish", publish | {"image": jpeg(content)})
            self.assertEqual(saved["image_sha256"], hashlib.sha256(content).hexdigest())
            self.assertNotIn("image", worker.request("read", ref))
            self.assertEqual(
                worker.request("list", {"owner": "moon"}),
                {"items": [{"key": "review", "image_sha256": saved["image_sha256"]}], "next": None},
            )
            self.assertEqual(
                Evidence(path).request("image", ref)["image"], jpeg(content)
            )
            self.assertEqual(
                worker.request("publish", publish | {"image": jpeg(content)}), saved
            )
            with self.assertRaises(RequestError) as conflict:
                worker.request("publish", publish | {"image": jpeg(b"\xff\xd8other")})
            self.assertEqual(conflict.exception.code, "evidence_conflict")

            with closing(sqlite3.connect(path)) as db, db:
                db.execute(
                    "UPDATE evidence_image SET url=?",
                    (jpeg(b"\xff\xd8corrupt")["url"],),
                )
            with self.assertRaises(RequestError) as integrity:
                worker.request("image", ref)
            self.assertEqual(integrity.exception.code, "image_integrity")

    def test_catalog_paginates_within_owner(self):
        with tempfile.TemporaryDirectory() as folder:
            worker = Evidence(Path(folder) / "visual.sqlite")
            for index in range(25):
                worker.request("publish", {"owner": "moon", "key": f"item-{index:02}", "summary": "fixture"})
            worker.request("publish", {"owner": "tide", "key": "item-00", "summary": "other owner"})
            first = worker.request("list", {"owner": "moon"})
            second = worker.request("list", {"owner": "moon", "after": first["next"]})
            expected = [{"key": f"item-{index:02}", "image_sha256": None} for index in range(25)]
            self.assertEqual(first["items"] + second["items"], expected)
            self.assertIsNone(second["next"])


class ExportTests(unittest.IsolatedAsyncioTestCase):
    async def test_export_verifies_bytes_and_refuses_to_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence.jpg"
            raw = b"\xff\xd8fixture"
            response = {"image": jpeg(raw), "image_sha256": hashlib.sha256(raw).hexdigest()}
            with patch("glassmoon.evidence.call_tool", AsyncMock(return_value=response)):
                self.assertEqual(await export_image("", "tools.evidence", "moon", "key", output), output)
                self.assertEqual(output.read_bytes(), raw)
                self.assertEqual(output.stat().st_mode & 0o777, 0o600)
                with self.assertRaises(FileExistsError):
                    await export_image("", "tools.evidence", "moon", "key", output)
                self.assertEqual(output.read_bytes(), raw)
                response["image_sha256"] = "wrong"
                suspect = output.with_name("other.jpg")
                with self.assertRaises(RequestError) as integrity:
                    await export_image("", "tools.evidence", "moon", "key", suspect)
                self.assertEqual(integrity.exception.code, "image_integrity")
                self.assertFalse(suspect.exists())
