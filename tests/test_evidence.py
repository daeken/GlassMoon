import base64
import hashlib
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from glassmoon.evidence import Evidence
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
