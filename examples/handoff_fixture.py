"""Local-only, durable side-effect receipts for handoff/review experiments.

A successful apply stores a note and its receipt in the same SQLite transaction.
Replaying an identical request returns the receipt without another side effect.
An interrupted caller can reopen the database and look up the key before retrying.
This fixture makes no claims about atomicity of external side effects.
"""

import sqlite3


class HandoffFixture:
    """A standalone SQLite fixture; use one instance per concurrent worker."""

    def __init__(self, path):
        self._db = sqlite3.connect(path, timeout=5, isolation_level=None)
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("""CREATE TABLE IF NOT EXISTS notes (
            receipt_id INTEGER PRIMARY KEY,
            idempotency_key TEXT NOT NULL UNIQUE,
            note TEXT NOT NULL
        )""")

    def close(self):
        self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @staticmethod
    def _validate(key, note):
        if not isinstance(key, str) or not key or len(key.encode("utf-8")) > 128:
            raise ValueError("key must contain 1..128 UTF-8 bytes")
        if not isinstance(note, str) or len(note.encode("utf-8")) > 2048:
            raise ValueError("note must be a string of at most 2048 UTF-8 bytes")

    @staticmethod
    def _receipt(row):
        return {"receipt_id": row[0], "key": row[1], "note": row[2]}

    def lookup(self, key):
        """Return the committed receipt, or None (no committed local effect)."""
        self._validate(key, "")
        row = self._db.execute(
            "SELECT receipt_id, idempotency_key, note FROM notes WHERE idempotency_key=?",
            (key,),
        ).fetchone()
        return None if row is None else self._receipt(row)

    def apply(self, key, note):
        """Store once; return success/replayed or conflict plus the original receipt."""
        self._validate(key, note)
        self._db.execute("BEGIN IMMEDIATE")
        try:
            receipt = self.lookup(key)
            replayed = receipt is not None
            if receipt is None:
                cursor = self._db.execute(
                    "INSERT INTO notes (idempotency_key, note) VALUES (?, ?)",
                    (key, note),
                )
                receipt = {"receipt_id": cursor.lastrowid, "key": key, "note": note}
            status = "success" if receipt["note"] == note else "conflict"
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise
        return {"status": status, "replayed": replayed, "receipt": receipt}

    def count(self):
        """Count committed note effects, not delivery attempts."""
        return self._db.execute("SELECT COUNT(*) FROM notes").fetchone()[0]
