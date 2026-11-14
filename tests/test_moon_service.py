"""Focused deterministic contract tests for the example text service."""

import unittest

from examples.moon_service import MAX_BYTES, handle
from glassmoon.ipc import RequestError


class MoonServiceTests(unittest.IsolatedAsyncioTestCase):
    async def invoke(self, name, params):
        async def emit(_):
            self.fail("This service must not emit streaming chunks")

        return await handle(name, params, emit, False)

    async def test_health_and_known_fingerprint(self):
        expected = {"service": "moon.text", "version": 1, "status": "ok"}
        result = await self.invoke("health", {})
        self.assertEqual(result, expected)
        result.clear()
        self.assertEqual(await self.invoke("health", {}), expected)
        self.assertEqual(
            await self.invoke("fingerprint", {"text": "abc"}),
            {
                "sha256": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                "bytes": 3,
                "characters": 3,
                "lines": 1,
            },
        )

    async def test_unicode_line_counts_and_byte_boundary(self):
        for text, size, chars, lines in [
            ("", 0, 0, 0),
            ("é\n", 3, 2, 1),
            ("a\r\nb", 4, 4, 2),
            ("\n\n", 2, 2, 2),
            ("é" * (MAX_BYTES // 2), MAX_BYTES, MAX_BYTES // 2, 1),
        ]:
            with self.subTest(text_length=len(text)):
                result = await self.invoke("fingerprint", {"text": text})
                self.assertEqual(
                    (result["bytes"], result["characters"], result["lines"]),
                    (size, chars, lines),
                )

    async def test_rejects_invalid_requests(self):
        for name, params, code in [
            ("missing", {}, "unknown_method"),
            ("health", {"extra": 1}, "invalid_params"),
            ("fingerprint", [], "invalid_params"),
            ("fingerprint", {}, "invalid_params"),
            ("fingerprint", {"text": 1}, "invalid_params"),
            ("fingerprint", {"text": "ok", "extra": 1}, "invalid_params"),
            ("fingerprint", {"text": "\ud800"}, "invalid_params"),
            ("fingerprint", {"text": "é" * (MAX_BYTES // 2 + 1)}, "invalid_params"),
        ]:
            with self.subTest(name=name, code=code, keys=type(params).__name__):
                with self.assertRaises(RequestError) as caught:
                    await self.invoke(name, params)
                self.assertEqual(caught.exception.code, code)


if __name__ == "__main__":
    unittest.main()
