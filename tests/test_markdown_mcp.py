"""Focused parser and real stdio MCP coverage (no network or credentials)."""

import asyncio
import json
import sys
import unittest
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from examples.markdown_mcp import MAX_INPUT_BYTES, extract_headings


class HeadingTests(unittest.TestCase):
    def test_atx(self):
        self.assertEqual(
            extract_headings(
                "# Moon\n   ## Tide ###  \n###### Six\n#\n## ###\n# C#\n# **bold**"
            ),
            [
                {"level": 1, "text": "Moon"},
                {"level": 2, "text": "Tide"},
                {"level": 6, "text": "Six"},
                {"level": 1, "text": ""},
                {"level": 2, "text": ""},
                {"level": 1, "text": "C#"},
                {"level": 1, "text": "**bold**"},
            ],
        )

    def test_non_headings(self):
        self.assertEqual(
            extract_headings(
                "####### Seven\n#No space\n    # indented\nTitle\n=====\n\\# escaped"
            ),
            [],
        )

    def test_fences(self):
        self.assertEqual(
            extract_headings(
                "# Before\n````python\n# hidden\n```\n# still hidden\n`````\n~~~ lang\n## hidden\n~~~\n## After"
            ),
            [{"level": 1, "text": "Before"}, {"level": 2, "text": "After"}],
        )

    def test_unclosed_and_invalid_fence(self):
        self.assertEqual(extract_headings("```\n# hidden"), [])
        self.assertEqual(
            extract_headings("```bad`info\n# visible"),
            [{"level": 1, "text": "visible"}],
        )
        self.assertEqual(
            extract_headings("~~~\n```\n# hidden\n~~~ trailing\n# hidden"), []
        )

    def test_unicode_crlf_and_bounds(self):
        self.assertEqual(
            extract_headings("# 月\r\n## café\r\n"),
            [{"level": 1, "text": "月"}, {"level": 2, "text": "café"}],
        )
        self.assertEqual(extract_headings("a" * MAX_INPUT_BYTES), [])
        self.assertEqual(extract_headings("é" * (MAX_INPUT_BYTES // 2)), [])
        for value in ("a" * (MAX_INPUT_BYTES + 1), "é" * (MAX_INPUT_BYTES // 2 + 1)):
            with self.assertRaises(ValueError):
                extract_headings(value)
        with self.assertRaises(TypeError):
            extract_headings(None)


class StdioTests(unittest.IsolatedAsyncioTestCase):
    async def test_client_call(self):
        async with asyncio.timeout(10):
            params = StdioServerParameters(
                command=sys.executable,
                args=["-B", "-m", "examples.markdown_mcp"],
                cwd=str(Path(__file__).resolve().parents[1]),
            )
            async with (
                stdio_client(params) as (reader, writer),
                ClientSession(reader, writer) as session,
            ):
                await session.initialize()
                tools = await session.list_tools()
                self.assertIn("extract_headings", [tool.name for tool in tools.tools])
                result = await session.call_tool(
                    "extract_headings",
                    {"markdown": "# Moon\n```\n## hidden\n```\n## Tide"},
                )
                self.assertFalse(result.is_error)
                expected = [
                    {"level": 1, "text": "Moon"},
                    {"level": 2, "text": "Tide"},
                ]
                payload = [
                    json.loads(c.text) for c in result.content if c.type == "text"
                ]
                self.assertEqual(payload, expected)


if __name__ == "__main__":
    unittest.main()
