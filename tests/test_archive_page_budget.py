"""Archive retrieval keeps returning source text under frame pressure."""

import json
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import AsyncMock, patch

from glassmoon.agent import READ_TOOL_RESULT, RESULT_READER, TOOL_BUDGET, Agent
from glassmoon.models import Transcripts, encoded_size
from glassmoon.tool_context import ToolContext


class ArchivePageBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_page_survives_oversized_provider_state(self):
        with tempfile.TemporaryDirectory() as folder, closing(Transcripts(Path(folder, "moon.db"))) as store:
            agent = Agent("", "moon", "models", {}, "", store)
            context = ToolContext(store, "moon")
            source = {"id": "source-call", "name": "fixture", "arguments": "{}"}
            source_id = store.record_tool_calls("moon", context.run, [source])["source-call"]
            original = "0123456789" * 700
            store.record_tool_result(source_id, original)
            page = store.tool_result("moon", source_id, 27)
            read = {"id": "retrieve", "name": READ_TOOL_RESULT, "arguments": '{"id": 1, "offset": 27}'}
            exchange, references = context.begin([read], {
                "message": {"role": "assistant", "content": ""},
                "tool_state": {"reasoning": "x" * TOOL_BUDGET},
            })
            context.record(exchange, references, read, json.dumps({"value": page}))
            generate = AsyncMock(return_value={"message": {"role": "assistant", "content": "Progress"}})
            with patch("glassmoon.agent.call", generate):
                params = await agent.generation_params(
                    {"provider": "fixture", "model": "fixture"},
                    [{"name": READ_TOOL_RESULT, **RESULT_READER}], context, [],
                    {"role": "user", "content": "a" * 57000}, "",
                )
            self.assertNotIn("tool_history", params)
            self.assertLessEqual(encoded_size(params), TOOL_BUDGET)
            delivered, = json.loads(params["messages"][-1]["content"].split("\n", 1)[1])
            self.assertEqual((delivered["id"], delivered["offset"]), (source_id, 27))
            self.assertGreater(len(delivered["text"]), 0)
            self.assertLess(len(delivered["text"]), len(page["text"]))
            self.assertEqual(delivered["next_offset"], 27 + len(delivered["text"]))
            self.assertEqual(delivered["text"], original[27:delivered["next_offset"]])
            self.assertEqual(store.tool_result("moon", source_id, 27), page)
