"""Keep a fresh archive page readable when older context can be reclaimed."""

import json
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import AsyncMock, patch

from glassmoon.agent import READ_TOOL_RESULT, RESULT_READER, TOOL_BUDGET, Agent
from glassmoon.models import Transcripts
from glassmoon.tool_context import ToolContext


class RetrievalBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_compacts_old_round_before_eliding_newest_result(self):
        with tempfile.TemporaryDirectory() as folder, closing(Transcripts(Path(folder, "agent.db"))) as store:
            agent = Agent("", "moon", "models", {}, "", store)
            context = ToolContext(store, "moon")
            call = {"id": "old", "name": "fixture", "arguments": "{}"}
            exchange, refs = context.begin([call], {
                "message": {"role": "assistant", "content": "Prior work"},
                "tool_state": {"reasoning": "x" * TOOL_BUDGET},
            })
            context.record(exchange, refs, call, '{"value": "source text from the fixture"}')
            page = store.tool_result("moon", refs["old"], 0)
            read = {"id": "fresh", "name": READ_TOOL_RESULT, "arguments": '{"id": 1, "offset": 0}'}
            exchange, refs = context.begin([read], {"message": {"role": "assistant", "content": ""}})
            output = json.dumps({"value": page})
            context.record(exchange, refs, read, output)
            model = AsyncMock(return_value={"message": {"role": "assistant", "content": "Older history summary"}})
            with patch("glassmoon.agent.call", model):
                result = await agent.generation_params(
                    {"provider": "fixture", "model": "fixture"},
                    [{"name": READ_TOOL_RESULT, **RESULT_READER}],
                    context, [], {"role": "user", "content": "Read the archived page"}, "",
                )
            model.assert_awaited_once()
            self.assertEqual(result["tool_history"], [exchange])
            self.assertEqual(result["tool_history"][0]["outputs"]["fresh"], output)
            self.assertIn("Older history summary", result["messages"][-1]["content"])
