"""Archive retrieval keeps returning source text under frame pressure."""

import json
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import AsyncMock, patch

from glassmoon import functions
from glassmoon.agent import READ_TOOL_RESULT, RESULT_READER, TOOL_BUDGET, Agent
from glassmoon.models import Transcripts, encoded_size
from glassmoon.tool_context import DISMISS_TOOL_RESULTS, ToolContext


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
            self.assertEqual(store.tool_checkpoint("moon", references["retrieve"]), {
                "id": references["retrieve"], "status": "compacted",
                "source_from": references["retrieve"], "through_result": references["retrieve"],
                "summary": "Progress",
            })

    async def test_agent_dismisses_and_ages_results_without_losing_the_archive(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder, "moon.db")
            with closing(Transcripts(path)) as store:
                agent = Agent("", "moon", "models", {}, "", store, tool_services=["tools.fixture"])
                phase = 0

                async def model(_socket, _service, _method, params, **_options):
                    nonlocal phase
                    phase += 1
                    rounds = functions.history(params)
                    if phase == 1:
                        calls = [("first", "tools_fixture__write", {"n": 1})]
                    elif phase == 2:
                        calls = [("second", "tools_fixture__write", {"n": 2})]
                    elif phase == 3:
                        first = rounds[0]["outputs"]["first"]
                        self.assertIn("receipt-1", first)
                        self.assertNotIn("original-text-1", first)
                        self.assertIn("original-text-2", rounds[1]["outputs"]["second"])
                        calls = [
                            ("invalid", DISMISS_TOOL_RESULTS, {"ids": [2, 9999]}),
                            ("dismiss", DISMISS_TOOL_RESULTS, {"ids": []}),
                        ]
                    elif phase == 4:
                        second = rounds[1]["outputs"]["second"]
                        self.assertIn("receipt-2", second)
                        self.assertIn("recorded", second)
                        self.assertNotIn("original-text-2", second)
                        self.assertEqual(json.loads(rounds[-1]["outputs"]["invalid"])["error"], "unknown_result")
                        self.assertEqual(json.loads(rounds[-1]["outputs"]["dismiss"]), {"value": {"dismissed": [2]}})
                        calls = [("read", READ_TOOL_RESULT, {"id": 2, "offset": 0})]
                    else:
                        restored = json.loads(rounds[-1]["outputs"]["read"])["value"]
                        self.assertIn("original-text-2", restored["text"])
                        self.assertEqual(restored["elisions"], ["explicit"])
                        return {"message": {"role": "assistant", "content": "Both receipts checked."}}
                    return {
                        "provider": "fixture", "model": "fixture",
                        "message": {"role": "assistant", "content": ""},
                        "tool_calls": [
                            {"id": call_id, "name": name, "arguments": json.dumps(args)}
                            for call_id, name, args in calls
                        ],
                    }

                async def tool(_socket, _service, _name, args, **_options):
                    n = args["n"]
                    return {"id": f"receipt-{n}", "value": f"original-text-{n}"}

                spec = [{
                    "name": "write", "description": "Record a receipt",
                    "parameters": {"type": "object"},
                }]
                effect = AsyncMock(side_effect=tool)
                with (
                    patch.dict("os.environ", {"GLASSMOON_TOOL_RESULT_RETAIN_EXCHANGES": "1"}),
                    patch("glassmoon.agent.tool_catalog", AsyncMock(return_value=(13, spec))),
                    patch("glassmoon.agent.call_tool", effect),
                    patch("glassmoon.agent.call", side_effect=model),
                ):
                    result = await agent.handle("chat", {"text": "Record two effects"}, None, False)
                self.assertEqual(result["message"]["content"], "Both receipts checked.")
                self.assertEqual(effect.await_count, 2)

            with closing(Transcripts(path)) as store:
                records = [store.tool_result("moon", index, 0) for index in (1, 2)]
                self.assertEqual([row["elisions"] for row in records], [["age"], ["explicit"]])
                for index, row in enumerate(records, start=1):
                    self.assertIn(f"original-text-{index}", row["text"])
