import asyncio
import base64
import hashlib
import json
import sys
import tempfile
import unittest
from collections import deque
from pathlib import Path
from unittest.mock import AsyncMock, patch

from mcp import StdioServerParameters

from glassmoon.agent import Agent
from glassmoon.compaction import compact, compactable_turn
from glassmoon.ipc import RequestError
from glassmoon.mcp_bridge import Bridge
from glassmoon.models import Transcripts
from glassmoon.toolbox import load
from glassmoon.tui import Terminal

MCP_SERVER = """
import base64
from mcp import types
from mcp.server.mcpserver import MCPServer
server = MCPServer("test")
@server.tool()
def add(a: int, b: int) -> int:
    return a + b
@server.tool()
def fixture_image() -> types.ImageContent:
    return types.ImageContent(data=base64.b64encode(b"\\xff\\xd8fixture").decode(), mime_type="image/jpeg")
server.run()
"""


async def ignore(_):
    pass


class BootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def test_compaction_retries_oversized_reply_with_shorter_prompt(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Transcripts(Path(folder, "agent.db"))
            self.addCleanup(store.close)
            store.create(identifier="moon")
            turn = ({"role": "user", "content": "Question"}, {"role": "assistant", "content": "Answer"})
            store.append("moon", 0, *(turn * 3))
            prompts = []

            async def generate(messages):
                prompts.append(messages[-1]["content"])
                return {"message": {"content": "x" * 6000 if len(prompts) == 1 else "Short summary"}}

            through, summary, remaining = await compact(store, "moon", *store.context("moon"), generate, force=True)
            self.assertEqual((through, summary, len(remaining)), (2, "Short summary", 4))
            self.assertIn("300 words or fewer", prompts[-1])
            self.assertEqual(len(store.history("moon")["messages"]), 6)

    async def test_mcp_health_reports_upstream_catalog_changes_without_swapping_tools(self):
        bridge = Bridge("fixture")
        published = {"removed": {}, "changed": {"description": "old"}, "same": {}}
        upstream = {"added": {}, "changed": {"description": "new"}, "same": {}}
        with patch.object(bridge, "tools", AsyncMock(side_effect=[published, upstream, OSError()])):
            healthy = await bridge.check_catalog(None, published)
            self.assertEqual((healthy["status"], healthy["tools"]), ("ok", 3))
            report = await bridge.check_catalog(None, published)
            self.assertEqual(report, {
                "status": "changed", "tools": 3, "change_count": 3,
                "added": ["added"], "removed": ["removed"], "changed": ["changed"],
            })
            self.assertEqual(published["changed"], {"description": "old"})
            with self.assertRaises(RequestError) as unavailable:
                await bridge.check_catalog(None, published)
            self.assertEqual(unavailable.exception.code, "mcp_catalog_unavailable")

    def test_compaction_omits_inline_attachments_and_signed_url_details(self):
        compacted = compactable_turn([{
            "role": "user",
            "content": [
                {"type": "text", "text": "Two screenshots"},
                {"type": "image", "url": "data:image/jpeg;base64,fixturebytes"},
                {"type": "image", "url": "https://fixture.example/sensitive-path?token=temporary"},
            ],
        }])
        self.assertEqual(compacted[0]["content"], "Two screenshots\n[image attachment]\n[image attachment from fixture.example]")

    async def test_agent_manages_only_operator_approved_shared_tools(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Transcripts(Path(folder, "agent.db"))
            self.addCleanup(store.close)
            agent = Agent("", "moon", "models", {}, "", store, self_tools=["mcp.notes"])

            async def configure(**params):
                return await agent.execute(
                    {"name": "configure_shared_tool", "arguments": json.dumps(params)}, {}
                )

            read = {"mode": "read", "expected_revision": 0, "service": ""}

            async def catalog(_socket, service):
                self.assertEqual(service, "mcp.notes")
                return 15, [{"name": "read", "description": "Read local note", "parameters": {"type": "object"}}]

            with patch("glassmoon.agent.tool_catalog", side_effect=catalog):
                definitions, routes, _, _ = await agent.discover_tools(ignore, False)
                self.assertEqual([item["name"] for item in definitions], ["configure_shared_tool"])
                self.assertEqual(routes, {})
                policy = (await configure(**read))["value"]
                denied = await configure(service="desktop", mode="enable", expected_revision=policy["revision"])
                self.assertEqual(denied["error"], "tool_policy_denied")
                approved = await configure(service="mcp.notes", mode="enable", expected_revision=policy["revision"])
                self.assertEqual(approved["value"]["enabled"], ["mcp.notes"])
                self.assertEqual(routes, {})
                definitions, routes, _, _ = await agent.discover_tools(ignore, False)
                self.assertEqual(routes["mcp_notes__read"], ("mcp.notes", "read", 15))
                replacement = Agent("", "moon", "models", {}, "", store)
                self.assertEqual(replacement.tool_policy()["enabled"], [])
                await agent.handle(
                    "set_tool_policy",
                    {"service": "mcp.notes", "mode": "disable", "expected_revision": 1},
                    ignore, False,
                )
                blocked = await configure(service="mcp.notes", mode="enable", expected_revision=2)
                self.assertEqual(blocked["error"], "tool_policy_denied")
                self.assertEqual((await configure(**read))["value"]["enabled"], [])

    async def test_model_preference_applies_after_current_turn_and_persists(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder, "agent.db")
            store = Transcripts(state)
            self.addCleanup(store.close)
            agent = Agent("", "moon", "models", {"provider": "first", "min_context_tokens": 1234}, "", store)
            for revision in (0, 2, 4):
                store.append(
                    "moon", revision,
                    {"role": "user", "content": "old question " + "x" * 4800},
                    {"role": "assistant", "content": "old answer " + "x" * 4800},
                )
            entered = asyncio.Event()
            resume = asyncio.Event()
            seen = []

            async def router(_socket, _name, _method, params, **_kwargs):
                seen.append(params)
                if len(seen) == 1:
                    entered.set()
                    await resume.wait()
                    return {"message": {"role": "assistant", "content": "Remember the oldest turn"}}
                return {
                    "message": {"role": "assistant", "content": "ready"},
                    "provider": params["provider"], "model": params.get("model", "chosen"),
                    "provider_generation": 123,
                }

            async def change(target, **params):
                return await target.handle("set_model_preference", params, ignore, False)

            with patch("glassmoon.agent.call", side_effect=router):
                turn = asyncio.create_task(agent.handle("chat", {"text": "first question"}, ignore, False))
                try:
                    await asyncio.wait_for(entered.wait(), 2)
                    preference = await change(agent, mode="pin", provider="second", model="chosen", expected_revision=0)
                    self.assertEqual(preference["revision"], 1)
                    with self.assertRaises(RequestError) as stale:
                        await change(agent, mode="auto", expected_revision=0)
                    self.assertEqual(stale.exception.code, "model_preference_changed")
                finally:
                    resume.set()
                    await turn
                reopened = Transcripts(state)
                self.addCleanup(reopened.close)
                replacement = Agent("", "moon", "models", agent.requirements, "", reopened)
                await replacement.handle("chat", {"text": "second question"}, ignore, False)
                auto = await change(replacement, mode="auto", expected_revision=1)
                self.assertNotIn("provider", auto["requirements"])
                restored = await change(replacement, mode="default", expected_revision=2)
                self.assertEqual(restored["requirements"]["provider"], "first")
                self.assertEqual(
                    restored["last_model"],
                    {"revision": 10, "provider": "second", "model": "chosen", "generation": 123},
                )
            self.assertEqual([params["provider"] for params in seen], ["first", "first", "second"])
            self.assertIn("Summarize", json.dumps(seen[0]["messages"]))
            self.assertIn("Remember the oldest turn", json.dumps(seen[1]["messages"]))
            self.assertEqual(seen[-1]["model"], "chosen")
            self.assertEqual(seen[-1]["min_context_tokens"], 1234)
            self.assertIn("first question", json.dumps(seen[-1]["messages"]))
            self.assertEqual(
                await replacement.handle("health", {}, ignore, False),
                {"status": "ok", "agent": "moon", "revision": 10},
            )

    async def test_tui_pages_older_history_without_duplicate_messages(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Transcripts(Path(folder, "agent.db"))
            self.addCleanup(store.close)
            agent = Agent("", "moon", "models", {}, "", store)
            for index in range(9):
                store.append(
                    "moon", index,
                    {"role": "user", "content": str(index) + "x" * 14_000},
                )

            async def history(_socket, _service, method, params):
                return await agent.handle(method, params, ignore, False)

            terminal = Terminal(None, "")
            terminal.windows["moon"] = deque(maxlen=500)
            terminal.select("moon")
            with patch("glassmoon.tui.call", side_effect=history):
                await terminal.query("moon")
                self.assertEqual(terminal.history_before["moon"], 5)
                pages = [list(terminal.windows["moon"])]
                await terminal.older_history("moon")
                self.assertEqual(terminal.history_before["moon"], 1)
                pages.append(terminal.history_page["moon"])
                await terminal.older_history("moon")
                self.assertIsNone(terminal.history_before["moon"])
                pages.append(terminal.history_page["moon"])
                await terminal.latest_history("moon")
                store.append("moon", 9, {"role": "assistant", "content": "A saved background reply"})
                await terminal.saved_history("moon")
                self.assertEqual(terminal.history_page["moon"][-1], "<moon> A saved background reply")
                self.assertNotIn("<moon> A saved background reply", terminal.windows["moon"])
                await terminal.latest_history("moon")
            self.assertNotIn("moon", terminal.history_page)
            self.assertEqual(list(terminal.windows["moon"]), pages[0])
            self.assertEqual(
                [line[len("<you> ")] for page in reversed(pages) for line in page],
                list("012345678"),
            )

    async def test_tui_search_pages_leave_live_chat_and_older_history_intact(self):
        terminal = Terminal(None, "")
        terminal.windows["moon"] = deque(["live message"], maxlen=500)
        terminal.select("moon")
        terminal.loaded.add("moon")
        terminal.history_before["moon"] = 5
        terminal.latest_before["moon"] = 8
        terminal.history_page["moon"] = ["older history"]
        with patch("glassmoon.tui.call", side_effect=[
            {"matches": [{"position": 7, "role": "user", "snippet": "newest"}], "before": 7},
            {"matches": [{"position": 3, "role": "user", "snippet": "earlier"}], "before": None},
        ]) as call:
            await terminal.find_history("moon", "keyword")
            self.assertEqual(terminal.search_page["moon"][2][0], "8  user  newest")
            await terminal.find_history("moon", "")
            self.assertEqual(terminal.search_page["moon"][2], ["4  user  earlier"])
            await terminal.find_history("moon", "")
            self.assertEqual(call.await_count, 2)
            self.assertEqual(call.await_args.args[3], {"query": "keyword", "before": 7})
            self.assertEqual(terminal.history_page["moon"], ["older history"])
            await terminal.latest_history("moon")
        self.assertNotIn("moon", terminal.search_page)
        self.assertNotIn("moon", terminal.history_page)
        self.assertEqual(terminal.history_before["moon"], 8)
        self.assertEqual(list(terminal.windows["moon"]), ["live message"])

    def test_short_history_pages_keep_every_cursor_position(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Transcripts(Path(folder, "agent.db"))
            self.addCleanup(store.close)
            store.create(identifier="moon")
            messages = [{"role": "user", "content": str(index)} for index in range(827)]
            store.append("moon", 0, *messages)
            page = store.history("moon")
            fetched = page["messages"]
            while page["before"] is not None:
                self.assertLessEqual(len(page["messages"]), 200)
                page = store.history("moon", page["before"])
                fetched = page["messages"] + fetched
            self.assertEqual(fetched, messages)

    def test_history_query_finds_compacted_and_uncompacted_messages(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Transcripts(Path(folder, "agent.db"))
            self.addCleanup(store.close)
            store.create(identifier="moon")
            messages = [{"role": "user", "content": f"Item {index} Straße"} for index in range(24)]
            store.append("moon", 0, *messages)
            store.compact("moon", 24, 0, 20, "Older messages summarized")
            first = store.find_history("moon", "STRASSE")
            self.assertEqual(len(first["matches"]), 20)
            older = store.find_history("moon", "STRASSE", first["before"])
            matches = first["matches"] + older["matches"]
            self.assertEqual([row["position"] for row in matches], list(reversed(range(24))))
            self.assertTrue(all("Straße" in row["snippet"] for row in matches))
            self.assertIsNone(older["before"])

    def test_history_query_pages_past_an_empty_block(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Transcripts(Path(folder, "agent.db"))
            self.addCleanup(store.close)
            store.create(identifier="moon")
            store.append("moon", 0, {"role": "user", "content": "remember me"},
                         *([{"role": "assistant", "content": "other"}] * 1000))
            first = store.find_history("moon", "remember")
            self.assertEqual(first, {"matches": [], "before": 1})
            older = store.find_history("moon", "remember", first["before"])
            self.assertEqual([row["position"] for row in older["matches"]], [0])
            self.assertIsNone(older["before"])

    async def test_tool_policy_rolls_back_and_survives_replacement(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder, "agent.db")
            store = Transcripts(path)
            agent = Agent("", "moon", "models", {}, "", store, tool_services=["mcp.base"])

            async def change(target, service, mode, revision):
                return await target.handle(
                    "set_tool_policy",
                    {"service": service, "mode": mode, "expected_revision": revision},
                    ignore, False,
                )

            added = await change(agent, "mcp.new", "enable", 0)
            self.assertEqual(added["enabled"], ["mcp.base", "mcp.new"])
            with self.assertRaises(RequestError) as conflict:
                await change(agent, "mcp.base", "disable", 0)
            self.assertEqual(conflict.exception.code, "policy_changed")
            await change(agent, "mcp.base", "disable", added["revision"])
            store.close()

            store = Transcripts(path)
            self.addCleanup(store.close)
            replacement = Agent(
                "", "moon", "models", {}, "", store, tool_services=["mcp.base", "mcp.fresh"],
            )
            catalog = AsyncMock(side_effect=OSError)
            with patch("glassmoon.agent.tool_catalog", catalog):
                status = await replacement.handle("tool_status", {}, ignore, False)
            self.assertEqual(status["policy_revision"], 2)
            self.assertEqual(status["services"]["mcp.base"]["state"], "disabled")
            self.assertEqual(status["services"]["mcp.new"]["state"], "unavailable")
            self.assertEqual([item.args[1] for item in catalog.await_args_list], ["mcp.fresh", "mcp.new"])
            policy = await change(replacement, "mcp.base", "default", 2)
            self.assertEqual(policy["enabled"], ["mcp.base", "mcp.fresh", "mcp.new"])
            policy = await change(replacement, "mcp.new", "default", 3)
            self.assertEqual(policy["enabled"], ["mcp.base", "mcp.fresh"])
            self.assertEqual(policy["overrides"], {})

    async def test_probe_shared_tools_concurrently_and_keep_configured_priority(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Transcripts(Path(folder, "agent.db"))
            self.addCleanup(store.close)
            agent = Agent("", "moon", "models", {}, "", store, tool_services=["tool.a", "tool_a"])
            second_started = asyncio.Event()
            definitions = [{"name": "run", "description": "Fixture", "parameters": {"type": "object"}}]

            async def catalog(_socket, service):
                if service == "tool.a":
                    await asyncio.wait_for(second_started.wait(), 0.3)
                    return 3, definitions
                second_started.set()
                return 4, definitions

            with patch("glassmoon.agent.tool_catalog", side_effect=catalog):
                status = await agent.handle("tool_status", {}, ignore, False)
            self.assertEqual(status["services"]["tool.a"]["generation"], 3)
            self.assertEqual(status["services"]["tool_a"]["state"], "unavailable")
            self.assertEqual([tool["service"] for tool in status["tools"]], ["tool.a"])

    async def test_summary_survives_agent_restart_and_keeps_history(self):
        with tempfile.TemporaryDirectory() as folder:
            database = Path(folder) / "agent.db"
            store = Transcripts(database)
            agent = Agent("", "moon", "models", {}, "instructions", store)
            turns = [(f"user:{i}", f"assistant:{i}") for i in range(4)]
            for index, (user, assistant) in enumerate(turns):
                store.append(
                    "moon",
                    2 * index,
                    {"role": "user", "content": user},
                    {"role": "assistant", "content": assistant},
                )
            answer = {"message": {"role": "assistant", "content": "Remember user:0"}}
            generate = AsyncMock(return_value=answer)
            with patch("glassmoon.agent.call", generate):
                response = await agent.handle("compact", {}, ignore, False)
            self.assertEqual(response["through_revision"], 4)
            self.assertEqual(
                generate.await_args.args[3]["messages"][1]["content"], "user:0"
            )
            self.assertEqual(len(store.history("moon")["messages"]), 8)
            store.close()

            with self.subTest("after opening the same state"):
                store = Transcripts(database)
                self.addCleanup(store.close)
                agent = Agent("", "moon", "models", {}, "instructions", store)
                self.assertEqual(
                    await agent.handle("memory", {}, ignore, False),
                    {"revision": 8, "through_revision": 4, "summary": "Remember user:0"},
                )
                with patch("glassmoon.agent.call", generate):
                    await agent.handle("chat", {"text": "continue"}, ignore, False)
                messages = generate.await_args.args[3]["messages"]
                self.assertIn("Remember user:0", messages[1]["content"])
                self.assertEqual(messages[-2]["content"], "assistant:3")
                self.assertEqual(store.history("moon")["revision"], 10)

    async def test_publisher_and_stdio_mcp(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            plugin = root / "plugin.py"
            plugin.write_text(
                'TOOLS = {"multiply": {"description": "Multiply", "parameters": {"type": "object"},'
                ' "handler": lambda a, b: a * b}}\n'
            )
            source_hash = hashlib.sha256(plugin.read_bytes()).hexdigest()
            published = load(plugin, source_hash)
            self.assertEqual(
                await published.handle("health", {}, ignore, False),
                {"status": "ok", "tools": 1, "source_sha256": source_hash},
            )
            response = await published.handle(
                "invoke",
                {"name": "multiply", "arguments": {"a": 6, "b": 7}},
                ignore,
                False,
            )
            self.assertEqual(response["value"], 42)
            plugin.write_text("raise AssertionError('unreviewed source executed')\n")
            with self.assertRaises(RequestError) as changed:
                load(plugin, source_hash)
            self.assertEqual(changed.exception.code, "source_changed")
            plugin.write_text('TOOLS = {"bad": {"parameters":{}, "handler": lambda: None}}\n')
            with self.assertRaisesRegex(TypeError, "Invalid declaration for bad"):
                load(plugin, hashlib.sha256(plugin.read_bytes()).hexdigest())
            module = root / "mcp.py"
            module.write_text(MCP_SERVER)
            bridge = Bridge(
                StdioServerParameters(command=sys.executable, args=["-I", str(module)]),
                image_tools=["fixture_image"],
            )
            async with bridge.client() as client:
                tools = await bridge.tools(client)
                self.assertEqual(set(tools), {"add", "fixture_image"})
                self.assertTrue(tools["fixture_image"]["returns_image"])
                answer = await bridge.invoke(client, "add", {"a": 12, "b": 30})
                image = await bridge.invoke(client, "fixture_image", {})
            self.assertEqual(answer["structuredContent"]["result"], 42)
            encoded = base64.b64encode(b"\xff\xd8fixture").decode()
            self.assertEqual(image["image"]["url"], "data:image/jpeg;base64," + encoded)
            self.assertNotIn("data", image["content"][0])

    async def test_agent_calls_discovered_tool(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Transcripts(Path(folder) / "agent.db")
            self.addCleanup(store.close)
            agent = Agent(
                "", "moon", "models", {}, "", store, tool_services=["mcp.math"]
            )
            discovered = [
                {"name": "add", "description": "Add", "parameters": {"type": "object"}}
            ]
            call = {
                "id": "tool1",
                "name": "mcp_math__add",
                "arguments": '{"a":12,"b":30}',
            }
            generate = AsyncMock(
                side_effect=[
                    {
                        "provider": "openai",
                        "provider_generation": 53,
                        "model": "test",
                        "tool_calls": [call],
                        "message": {"role": "assistant", "content": ""},
                    },
                    {"message": {"role": "assistant", "content": "42"}},
                ]
            )
            tool = AsyncMock(return_value={"structuredContent": {"result": 42}})
            with (
                patch("glassmoon.agent.tool_catalog", AsyncMock(return_value=(14, discovered))),
                patch("glassmoon.agent.call_tool", tool),
                patch("glassmoon.agent.call", generate),
            ):
                await agent.handle("chat", {"text": "add"}, ignore, False)
            tool.assert_awaited_once_with(
                "", "mcp.math", "add", {"a": 12, "b": 30}, generation=14
            )
            exchange = generate.await_args.args[3]["tool_history"][0]
            self.assertEqual(generate.await_args.args[3]["provider_generation"], 53)
            self.assertEqual(
                json.loads(exchange["outputs"]["tool1"])["value"]["structuredContent"][
                    "result"
                ],
                42,
            )

    async def test_agent_selects_shared_tools_before_enforcing_catalog_capacity(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Transcripts(Path(folder, "agent.db"))
            self.addCleanup(store.close)
            agent = Agent(
                "", "moon", "models", {}, "", store, tool_services=["many"],
                tool_selections={"many": {"tool_33"}},
            )
            sizes = [34, 33]

            async def catalog(*_):
                return 5, [
                    {
                        "name": f"tool_{i}", "description": "Tool", "parameters": {"type": "object"},
                        "returns_image": i == 0,
                    }
                    for i in range(sizes.pop(0))
                ]

            with patch("glassmoon.agent.tool_catalog", side_effect=catalog):
                status = await agent.handle("tool_status", {}, ignore, False)
                missing = await agent.handle("tool_status", {}, ignore, False)
            self.assertEqual(status["tools"], [
                {"name": "many__tool_33", "service": "many", "returns_image": False},
            ])
            self.assertEqual(missing["tools"], [])
            self.assertEqual(missing["services"]["many"]["missing"], ["tool_33"])

    async def test_agent_reports_catalog_capacity_separately_from_outages(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Transcripts(Path(folder, "agent.db"))
            self.addCleanup(store.close)
            agent = Agent("", "moon", "models", {}, "", store, tool_services=["first", "second", "third"])

            async def catalog(_socket, service):
                count, generation = {"first": (30, 10), "second": (3, 11), "third": (2, 12)}[service]
                return generation, [
                    {"name": f"tool_{i}", "description": "Tool", "parameters": {"type": "object"}}
                    for i in range(count)
                ]

            with patch("glassmoon.agent.tool_catalog", side_effect=catalog):
                status = await agent.handle("tool_status", {}, ignore, False)
            self.assertEqual(len(status["tools"]), 32)
            self.assertEqual(status["services"]["second"], {
                "state": "over_limit", "count": 3, "generation": 11, "available_slots": 2,
            })
            self.assertEqual(status["services"]["third"]["state"], "available")

    async def test_tool_limit_commits_review_receipt_without_running_another_call(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Transcripts(Path(folder, "agent.db"))
            self.addCleanup(store.close)
            agent = Agent("", "moon", "models", {}, "", store, tool_services=["tools.fixture"])
            catalog = [{"name": "note", "description": "Save note", "parameters": {"type": "object"}}]
            model_reply = {
                "provider": "test", "model": "test",
                "message": {"role": "assistant", "content": ""},
                "tool_calls": [{"id": "call", "name": "tools_fixture__note", "arguments": "{}"}],
            }
            tool = AsyncMock(return_value={"note": "Saved"})
            task = {"id": "d" * 32, "attempt": 1, "text": "Run the fixture"}
            with (
                patch("glassmoon.agent.MAX_TOOL_ROUNDS", 1),
                patch("glassmoon.agent.tool_catalog", AsyncMock(return_value=(14, catalog))),
                patch("glassmoon.agent.call", AsyncMock(return_value=model_reply)),
                patch("glassmoon.agent.call_tool", tool),
            ):
                response = await agent.handle("run_task", task, ignore, False)
                self.assertEqual(response["outcome"], "review")
                self.assertIn("tools_fixture__note", response["message"]["content"])
                self.assertEqual(await agent.handle("run_task", task, ignore, False), response)
                receipt = await agent.handle("task_receipt", {"id": task["id"], "attempt": 1}, ignore, False)
                self.assertEqual(receipt["reply"]["outcome"], "review")
            tool.assert_awaited_once()

    async def test_provider_cutover_after_tool_execution_saves_partial_receipt(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Transcripts(Path(folder, "agent.db"))
            self.addCleanup(store.close)
            agent = Agent("", "moon", "models", {}, "", store, tool_services=["tools.fixture"])
            catalog = [{"name": "note", "description": "Save note", "parameters": {"type": "object"}}]
            reply = {
                "provider": "test", "model": "test", "provider_generation": 31,
                "message": {"role": "assistant", "content": ""},
                "tool_calls": [{"id": "call", "name": "tools_fixture__note", "arguments": "{}"}],
            }
            changed = RequestError({"code": "provider_changed", "message": "Provider replaced"})
            effect = AsyncMock(return_value={"saved": True})
            task = {"id": "f" * 32, "attempt": 1, "text": "Save the note"}
            with (
                patch("glassmoon.agent.tool_catalog", AsyncMock(return_value=(14, catalog))),
                patch("glassmoon.agent.call", AsyncMock(side_effect=[reply, changed])),
                patch("glassmoon.agent.call_tool", effect),
            ):
                partial = await agent.handle("run_task", task, ignore, False)
                self.assertEqual(partial["outcome"], "review")
                self.assertIn("provider_changed", partial["message"]["content"])
                self.assertIn("tools_fixture__note", partial["message"]["content"])
                self.assertEqual(await agent.handle("run_task", task, ignore, False), partial)
            effect.assert_awaited_once()

    async def test_oversized_tool_result_saves_partial_report(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Transcripts(Path(folder, "agent.db"))
            self.addCleanup(store.close)
            agent = Agent("", "moon", "models", {}, "", store, tool_services=["tools.fixture"])
            catalog = [{"name": "note", "description": "Save note", "parameters": {"type": "object"}}]
            model = AsyncMock(return_value={
                "provider": "test", "model": "test",
                "message": {"role": "assistant", "content": ""},
                "tool_calls": [{"id": "call", "name": "tools_fixture__note", "arguments": "{}"}],
            })
            task = {"id": "f" * 32, "attempt": 1, "text": "Run the fixture"}
            with (
                patch("glassmoon.agent.tool_catalog", AsyncMock(return_value=(14, catalog))),
                patch("glassmoon.agent.call", model),
                patch("glassmoon.agent.call_tool", AsyncMock(return_value={"note": "\0" * 12000})),
            ):
                response = await agent.handle("run_task", task, ignore, False)
            self.assertEqual(response["outcome"], "review")
            self.assertIn("continuation budget", response["message"]["content"])
            self.assertEqual(store.revision("moon"), 2)
            model.assert_awaited_once()
