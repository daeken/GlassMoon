import asyncio
import json
import sys
import tempfile
import unittest
from collections import deque
from pathlib import Path
from unittest.mock import AsyncMock, patch

from mcp import StdioServerParameters

from glassmoon.agent import Agent
from glassmoon.ipc import RequestError
from glassmoon.mcp_bridge import Bridge
from glassmoon.models import Transcripts
from glassmoon.toolbox import load
from glassmoon.tui import Terminal

MCP_SERVER = """
from mcp.server.mcpserver import MCPServer
server = MCPServer("test")
@server.tool()
def add(a: int, b: int) -> int:
    return a + b
server.run()
"""


async def ignore(_):
    pass


class BootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def test_model_preference_applies_after_current_turn_and_persists(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder, "agent.db")
            store = Transcripts(state)
            self.addCleanup(store.close)
            agent = Agent("", "moon", "models", {"provider": "first", "min_context_tokens": 1234}, "", store)
            entered = asyncio.Event()
            resume = asyncio.Event()
            seen = []

            async def router(_socket, _name, _method, params, **_kwargs):
                seen.append(params)
                if len(seen) == 1:
                    entered.set()
                    await resume.wait()
                return {"message": {"role": "assistant", "content": "ready"}}

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
            self.assertEqual([params["provider"] for params in seen], ["first", "second"])
            self.assertEqual(seen[-1]["model"], "chosen")
            self.assertEqual(seen[-1]["min_context_tokens"], 1234)
            self.assertIn("first question", json.dumps(seen[-1]["messages"]))

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
            self.assertNotIn("moon", terminal.history_page)
            self.assertEqual(list(terminal.windows["moon"]), pages[0])
            self.assertEqual(
                [line[len("<you> ")] for page in reversed(pages) for line in page],
                list("012345678"),
            )

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
            published = load(plugin)
            response = await published.handle(
                "invoke",
                {"name": "multiply", "arguments": {"a": 6, "b": 7}},
                ignore,
                False,
            )
            self.assertEqual(response["value"], 42)
            module = root / "mcp.py"
            module.write_text(MCP_SERVER)
            bridge = Bridge(
                StdioServerParameters(command=sys.executable, args=["-I", str(module)])
            )
            async with bridge.client() as client:
                self.assertEqual(set(await bridge.tools(client)), {"add"})
                answer = await bridge.invoke(client, "add", {"a": 12, "b": 30})
            self.assertEqual(answer["structuredContent"]["result"], 42)

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
