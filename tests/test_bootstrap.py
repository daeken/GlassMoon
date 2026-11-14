import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from mcp import StdioServerParameters

from glassmoon.agent import Agent
from glassmoon.mcp_bridge import Bridge
from glassmoon.models import Transcripts
from glassmoon.toolbox import load

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
            self.assertEqual(set(await bridge.tools()), {"add"})
            answer = await bridge.invoke("add", {"a": 12, "b": 30})
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
                        "model": "test",
                        "tool_calls": [call],
                        "message": {"role": "assistant", "content": ""},
                    },
                    {"message": {"role": "assistant", "content": "42"}},
                ]
            )
            tool = AsyncMock(return_value={"structuredContent": {"result": 42}})
            with (
                patch("glassmoon.agent.list_tools", AsyncMock(return_value=discovered)),
                patch("glassmoon.agent.call_tool", tool),
                patch("glassmoon.agent.call", generate),
            ):
                await agent.handle("chat", {"text": "add"}, ignore, False)
            tool.assert_awaited_once_with("", "mcp.math", "add", {"a": 12, "b": 30})
            exchange = generate.await_args.args[3]["tool_history"][0]
            self.assertEqual(
                json.loads(exchange["outputs"]["tool1"])["value"]["structuredContent"][
                    "result"
                ],
                42,
            )
