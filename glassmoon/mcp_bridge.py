"""Expose one MCP server over the standard GlassMoon tool service interface."""

import argparse
import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path

import httpx2
from mcp import Client, StdioServerParameters
from mcp.client.streamable_http import streamable_http_client

from .ipc import ProtocolError, RequestError
from .service import fail, serve
from .toolbox import METHODS, ToolService

MCP_TIMEOUT = 25


class Bridge:
    def __init__(self, target, headers=None):
        self.target = target
        self.headers = headers or {}

    def client(self):
        if not self.headers:
            return Client(self.target, read_timeout_seconds=MCP_TIMEOUT)

        @asynccontextmanager
        async def transport():
            async with (
                httpx2.AsyncClient(headers=self.headers, timeout=MCP_TIMEOUT) as client,
                streamable_http_client(self.target, http_client=client) as streams,
            ):
                yield streams

        return Client(transport(), read_timeout_seconds=MCP_TIMEOUT)

    async def tools(self, client):
        async with asyncio.timeout(MCP_TIMEOUT):
            cursor = None
            tools = {}
            for _ in range(32):
                page = await client.list_tools(cursor=cursor)
                for tool in page.tools:
                    tools[tool.name] = {
                        "description": tool.description or tool.name,
                        "parameters": tool.input_schema,
                    }
                cursor = page.next_cursor
                if cursor is None:
                    return tools
        raise fail("mcp_catalog", "MCP tool catalog exceeds the page limit")

    async def invoke(self, client, name, arguments):
        try:
            async with asyncio.timeout(MCP_TIMEOUT):
                result = await client.call_tool(name, arguments)
                return result.model_dump(mode="json", by_alias=True, exclude_none=True)
        except TimeoutError:
            raise fail("mcp_timeout", "MCP tool did not respond in time") from None
        except Exception as error:  # noqa: BLE001 - sanitize transport and server failures.
            raise fail(
                "mcp_call_failed", f"MCP call failed ({type(error).__name__})"
            ) from None


async def run(args):
    if args.url:
        if args.command:
            raise ValueError("Choose a remote URL or a local command")
        key = os.environ.get(args.bearer_env) if args.bearer_env else None
        if args.bearer_env and not key:
            raise ValueError(f"Set {args.bearer_env} before starting this MCP bridge")
        bridge = Bridge(args.url, {"Authorization": f"Bearer {key}"} if key else None)
    else:
        command = args.command[1:] if args.command[:1] == ["--"] else args.command
        if not command:
            raise ValueError("Supply a stdio command after -- or pass --url")
        environment = {
            name: os.environ[name] for name in args.pass_env if name in os.environ
        }
        bridge = Bridge(
            StdioServerParameters(
                command=command[0],
                args=command[1:],
                cwd=Path(args.cwd),
                env=environment,
            )
        )
    async with bridge.client() as client:
        functions = await bridge.tools(client)

        async def invoke(name, arguments):
            return await bridge.invoke(client, name, arguments)

        service = ToolService(functions, invoke)
        await serve(
            args.socket,
            args.name,
            args.replace,
            METHODS,
            service.handle,
            reconnect=True,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--replace", type=int)
    parser.add_argument("--url", help="Streamable HTTP MCP endpoint")
    parser.add_argument(
        "--bearer-env", help="Environment variable containing a remote bearer token"
    )
    parser.add_argument(
        "--cwd", default=".", help="Working directory for a stdio server"
    )
    parser.add_argument(
        "--pass-env",
        action="append",
        default=[],
        help="Forward a named environment variable to a stdio server",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    try:
        asyncio.run(run(parser.parse_args()))
    except KeyboardInterrupt:
        return 130
    except (
        RequestError,
        ProtocolError,
        EOFError,
        OSError,
        ValueError,
        TimeoutError,
    ) as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
