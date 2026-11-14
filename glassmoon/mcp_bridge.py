"""Expose one MCP server over the standard GlassMoon tool service interface."""

import argparse
import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path

import httpx2
from mcp import Client, StdioServerParameters
from mcp.client.streamable_http import streamable_http_client

from .evidence import screenshot_bytes
from .ipc import ProtocolError, RequestError
from .service import fail, serve
from .toolbox import METHODS, ToolService

MCP_TIMEOUT = 25


class Bridge:
    def __init__(self, target, headers=None, image_tools=()):
        self.target = target
        self.headers = headers or {}
        self.image_tools = frozenset(image_tools)

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
                        "returns_image": tool.name in self.image_tools,
                    }
                cursor = page.next_cursor
                if cursor is None:
                    return tools
        raise fail("mcp_catalog", "MCP tool catalog exceeds the page limit")

    async def check_catalog(self, client, registered):
        try:
            current = await self.tools(client)
        except Exception as error:  # noqa: BLE001 - upstream failures can contain connection details.
            raise fail("mcp_catalog_unavailable", f"Could not read the MCP catalog ({type(error).__name__})") from None
        changes = {
            "added": sorted(current.keys() - registered.keys()),
            "removed": sorted(registered.keys() - current.keys()),
            "changed": sorted(name for name in registered.keys() & current.keys() if registered[name] != current[name]),
        }
        total_changes = sum(map(len, changes.values()))
        return {
            "status": "changed" if total_changes else "ok",
            "tools": len(current),
            "change_count": total_changes,
            **{kind: [name[:128] for name in names[:12]] for kind, names in changes.items()},
        }

    async def invoke(self, client, name, arguments):
        try:
            async with asyncio.timeout(MCP_TIMEOUT):
                result = await client.call_tool(name, arguments)
                payload = result.model_dump(mode="json", by_alias=True, exclude_none=True)
                if name in self.image_tools:
                    self.extract_image(payload)
                return payload
        except TimeoutError:
            raise fail("mcp_timeout", "MCP tool did not respond in time") from None
        except Exception as error:  # noqa: BLE001 - sanitize transport and server failures.
            raise fail(
                "mcp_call_failed", f"MCP call failed ({type(error).__name__})"
            ) from None

    @staticmethod
    def extract_image(payload):
        for index, block in enumerate(payload.get("content", [])):
            if block.get("type") != "image":
                continue
            image = {"type": "image", "url": f"data:{block['mimeType']};base64,{block['data']}"}
            accepted = False
            if "image" not in payload:
                try:
                    screenshot_bytes(image)
                    accepted = True
                except RequestError:
                    pass
            if accepted:
                payload["image"] = image
            payload["content"][index] = {
                "type": "text", "text": "MCP image attachment" if accepted else "MCP image omitted",
            }


async def run(args):
    if args.url:
        if args.command:
            raise ValueError("Choose a remote URL or a local command")
        key = os.environ.get(args.bearer_env) if args.bearer_env else None
        if args.bearer_env and not key:
            raise ValueError(f"Set {args.bearer_env} before starting this MCP bridge")
        bridge = Bridge(args.url, {"Authorization": f"Bearer {key}"} if key else None, args.image_tool)
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
            ),
            image_tools=args.image_tool,
        )
    async with bridge.client() as client:
        functions = await bridge.tools(client)

        async def invoke(name, arguments):
            return await bridge.invoke(client, name, arguments)

        service = ToolService(functions, invoke)

        async def handle(name, params, emit, stream):
            if name == "health":
                return await bridge.check_catalog(client, functions)
            return await service.handle(name, params, emit, stream)

        await serve(
            args.socket,
            args.name,
            args.replace,
            METHODS,
            handle,
            staged=args.staged,
            reconnect=True,
            maintainer=args.maintainer,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--replace", type=int)
    parser.add_argument("--staged", action="store_true", help="Wait for a separately probed activation")
    parser.add_argument("--maintainer", help="GlassMoon agent identity responsible for this service")
    parser.add_argument("--image-tool", action="append", default=[], help="Expose a tool's first bounded JPEG or PNG result as an image")
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
    args = parser.parse_args()
    if args.staged and args.replace is not None:
        parser.error("--staged and --replace cannot be used together")
    try:
        asyncio.run(run(args))
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
