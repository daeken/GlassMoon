"""Publish Python functions as a discoverable GlassMoon tool service."""

import asyncio
import importlib.util
import inspect
import re
from pathlib import Path

from .ipc import ProtocolError, RequestError, call
from .models import OBJECT, TEXT
from .service import fail, method, serve, service_args

METHODS = {
    "tools": method("Discover shared functions"),
    "invoke": method("Run a shared function", {"name": TEXT, "arguments": OBJECT}),
}


def tool(description, required=None, optional=None):
    return {
        "description": description,
        "parameters": method(description, required, optional)["params_schema"],
    }


def validate_name(name):
    if not isinstance(name, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", name):
        raise fail(
            "invalid_tool",
            "Tool names must contain 1–64 ASCII letters, digits, underscores, or hyphens",
        )
    return name


class ToolService:
    def __init__(self, functions, invoke):
        self.functions = {validate_name(name): spec for name, spec in functions.items()}
        if any(
            type(spec.get("returns_image", False)) is not bool
            for spec in self.functions.values()
        ):
            raise ValueError("returns_image must be a boolean")
        self.invoke = invoke

    async def handle(self, name, params, _emit, _stream):
        if name == "tools":
            return {
                "tools": [
                    {
                        "name": key,
                        "description": spec["description"],
                        "parameters": spec["parameters"],
                        **(
                            {"returns_image": True} if spec.get("returns_image") else {}
                        ),
                    }
                    for key, spec in self.functions.items()
                ]
            }
        if name == "invoke":
            tool = params["name"]
            if tool not in self.functions:
                raise fail(
                    "unknown_tool",
                    "This service does not export the requested function",
                )
            if not isinstance(params["arguments"], dict):
                raise fail("invalid_arguments", "Tool arguments must be an object")
            return {"value": await self.invoke(tool, params["arguments"])}
        raise fail("unknown_method", "Unknown tool service method")


def load(path):
    spec = importlib.util.spec_from_file_location(
        "glassmoon_published_tools", Path(path).resolve()
    )
    if spec is None or spec.loader is None:
        raise ValueError("Expected the path to a Python module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for name, declaration in module.TOOLS.items():
        validate_name(name)
        if not callable(declaration["handler"]) or not isinstance(
            declaration["parameters"], dict
        ):
            raise TypeError(f"Invalid declaration for {name}")

    async def invoke(name, arguments):
        handler = module.TOOLS[name]["handler"]
        if inspect.iscoroutinefunction(handler):
            return await handler(**arguments)
        return await asyncio.to_thread(handler, **arguments)

    return ToolService(module.TOOLS, invoke)


async def list_tools(socket, service):
    response = await call(socket, service, "tools", {})
    return response["tools"]


async def tool_catalog(socket, service):
    response = await call(socket, service, "tools", {}, envelope=True)
    return response["generation"], response["result"]["tools"]


async def call_tool(socket, service, name, arguments, *, generation=None):
    response = await call(
        socket, service, "invoke", {"name": name, "arguments": arguments},
        generation=generation,
    )
    return response["value"]


def main():
    parser = service_args(__doc__, "tools.python")
    parser.add_argument("module", help="Python module file exporting a TOOLS mapping")
    parser.add_argument(
        "--staged", action="store_true", help="Wait for a separately probed activation"
    )
    args = parser.parse_args()
    if args.staged and args.replace is not None:
        parser.error("--staged and --replace cannot be used together")
    try:
        service = load(args.module)
        asyncio.run(
            serve(
                args.socket,
                args.name,
                args.replace,
                METHODS,
                service.handle,
                staged=args.staged,
                reconnect=True,
                maintainer=args.maintainer,
            )
        )
    except KeyboardInterrupt:
        return 130
    except (
        RequestError,
        ProtocolError,
        EOFError,
        OSError,
        TypeError,
        ValueError,
    ) as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
