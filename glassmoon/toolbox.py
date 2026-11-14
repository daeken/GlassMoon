"""Publish Python functions as a discoverable GlassMoon tool service."""

import asyncio
import hashlib
import hmac
import importlib.util
import inspect
import re
from pathlib import Path

from .ipc import ProtocolError, RequestError, call
from .models import OBJECT, TEXT
from .service import fail, method, serve, service_args

METHODS = {
    "health": method("Check that the worker can answer and report its tool count"),
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
    def __init__(self, functions, invoke, *, source_sha256=None):
        self.functions = {validate_name(name): spec for name, spec in functions.items()}
        if any(
            type(spec.get("returns_image", False)) is not bool
            for spec in self.functions.values()
        ):
            raise ValueError("returns_image must be a boolean")
        self.invoke = invoke
        self.source_sha256 = source_sha256

    async def handle(self, name, params, _emit, _stream):
        if name == "health":
            result = {"status": "ok", "tools": len(self.functions)}
            if self.source_sha256 is not None:
                result["source_sha256"] = self.source_sha256
            return result
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


def load(path, expected_sha256=None):
    source = Path(path).resolve()
    spec = importlib.util.spec_from_file_location("glassmoon_published_tools", source)
    if spec is None or spec.loader is None:
        raise ValueError("Expected the path to a Python module")
    module = importlib.util.module_from_spec(spec)
    if expected_sha256 is None:
        spec.loader.exec_module(module)
    else:
        data = source.read_bytes()
        if not hmac.compare_digest(hashlib.sha256(data).hexdigest(), expected_sha256):
            raise fail("source_changed", "Python tool source did not match the reviewed SHA-256")
        # Execute the verified entry point without reading it a second time.
        exec(compile(data, str(source), "exec"), module.__dict__)  # noqa: S102
    declarations = getattr(module, "TOOLS", None)
    if not isinstance(declarations, dict):
        raise TypeError("The Python tool module must export a TOOLS mapping")
    for name, declaration in declarations.items():
        validate_name(name)
        if (
            not isinstance(declaration, dict)
            or not callable(declaration.get("handler"))
            or not isinstance(declaration.get("description"), str)
            or not isinstance(declaration.get("parameters"), dict)
        ):
            raise TypeError(f"Invalid declaration for {name}")

    async def invoke(name, arguments):
        handler = declarations[name]["handler"]
        if inspect.iscoroutinefunction(handler):
            return await handler(**arguments)
        return await asyncio.to_thread(handler, **arguments)

    return ToolService(declarations, invoke, source_sha256=expected_sha256)


async def list_tools(socket, service):
    response = await call(socket, service, "tools", {})
    return response["tools"]


async def inspect_tool(socket, service, name):
    generation, tools = await tool_catalog(socket, service)
    for definition in tools:
        if definition["name"] == name:
            return {"generation": generation, "tool": definition}
    raise fail("unknown_tool", "This service does not export the requested function")


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
    parser.add_argument("--sha256", help="Require this reviewed SHA-256 before executing the module")
    parser.add_argument(
        "--staged", action="store_true", help="Wait for a separately probed activation"
    )
    args = parser.parse_args()
    if args.staged and args.replace is not None:
        parser.error("--staged and --replace cannot be used together")
    if args.sha256 is not None and not re.fullmatch(r"[0-9a-f]{64}", args.sha256):
        parser.error("--sha256 expects 64 lowercase hexadecimal characters")
    try:
        service = load(args.module, args.sha256)
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
