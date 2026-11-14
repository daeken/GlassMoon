"""Provider-neutral function declarations and call/result exchanges."""

import re

from .service import fail

TOOL_FIELDS = {"tools": {"type": "array"}, "tool_history": {"type": "array"}}


def definitions(params):
    tools = params.get("tools", [])
    if not isinstance(tools, list) or len(tools) > 32:
        raise fail("invalid_tools", "Supply up to 32 function definitions")
    seen = set()
    for tool in tools:
        name = tool["name"]
        if (
            not isinstance(name, str)
            or not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", name)
            or name in seen
            or not isinstance(tool["description"], str)
            or not isinstance(tool["parameters"], dict)
        ):
            raise fail(
                "invalid_tools",
                "Each function needs a unique name, description, and parameter schema",
            )
        seen.add(name)
    return [
        {
            "name": tool["name"],
            "description": tool["description"],
            "parameters": tool["parameters"],
        }
        for tool in tools
    ]


def calls(items):
    if not isinstance(items, list) or len(items) > 16:
        raise fail("invalid_tool_calls", "Expected up to 16 function calls")
    output = []
    seen = set()
    for item in items:
        call = {key: item.get(key) for key in ("id", "name", "arguments")}
        if (
            not all(isinstance(value, str) for value in call.values())
            or not call["id"]
            or not call["name"]
            or call["id"] in seen
        ):
            raise fail(
                "invalid_tool_calls", "Provider returned an invalid function call"
            )
        seen.add(call["id"])
        output.append(call)
    return output


def history(params):
    rounds = params.get("tool_history", [])
    if not isinstance(rounds, list) or len(rounds) > 8:
        raise fail("invalid_tool_history", "Expected up to eight function exchanges")
    for exchange in rounds:
        announced = calls(exchange["calls"])
        outputs = exchange["outputs"]
        if (
            not isinstance(exchange.get("text", ""), str)
            or not isinstance(exchange.get("state", {}), dict)
            or not isinstance(outputs, dict)
            or set(outputs) != {call["id"] for call in announced}
            or any(not isinstance(value, str) for value in outputs.values())
        ):
            raise fail(
                "invalid_tool_history",
                "Each function call needs exactly one string result",
            )
    return rounds
