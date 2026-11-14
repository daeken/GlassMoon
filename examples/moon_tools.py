"""Read-only text sizing helper for byte-budgeted prompts and IPC payloads."""

import hashlib
import json


def text_budget(text, budget_bytes=61440):
    """Measure raw UTF-8 and compact JSON string sizes, not model tokens."""
    if not isinstance(text, str):
        raise TypeError("text must be a string")
    if (
        isinstance(budget_bytes, bool)
        or not isinstance(budget_bytes, int)
        or budget_bytes < 0
    ):
        raise ValueError("budget_bytes must be a nonnegative integer")
    raw = text.encode("utf-8")
    encoded = json.dumps(text, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    return {
        "characters": len(text),
        "utf8_bytes": len(raw),
        "json_string_bytes": len(encoded),
        "budget_bytes": budget_bytes,
        "json_string_fits": len(encoded) <= budget_bytes,
        "remaining_bytes": budget_bytes - len(encoded),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }


TOOLS = {
    "text_budget": {
        "description": "Measure UTF-8 and compact JSON string bytes against a byte budget; includes SHA-256. Excludes enclosing request fields and is not a token count. No filesystem access.",
        "parameters": {
            "type": "object",
            "properties": {
                "text": {"type": "string"},
                "budget_bytes": {"type": "integer", "minimum": 0, "default": 61440},
            },
            "required": ["text"],
            "additionalProperties": False,
        },
        "handler": text_budget,
    }
}
