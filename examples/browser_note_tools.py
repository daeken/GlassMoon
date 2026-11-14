"""Pure action-plan helper for the local browser note fixture; performs no IO."""


def note_actions(text):
    """Return browser action steps without executing them."""
    if not isinstance(text, str):
        raise TypeError("text must be a string")
    if not text or len(text) > 200:
        raise ValueError("text must contain 1 to 200 characters")
    return [
        {
            "action": "element",
            "operation": "fill",
            "by": "placeholder",
            "name": "Note",
            "text": text,
        },
        {
            "action": "element",
            "operation": "click",
            "by": "role",
            "role": "button",
            "name": "Save note",
        },
        {
            "action": "wait_for",
            "selector": "#status",
            "text": "Saved: " + text,
            "exact": True,
        },
    ]


TOOLS = {
    "note_actions": {
        "description": "Return JSON-compatible browser action steps to fill Note, click Save note, and wait for the exact saved status. Does not execute actions or perform IO.",
        "parameters": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "minLength": 1, "maxLength": 200},
            },
            "required": ["text"],
            "additionalProperties": False,
        },
        "handler": note_actions,
    }
}
