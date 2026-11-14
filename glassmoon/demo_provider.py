"""Offline model provider that echoes text for local development."""

import asyncio

from .ipc import ProtocolError, RequestError
from .models import CATALOG, GENERATE, message, result_text
from .service import fail, serve, service_args

MODEL = {
    "id": "echo",
    "input_modalities": ["text"],
    "output_modalities": ["text"],
    "context_tokens": 16_384,
    "streaming": True,
}


async def handle(name, params, emit, stream):
    if name == "models":
        return {"models": [MODEL]}
    if name != "generate" or params["model"] != MODEL["id"]:
        raise fail("unknown_model", "The demo provider serves the echo model")
    user = next(
        (item for item in reversed(params["messages"]) if item["role"] == "user"), None
    )
    if user is None:
        raise fail("invalid_message", "The demo provider expects a user message")
    text = "Echo: " + result_text(message(user)["content"])
    if stream:
        for offset in range(0, len(text), 256):
            await emit({"type": "text", "text": text[offset : offset + 256]})
    return {"message": {"role": "assistant", "content": text}, "model": MODEL["id"]}


def main():
    parser = service_args(__doc__, "demo")
    args = parser.parse_args()
    try:
        asyncio.run(
            serve(
                args.socket,
                args.name,
                args.replace,
                {"models": CATALOG, "generate": GENERATE},
                handle,
                kind="model_provider",
                maintainer=args.maintainer,
                reconnect=True,
            )
        )
    except KeyboardInterrupt:
        return 130
    except (RequestError, ProtocolError, EOFError, OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
