"""JSON payloads from provider Server-Sent Events streams."""

import json

from .service import fail


async def events(response):
    data = []
    async for line in response.aiter_lines():
        if line.startswith("data:"):
            data.append(line[5:].removeprefix(" "))
        elif not line and data:
            payload = "\n".join(data)
            data.clear()
            if payload == "[DONE]":
                return
            try:
                event = json.loads(payload)
            except ValueError:
                raise fail(
                    "provider_protocol", "Provider sent invalid event data"
                ) from None
            if not isinstance(event, dict):
                raise fail("provider_protocol", "Provider sent an invalid event")
            yield event
