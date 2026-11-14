"""Model provider for DeepSeek and Z.ai's GLM Coding Plan."""

import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path

import httpx

from . import functions
from .ipc import ProtocolError, RequestError
from .models import (
    CATALOG,
    GENERATE,
    capabilities,
    encoded_size,
    input_modalities,
    message,
    modalities,
    within_reply_limit,
)
from .service import add_service_args, fail, serve
from .sse import events


def descriptor(model, inputs, max_output):
    return {
        "id": model,
        "input_modalities": inputs,
        "output_modalities": ["text"],
        "context_tokens": 1_000_000,
        "max_output_tokens": max_output,
        "streaming": True,
        "tool_calls": True,
    }


@dataclass(frozen=True)
class Provider:
    url: str
    key: str
    models: tuple
    options: dict


PROVIDERS = {
    "deepseek": Provider(
        "https://api.deepseek.com/chat/completions",
        "DEEPSEEK_API_KEY",
        (
            descriptor("deepseek-flash", ["text", "image"], 384_000),
            descriptor("deepseek-v4-pro", ["text"], 384_000),
        ),
        {"thinking": {"type": "disabled"}},
    ),
    "zai": Provider(
        "https://api.z.ai/api/coding/paas/v4/chat/completions",
        "ZAI_API_KEY",
        (
            descriptor("glm-5.3", ["text"], 128_000),
            descriptor("glm-5.3-flash", ["text", "image"], 128_000),
        ),
        {"thinking": {"type": "enabled"}, "reasoning_effort": "low"},
    ),
}


def chat_message(item):
    role = item["role"]
    content = item["content"]
    if all(part["type"] == "text" for part in content):
        payload = "".join(part["text"] for part in content)
    else:
        payload = []
        for part in content:
            if part["type"] == "text":
                payload.append(part)
            elif (
                part["type"] == "image"
                and role == "user"
                and isinstance(part.get("url"), str)
            ):
                payload.append({"type": "image_url", "image_url": {"url": part["url"]}})
            else:
                raise fail(
                    "unsupported_content", f"Cannot encode {part['type']} for {role}"
                )
    return {"role": "system" if role == "developer" else role, "content": payload}


def exchange_messages(exchange):
    announced = exchange["calls"]
    assistant = {
        "role": "assistant",
        "content": exchange.get("text") or None,
        "tool_calls": [
            {
                "id": item["id"],
                "type": "function",
                "function": {"name": item["name"], "arguments": item["arguments"]},
            }
            for item in announced
        ],
    }
    if reasoning := exchange.get("state", {}).get("reasoning"):
        assistant["reasoning_content"] = reasoning
    return [
        assistant,
        *(
            {
                "role": "tool",
                "tool_call_id": item["id"],
                "content": exchange["outputs"][item["id"]],
            }
            for item in announced
        ),
    ]


class Completion:
    def __init__(self):
        self.content = []
        self.reasoning = []
        self.calls = {}
        self.finish = None
        self.usage = None
        self.size = 0

    async def receive(self, event, emit):
        if event.get("error"):
            raise fail(
                "provider_response", "Provider returned an error during generation"
            )
        if event.get("usage") is not None:
            self.usage = event["usage"]
        for choice in event["choices"]:
            if choice["index"] != 0:
                continue
            delta = choice.get("delta", choice.get("message")) or {}
            self.size += encoded_size(delta)
            # Count actual accumulated content below. The raw event also bounds
            # fragmented function calls and reasoning in unexpectedly long streams.
            if self.size > 4 * 1024 * 1024:
                raise fail(
                    "reply_too_large", "Provider response exceeded the stream budget"
                )
            if text := delta.get("content"):
                self.content.append(text)
                await emit({"type": "text", "text": text})
            if reasoning := delta.get("reasoning_content"):
                self.reasoning.append(reasoning)
            for position, item in enumerate(delta.get("tool_calls") or []):
                index = item.get("index", position)
                current = self.calls.setdefault(
                    index, {"id": "", "name": "", "arguments": ""}
                )
                function = item.get("function") or {}
                current["id"] += item.get("id") or ""
                current["name"] += function.get("name") or ""
                arguments = function.get("arguments")
                current["arguments"] += (
                    json.dumps(arguments)
                    if isinstance(arguments, dict)
                    else arguments or ""
                )
            if choice.get("finish_reason") is not None:
                self.finish = choice["finish_reason"]

    def result(self):
        if self.finish is None:
            raise fail(
                "stream_interrupted",
                "Provider disconnected before completing the response",
            )
        if self.finish not in ("stop", "tool_calls"):
            raise fail(
                "provider_incomplete", "Provider stopped before completing the response"
            )
        text = "".join(self.content)
        reply = {"message": {"role": "assistant", "content": text}, "usage": self.usage}
        if self.calls:
            reply["tool_calls"] = functions.calls(
                [call for _, call in sorted(self.calls.items())]
            )
            if self.reasoning:
                reply["tool_state"] = {"reasoning": "".join(self.reasoning)}
        elif self.finish == "tool_calls" or not text:
            raise fail(
                "empty_response", "Provider returned neither text nor a function call"
            )
        return within_reply_limit(reply)


class ChatProvider:
    def __init__(self, provider, client, key, catalog):
        self.provider = provider
        self.client = client
        self.headers = {"Authorization": f"Bearer {key}"}
        self.models = {}
        for item in catalog:
            model = capabilities(item)
            if (
                model["id"] in self.models
                or not set(model["input_modalities"]) <= {"text", "image"}
                or set(model["output_modalities"]) != {"text"}
            ):
                raise fail(
                    "invalid_catalog",
                    "Use unique IDs, text/image inputs, and text outputs",
                )
            self.models[model["id"]] = model
        if not self.models:
            raise fail("invalid_catalog", "Configure at least one model")

    async def handle(self, name, params, emit, stream):
        if name == "models":
            return {"models": list(self.models.values())}
        if name != "generate":
            raise fail("unknown_method", "Unknown model provider method")
        model = self.models.get(params["model"])
        if model is None:
            raise fail("unknown_model", "Model is absent from the provider catalog")
        messages = [message(item) for item in params["messages"]]
        if not messages:
            raise fail("empty_conversation", "Supply at least one message")
        if not input_modalities(messages) <= set(
            model["input_modalities"]
        ) or not modalities(params.get("output_modalities", ["text"])) <= set(
            model["output_modalities"]
        ):
            raise fail(
                "unsupported_modalities",
                "The model does not support the requested modalities",
            )
        if stream and not model["streaming"]:
            raise fail(
                "stream_unsupported", "The model is configured for buffered replies"
            )
        tools = functions.definitions(params)
        rounds = functions.history(params)
        wire_messages = [chat_message(item) for item in messages]
        for exchange in rounds:
            wire_messages.extend(exchange_messages(exchange))
        body = self.provider.options | {
            "model": model["id"],
            "messages": wire_messages,
            "stream": stream,
        }
        if tools:
            if not model.get("tool_calls"):
                raise fail(
                    "tools_unsupported",
                    "The model is not configured for function calling",
                )
            body["tools"] = [{"type": "function", "function": tool} for tool in tools]
        completion = Completion()
        try:
            if stream:
                async with self.client.stream(
                    "POST", self.provider.url, headers=self.headers, json=body
                ) as response:
                    check_status(response)
                    async for event in events(response):
                        await completion.receive(event, emit)
            else:
                response = await self.client.post(
                    self.provider.url, headers=self.headers, json=body
                )
                check_status(response)
                try:
                    event = response.json()
                except ValueError:
                    raise fail(
                        "provider_protocol", "Provider sent invalid JSON"
                    ) from None
                await completion.receive(event, discard)
        except httpx.RequestError:
            raise fail(
                "provider_connection", "The connection to the model provider failed"
            ) from None
        return completion.result()


async def discard(_):
    pass


def check_status(response):
    if response.status_code in (401, 403):
        raise fail(
            "provider_auth", f"Provider rejected access (HTTP {response.status_code})"
        )
    if response.status_code == 429:
        raise fail("provider_rate_limit", "Provider rate limit or plan quota reached")
    if response.status_code != 200:
        raise fail("provider_http", f"Provider returned HTTP {response.status_code}")


async def run(args):
    provider = PROVIDERS[args.provider]
    key = os.environ.get(provider.key, "").strip()
    if not key:
        raise ValueError(f"Set {provider.key} before starting this provider")
    catalog = (
        json.loads(Path(args.catalog).read_text())["models"]
        if args.catalog
        else provider.models
    )
    async with httpx.AsyncClient(timeout=httpx.Timeout(90, connect=15)) as client:
        adapter = ChatProvider(provider, client, key, catalog)
        await serve(
            args.socket,
            args.name or args.provider,
            args.replace,
            {"models": CATALOG, "generate": GENERATE},
            adapter.handle,
            kind="model_provider",
            reconnect=True,
            maintainer=args.maintainer,
        )


def main():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("provider", choices=PROVIDERS)
    add_service_args(parser, None)
    parser.add_argument("--catalog", help="Optional JSON catalog of model capabilities")
    try:
        asyncio.run(run(parser.parse_args()))
    except KeyboardInterrupt:
        return 130
    except (RequestError, ProtocolError, EOFError, OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
