"""GlassMoon conversation service backed by ChatGPT plan usage."""

import argparse
import asyncio
import json
from pathlib import Path

import httpx

from .chatgpt_auth import DEFAULT_PROFILE, RESOURCE, AuthError, Profile, login
from .ipc import ProtocolError, RequestError
from .models import (
    ARRAY,
    CATALOG,
    GENERATE,
    INTEGER,
    MESSAGE,
    TEXT,
    Transcripts,
    capabilities,
    input_modalities,
    message,
    modalities,
    within_reply_limit,
)
from .service import add_service_args, fail, method, serve

METHODS = {
    "models": CATALOG,
    "available_models": method("List the connected ChatGPT account's available models"),
    "generate": GENERATE,
    "start": method("Start a conversation for a configured model", {"model": TEXT}),
    "add": method(
        "Append a message to a conversation",
        {"conversation": TEXT, "message": MESSAGE},
        {"expected_revision": INTEGER},
    ),
    "history": method(
        "Read a page of conversation messages",
        {"conversation": TEXT},
        {"before": INTEGER},
    ),
    "respond": method(
        "Generate and store a response",
        {"conversation": TEXT},
        {"output_modalities": ARRAY},
    ),
}

STATE = Path.home() / ".local" / "state" / "glassmoon" / "openai.sqlite3"


def openai_message(item):
    role = item["role"]
    content = []
    for part in item["content"]:
        kind = part["type"]
        if kind == "text":
            value = {
                "type": "output_text" if role == "assistant" else "input_text",
                "text": part["text"],
            }
        elif kind == "image" and role == "user" and isinstance(part.get("url"), str):
            value = {"type": "input_image", "image_url": part["url"], "detail": "auto"}
        elif kind == "file" and role == "user" and isinstance(part.get("url"), str):
            value = {"type": "input_file", "file_url": part["url"]}
        else:
            raise fail(
                "unsupported_content",
                f"OpenAI cannot use this {kind} part with role {role}",
            )
        content.append(value)
    return {"type": "message", "role": role, "content": content}


async def events(response):
    data = []
    async for line in response.aiter_lines():
        if line.startswith("data:"):
            data.append(line[5:].lstrip(" "))
        elif not line and data:
            payload = "\n".join(data)
            data.clear()
            if payload != "[DONE]":
                try:
                    event = json.loads(payload)
                except ValueError:
                    raise fail(
                        "provider_protocol", "OpenAI sent invalid event data"
                    ) from None
                if not isinstance(event, dict):
                    raise fail("provider_protocol", "OpenAI sent an invalid event")
                yield event


def check_status(response):
    if response.status_code == 401:
        raise fail(
            "chatgpt_auth", "OpenAI rejected the ChatGPT credentials. Sign in again."
        )
    if response.status_code != 200:
        raise fail("openai_http", f"OpenAI returned HTTP {response.status_code}")


class OpenAI:
    def __init__(self, profile, client, catalog, transcripts):
        self.profile = profile
        self.client = client
        self.catalog = {}
        for model in catalog:
            model = capabilities(model)
            if (
                model["id"] in self.catalog
                or not set(model["input_modalities"]) <= {"text", "image", "file"}
                or model["output_modalities"] != ["text"]
            ):
                raise fail(
                    "invalid_catalog",
                    "Use unique model IDs, text/image/file input, and text output",
                )
            self.catalog[model["id"]] = model
        if not self.catalog:
            raise fail("invalid_catalog", "Configure at least one model")
        self.transcripts = transcripts

    def model(self, identifier):
        if identifier not in self.catalog:
            raise fail("unknown_model", "Model is absent from the configured catalog")
        return self.catalog[identifier]

    async def headers(self):
        try:
            token = await asyncio.to_thread(self.profile.access_token)
        except AuthError as error:
            raise fail("chatgpt_auth", str(error)) from None
        return {"Authorization": f"Bearer {token}"}

    async def generate(self, params, emit, stream):
        model = self.model(params["model"])
        messages = [message(item) for item in params["messages"]]
        desired = modalities(params.get("output_modalities", ["text"]))
        if not messages:
            raise fail("empty_conversation", "Add a message before requesting a reply")
        if not input_modalities(messages) <= set(
            model["input_modalities"]
        ) or not desired <= set(model["output_modalities"]):
            raise fail(
                "unsupported_modalities",
                "The model does not support the requested modalities",
            )
        if stream and not model["streaming"]:
            raise fail(
                "stream_unsupported", "This model is configured for buffered replies"
            )
        body = {
            "model": model["id"],
            "input": [openai_message(item) for item in messages],
            "store": False,
            "stream": True,
        }
        text = []
        size = 0
        try:
            async with self.client.stream(
                "POST",
                f"{RESOURCE}/responses",
                json=body,
                headers=await self.headers(),
            ) as response:
                check_status(response)
                async for event in events(response):
                    match event.get("type"):
                        case "response.output_text.delta" | "response.refusal.delta":
                            delta = event["delta"]
                            text.append(delta)
                            size += (
                                len(json.dumps(delta, ensure_ascii=False).encode()) - 2
                            )
                            if size > 57 * 1024:
                                raise fail(
                                    "reply_too_large",
                                    "OpenAI response exceeds the GlassMoon frame limit",
                                )
                            if stream:
                                await emit({"type": "text", "text": delta})
                        case "response.completed":
                            upstream = event["response"]
                            content = [
                                {
                                    "type": "text",
                                    "text": part.get("text", part.get("refusal", "")),
                                }
                                for item in upstream.get("output", [])
                                if item.get("type") == "message"
                                for part in item.get("content", [])
                                if part.get("type") in ("output_text", "refusal")
                            ]
                            if not content and not text:
                                raise fail(
                                    "empty_response", "OpenAI returned no text content"
                                )
                            return within_reply_limit(
                                {
                                    "message": {
                                        "role": "assistant",
                                        "content": content
                                        or [{"type": "text", "text": "".join(text)}],
                                    },
                                    "model": model["id"],
                                    "usage": upstream.get("usage"),
                                }
                            )
                        case "response.failed" | "response.incomplete" | "error":
                            details = event.get("response", {}).get("error") or {}
                            code = details.get("code") or event.get("code")
                            # Keep provider errors free of credentials and echoed inputs.
                            if code in (
                                "subscription_sharing_usage_limit_exceeded",
                                "subscription_sharing_usage_unavailable",
                            ):
                                raise fail(
                                    code, "ChatGPT plan usage is limited or unavailable"
                                )
                            raise fail(
                                "openai_response", f"OpenAI reported {event['type']}"
                            )
        except httpx.RequestError:
            raise fail("openai_connection", "The connection to OpenAI failed") from None
        raise fail(
            "stream_interrupted", "OpenAI disconnected before completing the response"
        )

    async def handle(self, name, params, emit, stream):
        if name == "models":
            return {"models": list(self.catalog.values())}
        if name == "available_models":
            try:
                response = await self.client.get(
                    f"{RESOURCE}/models", headers=await self.headers()
                )
                check_status(response)
                return {
                    "models": [
                        {"id": item["slug"], "display_name": item["display_name"]}
                        for item in response.json()["models"]
                        if item.get("visibility") == "list"
                    ]
                }
            except httpx.RequestError:
                raise fail(
                    "openai_connection", "The connection to OpenAI failed"
                ) from None
        if name == "generate":
            return await self.generate(params, emit, stream)
        if name == "start":
            model = self.model(params["model"])
            return {"conversation": self.transcripts.create(model["id"])}
        identifier = params["conversation"]
        if name == "history":
            return self.transcripts.history(identifier, params.get("before"))
        model, revision, messages = self.transcripts.read(identifier)
        if name == "add":
            item = within_reply_limit(message(params["message"]))
            if not input_modalities([item]) <= set(
                self.model(model)["input_modalities"]
            ):
                raise fail(
                    "unsupported_modalities",
                    "This model does not accept the message modality",
                )
            expected = params.get("expected_revision", revision)
            if type(expected) is not int or expected < 0:
                raise fail(
                    "invalid_revision",
                    "expected_revision must be a nonnegative integer",
                )
            return {"revision": self.transcripts.append(identifier, expected, item)}
        if name == "respond":
            result = await self.generate(
                {
                    "model": model,
                    "messages": messages,
                    "output_modalities": params.get("output_modalities", ["text"]),
                },
                emit,
                stream,
            )
            within_reply_limit(
                result | {"conversation": identifier, "revision": revision + 1}
            )
            return result | {
                "conversation": identifier,
                "revision": self.transcripts.append(
                    identifier, revision, result["message"]
                ),
            }
        raise fail("unknown_method", "Unknown conversation method")


async def run(args):
    profile = Profile(args.profile)
    catalog = json.loads(Path(args.catalog).read_text())["models"]
    transcripts = Transcripts(args.state)
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(90, connect=15)) as client:
            provider = OpenAI(profile, client, catalog, transcripts)
            await serve(
                args.socket,
                args.name,
                args.replace,
                METHODS,
                provider.handle,
                kind="model_provider",
            )
    finally:
        transcripts.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    sign_in = commands.add_parser("login", help="Continue with ChatGPT")
    worker = commands.add_parser("serve", help="Start the OpenAI service")
    add_service_args(worker, "openai")
    for command in (sign_in, worker):
        command.add_argument("--profile", default=DEFAULT_PROFILE)
    worker.add_argument("--catalog", required=True)
    worker.add_argument("--state", default=STATE)
    args = parser.parse_args()
    try:
        if args.command == "login":
            login(Profile(args.profile))
        else:
            asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130
    except (
        AuthError,
        RequestError,
        ProtocolError,
        EOFError,
        OSError,
        ValueError,
    ) as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
