"""Discover live model providers and select a model by capability."""

import asyncio

from . import functions
from .ipc import ProtocolError, RequestError, call, connect
from .models import (
    ARRAY,
    BOOLEAN,
    GENERATE,
    INTEGER,
    TEXT,
    capabilities,
    input_modalities,
    message,
    modalities,
    within_reply_limit,
)
from .service import fail, method, serve, service_args

METHODS = {
    "models": method("List models advertised by active providers"),
    "generate": method(
        "Route generation to a model that satisfies the requested capabilities",
        {"messages": GENERATE["params_schema"]["properties"]["messages"]},
        {
            "input_modalities": ARRAY,
            "output_modalities": ARRAY,
            "min_context_tokens": INTEGER,
            "provider": TEXT,
            "provider_generation": INTEGER,
            "model": TEXT,
            "tool_images": BOOLEAN,
            **functions.TOOL_FIELDS,
        },
    ),
}


class Router:
    def __init__(self, socket):
        self.socket = socket

    async def catalog(self, service):
        provider = service["name"]
        try:
            async with asyncio.timeout(4):
                result = await call(
                    self.socket,
                    provider,
                    "models",
                    {},
                    version=service["interface_version"],
                    generation=service["generation"],
                )
            entries = [capabilities(model) for model in result["models"]]
        except (
            RequestError,
            ProtocolError,
            EOFError,
            OSError,
            ValueError,
            TimeoutError,
            KeyError,
            TypeError,
            AttributeError,
        ):
            return [], provider
        return [
            entry
            | {
                "provider": provider,
                "provider_generation": service["generation"],
                "interface_version": service["interface_version"],
            }
            for entry in entries
        ], None

    async def models(self):
        async with connect(self.socket) as wire:
            services = (await wire.request("list", "list"))["result"]["services"]
        results = await asyncio.gather(
            *(
                self.catalog(item)
                for item in services
                if item["state"] == "active" and item.get("kind") == "model_provider"
            )
        )
        models = [model for entries, _ in results for model in entries]
        unavailable = sorted(
            provider for _, provider in results if provider is not None
        )
        return sorted(
            models, key=lambda model: (model["provider"], model["id"])
        ), unavailable

    async def handle(self, name, params, emit, stream):
        if name == "models":
            models, unavailable = await self.models()
            return {"models": models, "unavailable": unavailable}
        if name != "generate":
            raise fail("unknown_method", "Unknown router method")
        messages = [message(item) for item in params["messages"]]
        if not messages:
            raise fail("empty_conversation", "Supply at least one message")
        inputs = input_modalities(messages)
        if "input_modalities" in params:
            inputs |= modalities(params["input_modalities"])
        outputs = modalities(params.get("output_modalities", ["text"]))
        tools = functions.definitions(params)
        rounds = functions.history(params)
        generation = params.get("provider_generation")
        if generation is not None and (
            type(generation) is not int or generation <= 0 or not params.get("provider")
        ):
            raise fail("invalid_provider_generation", "Specify a provider and positive generation")
        images = params.get("tool_images", False)
        if type(images) is not bool:
            raise fail("invalid_tool_images", "tool_images must be a boolean")
        images |= any(
            isinstance(output, list)
            for exchange in rounds
            for output in exchange["outputs"].values()
        )
        if rounds and (
            not tools or not params.get("provider") or not params.get("model")
        ):
            raise fail(
                "invalid_tool_history",
                "Pin continuation to the provider and model that requested the calls",
            )
        min_context = params.get("min_context_tokens", 1)
        if type(min_context) is not int or min_context < 1:
            raise fail(
                "invalid_context", "min_context_tokens must be a positive integer"
            )

        models, _ = await self.models()
        if generation is not None and not any(
            item["provider"] == params["provider"] and item["provider_generation"] == generation
            for item in models
        ):
            raise fail("provider_changed", "The selected provider generation is no longer available")
        selected = next(
            (
                item
                for item in models
                if (
                    not params.get("provider") or item["provider"] == params["provider"]
                )
                and (not params.get("model") or item["id"] == params["model"])
                and (generation is None or item["provider_generation"] == generation)
                and inputs <= set(item["input_modalities"])
                and outputs <= set(item["output_modalities"])
                and item["context_tokens"] >= min_context
                and (not stream or item["streaming"])
                and (not tools or item.get("tool_calls", False))
                and (
                    not images
                    or (
                        item.get("tool_images", False)
                        and "image" in item["input_modalities"]
                    )
                )
            ),
            None,
        )
        if selected is None:
            raise fail(
                "no_matching_model",
                "No active provider has a model that meets the requirements",
            )

        provider = selected["provider"]
        request = {
            "model": selected["id"],
            "messages": messages,
            "output_modalities": sorted(outputs),
        }
        if tools:
            request["tools"] = tools
        if rounds:
            request["tool_history"] = rounds

        async def forward(chunk):
            if not isinstance(chunk, dict) or chunk.get("type") not in outputs:
                raise fail(
                    "invalid_provider_reply", "Provider streamed an unexpected modality"
                )
            await emit(chunk)

        try:
            result = await call(
                self.socket,
                provider,
                "generate",
                request,
                version=selected["interface_version"],
                generation=selected["provider_generation"],
                on_chunk=forward if stream else None,
            )
        except RequestError as error:
            if error.code == "generation_unavailable" and error.generation is None:
                raise fail("provider_changed", "The selected provider generation changed") from None
            raise
        except (OSError, EOFError):
            raise fail(
                "provider_unavailable", f"Connection to {provider} ended"
            ) from None
        reply = message(result["message"])
        announced = functions.calls(result.get("tool_calls", []))
        if announced and not tools:
            raise fail(
                "invalid_provider_reply",
                "Provider requested tools that were not enabled",
            )
        actual = input_modalities([reply])
        if reply["role"] != "assistant" or not actual <= outputs:
            raise fail(
                "invalid_provider_reply",
                "Provider returned an unexpected role or modality",
            )
        return within_reply_limit(
            result | {
                "message": reply,
                "model": selected["id"],
                "provider": provider,
                "provider_generation": selected["provider_generation"],
            }
        )


def main():
    parser = service_args(__doc__, "models")
    args = parser.parse_args()
    try:
        asyncio.run(
            serve(
                args.socket,
                args.name,
                args.replace,
                METHODS,
                Router(args.socket).handle,
                reconnect=True,
                maintainer=args.maintainer,
            )
        )
    except KeyboardInterrupt:
        return 130
    except (RequestError, ProtocolError, EOFError, OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
