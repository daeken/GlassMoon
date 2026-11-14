"""Persistent chat agent, exposed as a GlassMoon service."""

import asyncio
import hashlib
import json
import re
from pathlib import Path

from . import compaction, functions
from .ipc import ProtocolError, RequestError, call
from .models import (
    INTEGER,
    MESSAGE,
    TEXT,
    Transcripts,
    encoded_size,
    message,
    modalities,
    result_text,
    within_reply_limit,
)
from .service import fail, method, serve, service_args
from .toolbox import call_tool, tool_catalog, validate_name
from .workspace_tools import Workspace

METHODS = {
    "health": method("Check that the worker can read its conversation revision"),
    "info": method("Show agent identity and model settings"),
    "tool_status": method("Probe local and shared tool availability"),
    "tool_policy": method("Read the revisioned shared tool settings"),
    "set_tool_policy": method(
        "Change shared tool membership for subsequent turns",
        {"service": TEXT, "mode": TEXT, "expected_revision": INTEGER},
    ),
    "model_preference": method("Read this agent's model selection preference"),
    "set_model_preference": method(
        "Change model selection for subsequent turns",
        {"mode": TEXT, "expected_revision": INTEGER},
        {"provider": TEXT, "model": TEXT},
    ),
    "compact": method("Summarize older complete turns into durable context"),
    "history": method(
        "Read a page of agent chat history", optional={"before": INTEGER}
    ),
    "chat": method(
        "Send text or a multimodal user message to the agent",
        optional={"text": TEXT, "message": MESSAGE},
    ),
    "run_task": method(
        "Run one mailbox attempt and store a receipt",
        {"id": TEXT, "attempt": INTEGER, "text": TEXT},
    ),
    "task_receipt": method(
        "Read an agent's durable task receipt", {"id": TEXT, "attempt": INTEGER}
    ),
}

STATE = Path.home() / ".local" / "state" / "glassmoon"
TRANSCRIPT_BUDGET = 60 * 1024
TOOL_BUDGET = 57 * 1024
MAX_TOOL_ROUNDS = 6
MAX_TOOL_CALLS = 16


def task_key(params):
    identifier, attempt = params["id"], params["attempt"]
    if not isinstance(identifier, str) or not re.fullmatch("[a-f0-9]{32}", identifier):
        raise fail("invalid_task", "Expected a mailbox task ID")
    if type(attempt) is not int or not 1 <= attempt <= 2**63 - 1:
        raise fail("invalid_task", "Expected a positive task attempt")
    return identifier, attempt


def turn_messages(instructions, history, user, budget=TRANSCRIPT_BUDGET, summary=""):
    developer = {
        "role": "developer",
        "content": [{"type": "text", "text": instructions}],
    }
    base = [developer, compaction.context_summary(summary)] if summary else [developer]
    size = encoded_size([*base, user])
    if size > budget:
        raise fail(
            "message_too_large",
            "The message and instructions exceed the agent input budget",
        )
    start = len(history)
    while start >= 2:
        turn_size = (
            encoded_size(history[start - 2]) + encoded_size(history[start - 1]) + 2
        )
        if size + turn_size > budget:
            break
        size += turn_size
        start -= 2
    return [*base, *history[start:], user]


class Agent:
    def __init__(
        self,
        socket,
        identity,
        router,
        requirements,
        instructions,
        transcripts,
        workspace=None,
        tool_services=(),
    ):
        self.socket = socket
        self.identity = identity
        self.router = router
        self.requirements = requirements
        self.instructions = instructions
        self.transcripts = transcripts
        self.conversation = transcripts.create(identifier=identity)
        self.workspace = workspace
        self.tool_services = tuple(dict.fromkeys(tool_services))

    def model_preference(self, saved=None):
        if saved is None:
            saved = self.transcripts.model_preference(self.conversation)
        revision, mode, provider, model = saved
        requirements = self.requirements.copy()
        if mode != "default":
            requirements.pop("provider", None)
            requirements.pop("model", None)
            if provider:
                requirements["provider"] = provider
            if model:
                requirements["model"] = model
        return {"revision": revision, "mode": mode, "requirements": requirements}

    def tool_policy(self, state=None):
        revision, overrides = (
            self.transcripts.tool_policy(self.conversation) if state is None else state
        )
        enabled = [
            service
            for service in self.tool_services
            if overrides.get(service) != "disable"
        ]
        enabled.extend(
            service
            for service, mode in overrides.items()
            if mode == "enable" and service not in self.tool_services
        )
        return {"revision": revision, "overrides": overrides, "enabled": enabled}

    async def discover_tools(self, emit, stream, policy=None):
        if policy is None:
            policy = self.tool_policy()
        definitions = list(self.workspace.definitions) if self.workspace else []
        routes = {}
        images = set()
        status = {
            service: {"state": "disabled", "count": 0}
            for service, mode in policy["overrides"].items()
            if mode == "disable"
        }
        async def fetch(service):
            try:
                async with asyncio.timeout(5):
                    return await tool_catalog(self.socket, service)
            except (
                RequestError, ProtocolError, EOFError, OSError, TimeoutError,
                ValueError, KeyError, TypeError,
            ):
                return None

        services = policy["enabled"]
        catalogs = await asyncio.gather(*(fetch(service) for service in services))
        for service, catalog in zip(services, catalogs, strict=True):
            try:
                if catalog is None:
                    raise fail("tool_unavailable", "Tool catalog could not be read")
                generation, remote = catalog
                image_names = {
                    tool["name"] for tool in remote if tool.get("returns_image") is True
                }
                additions = {}
                current = {item["name"] for item in definitions}
                for tool in functions.definitions({"tools": remote}):
                    name = validate_name(f"{service.replace('.', '_')}__{tool['name']}")
                    if name in current or name in additions:
                        raise fail(
                            "duplicate_tool", f"Tool {name} is already available"
                        )
                    additions[name] = tool
                updated = [
                    *definitions,
                    *(tool | {"name": name} for name, tool in additions.items()),
                ]
                definitions = functions.definitions({"tools": updated})
                routes.update(
                    (name, (service, tool["name"], generation))
                    for name, tool in additions.items()
                )
                images.update(
                    name
                    for name, tool in additions.items()
                    if tool["name"] in image_names
                )
                status[service] = {
                    "state": "available", "count": len(additions), "generation": generation,
                }
            except (RequestError, KeyError, TypeError, AttributeError):
                status[service] = {"state": "unavailable", "count": 0}
                if stream:
                    await emit(
                        {
                            "type": "status",
                            "message": f"Tool service {service} unavailable or invalid.",
                        }
                    )
        return definitions, routes, images, status

    async def execute(self, item, routes):
        if item["name"] in routes:
            service, name, generation = routes[item["name"]]
            try:
                return {
                    "value": await call_tool(
                        self.socket, service, name, json.loads(item["arguments"]),
                        generation=generation,
                    )
                }
            except RequestError as error:
                if error.code == "generation_unavailable" and error.generation is None:
                    return {
                        "error": "tool_changed",
                        "message": f"{service} changed during this turn. A new turn will discover its current tools.",
                    }
                return {"error": error.code, "message": str(error)}
            except (ProtocolError, OSError, EOFError, ValueError):
                return {"error": "tool_failed", "message": f"Could not call {service}"}
        if self.workspace:
            return await self.workspace.execute(item)
        return {
            "error": "unknown_tool",
            "message": "This agent has no tool by that name",
        }

    async def generate(self, history, user, emit, stream, summary="", *, requirements=None):
        definitions, routes, images, _ = await self.discover_tools(emit, stream)
        requirements = (self.requirements if requirements is None else requirements).copy()
        if images:
            requirements["tool_images"] = True
        rounds = []
        used_ids = set()
        for _ in range(MAX_TOOL_ROUNDS + 1):
            params = dict(requirements)
            budget = TRANSCRIPT_BUDGET
            if definitions:
                params["tools"] = definitions
                if rounds:
                    params["tool_history"] = rounds
                budget = TOOL_BUDGET - encoded_size(params)
            params["messages"] = turn_messages(
                self.instructions, history, user, budget, summary
            )
            try:
                reply = await call(
                    self.socket,
                    self.router,
                    "generate",
                    params,
                    on_chunk=emit if stream else None,
                )
            except (EOFError, OSError):
                raise fail(
                    "model_unavailable", "Connection to the model router ended"
                ) from None
            announced = functions.calls(reply.get("tool_calls", []))
            if not announced:
                return reply
            if not definitions:
                raise fail("tools_disabled", "This agent has no workspace tools")
            if (
                len(rounds) >= MAX_TOOL_ROUNDS
                or len(used_ids) + len(announced) > MAX_TOOL_CALLS
            ):
                return self.limited_reply(reply, rounds)
            ids = {item["id"] for item in announced}
            if ids & used_ids:
                raise fail("duplicate_tool_call", "The provider reused a tool call ID")
            used_ids.update(ids)
            requirements.update(provider=reply["provider"], model=reply["model"])
            if "provider_generation" in reply:
                requirements["provider_generation"] = reply["provider_generation"]
            exchange = {
                "calls": announced,
                "outputs": {},
                "text": result_text(reply["message"]["content"]),
                "state": reply.get("tool_state", {}),
            }
            rounds.append(exchange)
            for item in announced:
                if stream:
                    await emit(
                        {"type": "tool", "name": item["name"], "phase": "started"}
                    )
                result = await self.execute(item, routes)
                value = result.get("value")
                if (
                    item["name"] in images
                    and isinstance(value, dict)
                    and functions.image_part(value.get("image"))
                ):
                    image = value["image"]
                    for old in rounds:
                        for key, prior in old["outputs"].items():
                            if isinstance(prior, list):
                                old["outputs"][key] = (
                                    prior[0]["text"] + " (Earlier screenshot omitted.)"
                                )
                    metadata = json.dumps(
                        {key: val for key, val in value.items() if key != "image"},
                        ensure_ascii=False,
                    )
                    output = [{"type": "text", "text": metadata}, image]
                else:
                    output = json.dumps(result, ensure_ascii=False)
                exchange["outputs"][item["id"]] = output
                if stream:
                    failed = (
                        "error" in result
                        or result.get("exit_code", 0)
                        or (
                            isinstance(value, dict)
                            and (value.get("isError") is True or "error" in value)
                        )
                    )
                    await emit(
                        {
                            "type": "tool",
                            "name": item["name"],
                            "phase": "failed" if failed else "finished",
                        }
                    )
        raise AssertionError("Tool loop exhausted")

    @staticmethod
    def limited_reply(reply, rounds):
        names = ", ".join(
            f"`{item['name']}`" for exchange in rounds for item in exchange["calls"]
        )
        explanation = "The tool limit ended this turn before the model saved a final report."
        if names:
            explanation += f" Calls already attempted: {names}. Review their effects before continuing."
        return {
            **{
                key: reply[key]
                for key in ("provider", "model", "provider_generation")
                if key in reply
            },
            "message": {"role": "assistant", "content": explanation},
            "outcome": "review",
        }

    async def compact(self, snapshot, force=False, *, requirements=None):
        revision, through, summary, history = snapshot
        requirements = self.requirements if requirements is None else requirements

        async def generate(messages):
            return await call(
                self.socket,
                self.router,
                "generate",
                {
                    **{
                        key: requirements[key]
                        for key in ("provider", "model")
                        if key in requirements
                    },
                    "messages": messages,
                    "output_modalities": ["text"],
                },
            )

        return await compaction.compact(
            self.transcripts,
            self.conversation,
            revision,
            through,
            summary,
            history,
            generate,
            force=force,
        )

    async def handle(self, name, params, emit, stream):
        if name == "health":
            revision = self.transcripts.revision(self.conversation)
            return {"status": "ok", "agent": self.identity, "revision": revision}
        if name == "model_preference":
            return self.model_preference()
        if name == "set_model_preference":
            mode, expected = params["mode"], params["expected_revision"]
            provider, model = params.get("provider"), params.get("model")
            if mode == "pin":
                if not isinstance(provider, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", provider):
                    raise fail("invalid_model_preference", "Pin a valid provider service name")
                if model is not None and (
                    not isinstance(model, str) or not model.strip() or len(model) > 160
                ):
                    raise fail("invalid_model_preference", "Supply a valid model ID")
            elif mode not in ("auto", "default") or provider is not None or model is not None:
                raise fail("invalid_model_preference", "Choose auto, default, or pin a provider and optional model")
            if type(expected) is not int or not 0 <= expected < 2**63 - 1:
                raise fail("invalid_revision", "Expected a valid model preference revision")
            saved = self.transcripts.set_model_preference(self.conversation, expected, mode, provider, model)
            return self.model_preference(saved)
        if name == "task_receipt":
            receipt = self.transcripts.task_receipt(
                self.conversation, *task_key(params)
            )
            if not receipt:
                return {"status": "absent"}
            if receipt["reply"] is None:
                return {"status": "started"}
            return {"status": "done", "reply": receipt["reply"]}
        if name == "tool_policy":
            return self.tool_policy()
        if name == "set_tool_policy":
            service, mode, expected = (
                params["service"], params["mode"], params["expected_revision"]
            )
            if mode not in ("enable", "disable", "default"):
                raise fail("invalid_tool_mode", "Choose enable, disable, or default")
            if not isinstance(service, str) or not re.fullmatch(
                r"[A-Za-z0-9_.-]{1,61}", service
            ):
                raise fail(
                    "invalid_tool_service", "Choose a service name that fits tool namespaces"
                )
            if type(expected) is not int or not 0 <= expected < 2**63 - 1:
                raise fail("invalid_revision", "Expected a valid tool policy revision")
            state = self.transcripts.set_tool_policy(
                self.conversation, expected, service, mode
            )
            return self.tool_policy(state)
        if name == "tool_status":
            policy = self.tool_policy()
            definitions, routes, images, status = await self.discover_tools(
                emit, stream, policy
            )
            return {
                "policy_revision": policy["revision"],
                "services": status,
                "tools": [
                    {
                        "name": tool["name"],
                        "service": routes.get(tool["name"], (None,))[0],
                        "returns_image": tool["name"] in images,
                    }
                    for tool in definitions
                ],
            }
        if name == "info":
            return {
                "agent": self.identity,
                "service": self.router,
                "requirements": self.model_preference()["requirements"],
                "tools": self.workspace.names if self.workspace else [],
                "tool_services": self.tool_policy()["enabled"],
                "compacted_through": self.transcripts.context(self.conversation)[1],
            }
        if name == "history":
            return self.transcripts.history(self.conversation, params.get("before"))
        if name == "compact":
            with self.transcripts.agent_turn():
                snapshot = self.transcripts.context(self.conversation)
                requirements = self.model_preference()["requirements"]
                through, summary, _ = await self.compact(snapshot, force=True, requirements=requirements)
                return {
                    "through_revision": through,
                    "summary": summary,
                    "changed": through != snapshot[1],
                }
        if name not in ("chat", "run_task"):
            raise fail("unknown_method", "Unknown agent method")
        key = task_key(params) if name == "run_task" else None
        if key is not None:
            params = {"text": params["text"]}
        if (
            set(params) == {"text"}
            and isinstance(params["text"], str)
            and params["text"].strip()
        ):
            user = message({"role": "user", "content": params["text"]})
        elif set(params) == {"message"} and params["message"]["role"] == "user":
            user = message(params["message"])
        else:
            raise fail(
                "invalid_message", "Supply nonempty text or a message with role user"
            )
        within_reply_limit(user)
        with self.transcripts.agent_turn():
            if key is not None:
                digest = hashlib.sha256(params["text"].encode()).hexdigest()
                previous = self.transcripts.task_receipt(self.conversation, *key)
                if previous:
                    if previous["digest"] != digest:
                        raise fail(
                            "task_conflict",
                            "The task attempt was recorded with different input",
                        )
                    if previous["reply"] is None:
                        raise fail(
                            "task_uncertain",
                            "The task attempt started without a saved result",
                        )
                    return previous["reply"]
                self.transcripts.begin_task(self.conversation, *key, digest)
            snapshot = self.transcripts.context(self.conversation)
            requirements = self.model_preference()["requirements"]
            revision, _, summary, history = snapshot
            try:
                _, summary, history = await self.compact(snapshot, requirements=requirements)
            except (RequestError, ProtocolError, EOFError, OSError):
                if stream:
                    await emit(
                        {
                            "type": "status",
                            "message": "Compaction failed. Using the last saved context.",
                        }
                    )
            reply = await self.generate(history, user, emit, stream, summary, requirements=requirements)
            assistant = message(reply["message"])
            result = within_reply_limit(
                {"agent": self.identity, "revision": revision + 2, **reply}
            )
            self.transcripts.append(
                self.conversation,
                revision,
                user,
                assistant,
                receipt=(*key, result) if key is not None else None,
            )
            return result


async def run(args):
    identity = args.name.removeprefix("agent.")
    if (
        not identity
        or not identity.isascii()
        or not all(char.isalnum() or char in "_-" for char in identity)
        or len(identity) > 90
    ):
        raise ValueError(
            "Agent name must use letters, numbers, underscores, or hyphens"
        )
    transcripts = Transcripts(args.state or STATE / f"agent-{identity}.sqlite3")
    try:
        requirements = {
            "input_modalities": sorted(modalities(args.input_modalities.split(","))),
            "output_modalities": sorted(modalities(args.output_modalities.split(","))),
            "min_context_tokens": args.min_context_tokens,
        }
        for field in ("provider", "model"):
            if value := getattr(args, field):
                requirements[field] = value
        agent = Agent(
            args.socket,
            identity,
            args.router,
            requirements,
            args.instructions,
            transcripts,
            Workspace(args.workspace, python=args.python_eval, socket=args.socket)
            if args.workspace
            else None,
            args.tool_service,
        )
        await serve(
            args.socket,
            f"agent.{identity}",
            args.replace,
            METHODS,
            agent.handle,
            reconnect=True,
            maintainer=args.maintainer,
        )
    finally:
        transcripts.close()


def main():
    parser = service_args(__doc__, "moon")
    parser.add_argument(
        "--router", default="models", help="Name of the model router service"
    )
    parser.add_argument("--model", help="Restrict selection to a model ID")
    parser.add_argument("--provider", help="Restrict selection to a provider service")
    parser.add_argument(
        "--input-modalities",
        default="text",
        help="Comma-separated model capabilities required for input",
    )
    parser.add_argument(
        "--output-modalities",
        default="text",
        help="Comma-separated allowed response modalities",
    )
    parser.add_argument("--min-context-tokens", type=int, default=1)
    parser.add_argument(
        "--tool-service",
        action="append",
        default=[],
        help="Discover and call tools from a named broker service",
    )
    parser.add_argument(
        "--workspace", help="Directory where the agent can read and edit files"
    )
    parser.add_argument(
        "--python-eval",
        action="store_true",
        help="Enable raw Python execution for a trusted agent",
    )
    parser.add_argument("--state", help="Path to the agent's SQLite state")
    parser.add_argument(
        "--instructions", default="You are an agent in the GlassMoon coding harness."
    )
    try:
        args = parser.parse_args()
        if args.min_context_tokens < 1:
            parser.error("--min-context-tokens must be positive")
        if args.python_eval and not args.workspace:
            parser.error("--python-eval requires --workspace")
        asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130
    except (RequestError, ProtocolError, EOFError, OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
