"""Persistent chat agent, exposed as a GlassMoon service."""

import asyncio
from pathlib import Path

from .ipc import ProtocolError, RequestError, call
from .models import INTEGER, TEXT, Transcripts, message, modalities, within_reply_limit
from .service import fail, method, serve, service_args

METHODS = {
    "info": method("Show agent identity and model settings"),
    "history": method("Read a page of agent chat history", optional={"before": INTEGER}),
    "chat": method("Send a message to the agent and receive a reply", {"text": TEXT}),
}

STATE = Path.home() / ".local" / "state" / "glassmoon"


class Agent:
    def __init__(self, socket, identity, router, requirements, instructions, transcripts):
        self.socket = socket
        self.identity = identity
        self.router = router
        self.requirements = requirements
        self.instructions = instructions
        self.transcripts = transcripts
        self.conversation = transcripts.create(identifier=identity)
        self.turn = asyncio.Lock()

    async def handle(self, name, params, emit, stream):
        if name == "info":
            return {"agent": self.identity, "service": self.router, "requirements": self.requirements}
        if name == "history":
            return self.transcripts.history(self.conversation, params.get("before"))
        if name != "chat":
            raise fail("unknown_method", "Unknown agent method")
        if self.turn.locked():
            raise fail("agent_busy", "This agent is already replying")
        if not isinstance(params["text"], str) or not params["text"].strip():
            raise fail("invalid_message", "Send a nonempty text message")
        user = within_reply_limit(message({"role": "user", "content": params["text"]}))
        async with self.turn:
            _, revision, history = self.transcripts.read(self.conversation)
            try:
                reply = await call(
                    self.socket, self.router, "generate", {
                        **self.requirements,
                        "messages": [{"role": "developer", "content": self.instructions}, *history, user],
                    },
                    on_chunk=emit if stream else None,
                )
            except (EOFError, OSError):
                raise fail("model_unavailable", "Connection to the model router ended") from None
            assistant = message(reply["message"])
            result = within_reply_limit({"agent": self.identity, "revision": revision + 2, **reply})
            self.transcripts.append(self.conversation, revision, user, assistant)
            return result


async def run(args):
    identity = args.name.removeprefix("agent.")
    if not identity or not identity.isascii() or not all(
        char.isalnum() or char in "_-" for char in identity
    ) or len(identity) > 90:
        raise ValueError("Agent name must use letters, numbers, underscores, or hyphens")
    transcripts = Transcripts(args.state or STATE / f"agent-{identity}.sqlite3")
    try:
        requirements = {
            "input_modalities": sorted(modalities(args.input_modalities.split(","))),
            "output_modalities": ["text"],
            "min_context_tokens": args.min_context_tokens,
        }
        for field in ("provider", "model"):
            if value := getattr(args, field):
                requirements[field] = value
        agent = Agent(args.socket, identity, args.router, requirements, args.instructions, transcripts)
        await serve(args.socket, f"agent.{identity}", args.replace, METHODS, agent.handle)
    finally:
        transcripts.close()


def main():
    parser = service_args(__doc__, "moon")
    parser.add_argument("--router", default="models", help="Name of the model router service")
    parser.add_argument("--model", help="Restrict selection to a model ID")
    parser.add_argument("--provider", help="Restrict selection to a provider service")
    parser.add_argument("--input-modalities", default="text", help="Comma-separated model capabilities required for input")
    parser.add_argument("--min-context-tokens", type=int, default=1)
    parser.add_argument("--state", help="Path to the agent's SQLite state")
    parser.add_argument("--instructions", default="You are an agent in the GlassMoon coding harness.")
    try:
        args = parser.parse_args()
        if args.min_context_tokens < 1:
            parser.error("--min-context-tokens must be positive")
        asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130
    except (RequestError, ProtocolError, EOFError, OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
