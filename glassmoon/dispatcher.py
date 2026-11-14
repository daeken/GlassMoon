"""Deliver queued mailbox tasks to running GlassMoon agents."""

import argparse
import asyncio
import contextlib
import json

from .ipc import ProtocolError, RequestError, call, connect
from .mailbox import label
from .models import result_text
from .toolbox import call_tool

LEASE_SECONDS = 30
CONNECTION_ERRORS = (ProtocolError, EOFError, OSError, TimeoutError)
RETRYABLE_CHAT = {"agent_busy", "busy", "unavailable"}


class Dispatch:
    def __init__(self, socket, mailbox, identity):
        self.socket = socket
        self.mailbox = mailbox
        self.identity = label(identity, "agent")
        self.agent = f"agent.{identity}"

    async def rpc(self, method, fields):
        async with asyncio.timeout(5):
            return await call_tool(self.socket, self.mailbox, method, fields)

    async def online(self):
        async with asyncio.timeout(5), connect(self.socket) as wire:
            rows = (await wire.request("list", "list"))["result"]["services"]
        return any(
            item["name"] == self.agent and item["state"] == "active" for item in rows
        )

    async def heartbeat(self, lease):
        while True:
            await asyncio.sleep(LEASE_SECONDS / 3)
            try:
                await self.rpc("renew", lease | {"lease_seconds": LEASE_SECONDS})
            except CONNECTION_ERRORS:
                # A failed broker connection is retried within the lease window.
                continue
            except RequestError:
                return

    async def deliver(self):
        if not await self.online():
            return None
        claimed = await self.rpc(
            "claim", {"agent": self.identity, "lease_seconds": LEASE_SECONDS}
        )
        task = claimed["task"]
        if task is None:
            return None
        lease = {"id": task["id"], "token": claimed["token"]}
        message = (
            f"GlassMoon task {task['id']} assigned by {task['sender']} (attempt {task['attempts']}).\n\n"
            f"{task['task']}\n\n"
            "Perform the requested work within your configured tools and report what you verified. "
            "If you cannot complete it, describe the blocker and any effects already observed."
        )
        heartbeat = asyncio.create_task(self.heartbeat(lease))
        try:
            try:
                async with asyncio.timeout(245):
                    reply = await call(
                        self.socket, self.agent, "chat", {"text": message}
                    )
                status = "done"
                result = (
                    result_text(reply["message"]["content"]).strip()
                    or "Agent returned an empty reply."
                )
                result = result.encode()[:12000].decode(errors="ignore")
            except RequestError as error:
                status = "queued" if error.code in RETRYABLE_CHAT else "review"
                result = f"Agent call returned {error.code}."
            except CONNECTION_ERRORS as error:
                status = "review"
                result = f"Delivery interrupted ({type(error).__name__}). Verify any effects before retrying."
            finish = lease | {"status": status, "result": result}
            for attempt in range(3):
                try:
                    completed = await self.rpc("finish", finish)
                    break
                except CONNECTION_ERRORS:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(0.5)
            print(
                json.dumps(
                    {
                        "event": "task_delivery",
                        "id": task["id"],
                        "agent": self.identity,
                        "status": completed["status"],
                    }
                ),
                flush=True,
            )
            return completed
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat


async def run(args):
    dispatchers = [
        Dispatch(args.socket, args.mailbox, agent)
        for agent in dict.fromkeys(args.agent)
    ]

    async def worker(dispatch):
        while True:
            try:
                result = await dispatch.deliver()
            except (*CONNECTION_ERRORS, RequestError) as error:
                print(
                    json.dumps(
                        {
                            "event": "dispatch_error",
                            "agent": dispatch.identity,
                            "error": error.code
                            if isinstance(error, RequestError)
                            else type(error).__name__,
                        }
                    ),
                    flush=True,
                )
                result = None
            if args.once:
                return
            if result is None or result["status"] == "queued":
                await asyncio.sleep(args.interval)

    await asyncio.gather(*(worker(dispatch) for dispatch in dispatchers))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--mailbox", default="tools.mailbox")
    parser.add_argument(
        "--agent", action="append", required=True, help="Agent identity to dispatch to"
    )
    parser.add_argument("--interval", type=float, default=2)
    parser.add_argument(
        "--once",
        action="store_true",
        help="Try one delivery per configured agent and exit",
    )
    args = parser.parse_args()
    if not 0.05 <= args.interval <= 60:
        parser.error("--interval must be between 0.05 and 60 seconds")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130
    except RequestError as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
