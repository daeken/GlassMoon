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
PRE_ADMISSION_ERRORS = {"busy", "unavailable"}


async def online_agents(socket):
    async with asyncio.timeout(5), connect(socket) as wire:
        rows = (await wire.request("list", "list"))["result"]["services"]
    return {
        name
        for row in rows
        if row["state"] == "active" and row["name"].startswith("agent.")
        and (name := row["name"].removeprefix("agent."))
    }


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
        return self.identity in await online_agents(self.socket)

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

    async def recover_reply(self, task):
        for attempt in range(3):
            try:
                async with asyncio.timeout(3):
                    receipt = await call(
                        self.socket,
                        self.agent,
                        "task_receipt",
                        {"id": task["id"], "attempt": task["attempts"]},
                    )
                if receipt["status"] == "done":
                    return receipt["reply"]
            except (*CONNECTION_ERRORS, RequestError):
                pass
            if attempt < 2:
                await asyncio.sleep(0.5)
        return None

    async def deliver(self, *, check_online=True):
        if check_online and not await self.online():
            return None
        claimed = await self.rpc(
            "claim", {"agent": self.identity, "lease_seconds": LEASE_SECONDS}
        )
        task = claimed["task"]
        if task is None:
            return None
        lease = {"id": task["id"], "token": claimed["token"]}
        recovery = task.get("recovery_check")
        recovery_context = (
            f"\nAfter an interruption, the operator can use this recovery check: {recovery}\n"
            if recovery else ""
        )
        message = (
            f"GlassMoon task {task['id']} assigned by {task['sender']} (attempt {task['attempts']}).\n\n"
            f"{task['task']}\n\n"
            "Perform the requested work within your configured tools and report what you verified. "
            "If you cannot complete it, describe the blocker and any effects already observed. "
            "When delegating from this task through the mailbox, set parent to its task ID."
            f"{recovery_context}"
        )
        heartbeat = asyncio.create_task(self.heartbeat(lease))
        event_index = 0

        async def on_chunk(event):
            nonlocal event_index
            if (
                not isinstance(event, dict)
                or event.get("type") != "tool"
                or event_index >= 64
            ):
                return
            event_index += 1
            try:
                async with asyncio.timeout(1):
                    await self.rpc(
                        "progress",
                        lease
                        | {
                            "index": event_index,
                            "name": event.get("name"),
                            "phase": event.get("phase"),
                        },
                    )
            except (*CONNECTION_ERRORS, RequestError):
                # The receipt remains authoritative when telemetry is unavailable.
                pass

        try:
            reply = None
            try:
                async with asyncio.timeout(245):
                    try:
                        reply = await call(
                            self.socket,
                            self.agent,
                            "run_task",
                            {
                                "id": task["id"],
                                "attempt": task["attempts"],
                                "text": message,
                            },
                            on_chunk=on_chunk,
                        )
                    except RequestError as error:
                        if (
                            error.code != "unknown_method"
                            or error.generation is not None
                        ):
                            raise
                        reply = await call(
                            self.socket,
                            self.agent,
                            "chat",
                            {"text": message},
                            on_chunk=on_chunk,
                        )
            except RequestError as error:
                retryable = error.code == "agent_busy" or (
                    error.generation is None and error.code in PRE_ADMISSION_ERRORS
                )
                status = "queued" if retryable else "review"
                result = f"Agent call returned {error.code}."
                if not retryable:
                    reply = await self.recover_reply(task)
            except CONNECTION_ERRORS as error:
                status = "review"
                result = f"Delivery interrupted ({type(error).__name__}). Verify any effects before retrying."
                reply = await self.recover_reply(task)
            if reply is not None:
                status = "review" if reply.get("outcome") == "review" else "done"
                result = (
                    result_text(reply["message"]["content"]).strip()
                    or "Agent returned an empty reply."
                )
                result = result.encode()[:12000].decode(errors="ignore")
            finish = lease | {"status": status, "result": result}
            for attempt in range(3):
                try:
                    completed = await self.rpc("finish", finish)
                    break
                except RequestError as error:
                    if error.code != "stale_lease" or status != "done":
                        raise
                    completed = await self.rpc(
                        "reconcile", {"id": task["id"], "attempt": task["attempts"]}
                    )
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
    fixed = {label(agent, "agent") for agent in args.agent}
    discovered = set()

    async def worker(dispatch):
        while not args.discover or dispatch.identity in discovered:
            try:
                result = await dispatch.deliver(check_online=not args.discover)
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

    def start(agent):
        return asyncio.create_task(worker(Dispatch(args.socket, args.mailbox, agent)))

    tasks = {}
    try:
        if not args.discover:
            tasks = {agent: start(agent) for agent in fixed}
            await asyncio.gather(*tasks.values())
            return
        while True:
            try:
                discovered = await online_agents(args.socket)
            except (*CONNECTION_ERRORS, RequestError):
                discovered = set()
            if fixed:
                discovered &= fixed
            for agent, task in list(tasks.items()):
                if task.done():
                    await task
                    del tasks[agent]
            for agent in discovered - tasks.keys():
                tasks[agent] = start(agent)
            if args.once:
                await asyncio.gather(*tasks.values())
                return
            await asyncio.sleep(args.interval)
    finally:
        for task in tasks.values():
            task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--mailbox", default="tools.mailbox")
    parser.add_argument(
        "--agent", action="append", default=[], help="Agent identity to dispatch to"
    )
    parser.add_argument(
        "--discover", action="store_true", help="Find agent recipients as they register"
    )
    parser.add_argument("--interval", type=float, default=2)
    parser.add_argument(
        "--once",
        action="store_true",
        help="Try one delivery per configured agent and exit",
    )
    args = parser.parse_args()
    if not args.discover and not args.agent:
        parser.error("Choose --agent or --discover")
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
