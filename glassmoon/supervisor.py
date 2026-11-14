"""Probe a local GlassMoon worker and restart it after a verified failure."""

import argparse
import asyncio
import json
import os
import signal
import uuid
from pathlib import Path

from .ipc import ProtocolError, RequestError, call, connect
from .toolbox import call_tool


async def probe(socket, service, method, params, expected, timeout=3):
    try:
        async with asyncio.timeout(timeout):
            reply = await call(socket, service, method, params)
    except (RequestError, ProtocolError, EOFError, OSError, TimeoutError) as error:
        code = error.code if isinstance(error, RequestError) else type(error).__name__
        return f"{service}.{method} failed ({code})"
    if not isinstance(reply, dict) or any(
        reply.get(key) != value for key, value in expected.items()
    ):
        return f"{service}.{method} returned unexpected health data"
    return None


async def notify(socket, agent, message):
    if not agent:
        return
    try:
        async with asyncio.timeout(90):
            reply = await call(socket, agent, "chat", {"text": message})
        print(
            json.dumps(
                {
                    "event": "maintainer_replied",
                    "agent": agent,
                    "reply": reply["message"],
                }
            ),
            flush=True,
        )
    except (RequestError, ProtocolError, EOFError, OSError, TimeoutError) as error:
        print(
            json.dumps(
                {
                    "event": "maintainer_unavailable",
                    "agent": agent,
                    "error": type(error).__name__,
                }
            ),
            flush=True,
        )


async def stop(process):
    for sig, grace in ((signal.SIGTERM, 3), (signal.SIGKILL, 3)):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            break
        try:
            await asyncio.wait_for(process.wait(), grace)
        except TimeoutError:
            continue
        if sig == signal.SIGTERM:
            # A descendant can outlive the process whose exit we observed.
            continue
        break


class Supervisor:
    def __init__(self, args):
        self.args = args
        self.process = None

    def event(self, event, **fields):
        print(
            json.dumps({"event": event, "service": self.args.service, **fields}),
            flush=True,
        )

    async def start(self):
        self.process = await asyncio.create_subprocess_exec(
            *self.args.command,
            cwd=self.args.cwd,
            stdin=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )
        self.event("worker_started", pid=self.process.pid)
        for _ in range(self.args.startup_probes):
            if self.process.returncode is not None:
                return f"Worker exited with status {self.process.returncode}"
            reason = await self.health()
            if reason is None:
                return None
            await asyncio.sleep(self.args.interval)
        return reason

    async def health(self):
        a = self.args
        return await probe(a.socket, a.service, a.probe, a.params, a.expect, a.timeout)

    async def monitor(self):
        failures = 0
        while True:
            try:
                await asyncio.wait_for(self.process.wait(), self.args.interval)
                return f"Worker exited with status {self.process.returncode}"
            except TimeoutError:
                pass
            reason = await self.health()
            failures = failures + 1 if reason else 0
            if failures >= self.args.failures:
                return reason

    async def wait_for_route_removal(self):
        async with asyncio.timeout(5), connect(self.args.socket) as wire:
            while True:
                response = await wire.request("list", "list")
                if not any(
                    item["name"] == self.args.service and item["state"] == "active"
                    for item in response["result"]["services"]
                ):
                    return
                await asyncio.sleep(0.05)

    async def run(self):
        notices = set()
        try:
            await self.wait_for_route_removal()
            for attempt in range(self.args.restarts + 1):
                reason = await self.start()
                if reason is None:
                    self.event("recovered" if attempt else "healthy")
                    reason = await self.monitor()
                self.event("unhealthy", reason=reason, attempt=attempt)
                await stop(self.process)
                self.process = None
                if self.args.maintainer:
                    status = (
                        "The supervisor is restarting the registered worker."
                        if attempt < self.args.restarts
                        else "The restart budget is exhausted."
                    )
                    message = (
                        f"GlassMoon incident for {self.args.service}: {reason}. {status} "
                        f"Diagnose with the {self.args.probe} IPC probe and report the result."
                    )
                    noticing = asyncio.create_task(self.notify(message))
                    notices.add(noticing)
                    noticing.add_done_callback(notices.discard)
                if attempt < self.args.restarts:
                    await self.wait_for_route_removal()
            self.event("restart_budget_exhausted")
            if notices:
                await asyncio.gather(*notices)
            return 1
        finally:
            if self.process is not None:
                await stop(self.process)
            for task in notices:
                task.cancel()
            await asyncio.gather(*notices, return_exceptions=True)

    async def notify(self, message):
        a = self.args
        if not a.mailbox:
            await notify(a.socket, a.maintainer, message)
            return
        fields = {
            "sender": f"supervisor.{a.service}"[:96],
            "agent": a.maintainer.removeprefix("agent."),
            "key": uuid.uuid4().hex,
            "task": message,
        }
        for attempt in range(3):
            try:
                async with asyncio.timeout(5):
                    task = await call_tool(a.socket, a.mailbox, "submit", fields)
                self.event("incident_queued", task=task["id"], agent=fields["agent"])
                return
            except (
                RequestError,
                ProtocolError,
                EOFError,
                OSError,
                TimeoutError,
            ) as error:
                if attempt == 2:
                    self.event("incident_publish_failed", error=type(error).__name__)
                else:
                    await asyncio.sleep(0.5)


async def run(args):
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(Supervisor(args).run())
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, task.cancel)
    try:
        return await task
    except asyncio.CancelledError:
        return 0
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--service", required=True)
    parser.add_argument("--probe", default="health")
    parser.add_argument("--params", type=json.loads, default={})
    parser.add_argument("--expect", type=json.loads, default={})
    parser.add_argument("--maintainer", help="Agent service to notify on failure")
    parser.add_argument(
        "--mailbox", help="Queue maintainer incidents in this task mailbox"
    )
    parser.add_argument("--cwd", type=Path, default=Path.cwd())
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--timeout", type=float, default=3)
    parser.add_argument("--startup-probes", type=int, default=10)
    parser.add_argument("--failures", type=int, default=2)
    parser.add_argument("--restarts", type=int, default=3)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command[:1] == ["--"]:
        args.command.pop(0)
    if not args.command:
        parser.error("Supply the worker command after --")
    if (
        args.interval <= 0
        or args.timeout <= 0
        or args.failures < 1
        or args.startup_probes < 1
        or args.restarts < 0
        or not isinstance(args.params, dict)
        or not isinstance(args.expect, dict)
    ):
        parser.error(
            "Supply positive probe settings, object parameters, and a nonnegative restart budget"
        )
    try:
        return asyncio.run(run(args))
    except (RequestError, ProtocolError, EOFError, OSError, TimeoutError) as error:
        parser.exit(
            1,
            f"Supervisor could not establish a clean broker route ({type(error).__name__})\n",
        )


if __name__ == "__main__":
    raise SystemExit(main())
