"""Supervise the services for one broker lifetime."""

import argparse
import asyncio
import logging
import signal
import sys
from contextlib import ExitStack
from pathlib import Path

from .ipc import ProtocolError, RequestError, active_service
from .service import fail, method, serve
from .stack import Stack, manifest

SERVICE = "system.bootstrap"
LOGGER = logging.getLogger(__name__)
READY = {"running", "replacement"}


class Worker:
    def __init__(self, stack, config=None):
        self.stack = stack
        self.config = config
        self.name = config["name"] if config else "dispatcher"
        self.service = config.get("service", self.name) if config else None
        self.process = None
        self.ready = asyncio.Event()
        self.state = "waiting"
        self.generation = None
        self.error = None

    def snapshot(self):
        return {
            "name": self.name,
            "service": self.service,
            "state": self.state,
            "pid": self.process.pid if self.process else None,
            "replacement_generation": self.generation,
            "error": self.error,
        }

    async def launch(self):
        config = self.config
        if config is None:
            return await self.stack.spawn_process(
                "dispatcher", self.stack.config["dispatcher"], new_session=False
            )
        return await self.stack.spawn_process(
            f"worker-{self.name}", config["command"], config.get("env"),
            config.get("pass_env", ()), new_session=False,
        )

    async def stop(self):
        process = self.process
        if process is None:
            return
        try:
            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 10 if self.config is None else 3)
                except TimeoutError:
                    process.kill()
                    await process.wait()
        except ProcessLookupError:
            await process.wait()
        finally:
            self.process = None

    async def run(self, previous):
        if previous is not None:
            await previous.ready.wait()

        delay = 0.25
        while True:
            try:
                if self.service:
                    async with asyncio.timeout(2):
                        active = await active_service(self.stack.socket, self.service)
                    if active:
                        self.generation = active["generation"]
                        probe = self.config.get("probe")
                        async with asyncio.timeout(2):
                            self.error = await self.stack.readiness_reason(self.service, probe) if probe else None
                        self.state = "checking replacement" if self.error else "replacement"
                        if not self.error:
                            self.ready.set()
                            delay = 0.25
                        await asyncio.sleep(0.5)
                        continue
                self.generation = None
                self.state = "starting"
                spawn = asyncio.create_task(self.launch())
                try:
                    self.process = await asyncio.shield(spawn)
                except asyncio.CancelledError:
                    # Keep ownership even if shutdown arrives during process creation.
                    outcome, = await asyncio.gather(spawn, return_exceptions=True)
                    if isinstance(outcome, asyncio.subprocess.Process):
                        self.process = outcome
                    raise
                if self.config is not None:
                    await self.stack.readiness(self.process, self.service, self.config.get("probe"))
                self.state = "running"
                self.error = None
                self.ready.set()
                started = asyncio.get_running_loop().time()
                status = await self.process.wait()
                if asyncio.get_running_loop().time() - started >= 10:
                    delay = 0.25
                self.error = f"Process exited with status {status}"
                if status == 0 and self.service:
                    # A retired generation should yield immediately to its active replacement.
                    delay = 0.25
            except (OSError, RuntimeError, TimeoutError, EOFError, ProtocolError, RequestError) as error:
                self.error = str(error) or type(error).__name__
            finally:
                await self.stop()
            self.state = "retrying"
            LOGGER.info("%s: %s (retry in %gs)", self.name, self.error, delay)
            await asyncio.sleep(delay)
            delay = min(2 * delay, 8)


class Bootstrap:
    def __init__(self, stack):
        self.stack = stack
        self.workers = [Worker(stack, config) for config in stack.config.get("worker", [])]
        self.dispatcher = Worker(stack) if "dispatcher" in stack.config else None
        self.stopping = asyncio.Event()

    async def status(self, name, params, emit, stream):
        if name != "status":
            raise fail("method_not_found", "Unknown bootstrap method")
        workers = [worker.snapshot() for worker in self.workers]
        dispatcher = self.dispatcher.snapshot() if self.dispatcher else None
        return {
            "ready": not self.stopping.is_set() and all(
                row["state"] in READY for row in workers + ([dispatcher] if dispatcher else [])
            ),
            "stopping": self.stopping.is_set(),
            "workers": workers,
            "dispatcher": dispatcher,
        }

    async def watch_parent(self, reader):
        await reader.read(1)
        self.stopping.set()

    async def register(self, registered):
        try:
            await serve(
                self.stack.socket, SERVICE, None,
                {"status": method("Show the progress and process state of manifest workers")},
                self.status, on_ready=registered.set,
            )
        except (EOFError, OSError) as error:
            # The ownership pipe also closes if the broker disappears.
            LOGGER.info("Bootstrap lost its broker connection: %s", error)
            self.stopping.set()
            return
        if not self.stopping.is_set():
            raise RuntimeError("Bootstrap registration retired")

    async def run(self):
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader()
        pipe, _ = await loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(reader), sys.stdin.buffer
        )
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stopping.set)
        try:
            with ExitStack() as lock:
                async with asyncio.TaskGroup() as group:
                    parent = group.create_task(self.watch_parent(reader))
                    deadline = loop.time() + 30
                    while not self.stopping.is_set():
                        try:
                            lock.enter_context(self.stack.state_lock(managed=True))
                        except BlockingIOError:
                            if loop.time() >= deadline:
                                raise RuntimeError("This stack state is already in use") from None
                            await asyncio.sleep(0.25)
                        else:
                            await self.supervise(group)
                            break
                    parent.cancel()
        finally:
            pipe.close()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)

    async def supervise(self, group):
        registered = asyncio.Event()
        service = group.create_task(self.register(registered))
        await self.stopping_or_registered(registered)
        tasks = []
        dispatcher = None
        if not self.stopping.is_set():
            previous = None
            for worker in self.workers:
                tasks.append(group.create_task(worker.run(previous)))
                previous = worker
            if self.dispatcher:
                dispatcher = group.create_task(self.dispatcher.run(previous))
        await self.stopping.wait()
        if dispatcher:
            dispatcher.cancel()
            await asyncio.gather(dispatcher, return_exceptions=True)
        for task in [*tasks, service]:
            task.cancel()

    async def stopping_or_registered(self, registered):
        registered_task = asyncio.create_task(registered.wait())
        stopping_task = asyncio.create_task(self.stopping.wait())
        try:
            await asyncio.wait((registered_task, stopping_task), return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (registered_task, stopping_task):
                task.cancel()
            await asyncio.gather(registered_task, stopping_task, return_exceptions=True)


def managed_manifest(path):
    config = manifest(path)
    services = [worker.get("service", worker["name"]) for worker in config.get("worker", [])]
    if SERVICE in services or len(set(services)) != len(services):
        raise ValueError(f"Managed worker service names must be unique and cannot include {SERVICE}")
    return config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--timeout", type=float, default=15, help="Worker readiness timeout")
    args = parser.parse_args()
    if not 0.1 <= args.timeout <= 120:
        parser.error("--timeout must be between 0.1 and 120 seconds")
    config = managed_manifest(args.manifest)
    logging.basicConfig(level=logging.INFO)
    asyncio.run(Bootstrap(Stack(config, Path.cwd(), args.state, args.timeout)).run())


if __name__ == "__main__":
    main()
