"""Launch a self-contained local GlassMoon stack from a manifest."""

import argparse
import asyncio
import errno
import fcntl
import json
import os
import signal
import stat
import sys
import tomllib
from contextlib import AsyncExitStack, contextmanager
from pathlib import Path

from .ipc import ProtocolError, RequestError, call, connect
from .supervisor import stop

CLEAN_ENV = ("PATH", "LANG", "LC_ALL", "TMPDIR")
CONNECT_ERROR = (EOFError, OSError, ProtocolError, RequestError)


def manifest(path):
    with path.open("rb") as source:
        data = tomllib.load(source)
    if not isinstance(data.get("broker"), list) or not data["broker"]:
        raise ValueError("Manifest needs a broker command array")
    workers = data.get("worker", [])
    if not isinstance(workers, list):
        raise TypeError("Workers use [[worker]] entries")
    names = set()
    for worker in workers:
        if not isinstance(worker, dict) or not isinstance(worker.get("name"), str):
            raise TypeError("Every worker needs a name")
        name = worker["name"]
        if (
            not name
            or name in names
            or not name.isascii()
            or not all(char.isalnum() or char in "._-" for char in name)
        ):
            raise ValueError(
                "Worker names must be unique and contain only letters, digits, dots, underscores, or hyphens"
            )
        names.add(name)
        if not isinstance(worker.get("command"), list) or not worker["command"]:
            raise ValueError(f"Worker {name} needs a command array")
        probe = worker.get("probe")
        if probe is not None and (
            not isinstance(probe, dict)
            or not isinstance(probe.get("method"), str)
            or not isinstance(probe.get("params", {}), dict)
            or not isinstance(probe.get("expect", {}), dict)
        ):
            raise ValueError(
                f"Worker {name} needs a probe method and JSON object arguments"
            )
    return data


def render(value, variables):
    if not isinstance(value, str):
        raise TypeError("Command and environment values must be strings")
    for key, replacement in variables.items():
        value = value.replace("{" + key + "}", replacement)
    return value


class Stack:
    def __init__(self, config, root, state, timeout, resume=False):
        self.config = config
        self.root = root
        self.state = state
        self.socket = state / "ipc"
        self.timeout = timeout
        self.resume = resume
        self.variables = {
            "socket": str(self.socket),
            "state": str(state),
            "python": sys.executable,
        }

    @contextmanager
    def state_lock(self):
        if not self.resume:
            self.state.mkdir(mode=0o700, parents=False, exist_ok=False)
        path = self.state / ".glassmoon-stack"
        flags = os.O_RDWR | os.O_NOFOLLOW
        if not self.resume:
            flags |= os.O_CREAT | os.O_EXCL
        with os.fdopen(os.open(path, flags, 0o600), "r+") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("This stack state is already in use") from None
            if self.resume and lock.read() != str(self.root):
                raise ValueError("Use the same working directory when resuming this stack")
            if not self.resume:
                lock.write(str(self.root))
                lock.flush()
            yield

    async def start_process(self, stack, name, command, extra=None, pass_env=()):
        env = {
            key: os.environ[key] for key in (*CLEAN_ENV, *pass_env) if key in os.environ
        }
        env.update(
            {key: render(value, self.variables) for key, value in (extra or {}).items()}
        )
        with (self.state / f"{name}.log").open("ab") as log:
            process = await asyncio.create_subprocess_exec(
                *(render(value, self.variables) for value in command),
                cwd=self.root,
                env=env,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                start_new_session=True,
            )
        stack.push_async_callback(stop, process)
        return process

    async def recover_socket(self):
        try:
            previous = self.socket.lstat()
        except FileNotFoundError:
            return
        if not stat.S_ISSOCK(previous.st_mode) or previous.st_uid != os.getuid():
            raise ValueError("The stack socket path must be an owned Unix socket")
        try:
            async with asyncio.timeout(2):
                _, writer = await asyncio.open_unix_connection(self.socket)
        except OSError as error:
            if error.errno not in (errno.ECONNREFUSED, errno.ENOENT):
                raise
        else:
            writer.close()
            await writer.wait_closed()
            raise RuntimeError("A broker is already listening at this stack socket")
        try:
            current = self.socket.lstat()
        except FileNotFoundError:
            return
        if (current.st_dev, current.st_ino) != (previous.st_dev, previous.st_ino):
            raise RuntimeError("The stack socket changed during recovery")
        self.socket.unlink()

    async def readiness(self, process, service=None, probe=None):
        async with asyncio.timeout(self.timeout):
            while process.returncode is None:
                try:
                    async with asyncio.timeout(2):
                        async with connect(self.socket) as wire:
                            rows = (await wire.request("list", "list"))["result"][
                                "services"
                            ]
                        if service:
                            active = next(
                                (
                                    row
                                    for row in rows
                                    if row["name"] == service
                                    and row["state"] == "active"
                                ),
                                None,
                            )
                            if not active:
                                await asyncio.sleep(0.1)
                                continue
                            if probe:
                                result = await call(
                                    self.socket,
                                    service,
                                    probe["method"],
                                    probe.get("params", {}),
                                    version=active["interface_version"],
                                    generation=active["generation"],
                                )
                                if not isinstance(result, dict) or any(
                                    key not in result or result[key] != value
                                    for key, value in probe.get("expect", {}).items()
                                ):
                                    await asyncio.sleep(0.1)
                                    continue
                        return
                except (*CONNECT_ERROR, TimeoutError):
                    await asyncio.sleep(0.1)
            raise RuntimeError(
                f"{service or 'broker'} exited with status {process.returncode}"
            )

    async def run(self, smoke):
        async with AsyncExitStack() as stack:
            stack.enter_context(self.state_lock())
            if self.resume:
                await self.recover_socket()
            processes = [
                (
                    "broker",
                    await self.start_process(stack, "broker", self.config["broker"]),
                )
            ]
            await self.readiness(processes[0][1])
            for worker in self.config.get("worker", []):
                name = worker["name"]
                process = await self.start_process(
                    stack,
                    f"worker-{name}",
                    worker["command"],
                    worker.get("env"),
                    worker.get("pass_env", ()),
                )
                processes.append((name, process))
                await self.readiness(
                    process, worker.get("service", name), worker.get("probe")
                )
            print(
                json.dumps(
                    {
                        "event": "ready",
                        "socket": str(self.socket),
                        "workers": [name for name, _ in processes[1:]],
                    }
                ),
                flush=True,
            )
            if smoke:
                return 0
            waits = {
                asyncio.create_task(process.wait()): name for name, process in processes
            }
            try:
                done, _ = await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    print(
                        json.dumps(
                            {
                                "event": "exited",
                                "process": waits[task],
                                "status": task.result(),
                            }
                        ),
                        flush=True,
                    )
                return 1
            finally:
                for task in waits:
                    task.cancel()
                await asyncio.gather(*waits, return_exceptions=True)


async def run(stack, smoke):
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(stack.run(smoke))
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
    parser.add_argument("manifest", type=Path)
    parser.add_argument(
        "--state",
        required=True,
        type=Path,
        help="Local directory for the socket, logs, and persisted state",
    )
    parser.add_argument(
        "--root", default=Path.cwd(), type=Path, help="Working directory for commands"
    )
    parser.add_argument(
        "--timeout", type=float, default=15, help="Readiness deadline per process"
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Exit successfully after all readiness checks",
    )
    parser.add_argument(
        "--resume", action="store_true", help="Resume a previously created stack directory"
    )
    args = parser.parse_args()
    if not 0.1 <= args.timeout <= 120:
        parser.error("--timeout must be between 0.1 and 120 seconds")
    try:
        config = manifest(args.manifest)
        return asyncio.run(
            run(
                Stack(config, args.root.resolve(), args.state.resolve(), args.timeout, args.resume),
                args.smoke,
            )
        )
    except (
        OSError,
        KeyError,
        TypeError,
        ValueError,
        RuntimeError,
        TimeoutError,
    ) as error:
        parser.exit(1, f"Stack did not start: {error or type(error).__name__}\n")


if __name__ == "__main__":
    raise SystemExit(main())
