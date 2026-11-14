"""Install and control a per-user GlassMoon LaunchAgent on macOS."""

import argparse
import asyncio
import fcntl
import json
import os
import plistlib
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from .bootstrap import SERVICE, managed_manifest
from .ipc import ProtocolError, RequestError, active_service, call
from .stack import Stack, render

DEFAULT_LABEL = "org.glassmoon.stack"
BOOTSTRAP = ["-m", "glassmoon.bootstrap"]
LOG = "kernel.log"
IPC_ERRORS = (EOFError, OSError, ProtocolError, RequestError, TimeoutError)


class LaunchAgent:
    def __init__(self, label, root):
        self.label = label
        self.root = root
        self.domain = f"gui/{os.getuid()}"
        self.target = f"{self.domain}/{label}"
        self.plist = Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"
        self.command = "scripts/glassmoon-service"
        if label != DEFAULT_LABEL:
            self.command += f" --label {label}"

    def launchctl(self, *args, check=True):
        result = subprocess.run(
            ["/bin/launchctl", *args], check=False, capture_output=True, text=True, timeout=45,
        )
        if check and result.returncode:
            detail = result.stderr.strip() or result.stdout.strip() or str(result.returncode)
            raise RuntimeError(f"launchctl {args[0]} failed: {detail}")
        return result

    def loaded(self):
        return self.launchctl("print", self.target, check=False).returncode == 0

    def installed(self):
        if self.plist.is_symlink():
            raise ValueError(f"{self.plist} must be a regular plist")
        try:
            with self.plist.open("rb") as source:
                job = plistlib.load(source)
        except FileNotFoundError:
            raise ValueError(f"{self.label} is not installed. Run {self.command} install.") from None
        program = job.get("ProgramArguments", []) if isinstance(job, dict) else []
        if (
            not isinstance(program, list) or len(program) not in (9, 10)
            or any(not isinstance(arg, str) for arg in program)
            or job.get("Label") != self.label or job.get("WorkingDirectory") != str(self.root)
            or program[-7] != "--bootstrap"
            or program[-5:-3] != BOOTSTRAP or program[-2] != "--state"
            or Path(program[1]) != Path(program[-1]) / "ipc"
        ):
            raise ValueError(f"{self.plist} does not belong to this GlassMoon checkout")
        return job

    def configure(self, manifest_path, state):
        config = managed_manifest(manifest_path)
        stack = Stack(config, self.root, state, timeout=1)
        if len(os.fsencode(stack.socket)) >= 104:
            raise ValueError("The Unix socket path is too long for macOS. Choose a shorter --state path.")
        command = [render(arg, stack.variables) for arg in config["broker"]]
        if len(command) not in (2, 3) or Path(command[1]) != stack.socket:
            raise ValueError("The managed broker command needs an executable, {socket}, and optional DRAIN_MS")
        if len(command) == 3 and (not command[2].isdecimal() or not 1 <= int(command[2]) <= 3_600_000):
            raise ValueError("DRAIN_MS must be between 1 and 3600000")
        broker = self.root / command[0]
        if not broker.is_file() or not os.access(broker, os.X_OK):
            raise ValueError(f"Broker executable not found at {broker}. Run cargo build --locked.")
        command[0] = str(broker.resolve())
        command.extend([
            "--bootstrap", sys.executable, *BOOTSTRAP, str(manifest_path), "--state", str(state),
        ])
        job = {
            "Label": self.label,
            "ProgramArguments": command,
            "WorkingDirectory": str(self.root),
            "RunAtLoad": True,
            "KeepAlive": True,
            "ExitTimeOut": 40,
            "StandardOutPath": str(state / LOG),
            "StandardErrorPath": str(state / LOG),
            "Umask": 0o077,
            "EnvironmentVariables": {"PATH": os.environ.get("PATH") or os.defpath},
        }
        if self.plist.exists() or self.plist.is_symlink():
            if self.installed() != job:
                raise ValueError("This label already has a different configuration. Uninstall it or choose --label.")
            return
        if self.loaded():
            raise ValueError(f"A launchd job already uses {self.label}. Choose --label.")

        state.mkdir(mode=0o700, parents=True, exist_ok=True)
        owner = state.stat()
        if owner.st_uid != os.getuid() or owner.st_mode & 0o077:
            raise ValueError(f"State must be private to your user: {state}")
        try:
            with stack.state_lock(managed=True):
                asyncio.run(stack.recover_socket())
        except BlockingIOError:
            raise ValueError(f"Stop the other GlassMoon stack using {state} before installing") from None
        self.plist.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=self.plist.parent, delete=False) as output:
            temporary = Path(output.name)
            try:
                plistlib.dump(job, output)
                output.close()
                os.link(temporary, self.plist)
            finally:
                temporary.unlink()

    def start(self):
        self.launchctl("enable", self.target)
        if not self.loaded():
            self.launchctl("bootstrap", self.domain, str(self.plist))

    def stop(self, socket):
        self.launchctl("disable", self.target)
        if self.loaded():
            self.launchctl("bootout", self.target)
        deadline = time.monotonic() + 40
        while True:
            try:
                with socket.with_name(socket.name + ".kernel-lock").open("r+") as lock:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return
            except FileNotFoundError:
                return
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("The broker is still shutting down")
                time.sleep(0.1)


async def snapshot(socket):
    async with asyncio.timeout(2):
        return await call(socket, SERVICE, "status", {})


async def wait_ready(socket, timeout, *, agent=None):
    async with asyncio.timeout(timeout):
        while True:
            try:
                if agent:
                    async with asyncio.timeout(2):
                        if await active_service(socket, f"agent.{agent}"):
                            return
                elif (await snapshot(socket))["ready"]:
                    return
            except IPC_ERRORS:
                pass
            await asyncio.sleep(0.25)


def open_tui(service, agent, timeout):
    job = service.installed()
    if not service.loaded():
        raise ValueError(f"{service.label} is stopped. Run {service.command} start.")
    socket = Path(job["ProgramArguments"][1])
    try:
        asyncio.run(wait_ready(socket, timeout, agent=agent))
    except TimeoutError:
        raise RuntimeError(
            f"Agent {agent} did not register. Check {service.command} status and {job['StandardErrorPath']}."
        ) from None
    os.execv(sys.executable, [sys.executable, "-m", "glassmoon.tui", "--socket", str(socket), "--query", agent])


def control(service, args):
    action = args.action
    if action == "install":
        service.configure(args.manifest.resolve(), args.state.resolve())
    job = service.installed()
    socket = Path(job["ProgramArguments"][1])
    if action == "status":
        running = service.loaded()
        summary = {
            "label": service.label,
            "loaded": running,
            "socket": str(socket),
            "logs": str(socket.parent),
        }
        if running:
            try:
                summary["bootstrap"] = asyncio.run(snapshot(socket))
            except IPC_ERRORS as error:
                summary["error"] = str(error) or type(error).__name__
        print(json.dumps(summary, indent=2))
        return int(not summary.get("bootstrap", {}).get("ready", False))
    if action in ("stop", "restart", "uninstall"):
        service.stop(socket)
    if action == "uninstall":
        service.plist.unlink()
        service.launchctl("enable", service.target)
        print(f"Uninstalled {service.label}. Stack data remains in {socket.parent}.")
    elif action == "stop":
        print(f"Stopped {service.label}.")
    else:
        service.start()
        try:
            asyncio.run(wait_ready(socket, args.wait))
        except TimeoutError:
            raise RuntimeError(
                f"{service.label} is installed and launchd will keep trying. "
                f"Check {service.command} status and {job['StandardErrorPath']}."
            ) from None
        print(f"Running {service.label} at {socket}.")
    return 0


def main():
    argv = sys.argv[1:]
    tui = argv[:1] == ["tui"]
    if tui:
        argv = argv[1:]
    parser = argparse.ArgumentParser(
        description="Open the terminal UI for a managed GlassMoon stack." if tui else __doc__
    )
    parser.add_argument("--label", default=DEFAULT_LABEL, help="LaunchAgent label")
    parser.add_argument("--wait", type=float, default=30, help="Startup wait in seconds")
    root = Path.cwd()
    if tui:
        parser.add_argument("agent", nargs="?", default="moon", help="Agent to open")
    else:
        sub = parser.add_subparsers(dest="action", required=True)
        install = sub.add_parser("install", help="Register a LaunchAgent and start the stack")
        install.add_argument("--manifest", type=Path, default=root / "examples" / "offline-stack.toml")
        install.add_argument("--state", type=Path, default=Path.home() / ".local/state/glassmoon/service")
        for action in ("start", "restart", "stop", "status", "uninstall"):
            sub.add_parser(action)
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[a-zA-Z\d][a-zA-Z\d_.-]{0,127}", args.label, re.ASCII):
        parser.error("Use letters, digits, dots, underscores, or hyphens in --label")
    if not 0.1 <= args.wait <= 300:
        parser.error("--wait must be between 0.1 and 300 seconds")
    if sys.platform != "darwin":
        parser.exit(1, "GlassMoon LaunchAgents require macOS. Use python -m glassmoon.stack on this platform.\n")
    service = LaunchAgent(args.label, root)
    try:
        if tui:
            return open_tui(service, args.agent, args.wait)
        service.launchctl("print", service.domain)
        return control(service, args)
    except (OSError, ValueError, TypeError, RuntimeError, subprocess.TimeoutExpired) as error:
        parser.exit(1, f"{error or type(error).__name__}\n")


if __name__ == "__main__":
    raise SystemExit(main())
