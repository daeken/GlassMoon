"""irssi-style terminal client for GlassMoon."""

import argparse
import asyncio
import curses
import json
import textwrap
import unicodedata
import uuid
from collections import deque

from .ipc import ProtocolError, RequestError, call, connect
from .models import result_text
from .toolbox import call_tool


def terminal_text(value):
    return "".join(
        char
        if char in "\n\t" or not unicodedata.category(char).startswith("C")
        else "�"
        for char in str(value)
    ).expandtabs(4)


class Terminal:
    def __init__(self, screen, socket, mailbox="tools.mailbox"):
        self.screen = screen
        self.socket = socket
        self.mailbox = mailbox
        self.windows = {"status": deque(maxlen=500)}
        self.active = "status"
        self.services = {}
        self.input = ""
        self.cursor = 0
        self.scroll = 0
        self.tasks = set()
        self.sending = set()
        self.partial = {}
        self.loaded = set()
        self.history_loading = {}
        self.command_history = []
        self.history_index = 0
        self.dirty = True

    def log(self, window, text):
        self.windows[window].append(terminal_text(text))
        self.dirty = True

    def display_services(self):
        if not self.services:
            self.log("status", "No registered services.")
            return
        for generation, service in sorted(
            self.services.items(),
            key=lambda entry: (entry[1]["name"], entry[0]),
        ):
            self.log(
                "status",
                f"{service['name']} #{generation} {service['state']}  "
                f"interface {service['interface_version']}  "
                f"{service.get('in_flight', '?')} in flight",
            )

    async def watch(self):
        delay = 0.2
        waiting = False
        while True:
            try:
                async with connect(self.socket) as wire:
                    snapshot = (await wire.request("watch", "watch"))["result"]
                    self.services = {
                        item["generation"]: item for item in snapshot["services"]
                    }
                    if waiting:
                        self.log("status", "Reconnected to the broker.")
                    self.display_services()
                    waiting = False
                    delay = 0.2
                    while True:
                        event = await wire.receive()
                        item = event["service"]
                        generation = item["generation"]
                        action = event["event"]
                        if action == "retired":
                            self.services.pop(generation, None)
                        else:
                            self.services[generation] = item
                        self.log("status", f"{item['name']} #{generation} {action}")
            except (EOFError, OSError, ProtocolError):
                self.services.clear()
                if not waiting:
                    self.log("status", "Broker unavailable. Waiting to reconnect.")
                    waiting = True
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5)

    async def inspect(self, target):
        async with connect(self.socket) as wire:
            if target.isdecimal():
                generation = int(target)
            else:
                current = (await wire.request("list", "list"))["result"]["services"]
                generation = next(
                    (
                        item["generation"]
                        for item in current
                        if item["name"] == target and item["state"] == "active"
                    ),
                    None,
                )
                if generation is None:
                    self.log("status", f"No active service named {target}")
                    return
            result = (
                await wire.request("describe", "describe", generation=generation)
            )["result"]
        self.log("status", json.dumps(result, indent=2, ensure_ascii=False))

    async def list_services(self):
        async with connect(self.socket) as wire:
            snapshot = (await wire.request("list", "list"))["result"]
        self.services = {item["generation"]: item for item in snapshot["services"]}
        self.display_services()

    async def query(self, window):
        lock = self.history_loading.setdefault(window, asyncio.Lock())
        async with lock:
            if window in self.loaded:
                return
            history = await call(self.socket, f"agent.{window}", "history", {})
            prior = deque(maxlen=500)
            for item in history["messages"]:
                label = "you" if item["role"] == "user" else window
                prior.append(terminal_text(f"<{label}> {result_text(item['content'])}"))
            prior.extend(self.windows[window])
            self.windows[window] = prior
            self.loaded.add(window)
            self.dirty = True

    async def list_models(self):
        result = await call(self.socket, "models", "models", {})
        if not result["models"]:
            self.log("status", "No models are registered.")
        for model in result["models"]:
            inputs = ", ".join(model["input_modalities"])
            outputs = ", ".join(model["output_modalities"])
            self.log(
                "status",
                f"{model['provider']}/{model['id']}  {inputs} → {outputs}  "
                f"{model['context_tokens']:,} tokens",
            )
        for provider in result["unavailable"]:
            self.log("status", f"{provider} catalog unavailable")

    async def chat(self, window, text):
        async def chunk(value):
            if value["type"] == "text":
                self.partial[window] = self.partial.get(window, "") + terminal_text(
                    value["text"]
                )
                self.dirty = True
            elif value["type"] == "status":
                self.log(window, value["message"])
            elif value["type"] == "tool" and value["phase"] != "finished":
                self.log(window, f"* {value['name']} {value['phase']}")

        try:
            await self.query(window)
            self.log(window, f"<you> {text}")
            result = await call(
                self.socket, f"agent.{window}", "chat", {"text": text}, on_chunk=chunk
            )
            self.log(window, f"<{window}> {result_text(result['message']['content'])}")
        finally:
            self.partial.pop(window, None)
            self.sending.discard(window)
            self.dirty = True

    async def compact(self, window):
        result = await call(self.socket, f"agent.{window}", "compact", {})
        text = (
            f"Compacted through revision {result['through_revision']}."
            if result["changed"]
            else "No older turns to compact."
        )
        self.log(window, text)

    async def tool_status(self, window):
        result = await call(self.socket, f"agent.{window}", "tool_status", {})
        for service, status in result["services"].items():
            self.log(window, f"{service}  {status['state']}  {status['count']} tools")
        for tool in result["tools"]:
            self.log(
                window, f"{tool['name']}{'  [image]' if tool['returns_image'] else ''}"
            )

    async def list_tasks(self, agent):
        params = {"agent": agent} if agent else {}
        response = await call_tool(self.socket, self.mailbox, "list_tasks", params)
        if not response["tasks"]:
            self.log("status", "No tasks.")
        for row in response["tasks"]:
            state = (
                "review · outcome unknown"
                if row["status"] == "review"
                else row["status"]
            )
            self.log(
                "status",
                f"{row['id']}  {state}  {row['sender']} → {row['agent']}  {row['key']}  attempts {row['attempts']}",
            )

    async def show_task(self, identifier):
        task = await call_tool(self.socket, self.mailbox, "get", {"id": identifier})
        self.log(
            "status", f"{task['id']}  {task['status']}  revision {task['revision']}"
        )
        self.log("status", task["task"])
        for field in ("result", "review_note"):
            if task[field]:
                self.log("status", task[field])
        for event in task.get("progress", [])[-8:]:
            detail = {
                "started": "started",
                "finished": "returned",
                "failed": "reported an error",
            }[event["phase"]]
            self.log("status", f"  {event['name']}  {detail}")
        if task["status"] == "review":
            self.log(
                "status",
                "Outcome unknown. Check the saved receipt with /reconcile ID or verify effects before retrying.",
            )

    async def reconcile_task(self, identifier):
        await call_tool(self.socket, self.mailbox, "reconcile", {"id": identifier})
        await self.show_task(identifier)

    async def queue_task(self, agent, instruction):
        task = await call_tool(
            self.socket,
            self.mailbox,
            "submit",
            {
                "sender": "operator",
                "agent": agent,
                "key": uuid.uuid4().hex,
                "task": instruction,
            },
        )
        self.log(
            "status",
            f"Queued {task['id']} for {agent}. Use /task {task['id']} to check it.",
        )

    def start(self, operation, window="status"):
        async def run():
            try:
                await operation()
            except (
                EOFError,
                OSError,
                ProtocolError,
                RequestError,
                ValueError,
            ) as error:
                self.log(window, str(error))

        task = asyncio.create_task(run())
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    def select(self, window):
        self.active = window
        self.scroll = 0

    def submit(self):
        text = self.input
        self.input = ""
        self.cursor = 0
        self.scroll = 0
        if not text.strip():
            return True
        self.command_history.append(text)
        self.history_index = len(self.command_history)
        if text.startswith("/") and not text.startswith("//"):
            command, _, argument = text.partition(" ")
            argument = argument.strip()
            match command:
                case "/quit":
                    return False
                case "/help":
                    self.log(
                        self.active,
                        "/query NAME  /compact  /tools  /services  /describe NAME|ID  /models  /status  /quit",
                    )
                    self.log(
                        self.active,
                        "F2 switches windows. Page Up and Page Down scroll. // sends a slash.",
                    )
                    self.log(
                        self.active,
                        "/queue AGENT TASK  /tasks [AGENT]  /task ID  /reconcile ID",
                    )
                case "/status":
                    self.select("status")
                case "/compact" if self.active != "status":
                    window = self.active
                    self.start(lambda: self.compact(window), window)
                case "/tools" if self.active != "status":
                    window = self.active
                    self.start(lambda: self.tool_status(window), window)
                case "/tasks":
                    self.select("status")
                    self.start(lambda: self.list_tasks(argument.removeprefix("agent.")))
                case "/queue" if " " in argument:
                    agent, _, instruction = argument.partition(" ")
                    self.select("status")
                    self.start(
                        lambda: self.queue_task(
                            agent.removeprefix("agent."), instruction.strip()
                        )
                    )
                case "/task" if argument:
                    self.select("status")
                    self.start(lambda: self.show_task(argument))
                case "/reconcile" if argument:
                    self.select("status")
                    self.start(lambda: self.reconcile_task(argument))
                case "/services":
                    self.select("status")
                    self.start(self.list_services)
                case "/models":
                    self.select("status")
                    self.start(self.list_models)
                case "/describe" if argument:
                    self.select("status")
                    self.start(lambda: self.inspect(argument))
                case "/query" if argument:
                    window = argument.removeprefix("agent.")
                    if (
                        window == "status"
                        or not window
                        or not all(
                            c.isascii() and (c.isalnum() or c in "_-") for c in window
                        )
                    ):
                        self.log(self.active, "Enter a valid agent name.")
                        return True
                    new = window not in self.windows
                    self.windows.setdefault(window, deque(maxlen=500))
                    self.select(window)
                    if new:
                        self.start(lambda: self.query(window), window)
                case _:
                    self.log(self.active, "Unknown or incomplete command. Try /help.")
        elif self.active == "status":
            self.log("status", "Open an agent chat with /query NAME.")
        elif self.active in self.sending:
            self.log(self.active, "This agent is still replying.")
        else:
            window = self.active
            self.sending.add(window)
            self.start(
                lambda: self.chat(window, text[1:] if text.startswith("//") else text),
                window,
            )
        return True

    def key(self, key):
        self.dirty = True
        match key:
            case "\x03" | "\x04":
                return False
            case "\n" | "\r" | curses.KEY_ENTER:
                return self.submit()
            case curses.KEY_F2:
                windows = list(self.windows)
                self.select(windows[(windows.index(self.active) + 1) % len(windows)])
            case curses.KEY_PPAGE:
                self.scroll += 10
            case curses.KEY_NPAGE:
                self.scroll = max(0, self.scroll - 10)
            case curses.KEY_LEFT:
                self.cursor = max(0, self.cursor - 1)
            case curses.KEY_RIGHT:
                self.cursor = min(len(self.input), self.cursor + 1)
            case curses.KEY_HOME:
                self.cursor = 0
            case curses.KEY_END:
                self.cursor = len(self.input)
            case curses.KEY_BACKSPACE | "\x7f" | "\b":
                if self.cursor:
                    self.input = (
                        self.input[: self.cursor - 1] + self.input[self.cursor :]
                    )
                    self.cursor -= 1
            case curses.KEY_DC:
                self.input = self.input[: self.cursor] + self.input[self.cursor + 1 :]
            case curses.KEY_UP | curses.KEY_DOWN:
                offset = -1 if key == curses.KEY_UP else 1
                self.history_index = min(
                    len(self.command_history), max(0, self.history_index + offset)
                )
                self.input = (
                    self.command_history[self.history_index]
                    if self.history_index < len(self.command_history)
                    else ""
                )
                self.cursor = len(self.input)
            case str() if key.isprintable():
                self.input = self.input[: self.cursor] + key + self.input[self.cursor :]
                self.cursor += len(key)
        return True

    def put(self, row, text, style=0):
        try:
            self.screen.addnstr(row, 0, text, self.screen.getmaxyx()[1] - 1, style)
        except curses.error:
            pass  # A resize or wide character can change the number of usable cells.

    def draw(self):
        self.screen.erase()
        height, width = self.screen.getmaxyx()
        if height < 4 or width < 12:
            self.put(0, "Resize terminal")
            self.screen.refresh()
            self.dirty = False
            return
        self.put(
            0,
            f" GlassMoon  |  {'status' if self.active == 'status' else '@' + self.active}",
            curses.A_REVERSE,
        )
        lines = []
        entries = list(self.windows[self.active])
        if self.active in self.partial:
            entries.append(f"<{self.active}> {self.partial[self.active]}")
        for entry in entries:
            for line in entry.split("\n"):
                lines.extend(
                    textwrap.wrap(line, width - 1, replace_whitespace=False) or [""]
                )
        self.scroll = min(self.scroll, max(0, len(lines) - height + 3))
        end = len(lines) - self.scroll
        for row, line in enumerate(lines[max(0, end - height + 3) : end], start=1):
            self.put(row, line)
        tabs = " ".join(
            f"[{name}]" if name == self.active else name for name in self.windows
        )
        self.put(height - 2, f" {tabs}  |  /help", curses.A_REVERSE)
        available = width - 4
        offset = max(0, self.cursor - available)
        shown = self.input[offset : offset + available]
        self.put(height - 1, "> " + shown)
        self.screen.move(height - 1, min(width - 1, 2 + self.cursor - offset))
        self.screen.refresh()
        self.dirty = False

    async def run(self, query):
        self.screen.nodelay(True)
        self.log("status", "/query NAME opens an agent chat. /help lists commands.")
        self.start(self.watch)
        if query:
            self.input = "/query " + query
            self.submit()
        try:
            while True:
                try:
                    key = self.screen.get_wch()
                except curses.error:
                    key = None
                if key is not None and not self.key(key):
                    return
                if self.dirty:
                    self.draw()
                await asyncio.sleep(0.025)
        finally:
            tasks = tuple(self.tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--query", help="Open an agent chat on startup")
    parser.add_argument(
        "--mailbox", default="tools.mailbox", help="Task service for /tasks and /task"
    )
    args = parser.parse_args()
    try:
        curses.wrapper(
            lambda screen: asyncio.run(
                Terminal(screen, args.socket, args.mailbox).run(args.query)
            )
        )
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
