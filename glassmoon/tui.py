"""irssi-style terminal client for GlassMoon."""

import argparse
import asyncio
import curses
import json
import textwrap
import unicodedata
import uuid
from collections import deque
from datetime import datetime

from .evidence import export_image
from .ipc import ProtocolError, RequestError, call, connect
from .models import result_text
from .toolbox import call_tool, inspect_tool


def terminal_text(value):
    return "".join(
        char
        if char in "\n\t" or not unicodedata.category(char).startswith("C")
        else "�"
        for char in str(value)
    ).expandtabs(4)


class Terminal:
    def __init__(self, screen, socket, mailbox="tools.mailbox", evidence="tools.evidence"):
        self.screen = screen
        self.socket = socket
        self.mailbox = mailbox
        self.evidence = evidence
        self.windows = {"status": deque(maxlen=500)}
        self.active = "status"
        self.services = {}
        self.input = ""
        self.cursor = 0
        self.scroll = 0
        self.scroll_to_top = False
        self.tasks = set()
        self.sending = set()
        self.partial = {}
        self.loaded = set()
        self.history_loading = {}
        self.history_before = {}
        self.latest_before = {}
        self.history_page = {}
        self.command_history = []
        self.history_index = 0
        self.task_page = None
        self.task_list_lock = asyncio.Lock()
        self.watched_tasks = {}
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
            maintainer = f"  maintainer {service['maintainer']}" if service.get("maintainer") else ""
            self.log(
                "status",
                f"{service['name']} #{generation} {service['state']}  "
                f"interface {service['interface_version']}  "
                f"{service.get('in_flight', '?')} in flight{maintainer}",
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
            prior, before = await self.load_history(window, {})
            self.history_before[window] = before
            self.latest_before[window] = before
            history = self.windows[window]
            existing = list(history)
            history.clear()
            history.extend(prior)
            history.extend(existing)
            self.loaded.add(window)
            self.dirty = True

    async def load_history(self, window, arguments):
        history = await call(self.socket, f"agent.{window}", "history", arguments)
        prior = []
        for item in history["messages"]:
            label = "you" if item["role"] == "user" else window
            prior.append(terminal_text(f"<{label}> {result_text(item['content'])}"))
        return prior, history["before"]

    async def older_history(self, window):
        await self.query(window)
        async with self.history_loading[window]:
            cursor = self.history_before[window]
            if cursor is None:
                notice = "Reached the start of the saved conversation."
                if window not in self.history_page:
                    self.log(window, notice)
                return
            entries, before = await self.load_history(window, {"before": cursor})
            self.history_page[window] = entries
            self.history_before[window] = before
            if self.active == window:
                self.scroll_to_top = True
            self.dirty = True

    async def latest_history(self, window):
        await self.query(window)
        async with self.history_loading[window]:
            self.history_before[window] = self.latest_before[window]
            self.history_page.pop(window, None)
            if self.active == window:
                self.scroll = 0
                self.scroll_to_top = False
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

    async def show_memory(self, window):
        memory = await call(self.socket, f"agent.{window}", "memory", {})
        through = memory["through_revision"]
        if not through:
            self.log(window, "No conversation summary has been saved.")
            return
        self.log(window, f"Saved summary through revision {through} of {memory['revision']}")
        self.log(window, memory["summary"])

    async def tool_status(self, window):
        result = await call(self.socket, f"agent.{window}", "tool_status", {})
        if "policy_revision" in result:
            self.log(window, f"Tool settings revision {result['policy_revision']}")
        for service, status in result["services"].items():
            generation = f"  #{status['generation']}" if "generation" in status else ""
            self.log(window, f"{service}  {status['state']}  {status['count']} tools{generation}")
        for tool in result["tools"]:
            self.log(
                window, f"{tool['name']}{'  [image]' if tool['returns_image'] else ''}"
            )

    async def describe_tool(self, argument):
        parts = argument.split()
        if len(parts) != 2:
            self.log("status", "Usage /describe-tool SERVICE TOOL")
            return
        service, tool = parts
        metadata = await inspect_tool(self.socket, service, tool)
        definition = metadata["tool"]
        self.log("status", f"{service} #{metadata['generation']}  {definition['name']}")
        self.log("status", definition["description"])
        self.log("status", json.dumps(definition["parameters"], ensure_ascii=False, indent=2))
        if definition.get("returns_image"):
            self.log("status", "Returns an image tool result.")

    async def change_tool(self, window, argument):
        parts = argument.split()
        if len(parts) != 2 or parts[0] not in ("enable", "disable", "default"):
            self.log(window, "Usage /tool enable|disable|default SERVICE")
            return
        mode, service = parts
        agent = f"agent.{window}"
        policy = await call(self.socket, agent, "tool_policy", {})
        updated = await call(
            self.socket, agent, "set_tool_policy",
            {"service": service, "mode": mode, "expected_revision": policy["revision"]},
        )
        membership = "enabled" if service in updated["enabled"] else "disabled"
        self.log(window, f"{service} {membership} for new turns. Use /tools to check availability.")

    async def model_preference(self, window, argument):
        agent = f"agent.{window}"
        current = await call(self.socket, agent, "model_preference", {})
        if argument:
            if argument in ("auto", "default"):
                fields = {"mode": argument}
            else:
                provider, sep, model = argument.partition("/")
                fields = {"mode": "pin", "provider": provider}
                if sep:
                    fields["model"] = model
            current = await call(
                self.socket, agent, "set_model_preference",
                fields | {"expected_revision": current["revision"]},
            )
        requirements = current["requirements"]
        selection = requirements.get("provider", "any provider")
        if "model" in requirements:
            selection += "/" + requirements["model"]
        self.log(window, f"Model preference {current['mode']} · {selection} · revision {current['revision']}")
        last = current.get("last_model")
        if last:
            generation = f" #{last['generation']}" if last.get("generation") else ""
            self.log(window, f"Last used {last['provider']}/{last['model']}{generation} at conversation revision {last['revision']}")

    async def list_tasks(self, agent=None, parent=None, status=None, before=None):
        async with self.task_list_lock:
            await self.load_tasks(agent, parent, status, before)

    async def load_tasks(self, agent, parent, status, before):
        params = {"agent": agent} if agent else {}
        if parent is not None:
            params["parent"] = parent
        if status is not None:
            params["status"] = status
        if before is not None:
            params["before"] = before
        response = await call_tool(self.socket, self.mailbox, "list_tasks", params)
        self.task_page = (agent, parent, status, response["before"])
        if not response["tasks"]:
            self.log("status", "No tasks need review." if status == "review" else "No tasks.")
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
        if response["before"] is not None:
            self.log("status", "Use /more-tasks to see older matching tasks.")

    async def more_tasks(self):
        async with self.task_list_lock:
            if self.task_page is None:
                self.log("status", "Open a task list with /tasks, /children, or /review first.")
                return
            agent, parent, status, before = self.task_page
            if before is None:
                self.log("status", "Reached the end of this task list.")
                return
            await self.load_tasks(agent, parent, status, before)

    async def review_queue(self, parent):
        await self.list_tasks(parent=parent or None, status="review")

    async def show_task(self, identifier):
        task = await call_tool(self.socket, self.mailbox, "get", {"id": identifier})
        self.log(
            "status", f"{task['id']}  {task['status']}  revision {task['revision']}"
        )
        self.log("status", task["task"])
        if task.get("parent"):
            self.log("status", f"Parent task {task['parent']}")
        for reference in task.get("evidence", []):
            self.log("status", f"Evidence /evidence {reference['owner']} {reference['key']}")
        if task.get("recovery_check"):
            self.log("status", f"Recovery check from submitter — {task['recovery_check']}")
        if task["status"] == "queued" and task.get("available_at", 0) > datetime.now().timestamp():
            retry_at = datetime.fromtimestamp(task["available_at"]).strftime("%H:%M:%S")
            self.log("status", f"Delivery deferred until {retry_at} local time.")
        for line in dict.fromkeys((task["result"], task["review_note"])):
            if line:
                self.log("status", line)
        if task.get("result_truncated"):
            self.log("status", "Result display shortened to fit the reply limit.")
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
                "Outcome unknown. Use /reconcile ID to check the receipt. Use /resolve ID REVISION ACTION NOTE after verifying effects.",
            )

    async def reconcile_task(self, identifier):
        await call_tool(self.socket, self.mailbox, "reconcile", {"id": identifier})
        await self.show_task(identifier)

    async def task_timeline(self, argument):
        identifier, separator, cursor = argument.partition(" ")
        params = {"id": identifier}
        if separator:
            if not cursor.isascii() or not cursor.isdecimal():
                raise ValueError("Usage /timeline ID [BEFORE]")
            params["before"] = int(cursor)
        page = await call_tool(self.socket, self.mailbox, "timeline", params)
        self.log("status", f"Task {identifier} events in local time")
        labels = {
            "submitted": "Queued",
            "claimed": "Claimed",
            "lease_expired": "Lease expired, outcome unknown",
            "reply_recorded": "Agent reply recorded",
            "review_required": "Outcome needs review",
            "delivery_deferred": "Delivery deferred",
            "requeued": "Operator requeued task",
            "cancelled": "Operator cancelled task",
            "verified": "Operator recorded verification",
            "receipt_recovered": "Recovered agent reply from receipt",
        }
        for event in reversed(page["events"]):
            when = datetime.fromtimestamp(event["observed_at"]).strftime("%b %d %H:%M:%S")
            detail = labels.get(event["kind"], event["kind"])
            if event["kind"] == "tool":
                detail = f"{event['name']} {event['phase']}"
            if event["kind"] == "evidence_attached":
                detail = f"Evidence linked {event['name']}"
            attempt = f"  attempt {event['attempt']}" if event["attempt"] else ""
            self.log("status", f"{when}{attempt}  {detail}")
        if not page["events"]:
            self.log("status", "No events recorded on this page.")
        if page["before"] is not None:
            self.log("status", f"Use /timeline {identifier} {page['before']} for earlier events.")

    async def watch_task(self, identifier):
        revision = None
        last_event = None
        while True:
            row = await call_tool(self.socket, self.mailbox, "get", {"id": identifier})
            progress = row.get("progress", [])
            if row["revision"] != revision:
                self.log("status", f"Task {identifier}  {row['status']}  attempt {row['attempts']}")
                revision = row["revision"]
            if progress:
                event = progress[-1]
                event_key = (row["attempts"], event["event_index"])
                if event_key != last_event:
                    self.log("status", f"  {event['name']}  {event['phase']}")
                    last_event = event_key
            if row["status"] in ("done", "review", "cancelled"):
                self.log("status", f"Use /task {identifier} to read the result.")
                return
            await asyncio.sleep(1)

    def toggle_task_watch(self, identifier):
        if identifier in self.watched_tasks:
            self.watched_tasks[identifier].cancel()
            self.log("status", f"Stopped watching {identifier}.")
            return
        task = self.start(lambda: self.watch_task(identifier))
        self.watched_tasks[identifier] = task
        task.add_done_callback(lambda _: self.watched_tasks.pop(identifier, None))

    async def save_evidence(self, argument):
        parts = argument.split(maxsplit=2)
        if len(parts) != 3:
            self.log("status", "Usage /save-image OWNER KEY PATH")
            return
        owner, key, path = parts
        try:
            saved = await export_image(self.socket, self.evidence, owner, key, path)
        except FileExistsError:
            self.log("status", "That path already exists. Choose a new path to save the screenshot.")
            return
        self.log("status", f"Saved verified screenshot to {saved}")

    async def show_evidence(self, argument):
        parts = argument.split()
        if len(parts) in (1, 3):
            if len(parts) == 3 and parts[1] != "after":
                self.log("status", "Usage /evidence OWNER [KEY | after KEY]")
                return
            owner = parts[0]
            params = {"owner": owner}
            if len(parts) == 3:
                params["after"] = parts[2]
            page = await call_tool(self.socket, self.evidence, "list", params)
            for item in page["items"]:
                image = "  [screenshot]" if item["image_sha256"] else ""
                self.log("status", f"{owner}/{item['key']}{image}")
            if not page["items"]:
                self.log("status", "No evidence on this page.")
            if page["next"] is not None:
                self.log("status", f"Use /evidence {owner} after {page['next']} for more.")
            return
        if len(parts) != 2:
            self.log("status", "Usage /evidence OWNER [KEY | after KEY]")
            return
        owner, key = parts
        evidence = await call_tool(self.socket, self.evidence, "read", {"owner": owner, "key": key})
        self.log("status", f"Evidence {owner}/{key}")
        if evidence.get("published_at") is not None:
            when = datetime.fromtimestamp(evidence["published_at"]).strftime("%b %d %H:%M:%S")
            self.log("status", f"Published {when} local time")
        self.log("status", evidence["summary"])
        if evidence["metadata"]:
            self.log("status", json.dumps(evidence["metadata"], ensure_ascii=False, indent=2))
        if evidence.get("image_sha256"):
            self.log("status", f"Screenshot SHA-256 {evidence['image_sha256']}")
            self.log("status", f"Use /save-image {owner} {key} PATH to save the screenshot.")

    async def resolve_task(self, arguments):
        fields = arguments.split(maxsplit=3)
        if len(fields) != 4 or not fields[1].isdecimal():
            self.log(
                "status", "Usage /resolve ID REVISION requeue|cancel|verified NOTE"
            )
            return
        identifier, revision, action, note = fields
        await call_tool(
            self.socket,
            self.mailbox,
            "review",
            {
                "id": identifier,
                "expected_revision": int(revision),
                "action": action,
                "note": note,
            },
        )
        await self.show_task(identifier)

    async def queue_task(self, agent, instruction, recovery_check=None):
        params = {
            "sender": "operator",
            "agent": agent,
            "key": uuid.uuid4().hex,
            "task": instruction,
        }
        if recovery_check is not None:
            params["recovery_check"] = recovery_check
        task = await call_tool(
            self.socket, self.mailbox, "submit", params
        )
        self.log(
            "status",
            f"Queued {task['id']} for {agent}. Use /task {task['id']} to check it.",
        )

    async def queue_maintainer(self, service, instruction, generation=None):
        async with connect(self.socket) as wire:
            rows = (await wire.request("list", "list"))["result"]["services"]
        active = next((row for row in rows if row["name"] == service and row["state"] == "active"), None)
        if generation is not None and (not active or active["generation"] != generation):
            self.log("status", f"{service} changed generation. Check /services before queuing the task.")
            return
        if not active or not active.get("maintainer"):
            self.log("status", f"No maintainer is declared for active service {service}.")
            return
        await self.queue_task(
            active["maintainer"],
            f"Service {service}, generation {active['generation']} at submission.\n\n{instruction}",
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
        return task

    def select(self, window):
        self.active = window
        self.scroll = 0
        self.scroll_to_top = False

    def submit(self):
        text = self.input
        self.input = ""
        self.cursor = 0
        self.scroll = 0
        self.scroll_to_top = False
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
                    self.log(self.active, "/queue-check AGENT CHECK | TASK")
                    self.log(self.active, "/resolve ID REVISION ACTION NOTE")
                    self.log(self.active, "/timeline ID [BEFORE] shows recorded task events")
                    self.log(self.active, "/watch-task ID follows a task. Repeat to stop watching.")
                    self.log(self.active, "/save-image OWNER KEY PATH saves a shared screenshot")
                    self.log(self.active, "/evidence OWNER [KEY] lists or shows shared observations")
                    self.log(self.active, "/children ID lists tasks delegated from that task")
                    self.log(self.active, "/review [PARENT] lists tasks with uncertain outcomes")
                    self.log(self.active, "/more-tasks pages through your last task list")
                    self.log(self.active, "/maintain SERVICE[#GENERATION] TASK queues work for the maintainer")
                    self.log(self.active, "/tool enable|disable|default SERVICE in an agent window")
                    self.log(self.active, "/describe-tool SERVICE TOOL shows the tool's inputs")
                    self.log(self.active, "/model [auto|default|PROVIDER[/MODEL]] in an agent window")
                    self.log(self.active, "/older browses earlier messages. /latest returns to live chat.")
                    self.log(self.active, "/memory shows the saved conversation summary")
                case "/status":
                    self.select("status")
                case "/compact" if self.active != "status":
                    window = self.active
                    self.start(lambda: self.compact(window), window)
                case "/memory" if self.active != "status":
                    window = self.active
                    self.start(lambda: self.show_memory(window), window)
                case "/tools" if self.active != "status":
                    window = self.active
                    self.start(lambda: self.tool_status(window), window)
                case "/older" if self.active != "status":
                    window = self.active
                    self.start(lambda: self.older_history(window), window)
                case "/latest" if self.active != "status":
                    window = self.active
                    self.start(lambda: self.latest_history(window), window)
                case "/tool" if self.active != "status":
                    window = self.active
                    self.start(lambda: self.change_tool(window, argument), window)
                case "/model" if self.active != "status":
                    window = self.active
                    self.start(lambda: self.model_preference(window, argument), window)
                case "/tasks":
                    self.select("status")
                    self.start(lambda: self.list_tasks(argument.removeprefix("agent.")))
                case "/review":
                    self.select("status")
                    self.start(lambda: self.review_queue(argument))
                case "/more-tasks":
                    self.select("status")
                    self.start(self.more_tasks)
                case "/maintain" if " " in argument:
                    target, _, instruction = argument.partition(" ")
                    service, marker, generation = target.partition("#")
                    if marker and (not generation.isascii() or not generation.isdecimal()):
                        self.log("status", "Usage /maintain SERVICE[#GENERATION] TASK")
                        return True
                    expected = int(generation) if marker else None
                    self.select("status")
                    self.start(lambda: self.queue_maintainer(service, instruction.strip(), expected))
                case "/queue" | "/queue-check" if " " in argument:
                    agent, _, body = argument.partition(" ")
                    recovery = None
                    instruction = body.strip()
                    if command == "/queue-check":
                        check, separator, task = body.partition("|")
                        if not separator or not check.strip() or not task.strip():
                            self.log("status", "Usage /queue-check AGENT CHECK | TASK")
                            return True
                        recovery, instruction = check.strip(), task.strip()
                    self.select("status")
                    self.start(
                        lambda: self.queue_task(
                            agent.removeprefix("agent."), instruction, recovery
                        )
                    )
                case "/task" if argument:
                    self.select("status")
                    self.start(lambda: self.show_task(argument))
                case "/timeline" if argument:
                    self.select("status")
                    self.start(lambda: self.task_timeline(argument))
                case "/watch-task" if argument:
                    self.select("status")
                    self.toggle_task_watch(argument)
                case "/save-image" if argument:
                    self.select("status")
                    self.start(lambda: self.save_evidence(argument))
                case "/evidence" if argument:
                    self.select("status")
                    self.start(lambda: self.show_evidence(argument))
                case "/children" if argument:
                    self.select("status")
                    self.start(lambda: self.list_tasks(parent=argument))
                case "/reconcile" if argument:
                    self.select("status")
                    self.start(lambda: self.reconcile_task(argument))
                case "/resolve" if argument:
                    self.select("status")
                    self.start(lambda: self.resolve_task(argument))
                case "/services":
                    self.select("status")
                    self.start(self.list_services)
                case "/models":
                    self.select("status")
                    self.start(self.list_models)
                case "/describe" if argument:
                    self.select("status")
                    self.start(lambda: self.inspect(argument))
                case "/describe-tool" if argument:
                    self.select("status")
                    self.start(lambda: self.describe_tool(argument))
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
            self.history_page.pop(window, None)
            if window in self.latest_before:
                self.history_before[window] = self.latest_before[window]
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
        browsing = self.active in self.history_page
        history_label = ""
        if browsing:
            history_label = "  |  saved history"
            if self.history_before[self.active] is None:
                history_label += "  |  start of conversation"
        self.put(
            0,
            f" GlassMoon  |  {'status' if self.active == 'status' else '@' + self.active}"
            + history_label,
            curses.A_REVERSE,
        )
        lines = []
        entries = list(self.history_page.get(self.active, self.windows[self.active]))
        if self.active in self.partial and not browsing:
            entries.append(f"<{self.active}> {self.partial[self.active]}")
        for entry in entries:
            for line in entry.split("\n"):
                lines.extend(
                    textwrap.wrap(line, width - 1, replace_whitespace=False) or [""]
                )
        maximum_scroll = max(0, len(lines) - height + 3)
        self.scroll = maximum_scroll if self.scroll_to_top else min(self.scroll, maximum_scroll)
        self.scroll_to_top = False
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
    parser.add_argument("--evidence", default="tools.evidence", help="Evidence service for /save-image")
    args = parser.parse_args()
    try:
        curses.wrapper(
            lambda screen: asyncio.run(
                Terminal(screen, args.socket, args.mailbox, args.evidence).run(args.query)
            )
        )
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
