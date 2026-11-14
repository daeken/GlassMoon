# GlassMoon

GlassMoon is an agentic coding harness built around a local IPC broker. It supports live service replacement, persistent agent conversations, and capability-based model selection.

## Start an offline stack

The example manifest starts a broker, model router, offline provider, task mailbox, and agent in a fresh directory. Build the broker first. Omit `--smoke` to keep the stack up until Ctrl-C.

```sh
cargo build --locked
uv sync --locked
uv run --locked python -m glassmoon.stack examples/offline-stack.toml \
  --state /tmp/glassmoon-my-test --smoke
```

The launcher checks registration and configured readiness probes, logs each child, and cleans up its own process groups on exit. Start with a new state directory. After stopping the stack, use `--resume` with the same state directory and working directory to preserve its tasks and conversations. Resume can clear a stale broker socket after an abrupt exit. The launcher forwards only the environment entries listed in the manifest or needed for local execution. Use trusted manifests and commands.

Use `--restart-broker` with workers that support reconnecting. The launcher replaces the broker once after an exit and waits for its workers to register and pass their readiness probes.

## Chat with an agent

Build the broker and install the Python dependencies. Choose a private socket directory and share the printed socket path across the terminals you use below.

```sh
cargo build --locked
uv sync --locked
glassmoon_dir=$(mktemp -d /tmp/glassmoon.XXXXXX)
export GLASSMOON_SOCKET="$glassmoon_dir/broker.sock"
printf '%s\n' "$GLASSMOON_SOCKET"
target/debug/glassmoon "$GLASSMOON_SOCKET"
```

Set `GLASSMOON_SOCKET` to the printed path in each other terminal. Start each of these commands in its own terminal. The demo provider echoes your input and works offline.

```sh
uv run --locked python -m glassmoon.provider_router --socket "$GLASSMOON_SOCKET"
uv run --locked python -m glassmoon.demo_provider --socket "$GLASSMOON_SOCKET"
uv run --locked python -m glassmoon.agent --socket "$GLASSMOON_SOCKET" --name moon
uv run --locked python -m glassmoon.tui --socket "$GLASSMOON_SOCKET" --query moon
```

Type a message to chat. Use `/query NAME` to open another agent, F2 to cycle windows, and Page Up or Page Down to scroll. The status window shows service lifecycle events. `/services` refreshes generation state, in-flight counts, and the time remaining for draining workers. `/describe NAME|ID` shows an interface, and `/models` lists active catalogs. Use `/quit` to exit.

Each agent saves its transcript in a local SQLite database keyed by name. `--state` sets the database path. The agent sends recent complete turns within the IPC frame budget and retains the full committed history on disk. `--provider` and `--model` set its default pin. Use `/model PROVIDER` or `/model PROVIDER/MODEL` in an agent's window to change selection for the next turn. `/model auto` selects by capabilities, and `/model default` restores the startup selection. `/model` shows both the saved preference and the model last used in a saved turn. The agent preserves its transcript across these changes. The next turn reports when a pinned model is unavailable. A selected provider can receive the conversation context, including when it summarizes earlier turns.

Use `/older` in an agent's TUI window to page backward through its saved conversation. `/latest` returns to the live chat. `/memory` shows the saved summary and the most recent revision it covers.

Use `/saved` to open the latest stored transcript page, including replies to mailbox tasks completed since you opened the chat. Then `/older` continues from that page and `/latest` returns to the live view.

Use `/find TEXT` to search saved messages, including those already summarized. Run `/find` again to see earlier matches and `/latest` to return to the live chat.

### Use your ChatGPT plan

Run the sign-in command and approve GlassMoon in your browser with an eligible account. The worker reads and refreshes its OAuth credentials from an owner-only local profile. `--profile` selects a different file when passed to both commands. [OpenAI's ChatGPT plan integration guide](https://developers.openai.com/siwc/token-sharing-open-source)

```sh
uv run --locked python -m glassmoon.openai_service login
uv run --locked python -m glassmoon.openai_service serve --socket "$GLASSMOON_SOCKET" --catalog examples/openai-models.json
```

Use the model router and TUI commands above. Omit the demo provider or start the agent with `--provider openai`. The example model descriptor is editable. Query the account-specific catalog for eligible model IDs, and use the official model documentation to set the capabilities you want to advertise. [OpenAI model capabilities](https://developers.openai.com/api/docs/models/gpt-6.1-sol)

```sh
uv run --locked python -m glassmoon --socket "$GLASSMOON_SOCKET" call openai available_models
```

OpenAI's dedicated conversation service exposes `models`, `start`, `add`, `history`, and `respond`. It stores conversations locally and reads the upstream response as a stream as required by ChatGPT plan usage. For a buffered reply, omit the CLI's `--stream` flag. [OpenAI’s plan usage requirements](https://developers.openai.com/siwc/token-sharing-open-source/preview-limitations)

```sh
uv run --locked python -m glassmoon --socket "$GLASSMOON_SOCKET" call openai start '{"model":"gpt-6.1-sol"}'
uv run --locked python -m glassmoon --socket "$GLASSMOON_SOCKET" call openai add '{"conversation":"YOUR_ID","message":{"role":"user","content":"Hello, GlassMoon"}}'
uv run --locked python -m glassmoon --socket "$GLASSMOON_SOCKET" call openai respond '{"conversation":"YOUR_ID"}' --stream
```

### Use DeepSeek or the Z.ai Coding Plan

Load `DEEPSEEK_API_KEY` for DeepSeek or `ZAI_API_KEY` for the Z.ai account with an active Coding Plan in the provider's shell. The provider reads its key from the environment. Z.ai uses the dedicated Coding Plan endpoint. [DeepSeek API](https://api-docs.deepseek.com/api/create-chat-completion/) · [Z.ai Coding Plan endpoints](https://zcode.z.ai/en/docs/configuration)

```sh
uv run --locked python -m glassmoon.chat_provider deepseek --socket "$GLASSMOON_SOCKET"
uv run --locked python -m glassmoon.chat_provider zai --socket "$GLASSMOON_SOCKET"
```

Run either or both in separate terminals. The router discovers them as `deepseek` and `zai`. `--catalog` overrides the built-in model descriptors. Start an agent with `--provider zai --model glm-5.3`, for example, to choose that subscription for its turns.

### Give an agent workspace tools

Start the agent with `--workspace` to let it browse directories and read, create, and edit UTF-8 files in that directory. `list_files` accepts `.` for the root and returns pages of visible directory entries. Add `--python-eval` to let it run raw Python expressions and statements. Keep the same name and state path and pass `--replace GENERATION` if the agent is already running.

```sh
uv run --locked python -m glassmoon.agent --socket "$GLASSMOON_SOCKET" \
  --name moon --provider openai --workspace "$PWD" --python-eval
```

Raw Python runs as your local user in a fresh process with the workspace as its working directory. It can access anything your local user can access. File tools accept workspace-relative paths and check the last-read revision before overwriting. An agent with workspace tools selects models that advertise function calling. The TUI shows tool activity during streamed turns.

Use `find_in_file` for literal, case-sensitive matches with line and character column numbers. Pass the returned `next` and `revision` to continue a search against the same file revision.

Use `workspace_diff` to review one UTF-8 file of up to 128 KiB against `HEAD` or an explicit full Git commit ID. The reply includes the resolved baseline, the current file's SHA-256 revision, and a bounded unified diff with a `truncated` indicator. It compares the file on disk against the commit and leaves the index untouched.

Add `--workspace-read-only` for a reviewer that needs directory listings, file contents, and search. This flag controls the built-in workspace tools. The agent's separately configured tool services retain their own capabilities.

Use `edit_file_batch` for up to eight exact replacements within one file revision. The edits run in order, and the file is written only after all replacements match.

### Register another model provider

Register a service with `kind: "model_provider"` and expose the `models` and `generate` methods. The [offline provider](glassmoon/demo_provider.py) is a complete example. Registration lasts as long as the provider has an active broker generation.

- `models({})` returns `{"models": [...]}`. Each model declares `id`, `input_modalities`, `output_modalities`, `context_tokens`, and `streaming`. IDs must be unique within the catalog. A model may also declare `max_output_tokens` and `tool_calls`.
- `generate({model, messages, output_modalities})` returns `{"message": {"role": "assistant", "content": [...]}}` and may include usage information. Streams emit content parts before the final response. Text parts use `{"type":"text","text":"..."}`. OpenAI also accepts user image and file parts with a `url`. Other providers define how their supported content parts are encoded.
- A provider with `tool_calls: true` accepts `tools` with name, description, and a JSON Schema for parameters. A tool request includes `tool_calls` with `id`, `name`, and JSON-encoded `arguments`, plus optional provider-specific `tool_state`. On continuation, the caller pins the same provider and model and supplies `tool_history` exchanges containing `calls`, `outputs` keyed by call ID, optional assistant `text`, and the saved `state`.
- The router exposes `models` and `generate`. A generation request supplies `messages` and may specify `input_modalities`, `output_modalities`, `min_context_tokens`, `provider`, or `model`. It picks the first compatible active provider and model in name order. It checks the message modalities and streaming requirement and includes the chosen provider and model in the reply. Each new turn can choose a different provider, which receives the supplied transcript.

All services on this local socket are trusted. If multiple model providers can serve an agent, each selected provider can receive the agent's recent transcript. Pin the provider to restrict where that transcript goes.

When a provider asks an agent to use tools, the continuation goes back to the same provider generation. A provider replaced mid-turn can return `provider_changed`. Review any tool effects before trying the task again.

## Bootstrap development inside GlassMoon

Agent state keeps the full transcript in SQLite and a separate summary of older complete turns. Agents compact automatically when their unsummarized transcript grows. Use `/compact` in an agent chat or the `compact` RPC to request it explicitly. Use `info` to inspect the covered revision.

```sh
uv run --locked python -m glassmoon --socket "$GLASSMOON_SOCKET" call agent.moon compact
```

### Publish Python tools and connect MCP servers

Start each publisher in a separate terminal. The Python file exports a `TOOLS` mapping with a description, JSON Schema, and a synchronous or asynchronous handler for each function. The MCP bridge keeps one initialized session open and publishes its tool catalog through the same interface.

For a reviewed Python entry point, pass its `--sha256 DIGEST` to the publisher. It checks the file before executing the exact bytes it hashed and includes `source_sha256` in its `health` reply. Combine this with `--staged` to verify the digest on the candidate before activation.

```sh
uv run --locked python -m glassmoon.toolbox --socket "$GLASSMOON_SOCKET" \
  --name tools.moon examples/moon_tools.py
uv run --locked python -m glassmoon.mcp_bridge --socket "$GLASSMOON_SOCKET" \
  --name mcp.math -- "$PWD/.venv/bin/python" -m examples.math_mcp
```

For a Streamable HTTP MCP server, use `--url` in place of the local command. `--bearer-env` selects the environment variable holding its bearer token. For local stdio servers, `--pass-env NAME` forwards a named environment variable.

Pass `--image-tool NAME` for an MCP function that returns images. The bridge exposes its first supported, bounded JPEG or PNG as an image tool result, so the router can select a model with image support.

Use `/health SERVICE` on an MCP bridge to check whether the upstream is reachable and whether its advertised tools changed. A `changed` report lists added, removed, and modified tools. Stage and probe a replacement bridge to adopt the new definitions.

An agent turn calls the same tool generation that described its tools. If a service changes during the turn, a stale call returns `tool_changed`. The next turn discovers the replacement's definitions.

Use `/describe-tool SERVICE TOOL` to inspect one shared tool's inputs and the generation that advertised them.

In an agent's TUI window, use `/tool enable SERVICE` or `/tool disable SERVICE` to change its shared tools for new turns. `/tool default SERVICE` restores the startup setting for that name. `/tools` reports the saved settings revision, availability, and the generation it probed. An enabled service can come online later. The agent stores these overrides in its own database, and changes can apply while its current turn finishes. Trusted broker clients can make the same revision-checked changes with `tool_policy` and `set_tool_policy`.

Each turn can expose up to 32 tools. If a service's catalog would exceed the remaining slots, `/tools` shows its size and the number of slots left.

Use `--select-tool SERVICE:TOOL` to expose selected functions from a configured tool service. Repeat for each function you want that agent to use. The selection applies when the service is enabled, including when enabled from the TUI. `/tools` reports a selection that is missing from the current worker generation.

Pass `--configurable-tool-service SERVICE` to let the agent manage that service with `configure_shared_tool`. Repeat the option for additional services. The tool shows the approved names and current revision, and applies changes on the next turn. Explicit operator settings take precedence. Removing a name from the approved list ends any agent override for that service while the name is absent.

Launch or replace an agent with `--tool-service tools.moon --tool-service mcp.math`. It discovers the current tools before each turn and sees them as `tools_moon__text_budget` and `mcp_math__multiply`. The same tools are callable from Python, including an agent's `python_eval`.

```python
import asyncio
import os

from glassmoon.toolbox import call_tool

result = asyncio.run(call_tool(
    os.environ["GLASSMOON_SOCKET"], "mcp.math", "multiply", {"a": 6, "b": 7}
))
print(result["structuredContent"]["result"])
```

### Give an agent a browser

Install Playwright's browser binaries once, then start one worker per engine. The default is headless. Add `--headed` to see its windows.

```sh
uv run --locked playwright install chromium webkit
uv run --locked python -m glassmoon.browser --socket "$GLASSMOON_SOCKET" \
  --name browser.chromium --engine chromium
uv run --locked python -m glassmoon.browser --socket "$GLASSMOON_SOCKET" \
  --name browser.webkit --engine webkit
```

Give an agent `--tool-service browser.chromium` or `--tool-service browser.webkit`. Open a window and keep the returned handle for navigation, inspection, JavaScript, screenshots, and input. `wait_for` checks an observable selector and optional text. `diagnostics` returns recent page errors, console warnings, and failed requests for the window. `actions` runs an ordered sequence of clicks, typing, keypresses, and scrolls within one window. Browser contexts start fresh and have independent storage. A worker replacement closes its old contexts.

Use `snapshot` to see the page's accessibility tree. With `element`, target a role and exact accessible name, label, or placeholder for a `read`, `click`, or `fill` action. An ambiguous target returns an error before the interaction. `actions` also accepts `element` steps with `operation` set to `click` or `fill`, and `wait_for` steps to check the result. It stops at the first error and reports how many earlier steps completed. [Playwright locator reference](https://playwright.dev/python/docs/locators)

For fixture-driven work, pass `--allow-origin http://127.0.0.1:8000` to limit intercepted HTTP requests to one exact scheme, host, and port. Repeat the option for additional origins. In this mode redirects with a destination and WebSockets are blocked. Use operating-system isolation if you need process-wide network restrictions.

Screenshots use viewport coordinates and arrive as image tool results. The router selects a model declaring `tool_images` and image input once a screenshot tool is available. The OpenAI example catalog includes this support. The runloop retains the latest screenshot within the IPC frame budget. Browser tools are also callable in Python with `glassmoon.toolbox.call_tool`. The TUI command `/tools` probes the agent's currently configured tool services.

Supply `width` and `height` to `open` to test a specific viewport. Each dimension accepts 240–1600 CSS pixels. Screenshots and coordinate clicks use the window's actual dimensions.
Use `screenshot_region` with `x`, `y`, `width`, and `height` for a closer look inside the viewport. The reply includes the region's offset for converting back to click coordinates.

For disposable browser workers, `--idle-seconds N` allows opening a new window to reclaim older idle windows. Windows with operations in progress remain available to those operations.

To check browser integration locally, run `uv run --locked python -m unittest discover -s tests -p test_browser.py`. With the browser binaries installed, the test opens `examples/browser-smoke.html` over loopback, fills and submits the note form by its accessible labels, checks the visible result, and exercises the screenshot contract in each installed engine.

### Share a browser screenshot between agents

Start a local visual evidence service with its own SQLite file. Pass `--evidence tools.evidence` when launching a browser worker, and add `--tool-service tools.evidence` to the receiving agent.

```sh
uv run --locked python -m glassmoon.evidence --socket "$GLASSMOON_SOCKET" --state /absolute/path/visual.sqlite3
```

The browser's `share` tool publishes a screenshot with an owner, key, and reproduction summary. It includes the viewport dimensions, engine, and counts of diagnostic categories. Share the owner and key in the mailbox task. The recipient can read the summary and explicitly request the image. The image call verifies the stored SHA-256 digest. Review what is visible before choosing to share, since the screenshot includes page content.

Use `share_checked` to verify the exact visible text of one CSS-matched element before and after capturing a screenshot. A successful publication includes the expected and observed text, selector, page path without query or fragment, and the usual evidence metadata. Link the resulting key to the task so a reviewer can inspect the saved check and screenshot after the window closes.

Use `/evidence OWNER` to find published keys and `/evidence OWNER KEY` to inspect a reproduction summary and its metadata. `/save-image OWNER KEY PATH` checks the digest and creates a new local file with owner-only permissions. Choose an unused destination.

Use `/link-evidence TASK OWNER KEY [ATTEMPT]` to confirm a published observation exists and link it to a task. Agents can also link it with the mailbox tool `attach_evidence`. `/task` and `/timeline` display the reference so recipients and operators can look up the same observation.

### Attach the macOS desktop

On macOS 14 or newer, build the native helper and start the desktop service.

```sh
mkdir -p target
swiftc -parse-as-library native/desktop.swift -o target/glassmoon-desktop
uv run --locked python -m glassmoon.desktop --socket "$GLASSMOON_SOCKET"
```

The desktop service captures the main display, including other visible apps. The helper reports the macOS Screen Recording and Accessibility permissions. Start the service with `--allow-input` to publish native clicks, typing, keypresses, and scrolling. Add `--target-pid PID` to require that app to be frontmost and keep pointer actions within its visible windows. Coordinate actions use the main-display screenshot dimensions. Text insertion uses the focused control’s Accessibility interface and requires that the control expose editable selected text. Attach the service to an agent with `--tool-service desktop`.

For a read-only worker scoped to one app, start it with `--window-capture --target-pid PID`. Call `windows` for that app's visible window IDs, then `window_screenshot` for the selected window. [ScreenCaptureKit window filter](https://developer.apple.com/documentation/screencapturekit/sccontentfilter/init%28desktopindependentwindow%3A%29)

With `--target-pid`, `accessibility_snapshot` returns a bounded tree of roles and labels from that app's windows. It requires macOS Accessibility permission. The tool reads labels from Apple's Accessibility API and omits control values. [Apple Accessibility attributes](https://developer.apple.com/documentation/applicationservices/1462085-axuielementcopyattributevalue)

Add `--evidence tools.evidence` to that worker to share a selected window using `share_window`. The evidence records the capture dimensions, process ID, and window ID with the screenshot digest.

### Keep shared working notes

Moon's scratchpad stores revisioned notes in SQLite. Use a dedicated absolute database path and add `--tool-service tools.scratchpad` to agents that should share it.

```sh
GLASSMOON_SCRATCHPAD_PATH="/absolute/path/scratchpad.sqlite3" \
  uv run --locked python -m glassmoon.toolbox --socket "$GLASSMOON_SOCKET" \
  --name tools.scratchpad examples/moon_scratchpad.py
```

### Queue work for an agent

Run the task mailbox and dispatcher in separate terminals. An agent can submit work through `--tool-service tools.mailbox`; a dispatcher delivers it when the named recipient is running.

```sh
uv run --locked python -m glassmoon.mailbox --socket "$GLASSMOON_SOCKET" --state /absolute/path/tasks.sqlite3
uv run --locked python -m glassmoon.dispatcher --socket "$GLASSMOON_SOCKET" --agent moon --agent tide
```

Use `--discover` on a trusted stack to deliver work to agents as they register. Combine it with `--agent` to restrict discovery to named recipients. Tasks wait in the mailbox until their recipient is online.

`submit` takes a sender, recipient agent name, idempotency key, task text, and optional `recovery_check`. Repeating the same submission returns its existing task ID. Use `/queue AGENT TASK` to submit from the TUI, or `/queue-check AGENT CHECK | TASK` to attach a recovery check. Use `/tasks [AGENT]` and `/task ID` to see delivery state, recorded tool events, the recovery check, and the final reply or verified outcome. Tool events store names and phases.

A dispatcher claims each task with a renewable lease. After losing a reply, the dispatcher checks the agent receipt before reporting an ambiguous delivery. An expired lease or ambiguous delivery moves the task to `review`. Use `/reconcile ID` in the TUI to recover a reply committed by the recipient when the delivery acknowledgment was lost. A saved partial reply is shown in the task while it stays in review. A receipt that only says execution started also leaves the task in review. After checking the work's effects, use `/resolve ID REVISION ACTION NOTE` in the TUI or the mailbox's `review` operation. Choose `verified` to record independently verified completion, `requeue` to retry, or `cancel` to close the task. The mailbox is shared among trusted broker clients.

Use `/timeline ID` to see when the mailbox recorded claims, tool events, replies, and review decisions across attempts. Task lists and details identify whether a completed task has an agent reply, a recovered reply, or an operator verification. Tasks created before recording was enabled have a partial timeline.

Use `/watch-task ID` to follow a task as it runs. The watch reconnects after a broker or mailbox outage and stops at a saved reply, review, or cancellation. Run the command again to stop watching sooner.

An agent can set `parent` to the current task ID when it submits a handoff. `/children ID` lists those related tasks, and a child's `/task ID` view shows its parent. Each task retains its own reply and review state.

When linking evidence with `attach_evidence`, include the attempt number if known. `/task ID` and `/timeline ID` then identify which attempt produced the observation even after a retry.

Use `/review` for tasks that need a decision. `/review PARENT` shows only the children of that task that are awaiting review. `/more-tasks` pages backward through your last task list with its filters intact.

Use `/handoff` when taking over a running stack. It shows review items first, followed by running, queued, and recently completed work, with an online check, last recorded activity, and links to the task details.

If a turn exhausts its tool budget or the model fails after tool calls, the agent saves a partial report in its transcript. The mailbox keeps that task in review so the operator can check the effects before retrying.

When delivery is rejected before the agent starts, the mailbox schedules a short retry delay. Other ready tasks can be claimed during that delay.

### Probe a candidate before switching traffic

Workers that accept `--staged` register a candidate generation while the current generation keeps serving. Run the release command with the candidate ID printed by the worker.

```sh
uv run --locked python -m examples.moon_service --socket "$GLASSMOON_SOCKET" --staged
uv run --locked python -m glassmoon.release --socket "$GLASSMOON_SOCKET" GENERATION \
  --probe health --expect '{"status":"ok","version":1}'
```

The release command sends the probe to the specified generation and compares the result before attempting activation. The broker checks that the active generation still matches the one observed before the probe. A failed probe leaves normal traffic on its current route. A draining generation can finish its existing calls.

Add `--check-only` to run the probe while leaving the candidate staged. The result identifies the checked generation and time, and includes a verified Python entry point digest when the probe reports one. A later release probes the candidate again before activation.
Pass `--expected-active GENERATION` to require the active route you reviewed, or `0` to require an empty route.

To retire a rejected candidate, run `python -m glassmoon --socket "$GLASSMOON_SOCKET" discard GENERATION`. The broker accepts this command for staged generations.

Shared tool workers expose `health` with `status` and their tool count. Agents report their identity and saved conversation revision. These probes check basic worker readiness. Use a service-specific probe when release depends on a downstream dependency.

Agents, browser and macOS desktop workers, the mailbox, the evidence service, and MCP bridges also accept `--staged`. Use the same state path for stateful replacements. For an agent, probe its saved revision before activation. For a browser replacement, old windows close when the old worker retires.

The model router and OpenAI, DeepSeek, and Z.ai providers also accept `--staged`. Probe `models` to check the candidate catalog. A catalog probe compares declared capabilities. An exact-generation `generate` probe can verify access to the upstream model when live verification is needed.

Browser handles belong to the worker that created them. A replacement returns `stale_window` for a handle issued by another worker. Open a fresh window to continue. A surviving browser worker keeps its handles when only the broker reconnects.

Generation-specific calls require a broker that advertises the `generation_calls` feature. Rebuild and restart an older broker before using staged release or generation-bound tool and provider calls.

### Supervise a local worker

Moon wrote the `moon.text` example service, which fingerprints UTF-8 text and exposes a deterministic health probe. The supervisor starts a worker, validates probe fields, restarts it after repeated failures or process exit, and can send an incident to a named maintainer agent.

```sh
uv run --locked python -m glassmoon.supervisor --socket "$GLASSMOON_SOCKET" \
  --service moon.text --expect '{"status":"ok","version":1}' --maintainer agent.moon \
  -- "$PWD/.venv/bin/python" -m examples.moon_service --socket "$GLASSMOON_SOCKET"
```

Use a fresh service name or stop any existing worker before starting the supervisor. Recovery is bounded by the restart budget. The supervisor owns its child process group. A healthy probe after restart confirms the broker route is back. The maintainer receives the service name, probe failure, and restart status. Add `--mailbox tools.mailbox` to save that incident for dispatch even if the maintainer is offline.

The supervisor waits through a broker outage while the worker is still running. When the broker is reachable again, the worker has the configured probe timeout to re-register before failed probes count toward a restart.

Workers can declare a maintainer with `--maintainer AGENT`. `/maintainer SERVICE` shows the active generation and its declared maintainer. `/services` and `/describe` also display this metadata. `/maintain SERVICE TASK` queues work for the current generation's maintainer and records the service and generation in the task. Use `SERVICE#GENERATION` to require a specific active generation at lookup. The maintainer can receive the task after coming online.

Use `/health SERVICE` to probe the active generation's `health` method and show its registered identity. The TUI reports a route change during the probe or a different service name declared in the health payload.

### Recover after a broker restart

The agent, browser, mailbox, model providers and router, Python tool publisher, MCP bridge, and desktop service reconnect when their broker socket returns. Promoted staged workers join this recovery when reconnect is enabled and the broker has sent their activation notice. An unpromoted candidate needs a fresh probe after losing the broker. The TUI restores its lifecycle feed with a fresh snapshot. Existing browser windows and agent state remain available when their own workers survive. In-flight calls whose connection was lost may have run, so task receipts and review govern their recovery.

## Try the live service cutover

You’ll need a Unix environment, a Rust toolchain with Cargo, and Python 3.11 or newer. Run these commands from the checkout.

In one terminal, build and start the broker in a private directory. Copy the printed socket path for the other terminals.

```sh
cargo build --locked
glassmoon_dir=$(mktemp -d /tmp/glassmoon.XXXXXX)
printf '%s\n' "$glassmoon_dir/broker.sock"
target/debug/glassmoon "$glassmoon_dir/broker.sock"
```

In each additional terminal, set `GLASSMOON_SOCKET` to that path. Start the first worker, which prints its generation number.

```sh
python3 -m glassmoon.echo --socket "$GLASSMOON_SOCKET" --version v1
```

From another terminal, inspect and call the service. You can also leave `watch` running in a separate terminal to see lifecycle events.

```sh
python3 -m glassmoon --socket "$GLASSMOON_SOCKET" list
python3 -m glassmoon --socket "$GLASSMOON_SOCKET" describe 1
python3 -m glassmoon --socket "$GLASSMOON_SOCKET" call echo echo '{"value":"hello"}'
python3 -m glassmoon --socket "$GLASSMOON_SOCKET" watch
```

Use the worker’s generation number in place of `1` if it differs. To see a live cutover, start a slow call and launch v2 before it finishes.

```sh
python3 -m glassmoon --socket "$GLASSMOON_SOCKET" call echo echo '{"value":"slow","delay_ms":15000}'
```

```sh
python3 -m glassmoon.echo --socket "$GLASSMOON_SOCKET" --version v2 --replace 1
```

Once v2 reports that it is active, another call returns a v2 result. The slow call returns its v1 result and the v1 process exits. The broker’s optional second argument sets the drain deadline in milliseconds. The default is 30000. The broker removes its socket on SIGINT or SIGTERM.

## IPC protocol v1

Each message is a UTF-8 JSON object followed by a newline. JSON payloads are limited to 64 KiB per frame. Clients send requests with a `type` and a string `id` that is unique among their own outstanding requests. The broker replies with `type: "response"`, the same `id`, and either `status: "ok"` with `result` or `status: "error"` with an `error` object containing `code` and `message`. Admitted call replies also include the serving `generation`. Replies can arrive out of order.

| Request | Fields and behavior |
| --- | --- |
| `hello` | Send `protocol: 1` first on each connection. |
| `list` | Get a snapshot of registered generations and their state. |
| `describe` | Send `generation` to retrieve its registered interface. |
| `watch` | Get the initial snapshot, then receive `type: "event"` messages for registration, promotion, draining, and retirement. |
| `register` | Send `name`, `interface_version`, and `methods`. Optionally set `kind` to `model_provider`. Each method includes `description`, `params_schema`, and `result_schema`. The broker returns a generation ID tied to this connection. |
| `activate` | Send `generation` and `expected_active` to switch a staged generation into service from a separate trusted connection. |
| `discard` | Send `generation` to retire a staged candidate. |
| `promote` | From the staged worker’s connection, send `expected_active` with the generation to replace, or `null` for an empty route. Promotion succeeds only if the expectation still matches. |
| `call` | Send `service`, `interface_version`, `method`, and `params`. Set `stream: true` to receive chunks. If the broker advertises `generation_calls`, specify `generation` to call an exact active or staged generation. The broker checks the selected generation’s version and method. Workers validate arguments using the declared schemas as an interface contract. |

The broker forwards work as `type: "invoke"` with `call_id`, `method`, and `params`. The worker replies with `type: "complete"`, the same `call_id`, and an outcome in the same format as a response. For streamed calls, a worker can send `type: "chunk"` with its `call_id` and `data` before completion. The broker forwards these as `type: "chunk"` with the caller's `id`, the worker's `generation`, and `data`. When promoted, a new generation takes all subsequent calls. The old worker receives a `draining` message with `deadline_ms`, followed by `retire` when its calls finish or the deadline expires. At expiry the broker fails unresolved calls with `drain_timeout` and the worker is expected to stop. A worker disconnect fails its admitted calls, and a caller disconnect leaves admitted work with its worker.

The prototype assumes trusted local processes with access to the socket. Generation IDs and routes live in broker memory. Each broker process starts its generation sequence at a different random base. Services own any shared-state handoff, and the example workers are launched manually.

## Verify the cutover

```sh
cargo build --locked
uv run --locked python -m unittest discover -s tests -v
```

The integration tests hold a v1 invocation open, promote v2, and accept the v2 reply before releasing v1. Both replies use the same client connection. They also cover a drain timeout, stale promotion, and an interface version mismatch.

The test suite also covers buffered and streaming OpenAI replies with a simulated upstream, provider discovery and modality selection, and agent history across a provider change and agent restart.
