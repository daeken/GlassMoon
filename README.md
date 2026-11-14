# GlassMoon

GlassMoon is an agentic coding harness built around a local IPC broker. It supports live service replacement, persistent agent conversations, and capability-based model selection.

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

Type a message to chat. Use `/query NAME` to open another agent, F2 to cycle windows, and Page Up or Page Down to scroll. The status window shows service lifecycle events. `/services` refreshes generation state and in-flight counts, `/describe NAME|ID` shows an interface, and `/models` lists active catalogs. Use `/quit` to exit.

Each agent saves its transcript in a local SQLite database keyed by name. `--state` sets the database path. The agent sends recent complete turns within the IPC frame budget and retains the full committed history on disk. For a different model on the next turn, launch or retire a provider, or replace the agent with the same name and state using `--replace GENERATION` and new requirements. `--provider` and `--model` pin selection when needed.

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

Start the agent with `--workspace` to let it read, create, and edit UTF-8 files in that directory. Add `--python-eval` to let it run raw Python expressions and statements. Keep the same name and state path and pass `--replace GENERATION` if the agent is already running.

```sh
uv run --locked python -m glassmoon.agent --socket "$GLASSMOON_SOCKET" \
  --name moon --provider openai --workspace "$PWD" --python-eval
```

Raw Python runs as your local user in a fresh process with the workspace as its working directory. It can access anything your local user can access. File tools accept workspace-relative paths and check the last-read revision before overwriting. An agent with workspace tools selects models that advertise function calling. The TUI shows tool activity during streamed turns.

### Register another model provider

Register a service with `kind: "model_provider"` and expose the `models` and `generate` methods. The [offline provider](glassmoon/demo_provider.py) is a complete example. Registration lasts as long as the provider has an active broker generation.

- `models({})` returns `{"models": [...]}`. Each model declares `id`, `input_modalities`, `output_modalities`, `context_tokens`, and `streaming`. It may also declare `max_output_tokens` and `tool_calls`.
- `generate({model, messages, output_modalities})` returns `{"message": {"role": "assistant", "content": [...]}}` and may include usage information. Streams emit content parts before the final response. Text parts use `{"type":"text","text":"..."}`. OpenAI also accepts user image and file parts with a `url`. Other providers define how their supported content parts are encoded.
- A provider with `tool_calls: true` accepts `tools` with name, description, and a JSON Schema for parameters. A tool request includes `tool_calls` with `id`, `name`, and JSON-encoded `arguments`, plus optional provider-specific `tool_state`. On continuation, the caller pins the same provider and model and supplies `tool_history` exchanges containing `calls`, `outputs` keyed by call ID, optional assistant `text`, and the saved `state`.
- The router exposes `models` and `generate`. A generation request supplies `messages` and may specify `input_modalities`, `output_modalities`, `min_context_tokens`, `provider`, or `model`. It picks the first compatible active provider and model in name order. It checks the message modalities and streaming requirement and includes the chosen provider and model in the reply. Each new turn can choose a different provider, which receives the supplied transcript.

All services on this local socket are trusted. If multiple model providers can serve an agent, each selected provider can receive the agent's recent transcript. Pin the provider to restrict where that transcript goes.

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
| `promote` | From the staged worker’s connection, send `expected_active` with the generation to replace, or `null` for an empty route. Promotion succeeds only if the expectation still matches. |
| `call` | Send `service`, `interface_version`, `method`, and `params`. Set `stream: true` to receive chunks. The broker checks the active generation’s version and method. Workers validate arguments, using the declared schemas as an interface contract. |

The broker forwards work as `type: "invoke"` with `call_id`, `method`, and `params`. The worker replies with `type: "complete"`, the same `call_id`, and an outcome in the same format as a response. For streamed calls, a worker can send `type: "chunk"` with its `call_id` and `data` before completion. The broker forwards these as `type: "chunk"` with the caller's `id`, the worker's `generation`, and `data`. When promoted, a new generation takes all subsequent calls. The old worker receives a `draining` message with `deadline_ms`, followed by `retire` when its calls finish or the deadline expires. At expiry the broker fails unresolved calls with `drain_timeout` and the worker is expected to stop. A worker disconnect fails its admitted calls, and a caller disconnect leaves admitted work with its worker.

The prototype assumes trusted local processes with access to the socket. Generation IDs and routes live in broker memory. Services own any shared-state handoff, and the example workers are launched manually.

## Verify the cutover

```sh
cargo build --locked
uv run --locked python -m unittest discover -s tests -v
```

The integration tests hold a v1 invocation open, promote v2, and accept the v2 reply before releasing v1. Both replies use the same client connection. They also cover a drain timeout, stale promotion, and an interface version mismatch.

The test suite also covers buffered and streaming OpenAI replies with a simulated upstream, provider discovery and modality selection, and agent history across a provider change and agent restart.
