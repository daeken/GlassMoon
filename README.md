# GlassMoon

GlassMoon is an agentic coding harness built around a local IPC broker. The first milestone supports service discovery and live replacement while clients keep their connections.

## Try it locally

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
| `register` | Send `name`, `interface_version`, and `methods`. Each method includes `description`, `params_schema`, and `result_schema`. The broker returns a generation ID tied to this connection. |
| `promote` | From the staged worker’s connection, send `expected_active` with the generation to replace, or `null` for an empty route. Promotion succeeds only if the expectation still matches. |
| `call` | Send `service`, `interface_version`, `method`, and `params`. The broker checks the active generation’s version and method. Workers validate arguments, using the declared schemas as an interface contract. |

The broker forwards work as `type: "invoke"` with `call_id`, `method`, and `params`. The worker replies with `type: "complete"`, the same `call_id`, and an outcome in the same format as a response. When promoted, a new generation takes all subsequent calls. The old worker receives a `draining` message with `deadline_ms`, followed by `retire` when its calls finish or the deadline expires. At expiry the broker fails unresolved calls with `drain_timeout` and the worker is expected to stop. A worker disconnect fails its admitted calls, and a caller disconnect leaves admitted work with its worker.

The prototype assumes trusted local processes with access to the socket. Generation IDs and routes live in broker memory. Services own any shared-state handoff, and the example workers are launched manually.

## Verify the cutover

```sh
cargo build --locked
python3 -m unittest discover -s tests -v
```

The integration tests hold a v1 invocation open, promote v2, and accept the v2 reply before releasing v1. Both replies use the same client connection. They also cover a drain timeout, stale promotion, and an interface version mismatch.
