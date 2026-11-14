"""Example service that can run multiple versions side by side."""

import argparse
import asyncio
import sys

from .ipc import ProtocolError, RequestError, connect

METHODS = {
    "echo": {
        "description": "Return a value and identify the running implementation",
        "params_schema": {
            "type": "object",
            "properties": {
                "value": {},
                "delay_ms": {"type": "integer", "minimum": 0, "maximum": 60_000},
            },
            "required": ["value"],
            "additionalProperties": False,
        },
        "result_schema": {
            "type": "object",
            "properties": {"version": {"type": "string"}, "value": {}},
            "required": ["version", "value"],
            "additionalProperties": False,
        },
    }
}


async def echo(wire, message, version):
    params = message["params"]
    delay = params.get("delay_ms", 0) if isinstance(params, dict) else None
    if (
        not isinstance(params, dict)
        or "value" not in params
        or params.keys() - {"value", "delay_ms"}
        or type(delay) is not int
        or not 0 <= delay <= 60_000
    ):
        await wire.send(
            "complete",
            call_id=message["call_id"],
            status="error",
            error={"code": "invalid_params", "message": "Expected value and an optional delay_ms (0–60000)"},
        )
        return
    await asyncio.sleep(delay / 1000)
    try:
        await wire.send(
            "complete",
            call_id=message["call_id"],
            status="ok",
            result={"value": params["value"], "version": version},
        )
    except ProtocolError:
        await wire.send(
            "complete",
            call_id=message["call_id"],
            status="error",
            error={"code": "frame_too_large", "message": "Echo result exceeds the frame limit"},
        )


async def run(args):
    async with connect(args.socket) as wire:
        registered = await wire.request(
            "register", "register", name=args.name, interface_version=1, methods=METHODS
        )
        generation = registered["result"]["generation"]
        await wire.request("promote", "promote", expected_active=args.replace)
        print(f"{args.name} {args.version} active as generation {generation}", flush=True)

        async with asyncio.TaskGroup() as group:
            tasks = set()
            try:
                while True:
                    message = await wire.receive()
                    match message["type"]:
                        case "invoke":
                            task = group.create_task(echo(wire, message, args.version))
                            tasks.add(task)
                            task.add_done_callback(tasks.discard)
                        case "draining":
                            print(f"{args.name} {args.version} draining", flush=True)
                        case "retire":
                            print(f"{args.name} {args.version} retired ({message['reason']})", flush=True)
                            return
                        case unknown:
                            raise ProtocolError(f"Unexpected message type {unknown!r}")
            except (EOFError, OSError, ProtocolError) as error:
                # Let the task group finish cancellation before reporting the connection failure.
                failure = error
            finally:
                for task in tasks:
                    task.cancel()
        raise failure


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True, help="Broker socket path")
    parser.add_argument("--name", default="echo")
    parser.add_argument("--version", required=True, help="Implementation label returned by echo")
    parser.add_argument("--replace", type=int, help="Expected active generation to replace")
    try:
        asyncio.run(run(parser.parse_args()))
        return 0
    except KeyboardInterrupt:
        return 130
    except RequestError as error:
        print(f"{error.code}: {error}", file=sys.stderr)
    except (EOFError, OSError, ProtocolError) as error:
        print(error, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
