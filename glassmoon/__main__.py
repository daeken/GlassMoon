"""Command-line client for the GlassMoon broker."""

import argparse
import asyncio
import json
import sys

from .ipc import ProtocolError, RequestError, call, connect


def print_json(value):
    print(json.dumps(value, ensure_ascii=False), flush=True)


async def run(args):
    if args.command == "call":

        async def chunk(data):
            print_json({"type": "chunk", "data": data})

        reply = await call(
            args.socket,
            args.service,
            args.method,
            args.params,
            version=args.interface_version,
            on_chunk=chunk if args.stream else None,
            generation=args.generation,
            envelope=not args.stream,
        )
        if args.stream:
            print_json({"type": "response", "result": reply})
        else:
            print_json({"generation": reply["generation"], "result": reply["result"]})
        return
    async with connect(args.socket) as wire:
        match args.command:
            case "describe":
                print_json(
                    (await wire.request("describe", "cli", generation=args.generation))[
                        "result"
                    ]
                )
            case "list" | "watch":
                print_json((await wire.request(args.command, "cli"))["result"])
                if args.command == "watch":
                    while True:
                        print_json(await wire.receive())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True, help="Broker socket path")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="List registered services and generations")
    commands.add_parser("watch", help="Print a snapshot followed by lifecycle events")
    describe = commands.add_parser("describe", help="Show a generation's interface")
    describe.add_argument("generation", type=int)
    call = commands.add_parser("call", help="Call an active service")
    call.add_argument("service")
    call.add_argument("method")
    call.add_argument("params", nargs="?", type=json.loads, default={})
    call.add_argument("--interface-version", type=int, default=1)
    call.add_argument(
        "--generation", type=int, help="Target an exact staged or active generation"
    )
    call.add_argument(
        "--stream", action="store_true", help="Print chunks as they arrive"
    )
    try:
        asyncio.run(run(parser.parse_args()))
        return 0
    except KeyboardInterrupt:
        return 130
    except RequestError as error:
        print(f"{error.code}: {error}", file=sys.stderr)
    except (EOFError, OSError, ValueError, ProtocolError) as error:
        print(error, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
