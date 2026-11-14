"""Probe an exact staged generation and then atomically activate it."""

import argparse
import asyncio
import json

from .ipc import ProtocolError, RequestError, call, connect
from .service import fail


async def release(socket, generation, probe, params, expected, timeout=8, *, check_only=False):
    if not isinstance(expected, dict) or not expected or not isinstance(params, dict):
        raise fail("invalid_probe", "Supply probe parameters and at least one expected result field")
    async with asyncio.timeout(timeout), connect(socket) as wire:
        interface = (
            await wire.request("describe", "interface", generation=generation)
        )["result"]
        service = interface["name"]
        snapshot = (await wire.request("list", "snapshot"))["result"]["services"]
        if not any(
            row["generation"] == generation and row["state"] == "staged"
            for row in snapshot
        ):
            raise fail("not_staged", "Choose the generation of a staged worker")
        previous = next(
            (
                row["generation"]
                for row in snapshot
                if row["name"] == service and row["state"] == "active"
            ),
            None,
        )
        result = await call(
            socket,
            service,
            probe,
            params,
            version=interface["interface_version"],
            generation=generation,
        )
        if not isinstance(result, dict) or any(
            key not in result or result[key] != value for key, value in expected.items()
        ):
            raise fail(
                "probe_mismatch",
                f"Staged generation {generation} failed {service}.{probe}",
            )
        if check_only:
            return {
                "name": service, "generation": generation, "previous": previous,
                "probe": probe, "probe_passed": True,
            }
        return (
            await wire.request(
                "activate",
                "activate",
                generation=generation,
                expected_active=previous,
            )
        )["result"] | {"name": service, "probe": probe}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("generation", type=int)
    parser.add_argument("--probe", default="health")
    parser.add_argument("--params", type=json.loads, default={})
    parser.add_argument("--expect", type=json.loads, required=True)
    parser.add_argument("--timeout", type=float, default=8)
    parser.add_argument("--check-only", action="store_true", help="Probe the candidate without activating it")
    args = parser.parse_args()
    if (
        not isinstance(args.params, dict)
        or not isinstance(args.expect, dict)
        or not args.expect
    ):
        parser.error(
            "Provide parameter and expected-value JSON objects, with at least one expected field"
        )
    if not 0 < args.timeout <= 60 or args.generation <= 0:
        parser.error("Use a positive generation and a timeout up to 60 seconds")
    try:
        result = asyncio.run(
            release(
                args.socket,
                args.generation,
                args.probe,
                args.params,
                args.expect,
                args.timeout,
                check_only=args.check_only,
            )
        )
    except (RequestError, ProtocolError, EOFError, OSError, TimeoutError) as error:
        detail = str(error) or type(error).__name__
        parser.exit(1, f"{detail}\n")
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
