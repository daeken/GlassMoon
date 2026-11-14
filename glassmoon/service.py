"""Run a Python service as one replaceable broker generation."""

import argparse
import asyncio
import logging

from .ipc import ProtocolError, RequestError, connect

LOGGER = logging.getLogger(__name__)


def method(description, required=None, optional=None, result=None):
    required = required or {}
    return {
        "description": description,
        "params_schema": {
            "type": "object",
            "properties": required | (optional or {}),
            "required": list(required),
            "additionalProperties": False,
        },
        "result_schema": result or {"type": "object"},
    }


def fail(code, message):
    return RequestError({"code": code, "message": message})


async def invoke(wire, message, handler):
    call_id = message["call_id"]

    async def emit(data):
        if message.get("stream", False):
            await wire.send("chunk", call_id=call_id, data=data)

    try:
        async with asyncio.timeout(240):
            result = await handler(
                message["method"], message["params"], emit, message.get("stream", False)
            )
        await wire.send("complete", call_id=call_id, status="ok", result=result)
        return
    except RequestError as error:
        code, detail = error.code, str(error)
    except TimeoutError:
        code, detail = "timeout", "Service call timed out"
    except ProtocolError as error:
        code, detail = "frame_too_large", str(error)
    except (TypeError, ValueError, KeyError):
        code, detail = "invalid_params", "Check the service method's parameter schema"
    except Exception as error:  # noqa: BLE001 - isolate failures to the individual RPC.
        LOGGER.error("Unhandled service exception (%s)", type(error).__name__)
        code, detail = "service_error", "The service could not complete the request"
    await wire.send(
        "complete",
        call_id=call_id,
        status="error",
        error={"code": code, "message": detail},
    )


async def serve(
    socket,
    name,
    replace,
    methods,
    handler,
    *,
    kind=None,
    on_ready=None,
    staged=False,
):
    async with connect(socket) as wire:
        registration = await wire.request(
            "register",
            "register",
            name=name,
            interface_version=1,
            methods=methods,
            kind=kind,
        )
        if not staged:
            await wire.request("promote", "promote", expected_active=replace)
        state = "staged" if staged else "active"
        print(
            f"{name} {state} as generation {registration['result']['generation']}",
            flush=True,
        )
        if on_ready is not None:
            on_ready()
        async with asyncio.TaskGroup() as group:
            tasks = set()
            try:
                while True:
                    message = await wire.receive()
                    match message["type"]:
                        case "invoke":
                            task = group.create_task(invoke(wire, message, handler))
                            tasks.add(task)
                            task.add_done_callback(tasks.discard)
                        case "draining":
                            continue
                        case "retire":
                            return
                        case unknown:
                            raise ProtocolError(f"Unexpected message type {unknown!r}")
            except (EOFError, OSError, ProtocolError) as error:
                failure = error
            finally:
                for task in tasks:
                    task.cancel()
        raise failure


def service_args(description, name):
    parser = argparse.ArgumentParser(description=description)
    add_service_args(parser, name)
    return parser


def add_service_args(parser, name):
    parser.add_argument("--socket", required=True)
    parser.add_argument("--name", default=name)
    parser.add_argument(
        "--replace", type=int, help="Expected active generation to replace"
    )
