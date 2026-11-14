"""Low-level GlassMoon protocol connection for local Python processes."""

import asyncio
import json
from contextlib import asynccontextmanager

PROTOCOL = 1
MAX_FRAME = 64 * 1024


class ProtocolError(Exception):
    """The connection ended or carried an invalid frame."""


class RequestError(Exception):
    """The broker or service rejected a request."""

    def __init__(self, error, *, generation=None):
        self.code = error["code"]
        self.generation = generation
        super().__init__(error["message"])


class Wire:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.features = ()

    async def send(self, kind, /, **fields):
        frame = json.dumps(
            {"type": kind, **fields},
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(frame) > MAX_FRAME:
            raise ProtocolError("Outgoing frame exceeds the frame limit")
        self.writer.write(frame + b"\n")
        await self.writer.drain()

    async def receive(self):
        try:
            frame = await self.reader.readline()
        except ValueError as error:
            raise ProtocolError("Incoming frame exceeds the frame limit") from error
        if not frame:
            raise EOFError("Broker disconnected")
        if len(frame) > MAX_FRAME + 1 or not frame.endswith(b"\n"):
            raise ProtocolError("Invalid frame length or delimiter")
        try:
            message = json.loads(frame)
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
            raise ProtocolError("Incoming frame is not valid JSON") from None
        if not isinstance(message, dict):
            raise ProtocolError("Expected a JSON object")
        return message

    async def request(self, kind, request_id, /, **fields):
        """Send one request when no other replies or events are expected."""
        await self.send(kind, id=request_id, **fields)
        message = await self.receive()
        if message.get("type") != "response" or message.get("id") != request_id:
            raise ProtocolError(f"Expected a response to {request_id!r}")
        if message["status"] == "error":
            raise RequestError(message["error"], generation=message.get("generation"))
        return message


@asynccontextmanager
async def connect(path):
    reader, writer = await asyncio.open_unix_connection(path, limit=MAX_FRAME + 1)
    wire = Wire(reader, writer)
    try:
        hello = await wire.request("hello", "hello", protocol=PROTOCOL)
        wire.features = hello["result"].get("features", ())
        yield wire
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass


async def active_service(path, name):
    async with connect(path) as wire:
        services = (await wire.request("list", "list"))["result"]["services"]
    return next(
        (row for row in services if row["name"] == name and row["state"] == "active"), None
    )


async def call(
    path, service, method, params, *, on_chunk=None, version=1, generation=None,
    envelope=False,
):
    """Call a service, optionally returning the response envelope with its generation."""
    async with connect(path) as wire:
        if generation is not None and "generation_calls" not in wire.features:
            raise RequestError({
                "code": "unsupported_generation_calls",
                "message": "Update the broker before sending generation-specific calls",
            })
        await wire.send(
            "call",
            id="call",
            service=service,
            method=method,
            params=params,
            interface_version=version,
            stream=on_chunk is not None,
            **({"generation": generation} if generation is not None else {}),
        )
        while True:
            message = await wire.receive()
            if message.get("id") != "call":
                raise ProtocolError("Unexpected response ID")
            if message["type"] == "chunk" and on_chunk is not None:
                if generation is not None and message.get("generation") != generation:
                    raise ProtocolError("Stream chunk came from an unexpected generation")
                await on_chunk(message["data"])
            elif message["type"] == "response":
                if message["status"] == "error":
                    raise RequestError(
                        message["error"], generation=message.get("generation")
                    )
                if generation is not None and message.get("generation") != generation:
                    raise ProtocolError("Call response came from an unexpected generation")
                return message if envelope else message["result"]
            else:
                raise ProtocolError("Unexpected call response")
