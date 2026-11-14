import asyncio
import os
import sys
import tempfile
import unittest
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, patch

from glassmoon.ipc import RequestError, call
from glassmoon.models import CATALOG, GENERATE
from glassmoon.provider_router import Router
from glassmoon.service import serve

BROKER = Path(os.environ.get("GLASSMOON_BROKER", "target/debug/glassmoon"))


class RoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_pinned_provider_catalog_skips_unrelated_workers(self):
        router = Router("unused")
        rows = [
            {"name": "selected", "generation": 11, "state": "active", "kind": "model_provider"},
            {"name": "other", "generation": 12, "state": "active", "kind": "model_provider"},
        ]
        wire = AsyncMock()
        wire.request.return_value = {"result": {"services": rows}}

        @asynccontextmanager
        async def connect(_):
            yield wire

        catalog = AsyncMock(return_value=([], None))
        with patch("glassmoon.provider_router.connect", connect), patch.object(router, "catalog", catalog):
            await router.models(provider="selected", generation=11)
            await router.models(provider="selected", generation=12)
        catalog.assert_awaited_once_with(rows[0])

    async def test_catalog_rejects_ambiguous_model_ids_within_one_provider(self):
        router = Router("unused")
        service = {"name": "fixture", "generation": 11, "interface_version": 1}
        model = {
            "id": "model", "input_modalities": ["text"], "output_modalities": ["text"],
            "context_tokens": 1000, "streaming": True,
        }
        with patch("glassmoon.provider_router.call", AsyncMock(return_value={"models": [
            model, model | {"input_modalities": ["text", "image"]},
        ]})):
            self.assertEqual(await router.catalog(service), ([], "fixture"))

    async def test_provider_generation_change_rejects_continuation(self):
        router = Router("unused")
        active = {
            "provider": "fixture", "provider_generation": 11, "id": "model",
            "interface_version": 1, "input_modalities": ["text"],
            "output_modalities": ["text"], "context_tokens": 1_000, "streaming": True,
        }
        params = {
            "messages": [{"role": "user", "content": "Test"}],
            "provider": "fixture", "model": "model",
        }
        catalog = AsyncMock(return_value=([active], []))
        invoke = AsyncMock(return_value={"message": {"role": "assistant", "content": "ready"}})
        with patch.object(router, "models", catalog), patch("glassmoon.provider_router.call", invoke):
            reply = await router.handle("generate", params, None, False)
            self.assertEqual(reply["provider_generation"], 11)
            self.assertEqual(invoke.await_args.kwargs["generation"], 11)
            active["provider_generation"] = 12
            with self.assertRaises(RequestError) as changed:
                await router.handle("generate", params | {"provider_generation": 11}, None, False)
            self.assertEqual(changed.exception.code, "provider_changed")
            invoke.assert_awaited_once()

    async def process(self, stack, *args):
        process = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE
        )

        async def cleanup():
            if process.returncode is None:
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
            await asyncio.wait_for(process.wait(), 5)

        stack.push_async_callback(cleanup)
        return process

    async def service(self, stack, socket, module, *args):
        service = await self.process(
            stack,
            sys.executable,
            "-m",
            f"glassmoon.{module}",
            "--socket",
            socket,
            *args,
        )
        self.assertIn(b"active as generation", await service.stdout.readline())
        return service

    async def test_agent_switches_providers_without_losing_history(self):
        async with AsyncExitStack() as stack, asyncio.timeout(12):
            folder = Path(
                stack.enter_context(
                    tempfile.TemporaryDirectory(prefix="gm-", dir="/tmp")
                )
            )
            socket = folder / "broker.sock"
            broker = await self.process(stack, BROKER, socket)
            while not socket.is_socket():
                self.assertIsNone(broker.returncode)
                await asyncio.sleep(0.01)
            await self.service(stack, socket, "provider_router")
            first = await self.service(stack, socket, "demo_provider")
            first_args = ("--state", folder / "agent.sqlite3")
            agent = await self.service(stack, socket, "agent", *first_args)
            chunks = []

            async def emit(chunk):
                chunks.append(chunk["text"])

            result = await call(
                socket, "agent.moon", "chat", {"text": "first turn"}, on_chunk=emit
            )
            self.assertEqual(result["provider"], "demo")
            self.assertEqual(chunks, ["Echo: first turn"])
            first.terminate()
            await first.wait()
            for _ in range(100):
                if not (await call(socket, "models", "models", {}))["models"]:
                    break
                await asyncio.sleep(0.01)

            with self.assertRaises(RequestError) as caught:
                await call(
                    socket, "agent.moon", "chat", {"text": "before provider returns"}
                )
            self.assertEqual(caught.exception.code, "no_matching_model")

            seen = []
            ready = asyncio.Event()

            async def other_provider(name, params, emit, stream):
                if name == "models":
                    return {
                        "models": [
                            {
                                "id": "other",
                                "input_modalities": ["text", "image"],
                                "output_modalities": ["text"],
                                "context_tokens": 32_768,
                                "streaming": True,
                            }
                        ]
                    }
                seen.extend(params["messages"])
                if stream:
                    await emit({"type": "text", "text": "Second"})
                return {"message": {"role": "assistant", "content": "Second"}}

            async def second():
                await serve(
                    socket,
                    "other",
                    None,
                    {"models": CATALOG, "generate": GENERATE},
                    other_provider,
                    kind="model_provider",
                    on_ready=ready.set,
                )

            provider = asyncio.create_task(second())

            async def stop_provider():
                provider.cancel()
                await asyncio.gather(provider, return_exceptions=True)

            stack.push_async_callback(stop_provider)
            await ready.wait()
            result = await call(
                socket, "agent.moon", "chat", {"text": "second turn"}, on_chunk=emit
            )
            self.assertEqual((result["provider"], result["model"]), ("other", "other"))
            self.assertEqual(
                [part["content"][0]["text"] for part in seen[1:]],
                [
                    "first turn",
                    "Echo: first turn",
                    "second turn",
                ],
            )
            self.assertEqual(chunks[-1], "Second")
            agent.terminate()
            await agent.wait()
            for _ in range(100):
                try:
                    await call(socket, "agent.moon", "info", {})
                except RequestError as error:
                    if error.code == "unavailable":
                        break
                await asyncio.sleep(0.01)
            await self.service(stack, socket, "agent", *first_args)
            history = await call(socket, "agent.moon", "history", {})
            self.assertEqual(history["revision"], 4)

            route = {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "image", "url": "https://example.org/example.png"},
                        ],
                    }
                ],
                "min_context_tokens": 32_768,
            }
            self.assertEqual(
                (await call(socket, "models", "generate", route))["provider"], "other"
            )
            route["min_context_tokens"] += 1
            with self.assertRaises(RequestError) as caught:
                await call(socket, "models", "generate", route)
            self.assertEqual(caught.exception.code, "no_matching_model")
