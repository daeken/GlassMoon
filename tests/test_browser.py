import asyncio
import base64
import unittest
from contextlib import suppress
from pathlib import Path
from time import monotonic

from playwright.async_api import async_playwright

from glassmoon.browser import Browser
from glassmoon.ipc import RequestError
from glassmoon.openai_service import openai_tool_output

PAGE = """
<title>Fixture</title>
<style>
input, button { position: absolute; left: 20px; width: 140px; height: 32px; }
input { top: 20px; } button { top: 70px; }
</style>
<input id="note" placeholder="Note" aria-label="Fixture note"><button onclick="document.querySelector('#status').textContent = document.querySelector('#note').value">Save</button>
<p id="status" style="position:absolute;top:110px"></p>
"""


class BrowserTests(unittest.IsolatedAsyncioTestCase):
    async def test_visual_window_contract(self):
        async with async_playwright() as playwright:
            installed = [
                (engine, getattr(playwright, engine))
                for engine in ("chromium", "webkit")
            ]
            installed = [
                (engine, driver)
                for engine, driver in installed
                if Path(driver.executable_path).is_file()
            ]
            if not installed:
                self.skipTest(
                    "Install a Playwright browser to run the visual window tests"
                )
            for engine, driver in installed:
                with self.subTest(engine=engine):
                    backend = await driver.launch()
                    try:
                        await self.exercise_browser(Browser(engine, backend))
                        await self.exercise_allowed_origins(engine, backend)
                        await self.exercise_expiring_windows(engine, backend)
                    finally:
                        await backend.close()

    async def exercise_expiring_windows(self, engine, backend):
        worker = Browser(engine, backend, max_windows=1, idle_seconds=10)
        opened = await worker.invoke("open", {})
        handle = opened["window"]
        window = worker.windows[handle]
        window.last_used = monotonic() - 11
        async with window.lock:
            with self.assertRaises(RequestError) as fullness:
                await worker.invoke("open", {})
            self.assertEqual(fullness.exception.code, "window_limit")
        replacement = await worker.invoke("open", {})
        self.assertNotIn(handle, worker.windows)
        await worker.invoke("close", {"window": replacement["window"]})

    async def exercise_allowed_origins(self, engine, backend):
        unexpected = []

        async def respond(reader, writer, handler):
            try:
                request = (await reader.readuntil(b"\r\n\r\n")).split()[1]
                status, headers, body = handler(request)
                writer.write(
                    f"HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: {len(body)}\r\n".encode()
                    + headers + b"\r\n" + body
                )
                await writer.drain()
            except (asyncio.IncompleteReadError, ConnectionError):
                pass
            finally:
                writer.close()
                with suppress(ConnectionError):
                    await writer.wait_closed()

        def forbidden(request):
            unexpected.append(request)
            return "200 OK", b"", b"unexpected"

        def allowed(request):
            if request == b"/jump":
                return "302 Found", f"Location: {other}/trap\r\n".encode(), b""
            if request == b"/same-origin-redirect":
                return "302 Found", b"Location: /status\r\n", b""
            if request == b"/status":
                return "200 OK", b"", b"ready"
            return "200 OK", b"Content-Type: text/html\r\n", PAGE.encode()

        async def start(handler):
            return await asyncio.start_server(
                lambda reader, writer: respond(reader, writer, handler), "127.0.0.1", 0
            )

        async with await start(forbidden) as second, await start(allowed) as first:
            other = f"http://127.0.0.1:{second.sockets[0].getsockname()[1]}"
            site = f"http://127.0.0.1:{first.sockets[0].getsockname()[1]}"
            worker = Browser(engine, backend, allowed_origins=[site])
            window = await worker.invoke("open", {"url": site})
            handle = {"window": window["window"]}
            try:
                self.assertEqual((await worker.invoke("inspect", handle))["title"], "Fixture")
                status = await worker.invoke(
                    "evaluate", handle | {"script": "() => fetch('/status').then(r => r.text())"}
                )
                self.assertEqual(status["result"], "ready")
                blocked = await worker.invoke(
                    "evaluate",
                    handle | {
                        "script": "async url => (await Promise.allSettled([fetch(url), fetch('/jump'), fetch('/same-origin-redirect')])).map(r => r.status)",
                        "argument": other,
                    },
                )
                self.assertEqual(blocked["result"], ["rejected"] * 3)
                with self.assertRaises(RequestError) as direct:
                    await worker.invoke("navigate", handle | {"url": other})
                self.assertEqual(direct.exception.code, "origin_blocked")
                with self.assertRaises(RequestError):
                    await worker.invoke("navigate", handle | {"url": f"{site}/jump"})
                self.assertEqual(unexpected, [])
            finally:
                await worker.invoke("close", handle)

    async def exercise_browser(self, worker):
        first = await worker.invoke("open", {})
        second = await worker.invoke("open", {"width": 320, "height": 480})
        a = {"window": first["window"]}
        b = {"window": second["window"]}
        for window in (a, b):
            await worker.invoke(
                "evaluate",
                window
                | {
                    "script": "html => { document.open(); document.write(html); document.close(); }",
                    "argument": PAGE,
                },
            )
        screenshot = await worker.invoke("screenshot", a)
        self.assertEqual((screenshot["width"], screenshot["height"]), (768, 512))
        compact = await worker.invoke("screenshot", b)
        self.assertEqual((compact["width"], compact["height"]), (320, 480))
        with self.assertRaises(RequestError) as offscreen:
            await worker.invoke("click", b | {"x": 320, "y": 1})
        self.assertEqual(offscreen.exception.code, "invalid_position")
        with self.assertRaises(RequestError) as invalid:
            await worker.invoke("open", {"width": 239})
        self.assertEqual(invalid.exception.code, "invalid_viewport")
        image = screenshot["image"]
        self.assertTrue(
            base64.b64decode(image["url"].split(",", 1)[1]).startswith(b"\xff\xd8")
        )
        model_parts = openai_tool_output([{"type": "text", "text": "768x512"}, image])
        self.assertEqual(model_parts[1]["image_url"], image["url"])
        await worker.invoke(
            "evaluate", a | {"script": "() => console.warn('fixture warning')"}
        )
        self.assertEqual(
            (await worker.invoke("diagnostics", a))["events"][-1]["text"],
            "fixture warning",
        )
        self.assertEqual((await worker.invoke("diagnostics", b))["events"], [])
        result = await worker.invoke(
            "actions",
            a
            | {
                "steps": [
                    {"action": "click", "x": 50, "y": 35},
                    {"action": "type_text", "text": "visual check"},
                    {"action": "click", "x": 50, "y": 85},
                ]
            },
        )
        self.assertEqual(result["completed"], 3)
        await worker.invoke(
            "wait_for", a | {"selector": "#status", "text": "visual check"}
        )
        snapshot = await worker.invoke("snapshot", a)
        self.assertIn('textbox "Fixture note"', snapshot["tree"])
        self.assertIn('button "Save"', snapshot["tree"])
        self.assertFalse(snapshot["truncated"])
        await worker.invoke(
            "element", a | {
                "action": "fill", "by": "label", "name": "Fixture note",
                "role": "textbox", "text": "semantic check",
            }
        )
        save = a | {"action": "click", "by": "role", "role": "button", "name": "Save"}
        await worker.invoke("element", save)
        await worker.invoke("wait_for", a | {"selector": "#status", "text": "semantic check"})
        await worker.invoke(
            "evaluate", a | {"script": "() => document.body.insertAdjacentHTML('beforeend', '<button>Save</button>')"}
        )
        with self.assertRaises(RequestError) as ambiguous:
            await worker.invoke("element", save)
        self.assertEqual(ambiguous.exception.code, "ambiguous_target")
        sequence = await worker.invoke("actions", a | {"steps": [
            {"action": "element", "operation": "fill", "by": "placeholder", "name": "Note", "text": "unsaved check"},
            {"action": "element", "operation": "click", "by": "role", "role": "button", "name": "Save"},
            {"action": "element", "operation": "fill", "by": "placeholder", "name": "Note", "text": "should not run"},
        ]})
        self.assertEqual((sequence["completed"], sequence["reason"]), (1, "ambiguous_target"))
        value = await worker.invoke("evaluate", a | {"script": "document.querySelector('#note').value"})
        self.assertEqual(value["result"], "unsaved check")
        self.assertEqual(
            (
                await worker.invoke(
                    "evaluate", b | {"script": "document.querySelector('#note').value"}
                )
            )["result"],
            "",
        )
        with self.assertRaises(RequestError) as timeout:
            await worker.invoke(
                "wait_for", a | {"selector": "#missing", "timeout_ms": 30}
            )
        self.assertEqual(timeout.exception.code, "browser_timeout")
        await worker.invoke("close", a)
        with self.assertRaises(RequestError) as closed:
            await worker.invoke("inspect", a)
        self.assertEqual(closed.exception.code, "unknown_window")
