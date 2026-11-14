import base64
import unittest
from pathlib import Path

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
<input id="note"><button onclick="document.querySelector('#status').textContent = document.querySelector('#note').value">Save</button>
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
                    finally:
                        await backend.close()

    async def exercise_browser(self, worker):
        first = await worker.invoke("open", {})
        second = await worker.invoke("open", {})
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
        image = screenshot["image"]
        self.assertTrue(
            base64.b64decode(image["url"].split(",", 1)[1]).startswith(b"\xff\xd8")
        )
        model_parts = openai_tool_output([{"type": "text", "text": "768x512"}, image])
        self.assertEqual(model_parts[1]["image_url"], image["url"])
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
