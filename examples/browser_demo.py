"""Run the GlassMoon note fixture in a disposable local browser."""

import argparse
import asyncio
import json
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread

from playwright.async_api import Error, async_playwright

from glassmoon.browser import Browser
from glassmoon.ipc import RequestError

from .browser_note_tools import note_actions


class FixtureHandler(SimpleHTTPRequestHandler):
    def log_message(self, *_):
        pass


async def demo(engine, note):
    steps = note_actions(note)
    with TemporaryDirectory(prefix="glassmoon-browser-demo-") as directory:
        filename = "browser-smoke.html"
        (Path(directory) / filename).write_bytes(Path(__file__).with_name(filename).read_bytes())
        handler = partial(FixtureHandler, directory=directory)
        with ThreadingHTTPServer(("127.0.0.1", 0), handler) as server:
            thread = Thread(target=server.serve_forever, kwargs={"poll_interval": .1})
            thread.start()
            try:
                origin = f"http://127.0.0.1:{server.server_port}"
                async with async_playwright() as playwright:
                    driver = getattr(playwright, engine)
                    if not Path(driver.executable_path).is_file():
                        raise ValueError(f"Install the browser with uv run --locked playwright install {engine}")
                    browser = await driver.launch()
                    try:
                        worker = Browser(engine, browser, allowed_origins=[origin])
                        opened = await worker.invoke("open", {"url": f"{origin}/{filename}"})
                        handle = {"window": opened["window"]}
                        try:
                            result = await worker.invoke("actions", handle | {"steps": steps})
                            if result.get("error"):
                                raise ValueError(f"Step {result['failed_step']} failed ({result['reason']})")
                            return {"engine": engine, "completed": result["completed"], "observed": steps[-1]["text"]}
                        finally:
                            await worker.invoke("close", handle)
                    finally:
                        await browser.close()
            finally:
                server.shutdown()
                thread.join()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("chromium", "webkit"), default="chromium")
    parser.add_argument("--note", default="GlassMoon browser demo")
    args = parser.parse_args()
    try:
        print(json.dumps(asyncio.run(demo(args.engine, args.note)), ensure_ascii=False))
    except (Error, OSError, RequestError, ValueError) as error:
        parser.exit(1, f"Browser demo failed: {error}\n")


if __name__ == "__main__":
    main()
