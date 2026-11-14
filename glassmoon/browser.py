"""A browser window service for Chromium or WebKit."""

import asyncio
import base64
import secrets
from collections import deque
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from playwright.async_api import Error, async_playwright
from playwright.async_api import TimeoutError as BrowserTimeout

from .ipc import ProtocolError, RequestError
from .models import ARRAY, INTEGER, TEXT, within_reply_limit
from .service import fail, serve, service_args
from .toolbox import METHODS, ToolService, call_tool, tool

VIEWPORT = {"width": 768, "height": 512}
IMAGE_LIMIT = 21_000
TIMEOUT = 10_000


WINDOW = {"window": TEXT}
POSITION = {"x": INTEGER, "y": INTEGER}
TOOLS = {
    "open": tool(
        "Open an isolated browser window. Returns the window handle.",
        optional={"url": TEXT},
    ),
    "navigate": tool(
        "Navigate to an HTTP(S) page and wait for DOM readiness.",
        WINDOW | {"url": TEXT},
    ),
    "inspect": tool("Read the page title, URL, and visible text.", WINDOW),
    "diagnostics": tool(
        "Read recent page errors, console warnings, and failed requests for this window.",
        WINDOW,
    ),
    "evaluate": tool(
        "Run JavaScript in the page. Use a function expression to receive the optional JSON argument.",
        WINDOW | {"script": TEXT},
        {"argument": {}},
    ),
    "wait_for": tool(
        "Wait for a CSS selector to become visible and optionally contain matching text.",
        WINDOW | {"selector": TEXT},
        {"text": TEXT, "timeout_ms": INTEGER},
    ),
    "screenshot": tool(
        "See an image of this window's browser viewport. Coordinates are CSS pixels.",
        WINDOW,
    ),
    "click": tool("Click at viewport coordinates in this window.", WINDOW | POSITION),
    "type_text": tool(
        "Type text into this window's focused control.", WINDOW | {"text": TEXT}
    ),
    "press": tool(
        "Send a Playwright key or chord to this window.", WINDOW | {"key": TEXT}
    ),
    "scroll": tool(
        "Scroll the page by a number of CSS pixels.",
        WINDOW | {"dx": INTEGER, "dy": INTEGER},
    ),
    "actions": tool(
        "Run up to eight ordered viewport actions in one window. Each item has action (click, type_text, press, or scroll) and its usual arguments.",
        WINDOW | {"steps": ARRAY | {"items": {"type": "object"}}},
    ),
    "close": tool("Close only the browser window identified by this handle.", WINDOW),
}

TOOLS["screenshot"]["returns_image"] = True
SHARE = tool(
    "Explicitly publish this viewport screenshot and a short reproduction summary. Include no secrets. Returns a share key.",
    WINDOW | {"owner": TEXT, "key": TEXT, "summary": TEXT},
)


def web_url(value):
    try:
        url = urlsplit(value)
        port = url.port
    except ValueError as error:
        raise fail("invalid_url", "Use an HTTP(S) URL with a valid host and port") from error
    if (
        url.scheme not in ("http", "https")
        or not url.hostname
        or url.username is not None
        or url.password is not None
        or port == 0
    ):
        raise fail("invalid_url", "Use an HTTP(S) URL without embedded credentials")
    return value


def origin(url, *, bare=False):
    parts = urlsplit(web_url(url))
    if bare and (parts.path not in ("", "/") or parts.query or parts.fragment):
        raise ValueError("--allow-origin expects an origin such as http://127.0.0.1:8000")
    return parts.scheme, parts.hostname, parts.port or (443 if parts.scheme == "https" else 80)


def image_result(data, width, height, **meta):
    if len(data) > IMAGE_LIMIT:
        raise fail("image_too_large", "Screenshot exceeds the IPC frame budget")
    return {
        **meta,
        "width": width,
        "height": height,
        "image": {
            "type": "image",
            "url": "data:image/jpeg;base64," + base64.b64encode(data).decode("ascii"),
        },
    }


def request_path(url):
    parts = urlsplit(url)
    if parts.scheme in ("http", "https"):
        return f"{parts.scheme}://{parts.hostname}{parts.path}"[:400]
    return parts.scheme


@dataclass
class Window:
    context: object
    page: object
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    diagnostics: deque = field(default_factory=lambda: deque(maxlen=24))

    def listen(self):
        def record(kind, text):
            self.diagnostics.append({"kind": kind, "text": str(text)[:400]})

        def console(message):
            if message.type in ("warning", "error"):
                record(f"console_{message.type}", message.text)

        def response(reply):
            if reply.status >= 400:
                record("http_error", f"{reply.status} {request_path(reply.url)}")

        self.page.on("console", console)
        self.page.on("pageerror", lambda error: record("page_error", error))
        self.page.on(
            "requestfailed",
            lambda request: record("request_failed", request_path(request.url)),
        )
        self.page.on("response", response)


class Browser:
    def __init__(self, engine, browser, max_windows=8, publish=None, allowed_origins=()):
        self.engine = engine
        self.browser = browser
        self.max_windows = max_windows
        self.publish = publish
        self.allowed_origins = frozenset(origin(url, bare=True) for url in allowed_origins)
        self.windows = {}
        self.opening = asyncio.Lock()

    def navigation_url(self, url):
        url = web_url(url)
        if self.allowed_origins and origin(url) not in self.allowed_origins:
            raise fail("origin_blocked", "This browser worker does not allow that origin")
        return url

    async def route(self, route):
        url = route.request.url
        if url.startswith(("data:", "blob:")):
            await route.continue_()
            return
        try:
            self.navigation_url(url)
        except RequestError:
            await route.abort()
            return
        if not self.allowed_origins:
            await route.continue_()
            return
        try:
            response = await route.fetch(max_redirects=0, timeout=TIMEOUT)
        except Error:
            await route.abort()
            return
        try:
            # Playwright only intercepts the first request of a redirect chain.
            if 300 <= response.status < 400 and "location" in response.headers:
                await route.abort()
            else:
                await route.fulfill(response=response)
        finally:
            await response.dispose()

    async def close_websocket(self, websocket):
        await websocket.close()

    async def close_context(self, context):
        if self.allowed_origins:
            await context.unroute_all(behavior="ignoreErrors")
        await context.close()

    async def open(self, args):
        url = self.navigation_url(args["url"]) if args.get("url") else None
        async with self.opening:
            if len(self.windows) >= self.max_windows:
                raise fail("window_limit", "Close a window before opening another")
            context = await self.browser.new_context(
                viewport=VIEWPORT,
                device_scale_factor=1,
                accept_downloads=False,
                service_workers="block",
            )
            try:
                # HTTP and HTTPS cover normal pages, assets, and local dev servers.
                await context.route("**/*", self.route)
                if self.allowed_origins:
                    await context.route_web_socket("**/*", self.close_websocket)
                page = await context.new_page()
                window = Window(context, page)
                window.listen()
                page.on("popup", lambda popup: asyncio.create_task(popup.close()))
                page.set_default_timeout(TIMEOUT)
                if url:
                    await page.goto(url, wait_until="domcontentloaded")
            except BaseException:
                await self.close_context(context)
                raise
            handle = secrets.token_urlsafe(18)
            self.windows[handle] = window
            context.on("close", lambda _: self.windows.pop(handle, None))
            return {
                "window": handle,
                "engine": self.engine,
                "url": page.url,
                **VIEWPORT,
            }

    async def invoke(self, action, args):
        try:
            async with asyncio.timeout(25):
                if action == "open":
                    return await self.open(args)
                handle = args["window"]
                if handle not in self.windows:
                    raise fail(
                        "unknown_window",
                        "The browser window handle is unknown or closed",
                    )
                window = self.windows[handle]
                page = window.page
                async with window.lock:
                    if page.is_closed():
                        raise fail("unknown_window", "The browser window is closed")
                    if action == "close":
                        await self.close_context(window.context)
                        return {"closed": handle}
                    if action == "diagnostics":
                        return {"events": list(window.diagnostics)}
                    if action == "share":
                        if self.publish is None:
                            raise fail(
                                "sharing_unavailable",
                                "No evidence service is configured",
                            )
                        shot = await self.page_action("screenshot", page, {})
                        kinds = {}
                        for event in window.diagnostics:
                            kind = event["kind"]
                            kinds[kind] = kinds.get(kind, 0) + 1
                        return await self.publish(
                            {
                                "owner": args["owner"],
                                "key": args["key"],
                                "summary": args["summary"],
                                "image": shot["image"],
                                "metadata": {
                                    "engine": self.engine,
                                    "width": shot["width"],
                                    "height": shot["height"],
                                    "diagnostic_counts": kinds,
                                },
                            }
                        )
                    if action == "actions":
                        steps = args["steps"]
                        if (
                            not isinstance(steps, list)
                            or not 1 <= len(steps) <= 8
                            or any(
                                not isinstance(step, dict)
                                or step.get("action")
                                not in ("click", "type_text", "press", "scroll")
                                for step in steps
                            )
                        ):
                            raise fail(
                                "invalid_actions",
                                "Supply one to eight viewport actions",
                            )
                        completed = []
                        for step in steps:
                            try:
                                completed.append(
                                    await self.page_action(step["action"], page, step)
                                )
                            except (
                                RequestError,
                                ValueError,
                                KeyError,
                                Error,
                                TypeError,
                            ):
                                return {
                                    "completed": len(completed),
                                    "error": "action_failed",
                                    "action": step["action"],
                                    "url": page.url,
                                }
                        return {"completed": len(completed), "url": page.url}
                    return await self.page_action(action, page, args)
        except (TimeoutError, BrowserTimeout):
            raise fail(
                "browser_timeout", f"Browser action {action} timed out"
            ) from None
        except Error:
            # Browser error strings can contain page content or evaluated source.
            raise fail("browser_error", f"Browser action {action} failed") from None

    async def page_action(self, action, page, args):
        match action:
            case "navigate":
                response = await page.goto(
                    self.navigation_url(args["url"]), wait_until="domcontentloaded"
                )
                return {
                    "url": page.url,
                    "status": response.status if response else None,
                }
            case "inspect":
                content = await page.evaluate(
                    "document.body?.innerText.slice(0, 7000) ?? ''"
                )
                return within_reply_limit(
                    {"url": page.url, "title": await page.title(), "text": content}
                )
            case "evaluate":
                return within_reply_limit(
                    {
                        "result": await page.evaluate(
                            args["script"], args.get("argument")
                        )
                    }
                )
            case "wait_for":
                selector = args["selector"]
                timeout = args.get("timeout_ms", TIMEOUT)
                if type(timeout) is not int or not 1 <= timeout <= 20_000:
                    raise fail(
                        "invalid_timeout",
                        "Use a timeout between 1 and 20000 milliseconds",
                    )
                locator = page.locator(selector)
                if "text" in args:
                    locator = locator.filter(has_text=args["text"])
                try:
                    await locator.first.wait_for(state="visible", timeout=timeout)
                except BrowserTimeout:
                    raise fail(
                        "browser_timeout",
                        f"Visible selector condition unmet after {timeout} ms",
                    ) from None
                return {"visible": True}
            case "screenshot":
                for quality in (70, 45, 25, 10):
                    data = await page.screenshot(
                        type="jpeg", quality=quality, scale="css", animations="disabled"
                    )
                    if len(data) <= IMAGE_LIMIT:
                        return image_result(data, **VIEWPORT, url=page.url)
                raise fail(
                    "image_too_large", "Viewport screenshot exceeds the IPC budget"
                )
            case "click":
                x, y = args["x"], args["y"]
                if (
                    type(x) is not int
                    or type(y) is not int
                    or not (0 <= x < VIEWPORT["width"] and 0 <= y < VIEWPORT["height"])
                ):
                    raise fail(
                        "invalid_position",
                        "Click coordinates must be inside the viewport",
                    )
                await page.mouse.click(x, y)
            case "type_text":
                if not isinstance(args["text"], str) or len(args["text"]) > 4000:
                    raise fail(
                        "invalid_text", "Use a string of at most 4000 characters"
                    )
                await page.keyboard.insert_text(args["text"])
            case "press":
                await page.keyboard.press(args["key"])
            case "scroll":
                dx, dy = args["dx"], args["dy"]
                if type(dx) is not int or type(dy) is not int:
                    raise fail("invalid_scroll", "Use integer CSS pixel deltas")
                # scrollBy is synchronous, unlike mouse.wheel, so the next snapshot sees it.
                await page.evaluate(
                    "([x,y]) => window.scrollBy({left:x, top:y, behavior:'instant'})",
                    [dx, dy],
                )
            case _:
                raise fail("unknown_tool", "Unknown browser action")
        return {"url": page.url, "action": action}


async def run(args):
    async with async_playwright() as playwright:
        browser = await getattr(playwright, args.engine).launch(
            headless=not args.headed
        )
        try:

            async def publish(payload):
                return await call_tool(
                    args.socket, args.evidence, "publish_image", payload
                )

            worker = Browser(
                args.engine,
                browser,
                args.max_windows,
                publish if args.evidence else None,
                args.allow_origin,
            )
            tools = TOOLS | {"share": SHARE} if args.evidence else TOOLS
            service = ToolService(tools, worker.invoke)
            await serve(
                args.socket,
                args.name,
                args.replace,
                METHODS,
                service.handle,
                reconnect=True,
            )
        finally:
            await browser.close()


def main():
    parser = service_args(__doc__, "browser")
    parser.add_argument("--engine", choices=["chromium", "webkit"], default="chromium")
    parser.add_argument("--headed", action="store_true", help="Show browser windows")
    parser.add_argument("--max-windows", type=int, default=8)
    parser.add_argument(
        "--evidence",
        help="Enable explicit screenshot publication to the named evidence service",
    )
    parser.add_argument(
        "--allow-origin", action="append", default=[],
        help="Limit browser HTTP requests to this origin. Repeat to allow multiple origins.",
    )
    args = parser.parse_args()
    if not 1 <= args.max_windows <= 32:
        parser.error("--max-windows must be between 1 and 32")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130
    except (RequestError, ProtocolError, EOFError, OSError, Error, ValueError) as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
