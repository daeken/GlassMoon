"""Explicitly attach the macOS main display to a GlassMoon tool service."""

import asyncio
import json
from pathlib import Path

from .ipc import ProtocolError, RequestError
from .models import INTEGER, TEXT
from .service import fail, serve, service_args
from .toolbox import METHODS, ToolService, tool

TOOLS = {
    "status": tool(
        "Check main display dimensions and macOS capture and input permissions."
    ),
    "screenshot": tool(
        "Capture the macOS main display at the current screenshot coordinate scale."
    ),
    "windows": tool("List visible window IDs for the configured macOS app."),
    "window_screenshot": tool(
        "Capture a window from the configured app. Use a window ID from windows.",
        {"window_id": INTEGER},
    ),
    "click": tool(
        "Click at coordinates from the latest main display screenshot.",
        {"x": INTEGER, "y": INTEGER},
    ),
    "type_text": tool(
        "Type text into the current focused macOS control.", {"text": TEXT}
    ),
    "press": tool(
        "Send Enter, Tab, Space, Backspace, Escape, or an arrow key.", {"key": TEXT}
    ),
    "scroll": tool(
        "Scroll at the current macOS pointer position.", {"dx": INTEGER, "dy": INTEGER}
    ),
}
TOOLS["screenshot"]["returns_image"] = True
TOOLS["window_screenshot"]["returns_image"] = True
CAPTURE_TOOLS = ("status", "screenshot", "windows", "window_screenshot")


class Desktop:
    def __init__(self, helper, allow_input, target_pid=None, window_capture=False):
        self.helper = helper
        self.allow_input = allow_input
        self.target_pid = target_pid
        self.window_capture = window_capture
        self.lock = asyncio.Lock()

    async def invoke(self, action, args):
        if action in ("windows", "window_screenshot") and not self.window_capture:
            raise fail("window_capture_disabled", "Start the desktop service with --window-capture")
        if action == "screenshot" and self.window_capture:
            raise fail("display_capture_disabled", "Use window_screenshot for the selected app")
        if action not in CAPTURE_TOOLS and not self.allow_input:
            raise fail("input_disabled", "Start the desktop service with --allow-input")
        async with self.lock:
            process = await asyncio.create_subprocess_exec(
                self.helper,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            try:
                async with asyncio.timeout(15):
                    data, _ = await process.communicate(
                        json.dumps(
                            args | {"action": action, "target_pid": self.target_pid}
                        ).encode()
                    )
            except TimeoutError:
                raise fail(
                    "desktop_timeout", "The macOS desktop helper did not respond"
                ) from None
            finally:
                if process.returncode is None:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    await process.wait()
        if process.returncode != 0:
            raise fail(
                "desktop_helper", "The native desktop helper exited unsuccessfully"
            )
        result = json.loads(data)
        if "error" in result:
            raise fail(result["error"], result["message"])
        return result


def main():
    parser = service_args(__doc__, "desktop")
    parser.add_argument("--helper", type=Path, default=Path("target/glassmoon-desktop"))
    parser.add_argument(
        "--allow-input",
        action="store_true",
        help="Expose real desktop mouse and keyboard actions",
    )
    parser.add_argument("--window-capture", action="store_true", help="Expose capture of the configured app's windows")
    parser.add_argument(
        "--target-pid",
        type=int,
        help="Allow input only while this macOS application is frontmost",
    )
    args = parser.parse_args()
    if args.target_pid is not None and not 1 <= args.target_pid <= 2_147_483_647:
        parser.error("--target-pid must be a valid macOS process ID")
    if args.window_capture and args.target_pid is None:
        parser.error("--window-capture requires --target-pid")
    if args.window_capture and args.allow_input:
        parser.error("Use --window-capture for a read-only worker")
    try:
        bridge = Desktop(
            args.helper.resolve(strict=True), args.allow_input, args.target_pid, args.window_capture
        )
        tools = {
            name: spec for name, spec in TOOLS.items()
            if (args.allow_input or name in CAPTURE_TOOLS)
            and (name not in ("windows", "window_screenshot") or args.window_capture)
            and (name != "screenshot" or not args.window_capture)
        }
        service = ToolService(tools, bridge.invoke)
        asyncio.run(
            serve(
                args.socket,
                args.name,
                args.replace,
                METHODS,
                service.handle,
                reconnect=True,
                maintainer=args.maintainer,
            )
        )
    except KeyboardInterrupt:
        return 130
    except (RequestError, ProtocolError, EOFError, OSError) as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
