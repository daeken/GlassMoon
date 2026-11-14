"""Deterministic text fingerprints for checking coding-harness artifacts.

Run from the repository root with: python -m examples.moon_service --socket PATH
No filesystem reads, text normalization, or external network requests are performed.
"""

import asyncio
import hashlib

from glassmoon.service import fail, method, serve, service_args

NAME = "moon.text"
MAX_BYTES = 32 * 1024
HEALTH = {"service": NAME, "version": 1, "status": "ok"}
METHODS = {
    "health": method("Return a deterministic readiness/version probe"),
    "fingerprint": method(
        "Hash exact UTF-8 text and count bytes, Unicode code points, and LF lines",
        required={"text": {"type": "string", "maxLength": MAX_BYTES}},
    ),
}


async def handle(name, params, emit, stream):
    if name not in METHODS:
        raise fail("unknown_method", "Expected health or fingerprint")
    expected = set() if name == "health" else {"text"}
    if not isinstance(params, dict) or set(params) != expected:
        raise fail("invalid_params", "Provide exactly the method's documented fields")
    if name == "health":
        return dict(HEALTH)
    text = params["text"]
    if not isinstance(text, str):
        raise fail("invalid_params", "text must be a string")
    try:
        raw = text.encode("utf-8")
    except UnicodeEncodeError:
        raise fail("invalid_params", "text must be valid UTF-8") from None
    if len(raw) > MAX_BYTES:
        raise fail("invalid_params", "text exceeds 32768 UTF-8 bytes")
    return {
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
        "characters": len(text),
        # LF delimits lines; a final LF does not create an extra empty line.
        "lines": text.count("\n") + int(bool(text) and not text.endswith("\n")),
    }


def main():
    parser = service_args(__doc__, NAME)
    parser.add_argument("--staged", action="store_true")
    args = parser.parse_args()
    if args.staged and args.replace is not None:
        parser.error("--staged and --replace cannot be used together")

    async def handler(name, params, emit, stream):
        result = await handle(name, params, emit, stream)
        if name == "health":
            result["service"] = args.name
        return result

    asyncio.run(
        serve(
            args.socket, args.name, args.replace, METHODS, handler,
            staged=args.staged, reconnect=True, maintainer=args.maintainer,
        )
    )


if __name__ == "__main__":
    main()
