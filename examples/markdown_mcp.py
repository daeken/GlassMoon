"""Harmless stdio MCP fixture for bounded Markdown ATX heading extraction.

Run with: .venv/bin/python -m examples.markdown_mcp
Only ATX headings are supported; this is not a full Markdown parser.
"""

import re

from mcp.server.mcpserver import MCPServer

MAX_INPUT_BYTES = 65_536
server = MCPServer("GlassMoon Markdown headings")
_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
_HEADING = re.compile(r"^ {0,3}(#{1,6})(?:[ \t]+(.*)|$)")


@server.tool()
def extract_headings(markdown: str) -> list[dict[str, int | str]]:
    """Extract ATX headings (level and text), ignoring fenced code.

    Accept at most 65,536 UTF-8 bytes. Preserve inline Markdown; strip
    optional closing hashes. Setext headings are intentionally unsupported.
    """
    if not isinstance(markdown, str):
        raise TypeError("markdown must be text")
    if (
        len(markdown) > MAX_INPUT_BYTES
        or len(markdown.encode("utf-8")) > MAX_INPUT_BYTES
    ):
        raise ValueError("markdown exceeds 65536 UTF-8 bytes")
    headings = []
    fence_char = None
    fence_length = 0
    for line in markdown.split("\n"):
        line = line.removesuffix("\r")
        fence = _FENCE.match(line)
        if fence_char is not None:
            if (
                fence
                and fence[1][0] == fence_char
                and len(fence[1]) >= fence_length
                and not fence[2].strip(" \t")
            ):
                fence_char = None
            continue
        if fence and not (fence[1][0] == "`" and "`" in fence[2]):
            fence_char, fence_length = fence[1][0], len(fence[1])
            continue
        heading = _HEADING.match(line)
        if heading:
            text = (heading[2] or "").strip(" \t")
            # A hash-only body is an optional closing sequence as well.
            text = (
                "" if text and not text.strip("#") else re.sub(r"[ \t]+#+$", "", text)
            )
            headings.append({"level": len(heading[1]), "text": text})
    return headings


if __name__ == "__main__":
    server.run()
