"""Local MCP server used to demonstrate GlassMoon's MCP bridge."""

from mcp.server.mcpserver import MCPServer

server = MCPServer("GlassMoon arithmetic")


@server.tool()
def multiply(a: int, b: int) -> int:
    """Multiply two integers."""
    return a * b


if __name__ == "__main__":
    server.run()
