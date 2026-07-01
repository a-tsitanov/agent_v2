"""Thin smoke test: verify kb_analyze is registered on the MCP-1 server."""


def test_kb_analyze_is_registered():
    import src.mcp.search_server as s

    # FastMCP stores tools; confirm the symbol exists / is decorated
    assert hasattr(s, "kb_analyze")
