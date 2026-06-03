"""Unit tests for the SSE auth provider factory."""

from __future__ import annotations

from types import SimpleNamespace

from src.mcp import _shared


def test_build_sse_auth_returns_none_when_disabled(monkeypatch):
    monkeypatch.setenv("KB_MCP_REQUIRE_AUTH", "false")
    monkeypatch.setattr(
        _shared, "settings",
        SimpleNamespace(api=SimpleNamespace(keys_list=["secret-key"])),
    )
    assert _shared.build_sse_auth() is None


def test_build_sse_auth_returns_none_when_no_keys(monkeypatch):
    monkeypatch.setenv("KB_MCP_REQUIRE_AUTH", "true")
    monkeypatch.setattr(
        _shared, "settings",
        SimpleNamespace(api=SimpleNamespace(keys_list=[])),
    )
    assert _shared.build_sse_auth() is None


def test_build_sse_auth_builds_verifier_with_keys(monkeypatch):
    from fastmcp.server.auth import StaticTokenVerifier

    monkeypatch.setenv("KB_MCP_REQUIRE_AUTH", "true")
    monkeypatch.setattr(
        _shared, "settings",
        SimpleNamespace(api=SimpleNamespace(keys_list=["secret-key", "k2"])),
    )
    auth = _shared.build_sse_auth()
    assert isinstance(auth, StaticTokenVerifier)
    assert "secret-key" in auth.tokens
    assert "k2" in auth.tokens
    assert auth.tokens["secret-key"]["client_id"] == "kb"


def test_both_servers_wire_sse_auth(monkeypatch):
    """Guard: `auth=build_sse_auth()` must stay wired into BOTH FastMCP
    server constructors. Reload the server modules with auth required +
    a key configured, and assert each module's `mcp.auth` is set."""
    import importlib

    from fastmcp.server.auth import StaticTokenVerifier

    monkeypatch.setenv("KB_MCP_REQUIRE_AUTH", "true")
    # build_sse_auth reads _shared.settings.api.keys_list — patch it so a
    # key is present regardless of the ambient environment.
    monkeypatch.setattr(
        _shared, "settings",
        SimpleNamespace(api=SimpleNamespace(keys_list=["wiring-test-key"])),
    )

    import src.mcp.search_server as search_server
    import src.mcp.tools_server as tools_server

    tools_server = importlib.reload(tools_server)
    search_server = importlib.reload(search_server)

    assert isinstance(tools_server.mcp.auth, StaticTokenVerifier)
    assert isinstance(search_server.mcp.auth, StaticTokenVerifier)
