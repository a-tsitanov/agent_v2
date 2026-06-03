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
