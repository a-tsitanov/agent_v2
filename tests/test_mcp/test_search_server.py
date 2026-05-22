"""Smoke tests for the MCP-1 search server.

We don't talk to a real Temporal cluster here — only assert that:
  * `_list_tools` exposes exactly the kb_search tool with the right
    name + description excerpt,
  * the tool's input schema reflects the mode literal options,
  * the auth gate fails when KB_MCP_REQUIRE_AUTH=true and no API_KEYS
    is set.
"""

from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_search_server_lists_kb_search_tool():
    from src.mcp import search_server
    tools = await search_server.mcp._list_tools()
    names = [t.name for t in tools]
    assert names == ["kb_search"]
    desc = tools[0].description
    assert "knowledge base" in desc.lower()
    # The mode parameter shows up in the JSON-schema, not necessarily
    # in the first line of the docstring (which fastmcp uses as the
    # tool description summary).


@pytest.mark.asyncio
async def test_kb_search_schema_includes_mode_enum():
    from src.mcp import search_server
    tools = await search_server.mcp._list_tools()
    schema = tools[0].parameters
    props = schema.get("properties", {})
    assert "mode" in props
    enum_or_default = props["mode"].get("enum") or [props["mode"].get("default")]
    assert any(
        v in {"simple", "agent", "selfrag"}
        for v in enum_or_default if v is not None
    )


def test_auth_gate_blocks_when_keys_missing(monkeypatch):
    """assert_api_key_env_set raises SystemExit when require=true and
    api_keys list is empty.  Uses the fresh-ApiSettings pattern from
    tests/test_config.py to avoid module-reload pollution that would
    break sibling tests in this session."""
    monkeypatch.setenv("KB_MCP_REQUIRE_AUTH", "true")
    monkeypatch.setenv("API_KEYS", "")
    from src.config import ApiSettings, settings
    # Patch the keys_list on the live settings.api so _shared.py's
    # `settings.api.keys_list` sees an empty list during the test only.
    monkeypatch.setattr(
        settings.api.__class__, "keys_list",
        property(lambda self: []),
    )
    import src.mcp._shared as shared
    with pytest.raises(SystemExit, match="API_KEYS"):
        shared.assert_api_key_env_set()


def test_auth_gate_passes_when_disabled(monkeypatch):
    monkeypatch.setenv("KB_MCP_REQUIRE_AUTH", "false")
    import src.mcp._shared as shared
    # Should not raise even with empty keys.
    shared.assert_api_key_env_set()
    assert shared.is_valid_key("anything") is True
