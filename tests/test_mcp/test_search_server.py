"""Smoke tests for the MCP-1 search server.

We don't talk to a real Temporal cluster here — only assert that:
  * `_list_tools` exposes the four orchestrated search tools (kb_search
    + kb_global_search + kb_drift_search + kb_auto_search) with the
    right names + description excerpt,
  * each tool's input schema exposes the `query` parameter and no legacy
    `mode` selector (R7b: the local flow submits the plan-execute
    ``SearchOrchestratorWorkflow``),
  * the auth gate fails when KB_MCP_REQUIRE_AUTH=true and no API_KEYS
    is set.
"""

from __future__ import annotations

import pytest

_EXPECTED_SEARCH_TOOLS = {
    "kb_search",
    "kb_global_search",
    "kb_drift_search",
    "kb_auto_search",
    "kb_analyze",
}


@pytest.mark.asyncio
async def test_search_server_lists_all_search_tools():
    from src.mcp import search_server
    tools = await search_server.mcp._list_tools()
    by_name = {t.name: t for t in tools}
    assert set(by_name) == _EXPECTED_SEARCH_TOOLS
    assert "knowledge base" in by_name["kb_search"].description.lower()
    assert "global" in by_name["kb_global_search"].description.lower()


@pytest.mark.asyncio
async def test_search_tools_schema_includes_query_param():
    from src.mcp import search_server
    tools = await search_server.mcp._list_tools()
    by_name = {t.name: t for t in tools}
    # R7b: orchestrator is local-only — `mode` is gone; `query` is the
    # required input on every search tool.
    for name in _EXPECTED_SEARCH_TOOLS:
        props = by_name[name].parameters.get("properties", {})
        assert "query" in props, f"{name} missing query param"
        assert "mode" not in props, f"{name} should not expose mode"


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
