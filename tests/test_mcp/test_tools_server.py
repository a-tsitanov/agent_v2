"""Smoke tests for the MCP-2 atomic tools server.

Verifies the 8 atomic tools are exposed with the right names and
descriptions; deeper behaviour is already covered by the
atomic_tools unit suite.
"""

from __future__ import annotations

import pytest

_EXPECTED_TOOLS = {
    "vector_search",
    "graph_search",
    "graph_walk",
    "find_entity_by_id",
    "find_entity_by_name",
    "find_neighbours",
    "get_chunks_by_doc_id",
    "read_full_document",
    # Track 7b: read-only GDS graph-analysis tools.
    "graph_pagerank",
    "graph_personalized_pagerank",
    "graph_components",
    "graph_shortest_path",
    "graph_stats",
    # Ingest statistics over the documents pipeline.
    "channel_message_stats",
    "channel_message_timeline",
}


def test_parse_args_accepts_http_transport(monkeypatch):
    from src.mcp._shared import parse_args
    monkeypatch.setattr("sys.argv", ["prog", "--transport", "http", "--port", "9002"])
    a = parse_args()
    assert a["transport"] == "http"
    assert a["port"] == 9002


def test_main_uses_streamable_http_for_non_stdio(monkeypatch):
    """MCP-2 serves over Streamable HTTP (transport='http'), not SSE."""
    from src.mcp import tools_server

    calls: dict = {}
    monkeypatch.setattr(
        tools_server, "parse_args",
        lambda: {"transport": "http", "host": "0.0.0.0", "port": 9002},
    )
    monkeypatch.setattr(tools_server, "assert_api_key_env_set", lambda: None)
    monkeypatch.setattr(tools_server, "log_banner", lambda *a, **k: None)
    monkeypatch.setattr(tools_server.mcp, "run", lambda **kw: calls.update(kw))

    tools_server.main()
    assert calls["transport"] == "http"
    assert calls["host"] == "0.0.0.0"
    assert calls["port"] == 9002


def test_main_stdio_still_supported(monkeypatch):
    from src.mcp import tools_server

    calls: dict = {}
    monkeypatch.setattr(
        tools_server, "parse_args",
        lambda: {"transport": "stdio", "host": "0.0.0.0", "port": 9002},
    )
    monkeypatch.setattr(tools_server, "assert_api_key_env_set", lambda: None)
    monkeypatch.setattr(tools_server, "log_banner", lambda *a, **k: None)
    monkeypatch.setattr(tools_server.mcp, "run", lambda **kw: calls.update(kw))

    tools_server.main()
    assert calls == {"transport": "stdio"}


@pytest.mark.asyncio
async def test_tools_server_lists_all_atomic_tools():
    from src.mcp import tools_server
    tools = await tools_server.mcp._list_tools()
    names = {t.name for t in tools}
    assert names == _EXPECTED_TOOLS


@pytest.mark.asyncio
async def test_personalized_pagerank_schema_has_seeds():
    from src.mcp import tools_server
    tools = await tools_server.mcp._list_tools()
    by_name = {t.name: t for t in tools}
    props = by_name["graph_personalized_pagerank"].parameters.get("properties", {})
    assert "seeds" in props
    assert "top_n" in props


@pytest.mark.asyncio
async def test_filter_by_metadata_intentionally_not_exposed():
    """filter_by_metadata operates on an in-process accumulator —
    doesn't make sense over MCP-2's stateless tool-call boundary."""
    from src.mcp import tools_server
    tools = await tools_server.mcp._list_tools()
    names = {t.name for t in tools}
    assert "filter_by_metadata" not in names


@pytest.mark.asyncio
async def test_vector_search_schema_has_query_and_top_k():
    from src.mcp import tools_server
    tools = await tools_server.mcp._list_tools()
    by_name = {t.name: t for t in tools}
    schema = by_name["vector_search"].parameters
    props = schema.get("properties", {})
    assert "query" in props
    assert "top_k" in props


@pytest.mark.asyncio
async def test_channel_message_stats_schema_has_group_by():
    from src.mcp import tools_server
    tools = await tools_server.mcp._list_tools()
    by_name = {t.name: t for t in tools}
    props = by_name["channel_message_stats"].parameters.get("properties", {})
    assert "group_by" in props
    assert "since" in props
    assert "until" in props


@pytest.mark.asyncio
async def test_channel_message_stats_maps_group_by_and_calls_pg(monkeypatch):
    from unittest.mock import AsyncMock

    from src.mcp import tools_server
    from src.storage.postgres import AsyncPostgres

    fake = [{"key": "alpha", "total": 3, "pending": 0, "processing": 0,
             "completed": 2, "vector_only": 0, "failed": 1, "skipped": 0}]
    m = AsyncMock(return_value=fake)
    monkeypatch.setattr(AsyncPostgres, "status_counts_by", m)

    out = await tools_server._stats_by("group", None, None)
    assert out == {"group_by": "group", "rows": fake}
    # 'group' → source_group dimension, passed positionally to the method.
    assert m.call_args.args[0] == "source_group"


@pytest.mark.asyncio
async def test_channel_message_stats_bad_group_by_errors():
    from src.mcp import tools_server
    out = await tools_server._stats_by("bogus", None, None)
    assert "error" in out


@pytest.mark.asyncio
async def test_channel_message_stats_bad_date_errors():
    from src.mcp import tools_server
    out = await tools_server._stats_by("channel", "not-a-date", None)
    assert "error" in out


@pytest.mark.asyncio
async def test_channel_message_timeline_stringifies_day(monkeypatch):
    from datetime import date
    from unittest.mock import AsyncMock

    from src.mcp import tools_server
    from src.storage.postgres import AsyncPostgres

    monkeypatch.setattr(
        AsyncPostgres, "timeline_counts",
        AsyncMock(return_value=[{"day": date(2026, 7, 21), "count": 5}]),
    )
    out = await tools_server._timeline("doc_date", None, None, None, None, None)
    assert out["date_field"] == "doc_date"
    assert out["buckets"] == [{"day": "2026-07-21", "count": 5}]


@pytest.mark.asyncio
async def test_channel_message_timeline_bad_date_field_errors():
    from src.mcp import tools_server
    out = await tools_server._timeline("bogus", None, None, None, None, None)
    assert "error" in out


@pytest.mark.asyncio
async def test_descriptions_use_atomic_tools_text():
    """The MCP tool descriptions came from atomic_tools.TOOL_DESCRIPTIONS
    plus a few extra-context sentences — verify wires aren't crossed
    (e.g. vector_search showing the graph_search description)."""
    from src.mcp import tools_server
    tools = await tools_server.mcp._list_tools()
    by_name = {t.name: t for t in tools}
    assert "Hybrid" in by_name["vector_search"].description \
        or "BM25" in by_name["vector_search"].description \
        or "hybrid" in by_name["vector_search"].description.lower()
    assert "graph" in by_name["graph_search"].description.lower()
    assert "INN" in by_name["find_entity_by_id"].description \
        or "canonical" in by_name["find_entity_by_id"].description.lower()
