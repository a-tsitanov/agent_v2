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
}


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
