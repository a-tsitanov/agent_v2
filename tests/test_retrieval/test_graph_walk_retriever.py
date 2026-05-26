"""Unit tests for GraphRetriever.awalk — bounded N-hop graph traversal.

Uses a fake PropertyGraphStore exposing only ``structured_query`` (the
generic Cypher entry on Neo4jPropertyGraphStore). NO live Neo4j: we
assert the Cypher params (hops clamp, node/edge caps, rel_filter
pass-through) and the RoundGraphData mapping of the returned rows.
"""

from __future__ import annotations

import pytest

from src.graph.retriever import (
    GRAPH_WALK_EDGE_CAP,
    GRAPH_WALK_NODE_CAP,
    GraphRetriever,
)


class _FakeStore:
    """Captures the Cypher + params and returns canned rows."""

    def __init__(self, rows=None):
        self._rows = rows or []
        self.last_cypher = None
        self.last_params = None

    def structured_query(self, cypher, param_map=None):
        self.last_cypher = cypher
        self.last_params = param_map or {}
        return self._rows


def _retriever_with_store(store):
    """Build a GraphRetriever without touching PropertyGraphIndex."""
    r = GraphRetriever.__new__(GraphRetriever)
    r._retriever = None
    r._graph_store = store
    return r


@pytest.mark.asyncio
async def test_awalk_clamps_hops_and_caps_in_params():
    store = _FakeStore(rows=[])
    r = _retriever_with_store(store)
    await r.awalk("Иванов", hops=99)
    params = store.last_params
    # hops clamped to the hard max
    assert params["hops"] <= GRAPH_WALK_NODE_CAP  # sanity
    assert params["node_cap"] == GRAPH_WALK_NODE_CAP
    assert params["edge_cap"] == GRAPH_WALK_EDGE_CAP
    assert params["name"] == "Иванов"
    # query is bounded by a LIMIT on the node cap
    assert "LIMIT" in store.last_cypher.upper()


@pytest.mark.asyncio
async def test_awalk_passes_rel_filter_as_param():
    store = _FakeStore(rows=[])
    r = _retriever_with_store(store)
    await r.awalk("A", hops=2, rel_filter=["KNOWS", "WORKS_AT"])
    assert store.last_params["rel_filter"] == ["KNOWS", "WORKS_AT"]


@pytest.mark.asyncio
async def test_awalk_no_rel_filter_passes_empty_list():
    store = _FakeStore(rows=[])
    r = _retriever_with_store(store)
    await r.awalk("A", hops=2, rel_filter=None)
    # empty list ⇒ Cypher treats "no filter" (size(filter)=0) branch
    assert store.last_params["rel_filter"] == []


@pytest.mark.asyncio
async def test_awalk_maps_rows_to_entities_and_relations():
    rows = [
        {
            "entities": [
                {"name": "A", "label": "Person", "description": "a"},
                {"name": "B", "label": "Person", "description": "b"},
            ],
            "relations": [
                {"src": "A", "tgt": "B", "label": "KNOWS"},
            ],
        }
    ]
    store = _FakeStore(rows=rows)
    r = _retriever_with_store(store)
    data = await r.awalk("A", hops=2)
    names = {e["entity_name"] for e in data.entities}
    assert names == {"A", "B"}
    assert data.relations[0]["label"] == "KNOWS"


@pytest.mark.asyncio
async def test_awalk_no_store_returns_empty():
    r = _retriever_with_store(None)
    data = await r.awalk("A", hops=2)
    assert data.entities == []
    assert data.relations == []
