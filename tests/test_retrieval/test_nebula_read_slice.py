"""Nebula read slice: store-only retriever construction + aretrieve guard."""
from __future__ import annotations

import pytest

from src.graph.retriever import GraphRetriever, RoundGraphData


class _FakeStore:
    def __init__(self, rows=None):
        self._rows = rows or []
        self.last_query = None
    def structured_query(self, query, param_map=None):
        self.last_query = query
        assert not param_map, "nebula path must not pass param_map"
        return self._rows


def test_for_store_builds_without_llamaindex_retriever():
    store = _FakeStore()
    r = GraphRetriever.for_store(store)
    assert r._graph_store is store
    assert r._retriever is None


@pytest.mark.asyncio
async def test_aretrieve_empty_without_retriever():
    r = GraphRetriever.for_store(_FakeStore())
    out = await r.aretrieve("что угодно")
    assert isinstance(out, RoundGraphData)
    assert out.entities == [] and out.relations == [] and out.chunks == []
