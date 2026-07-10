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


@pytest.mark.asyncio
async def test_find_by_name_nebula_lookup(monkeypatch):
    # Patch the backend flag on the exact `settings` object retriever.py
    # reads from (its own module-level binding), not `src.config.settings`
    # directly: test_llm_factory.py's `importlib.reload(src.config)` can
    # rebind the latter to a fresh Settings() instance mid-session, which
    # would silently desync the two and leave this test patching an object
    # afind_entities_by_name never reads.
    monkeypatch.setattr(
        "src.graph.retriever.settings.graph.backend", "nebula", raising=False,
    )
    rows = [{"vid": "abc", "p": {"name": "Иванов Иван", "label": "PERSON",
                                 "description": "инженер"}}]
    store = _FakeStore(rows=rows)
    r = GraphRetriever.for_store(store)
    out = await r.afind_entities_by_name("Иванов", limit=5)
    assert "LOOKUP ON `Entity`" in store.last_query
    assert '"Иванов"' in store.last_query
    assert out.entities == [{"entity_name": "Иванов Иван",
                             "entity_type": "PERSON", "description": "инженер"}]
