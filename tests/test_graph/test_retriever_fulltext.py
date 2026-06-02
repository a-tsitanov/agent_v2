"""Tests for the full-text entity-name lookup helpers."""

from __future__ import annotations


def test_build_fulltext_query_or_tokens_escaped():
    from src.graph.retriever import build_fulltext_query

    assert build_fulltext_query("Иванов Иван") == "Иванов OR Иван"
    # Lucene special chars are escaped per token.
    assert build_fulltext_query("a:b (x)") == r"a\:b OR \(x\)"
    # Blank input → empty query (caller short-circuits).
    assert build_fulltext_query("   ") == ""
    assert build_fulltext_query("") == ""


import pytest


class _StubStore:
    def __init__(self, rows=None, raise_=False):
        self._rows = rows or []
        self._raise = raise_
        self.last = None

    def structured_query(self, cypher, params):
        self.last = (cypher, params)
        if self._raise:
            raise RuntimeError("no fulltext index")
        return self._rows


def _retriever(store):
    from src.graph.retriever import GraphRetriever
    r = GraphRetriever.__new__(GraphRetriever)  # bypass LlamaIndex wiring
    r._graph_store = store
    r._similarity_top_k = 10
    return r


@pytest.mark.asyncio
async def test_afind_entities_by_name_maps_rows():
    store = _StubStore(rows=[
        {"name": "Иванов Иван Иванович", "labels": ["Person"], "description": "д."},
        {"name": "", "labels": [], "description": ""},  # skipped (no name)
    ])
    data = await _retriever(store).afind_entities_by_name("Иванов", limit=5)
    assert [e["entity_name"] for e in data.entities] == ["Иванов Иван Иванович"]
    assert data.entities[0]["entity_type"] == "Person"
    assert store.last[1] == {"lucene": "Иванов", "limit": 5}


@pytest.mark.asyncio
async def test_afind_entities_by_name_blank_and_failopen():
    store = _StubStore(rows=[{"name": "X"}])
    assert (await _retriever(store).afind_entities_by_name("   ")).entities == []
    assert store.last is None
    boom = _StubStore(raise_=True)
    assert (await _retriever(boom).afind_entities_by_name("Иванов")).entities == []
    assert (await _retriever(None).afind_entities_by_name("Иванов")).entities == []
