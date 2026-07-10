"""EntityVectorStore: Neo4j impl query/mapping + factory dispatch (DB-free)."""
from __future__ import annotations

import json

import src.graph.entity_vector_store as evs


class _FakeGraphStore:
    def __init__(self, rows):
        self._rows = rows
        self.last_query = None
        self.last_params = None
    def structured_query(self, query, param_map=None):
        self.last_query = query
        self.last_params = param_map or {}
        return self._rows


def test_neo4j_knn_maps_rows_with_embedding_and_label(monkeypatch):
    monkeypatch.setattr(evs, "ensure_er_vector_index", lambda *a, **k: True)
    rows = [
        {"name": "Иванов", "labels": ["__Entity__", "PERSON"], "er_vec": [0.1, 0.2],
         "er_embedding": None, "mention_count": 3, "description": "инженер"},
        {"name": "Ветеран", "labels": ["__Entity__", "PERSON"], "er_vec": None,
         "er_embedding": json.dumps([0.3, 0.4]), "mention_count": 1, "description": ""},
    ]
    store = evs.Neo4jEntityVectorStore(_FakeGraphStore(rows))
    out = store.knn([0.0, 0.0], 5)
    assert "db.index.vector.queryNodes('er_embedding_vec'" in store._graph_store.last_query
    assert store._graph_store.last_params == {"k": 5, "vec": [0.0, 0.0]}
    assert out[0] == {"name": "Иванов", "label": "PERSON", "embedding": [0.1, 0.2],
                      "mention_count": 3, "description": "инженер"}
    # legacy er_embedding JSON is parsed when er_vec is absent
    assert out[1]["embedding"] == [0.3, 0.4] and out[1]["label"] == "PERSON"


def test_neo4j_upsert_is_noop():
    store = evs.Neo4jEntityVectorStore(_FakeGraphStore([]))
    store.upsert([{"name": "x", "label": "T", "embedding": [0.1],
                   "mention_count": 1, "description": ""}])  # must not raise / query
    assert store._graph_store.last_query is None


def test_factory_dispatches_on_backend(monkeypatch):
    monkeypatch.setattr(evs.settings.graph, "backend", "neo4j", raising=False)
    monkeypatch.setattr(evs.settings.agent, "er_vector_backend", "native", raising=False)
    assert isinstance(evs.build_entity_vector_store(_FakeGraphStore([])), evs.Neo4jEntityVectorStore)

    monkeypatch.setattr(evs.settings.graph, "backend", "nebula", raising=False)
    import pytest
    with pytest.raises(ModuleNotFoundError):
        # Milvus impl arrives in Task 2; dispatch must ATTEMPT it under nebula.
        evs.build_entity_vector_store(_FakeGraphStore([]))
