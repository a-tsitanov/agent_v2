"""CommunityReportVectorStore: Neo4j impl query/mapping + factory dispatch."""
from __future__ import annotations

import pytest

import src.graph.community_vector_store as cvs


class _FakeGraphStore:
    def __init__(self, rows):
        self._rows = rows; self.last_query = None; self.last_params = None
    def structured_query(self, query, param_map=None):
        self.last_query = query; self.last_params = param_map or {}
        return self._rows


def test_neo4j_knn_maps_rows():
    rows = [{"community_id": "c1", "level": 0, "summary": "s1"},
            {"community_id": "c2", "level": 0, "summary": "  "}]  # blank skipped
    store = cvs.Neo4jCommunityReportVectorStore(_FakeGraphStore(rows))
    out = store.knn([0.1, 0.2], level=0, limit=5)
    assert "db.index.vector.queryNodes('community_report_vec'" in store._graph_store.last_query
    assert store._graph_store.last_params == {"vec": [0.1, 0.2], "level": 0, "limit": 5}
    assert out == [{"community_id": "c1", "level": 0, "summary": "s1"}]


def test_neo4j_upsert_is_noop():
    store = cvs.Neo4jCommunityReportVectorStore(_FakeGraphStore([]))
    store.upsert([{"community_id": "c1", "level": 0, "summary": "s", "embedding": [0.1]}])
    assert store._graph_store.last_query is None


def test_factory_dispatches(monkeypatch):
    monkeypatch.setattr(cvs.settings.graph, "backend", "neo4j", raising=False)
    monkeypatch.setattr(cvs.settings.agent, "community_vector_backend", "native", raising=False)
    assert isinstance(cvs.build_community_report_vector_store(_FakeGraphStore([])),
                      cvs.Neo4jCommunityReportVectorStore)
    # nebula -> milvus impl (Task 2 not landed yet in this task: the module
    # doesn't exist, so dispatch raises ModuleNotFoundError from the lazy
    # import). Task 2 reconciles this to the sentinel-patch form once
    # src/graph/community_vector_store_milvus.py exists.
    monkeypatch.setattr(cvs.settings.graph, "backend", "nebula", raising=False)
    with pytest.raises(ModuleNotFoundError):
        cvs.build_community_report_vector_store(_FakeGraphStore([]))
