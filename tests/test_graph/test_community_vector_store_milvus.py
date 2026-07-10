# tests/test_graph/test_community_vector_store_milvus.py
"""MilvusCommunityReportVectorStore (community_report_vec) — fake MilvusClient, DB-free."""
from __future__ import annotations

from src.graph.community_vector_store_milvus import MilvusCommunityReportVectorStore


class _FakeClient:
    def __init__(self, search_result=None):
        self.upserts = []; self.searches = []; self._search_result = search_result or []; self._c = []
    def has_collection(self, name): return name in self._c
    def create_collection(self, **kw): self._c.append(kw.get("collection_name"))
    def create_schema(self, **kw): return _S()
    def prepare_index_params(self, **kw): return _I()
    def upsert(self, collection_name, data): self.upserts.append((collection_name, data))
    def search(self, **kw): self.searches.append(kw); return self._search_result
class _S:
    def add_field(self, **kw): return self
class _I:
    def add_index(self, **kw): return self


def _store(c):
    s = MilvusCommunityReportVectorStore.__new__(MilvusCommunityReportVectorStore)
    s._client = c; s._collection = "community_report_vec"; s._ensured = True
    return s


def test_upsert_rows():
    c = _FakeClient()
    _store(c).upsert([{"community_id": "c1", "level": 2, "summary": "s", "embedding": [0.1, 0.2]}])
    coll, data = c.upserts[0]
    assert coll == "community_report_vec"
    assert data[0]["pk"] == "c1:2" and data[0]["report_vec"] == [0.1, 0.2]
    assert data[0]["community_id"] == "c1" and data[0]["level"] == 2 and data[0]["summary"] == "s"


def test_knn_filters_level_and_maps():
    hit = {"entity": {"community_id": "c1", "level": 0, "summary": "s1"}}
    c = _FakeClient(search_result=[[hit]])
    out = _store(c).knn([0.0, 0.0], level=0, limit=5)
    assert c.searches[0]["filter"] == "level == 0" and c.searches[0]["limit"] == 5
    assert out == [{"community_id": "c1", "level": 0, "summary": "s1"}]
