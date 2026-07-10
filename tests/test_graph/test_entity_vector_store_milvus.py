# tests/test_graph/test_entity_vector_store_milvus.py
"""MilvusEntityVectorStore knn/upsert against a fake MilvusClient."""
from __future__ import annotations

from src.graph.entity_vector_store_milvus import MilvusEntityVectorStore


class _FakeClient:
    def __init__(self, search_result=None):
        self.upserts = []
        self.searches = []
        self._search_result = search_result or []
        self._collections = []
    def has_collection(self, name): return name in self._collections
    def create_collection(self, **kw): self._collections.append(kw.get("collection_name"))
    def create_schema(self, **kw): return _FakeSchema()
    def prepare_index_params(self, **kw): return _FakeIndex()
    def upsert(self, collection_name, data): self.upserts.append((collection_name, data))
    def search(self, **kw):
        self.searches.append(kw)
        return self._search_result


class _FakeSchema:
    def add_field(self, **kw): return self
class _FakeIndex:
    def add_index(self, **kw): return self


def _store(client):
    s = MilvusEntityVectorStore.__new__(MilvusEntityVectorStore)
    s._client = client
    s._collection = "entity_er_vec"
    s._ensured = True   # skip DDL in the unit test
    return s


def test_upsert_writes_expected_rows():
    c = _FakeClient()
    _store(c).upsert([{"name": "Иванов", "label": "PERSON", "embedding": [0.1, 0.2],
                       "mention_count": 3, "description": "инженер"}])
    coll, data = c.upserts[0]
    assert coll == "entity_er_vec"
    assert data[0]["name"] == "Иванов" and data[0]["er_vec"] == [0.1, 0.2]
    assert data[0]["label"] == "PERSON" and data[0]["mention_count"] == 3


def test_knn_maps_hits_with_embedding_and_label():
    hit = {"entity": {"name": "Иванов", "label": "PERSON", "mention_count": 3,
                      "description": "инженер", "er_vec": [0.1, 0.2]}}
    c = _FakeClient(search_result=[[hit]])   # pymilvus: list-per-query of hits
    out = _store(c).knn([0.0, 0.0], 5)
    assert c.searches[0]["anns_field"] == "er_vec" and c.searches[0]["limit"] == 5
    assert out == [{"name": "Иванов", "label": "PERSON", "embedding": [0.1, 0.2],
                    "mention_count": 3, "description": "инженер"}]
