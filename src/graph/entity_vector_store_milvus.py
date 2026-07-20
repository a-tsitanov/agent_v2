# src/graph/entity_vector_store_milvus.py
"""Milvus-backed EntityVectorStore (collection `entity_er_vec`).

Direct pymilvus.MilvusClient (mirrors src/storage/chunk_repository.py),
separate from the chunk collection. Only canonical entities are stored.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from src.config import settings
from src.graph.entity_vector_store import EntityCandidate

_COLLECTION = "entity_er_vec"
_NAME_MAX, _LABEL_MAX, _DESC_MAX = 512, 256, 4096


def _btrunc(s: str, max_bytes: int) -> str:
    """Truncate to fit a Milvus VARCHAR: max_length counts UTF-8 BYTES, not chars.

    A plain ``s[:max_bytes]`` slices code points, so non-ASCII text (Cyrillic ≈ 2
    bytes/char) still overflows max_length and the upsert is rejected with
    code=1100. Slice the encoded bytes and drop any partial trailing char.
    """
    b = (s or "").encode("utf-8")
    if len(b) <= max_bytes:
        return s or ""
    return b[:max_bytes].decode("utf-8", "ignore")


class MilvusEntityVectorStore:
    def __init__(self, client: Any | None = None, collection: str = _COLLECTION):
        from pymilvus import MilvusClient

        self._client = client or MilvusClient(
            uri=settings.milvus.uri, timeout=settings.milvus.timeout_s,
        )
        self._collection = collection
        self._ensured = False

    def _ensure(self) -> None:
        if self._ensured:
            return
        try:
            if not self._client.has_collection(self._collection):
                from pymilvus import DataType

                schema = self._client.create_schema(auto_id=False, enable_dynamic_field=False)
                schema.add_field("name", DataType.VARCHAR, is_primary=True, max_length=_NAME_MAX)
                schema.add_field("er_vec", DataType.FLOAT_VECTOR, dim=settings.milvus.dim)
                schema.add_field("label", DataType.VARCHAR, max_length=_LABEL_MAX)
                schema.add_field("mention_count", DataType.INT64)
                schema.add_field("description", DataType.VARCHAR, max_length=_DESC_MAX)
                index = self._client.prepare_index_params()
                index.add_index(
                    field_name="er_vec", index_type=settings.milvus.index_type,
                    metric_type="COSINE",
                    params={"M": settings.milvus.hnsw_m,
                            "efConstruction": settings.milvus.hnsw_ef_construction},
                )
                self._client.create_collection(
                    collection_name=self._collection, schema=schema, index_params=index,
                )
            self._ensured = True
        except Exception as exc:
            logger.warning("ensure entity_er_vec collection failed: {e}", e=exc)

    def upsert(self, entities: list[EntityCandidate]) -> None:
        if not entities:
            return
        self._ensure()
        data = [{
            "name": _btrunc(e["name"], _NAME_MAX),
            "er_vec": list(e["embedding"]),
            "label": _btrunc(e.get("label") or "", _LABEL_MAX),
            "mention_count": int(e.get("mention_count") or 1),
            "description": _btrunc(e.get("description") or "", _DESC_MAX),
        } for e in entities if e.get("embedding")]
        if data:
            self._client.upsert(collection_name=self._collection, data=data)

    def knn(self, query_vec: list[float], k: int) -> list[EntityCandidate]:
        self._ensure()
        try:
            res = self._client.search(
                collection_name=self._collection, data=[list(query_vec)],
                anns_field="er_vec", limit=int(k),
                output_fields=["name", "label", "mention_count", "description", "er_vec"],
                search_params={"metric_type": "COSINE",
                               "params": {"ef": settings.milvus.hnsw_ef_search}},
            )
        except Exception as exc:
            logger.warning("entity_er_vec knn failed: {e}", e=exc)
            return []
        out: list[EntityCandidate] = []
        for hits in (res or []):
            for h in hits:
                e = h.get("entity", h) if isinstance(h, dict) else h
                name = e.get("name") or ""
                emb = e.get("er_vec")
                if not name or not emb:
                    continue
                out.append({
                    "name": name, "label": e.get("label") or "Other",
                    "embedding": list(emb),
                    "mention_count": int(e.get("mention_count") or 1),
                    "description": e.get("description") or "",
                })
        return out
