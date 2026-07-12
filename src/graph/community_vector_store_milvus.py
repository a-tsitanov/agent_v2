# src/graph/community_vector_store_milvus.py
"""Milvus-backed CommunityReportVectorStore (collection `community_report_vec`).

Direct pymilvus.MilvusClient (mirrors src/graph/entity_vector_store_milvus.py),
separate from the entity/chunk collections. Used when NebulaGraph has no
in-graph vector index for community reports, or opted into on Neo4j via
settings.agent.community_vector_backend == "milvus".
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from src.config import settings
from src.graph.community_vector_store import CommunityRef, CommunityReport

_COLLECTION = "community_report_vec"
_PK_MAX, _CID_MAX, _SUM_MAX = 256, 128, 8192


class MilvusCommunityReportVectorStore:
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
                schema.add_field("pk", DataType.VARCHAR, is_primary=True, max_length=_PK_MAX)
                schema.add_field("report_vec", DataType.FLOAT_VECTOR, dim=settings.milvus.dim)
                schema.add_field("community_id", DataType.VARCHAR, max_length=_CID_MAX)
                schema.add_field("level", DataType.INT64)
                schema.add_field("summary", DataType.VARCHAR, max_length=_SUM_MAX)
                index = self._client.prepare_index_params()
                index.add_index(
                    field_name="report_vec", index_type=settings.milvus.index_type,
                    metric_type="COSINE",
                    params={"M": settings.milvus.hnsw_m,
                            "efConstruction": settings.milvus.hnsw_ef_construction},
                )
                self._client.create_collection(
                    collection_name=self._collection, schema=schema, index_params=index,
                )
            self._ensured = True
        except Exception as exc:
            logger.warning("ensure community_report_vec collection failed: {e}", e=exc)

    def upsert(self, reports: list[CommunityReport]) -> None:
        if not reports:
            return
        self._ensure()
        data = [{
            "pk": f"{r['community_id']}:{int(r['level'])}"[:_PK_MAX],
            "report_vec": list(r["embedding"]),
            "community_id": str(r["community_id"])[:_CID_MAX],
            "level": int(r["level"]),
            "summary": (r.get("summary") or "")[:_SUM_MAX],
        } for r in reports if r.get("embedding")]
        if data:
            self._client.upsert(collection_name=self._collection, data=data)

    def fetch_vectors(self, refs: list[tuple[str, int]]) -> dict[tuple[str, int], list[float]]:
        """Report vectors for the given ``(community_id, level)`` pairs, keyed by
        that pair. Backs the nebula hierarchy-descent selection, which reads the
        community tree from the graph but needs each report_vec from Milvus (the
        vectors do not live on the nebula vertex). Missing / errored → omitted."""
        if not refs:
            return {}
        self._ensure()
        pks = [f"{cid}:{int(lvl)}"[:_PK_MAX] for cid, lvl in refs]
        quoted = ", ".join('"' + p.replace('"', '\\"') + '"' for p in pks)
        try:
            rows = self._client.query(
                collection_name=self._collection,
                filter=f"pk in [{quoted}]",
                output_fields=["community_id", "level", "report_vec"],
            )
        except Exception as exc:
            logger.warning("community_report_vec fetch_vectors failed: {e}", e=exc)
            return {}
        out: dict[tuple[str, int], list[float]] = {}
        for e in rows or []:
            cid = e.get("community_id")
            vec = e.get("report_vec")
            if cid is None or not vec:
                continue
            out[(str(cid), int(e.get("level") or 0))] = list(vec)
        return out

    def knn(self, query_vec: list[float], *, level: int, limit: int) -> list[CommunityRef]:
        self._ensure()
        try:
            res = self._client.search(
                collection_name=self._collection, data=[list(query_vec)],
                anns_field="report_vec", limit=max(0, int(limit)),
                filter=f"level == {int(level)}",
                output_fields=["community_id", "level", "summary"],
                search_params={"metric_type": "COSINE",
                               "params": {"ef": settings.milvus.hnsw_ef_search}},
            )
        except Exception as exc:
            logger.warning("community_report_vec knn failed: {e}", e=exc)
            return []
        out: list[CommunityRef] = []
        for hits in (res or []):
            for h in hits:
                e = h.get("entity", h) if isinstance(h, dict) else h
                cid = e.get("community_id")
                summary = (e.get("summary") or "").strip()
                if cid is None or not summary:
                    continue
                out.append({"community_id": str(cid),
                            "level": int(e.get("level") or 0), "summary": summary})
        return out
