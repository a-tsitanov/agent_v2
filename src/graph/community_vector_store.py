# src/graph/community_vector_store.py
"""Vector store for semantic community-report selection — backend-dispatched.

Neo4j serves the kNN from the in-graph `community_report_vec` index;
NebulaGraph has no such index, so it goes to a Milvus collection. Mirrors
src/graph/entity_vector_store.py (the merged er_vec slice)."""

from __future__ import annotations

from typing import Any, Protocol, TypedDict, runtime_checkable

from src.config import settings

# The exact query the current select_communities_semantic issues.
_SELECT_SEMANTIC_CYPHER = """
CALL db.index.vector.queryNodes('community_report_vec', $limit, $vec) YIELD node, score
WHERE node.level = $level AND node.summary IS NOT NULL AND trim(node.summary) <> ''
RETURN node.id AS community_id, node.level AS level, node.summary AS summary
ORDER BY score DESC
"""


class CommunityRef(TypedDict):
    community_id: str
    level: int
    summary: str


class CommunityReport(TypedDict):
    community_id: str
    level: int
    summary: str
    embedding: list[float]


@runtime_checkable
class CommunityReportVectorStore(Protocol):
    def knn(self, query_vec: list[float], *, level: int, limit: int) -> list[CommunityRef]: ...
    def upsert(self, reports: list[CommunityReport]) -> None: ...


class Neo4jCommunityReportVectorStore:
    def __init__(self, graph_store: Any):
        self._graph_store = graph_store

    def knn(self, query_vec: list[float], *, level: int, limit: int) -> list[CommunityRef]:
        rows = self._graph_store.structured_query(
            _SELECT_SEMANTIC_CYPHER,
            {"vec": list(query_vec), "level": int(level), "limit": max(0, int(limit))},
        )
        out: list[CommunityRef] = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            cid = row.get("community_id")
            summary = (row.get("summary") or "").strip()
            if cid is None or not summary:
                continue
            out.append({"community_id": str(cid),
                        "level": int(row.get("level") or 0), "summary": summary})
        return out[: max(0, int(limit))]

    def upsert(self, reports: list[CommunityReport]) -> None:
        # No-op: report_vec is persisted on the :Community node by
        # community._WRITE_REPORT_CYPHER (unchanged neo4j write path).
        return None


def build_community_report_vector_store(graph_store: Any) -> CommunityReportVectorStore:
    use_milvus = (
        settings.graph.backend == "nebula"
        or settings.agent.community_vector_backend == "milvus"
    )
    if use_milvus:
        from src.graph.community_vector_store_milvus import MilvusCommunityReportVectorStore

        return MilvusCommunityReportVectorStore()
    return Neo4jCommunityReportVectorStore(graph_store)
