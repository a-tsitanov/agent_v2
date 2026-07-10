# src/graph/entity_vector_store.py
"""Vector store for ER candidate-kNN — backend-dispatched.

The ER candidate lookup (`entity_resolution._load_candidates_via_store`)
finds the k nearest stored CANONICAL entities to each new entity. Neo4j
serves this from an in-graph vector index; NebulaGraph has no such index,
so it goes to a Milvus collection. Both impls return candidates WITH their
embeddings (ER's `_candidate_pairs` cosines every item).
"""

from __future__ import annotations

import json
from typing import Any, Protocol, TypedDict, runtime_checkable

from loguru import logger

from src.config import settings
from src.graph.index import ensure_er_vector_index

_NEO4J_ER_KNN_CYPHER = """
CALL db.index.vector.queryNodes('er_embedding_vec', $k, $vec)
YIELD node
WHERE node.er_canonical_name IS NOT NULL
RETURN node.name AS name,
       labels(node) AS labels,
       node.er_vec AS er_vec,
       node.er_embedding AS er_embedding,
       coalesce(node.mention_count, 1) AS mention_count,
       coalesce(node.description, '') AS description
"""


class EntityCandidate(TypedDict):
    name: str
    label: str
    embedding: list[float]
    mention_count: int
    description: str


@runtime_checkable
class EntityVectorStore(Protocol):
    def knn(self, query_vec: list[float], k: int) -> list[EntityCandidate]: ...
    def upsert(self, entities: list[EntityCandidate]) -> None: ...


def _row_embedding(row: dict) -> list[float]:
    emb = row.get("er_vec")
    if emb:
        return list(emb)
    raw = row.get("er_embedding") or "[]"
    try:
        return json.loads(raw) if isinstance(raw, str) else list(raw)
    except (json.JSONDecodeError, TypeError):
        return []


class Neo4jEntityVectorStore:
    """Wraps the existing in-graph ER vector index (unchanged behavior)."""

    def __init__(self, graph_store: Any, *, dim: int | None = None):
        self._graph_store = graph_store
        self._dim = dim if dim is not None else settings.milvus.dim
        self._ensured = False

    def knn(self, query_vec: list[float], k: int) -> list[EntityCandidate]:
        if not self._ensured:
            try:
                ensure_er_vector_index(self._graph_store, self._dim)
            except Exception as exc:
                logger.warning("ensure ER vector index failed: {e}", e=exc)
            self._ensured = True
        rows = self._graph_store.structured_query(
            _NEO4J_ER_KNN_CYPHER, param_map={"k": int(k), "vec": list(query_vec)},
        )
        out: list[EntityCandidate] = []
        for row in rows or []:
            name = row.get("name") or ""
            emb = _row_embedding(row)
            if not name or not emb:
                continue
            labels = [lab for lab in (row.get("labels") or [])
                      if lab not in ("__Entity__", "__Node__")]
            out.append({
                "name": name,
                "label": labels[0] if labels else "Other",
                "embedding": emb,
                "mention_count": int(row.get("mention_count") or 1),
                "description": row.get("description") or "",
            })
        return out

    def upsert(self, entities: list[EntityCandidate]) -> None:
        # No-op: the er_vec node property is persisted by the normal graph
        # node upsert in entity_resolution (unchanged neo4j write path).
        return None


def build_entity_vector_store(graph_store: Any) -> EntityVectorStore:
    """Dispatch: nebula (or the opt-in flag) -> Milvus; else Neo4j native."""
    use_milvus = (
        settings.graph.backend == "nebula"
        or settings.agent.er_vector_backend == "milvus"
    )
    if use_milvus:
        from src.graph.entity_vector_store_milvus import MilvusEntityVectorStore

        return MilvusEntityVectorStore()
    return Neo4jEntityVectorStore(graph_store)
