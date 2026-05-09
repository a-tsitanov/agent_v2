"""Graph-store factory.

Two flavours:
  * Neo4j — production / live-stack path; used by the worker and
    the merge-job in Stage 9.
  * SimplePropertyGraphStore (in-memory) — tests and quick local
    iteration.  No external service required.
"""

from __future__ import annotations

from llama_index.core.graph_stores.types import PropertyGraphStore
from llama_index.graph_stores.neo4j import Neo4jPropertyGraphStore

from src.config import settings


def build_neo4j_graph_store() -> PropertyGraphStore:
    """Open the Neo4j graph store from ``Neo4jSettings``."""
    cfg = settings.neo4j
    return Neo4jPropertyGraphStore(
        url=cfg.uri,
        username=cfg.user,
        password=cfg.password.get_secret_value(),
        database=cfg.database,
    )
