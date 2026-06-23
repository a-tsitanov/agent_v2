"""Graph-store factory.

Two flavours:
  * Neo4j — production / live-stack path; used by the worker and
    the merge-job in Stage 9.
  * SimplePropertyGraphStore (in-memory) — tests and quick local
    iteration.  No external service required.

The Neo4j store is cached PROCESS-GLOBALLY (Track A2).  Each call used to
build a brand-new ``Neo4jPropertyGraphStore`` — a fresh Bolt driver (its
own connection pool) plus an init-time schema refresh + index DDL — so
under ``max_inflight`` > 1 every activity added a connect/DDL storm on
top of the merge/graph write-lock contention.  One cached, pool-tuned
store per process now shares a single driver (mirrors ``get_pg_pool`` /
``get_llm_pool``).  The neo4j ``Driver`` is thread-safe and opens a
session per query, so the cached store is safe to share across the
activities' ``asyncio.to_thread`` workers.
"""

from __future__ import annotations

import contextlib
import threading

from llama_index.core.graph_stores.types import PropertyGraphStore
from llama_index.graph_stores.neo4j import Neo4jPropertyGraphStore

from src.config import settings

_store: PropertyGraphStore | None = None
_lock = threading.Lock()


def _neo4j_driver_kwargs() -> dict:
    """Pool/timeout kwargs forwarded to ``GraphDatabase.driver`` via
    ``Neo4jPropertyGraphStore(**neo4j_kwargs)`` — the store passes any
    extra kwargs straight to the Bolt driver."""
    cfg = settings.neo4j
    return {
        "max_connection_pool_size": cfg.max_connection_pool_size,
        "connection_acquisition_timeout": cfg.connection_acquisition_timeout_s,
        "connection_timeout": cfg.connection_timeout_s,
    }


def _construct_neo4j_graph_store() -> PropertyGraphStore:
    """Build a fresh Neo4j store from ``Neo4jSettings`` (no caching).
    Factored out so the cache can be exercised in tests without a live DB."""
    cfg = settings.neo4j
    return Neo4jPropertyGraphStore(
        url=cfg.uri,
        username=cfg.user,
        password=cfg.password.get_secret_value(),
        database=cfg.database,
        **_neo4j_driver_kwargs(),
    )


def build_neo4j_graph_store() -> PropertyGraphStore:
    """Return the process-global Neo4j graph store, building it once.

    Lazily constructed and cached so the Bolt driver/pool and the
    init-time schema refresh + index DDL happen ONCE per process instead
    of once per activity call."""
    global _store
    if _store is not None:
        return _store
    with _lock:
        if _store is None:
            _store = _construct_neo4j_graph_store()
    return _store


def reset_neo4j_graph_store() -> None:
    """Drop the cached store (closing its driver) — shutdown + test hook."""
    global _store
    if _store is not None:
        close = getattr(_store, "close", None)
        if callable(close):
            with contextlib.suppress(Exception):
                close()
        _store = None
