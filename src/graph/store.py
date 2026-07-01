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
import time

from llama_index.core.graph_stores.types import PropertyGraphStore
from llama_index.graph_stores.neo4j import Neo4jPropertyGraphStore
from loguru import logger

from src.config import settings

_store: PropertyGraphStore | None = None
_lock = threading.Lock()


def _install_query_logging(store: PropertyGraphStore) -> PropertyGraphStore:
    """Wrap ``store.structured_query`` to log EVERY Cypher (flag-gated).

    Emits one INFO line per query — collapsed+truncated Cypher, the param
    KEYS only (never values, so no data leaks), row count and elapsed ms —
    so you can confirm/inspect the graph queries the search path issues
    (``MATCH (c:Community ...)`` etc.) straight from the app logs, without
    enabling Neo4j's own query.log.  Idempotent: a second call is a no-op.
    """
    if getattr(store, "_kb_query_logging", False):
        return store
    orig = store.structured_query

    def _logged(cypher, param_map=None, **kwargs):
        t0 = time.perf_counter()
        one_line = " ".join(str(cypher).split())[:160]
        keys = sorted((param_map or {}).keys())
        try:
            rows = orig(cypher, param_map=param_map, **kwargs)
        except Exception:
            dt = (time.perf_counter() - t0) * 1000
            logger.info(
                "neo4j query [{ms:.0f}ms FAILED] params={k} :: {q}",
                ms=dt, k=keys, q=one_line,
            )
            raise
        dt = (time.perf_counter() - t0) * 1000
        n = len(rows) if isinstance(rows, list) else "?"
        logger.info(
            "neo4j query [{ms:.0f}ms rows={n}] params={k} :: {q}",
            ms=dt, n=n, k=keys, q=one_line,
        )
        return rows

    store.structured_query = _logged  # type: ignore[method-assign]
    store._kb_query_logging = True  # type: ignore[attr-defined]
    return store


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
            store = _construct_neo4j_graph_store()
            if settings.neo4j.query_log:
                store = _install_query_logging(store)
            _store = store
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
