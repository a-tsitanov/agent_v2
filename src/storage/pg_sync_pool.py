"""Process-global SYNCHRONOUS Postgres connection pool.

The async twin of this module (``pg_pool.py``) serves everything that
runs on the event loop.  This one exists for the one caller that cannot:
the ER verdict cache (``src/graph/er_graph_ops.py``).  Its interface —
``load_verdicts`` / ``store_verdicts`` — was written against a
synchronous ``structured_query`` and is called from deep inside
``entity_resolution.py``; making it async would push ``await`` up
through a large, well-tested call chain for no behavioural gain.

Why a pool rather than ``psycopg.connect`` per call: ``pg_pool.py``'s
docstring records that a connect/close storm against this same Postgres
(the one Temporal's history shards also hammer) was a confirmed
contributor to the merge/graph-phase freeze.  Verdict-cache calls are
per-batch rather than per-pair, so demand is low either way — but the
pool costs nothing and settles the question.

``min_size=0`` means importing this module, or building the pool, opens
no connection: offline-safe, same as the async pool.
"""

from __future__ import annotations

import threading

from psycopg_pool import ConnectionPool

from src.config import settings

_pool: ConnectionPool | None = None
_lock = threading.Lock()


def get_pg_sync_pool() -> ConnectionPool:
    """Return the per-process sync pool, opening it lazily on first use."""
    global _pool
    if _pool is not None:
        return _pool
    with _lock:
        if _pool is None:
            pool = ConnectionPool(
                conninfo=settings.postgres.dsn,
                min_size=settings.postgres.pool_min_size,
                max_size=settings.postgres.pool_max_size,
                timeout=settings.postgres.pool_timeout_s,
                name="kb-pg-sync",
                open=False,
            )
            pool.open()
            _pool = pool
    return _pool


def close_pg_sync_pool() -> None:
    """Close the pool on process shutdown."""
    global _pool
    with _lock:
        if _pool is not None:
            _pool.close()
            _pool = None


__all__ = ["close_pg_sync_pool", "get_pg_sync_pool"]
