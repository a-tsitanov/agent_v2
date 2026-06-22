"""Process-global async Postgres connection pool.

The ``documents`` and ``ingest_metrics`` stores used to open a fresh
``psycopg.AsyncConnection`` per call.  Under load that meant a
connect/close storm against a single shared Postgres (the same
instance Temporal's 512 history shards hammer) — a confirmed
contributor to the merge/graph-phase freeze.  This module hands out a
single pooled connection source per process, mirroring the
``get_llm_pool()`` singleton pattern in ``src/retrieval/llm_pool.py``.

Sizing lives in ``PostgresSettings`` (pool_min_size/pool_max_size) and
is deliberately conservative: total app demand is
``pool_max_size * n_processes`` and must fit under Postgres
``max_connections`` alongside Temporal.
"""

from __future__ import annotations

import asyncio

from psycopg_pool import AsyncConnectionPool

from src.config import settings

_pool: AsyncConnectionPool | None = None
_lock = asyncio.Lock()


async def get_pg_pool() -> AsyncConnectionPool:
    """Return the per-process pool, opening it lazily on first use.

    With ``min_size=0`` the pool opens without establishing any
    connection (offline-safe); connections are created on demand and
    returned to the pool for reuse instead of being torn down.
    """
    global _pool
    if _pool is not None:
        return _pool
    async with _lock:
        if _pool is None:
            pool = AsyncConnectionPool(
                conninfo=settings.postgres.dsn,
                min_size=settings.postgres.pool_min_size,
                max_size=settings.postgres.pool_max_size,
                timeout=settings.postgres.pool_timeout_s,
                name="kb-pg",
                open=False,
            )
            await pool.open()
            _pool = pool
    return _pool


async def close_pg_pool() -> None:
    """Close the pool on process shutdown (API lifespan / worker exit)."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def reset_for_tests() -> None:
    """Test hook — drop the singleton so the next get_pg_pool rebuilds."""
    global _pool
    _pool = None
