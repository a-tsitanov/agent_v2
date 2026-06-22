"""Unit tests for the per-process Postgres connection pool wiring.

These are offline-safe: the pool is created with ``min_size=0`` so it
opens without touching a real database, and the store-routing tests
inject a fake pool — no Postgres required.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from src.storage import pg_pool
from src.storage.ingest_metrics import AsyncIngestMetrics
from src.storage.postgres import AsyncPostgres


def test_get_pg_pool_is_a_process_singleton():
    pg_pool.reset_for_tests()

    async def go():
        a = await pg_pool.get_pg_pool()
        b = await pg_pool.get_pg_pool()
        try:
            assert a is b  # same object → one pool per process
        finally:
            await pg_pool.close_pg_pool()

    asyncio.run(go())


def test_close_pg_pool_drops_singleton():
    pg_pool.reset_for_tests()

    async def go():
        a = await pg_pool.get_pg_pool()
        await pg_pool.close_pg_pool()
        b = await pg_pool.get_pg_pool()
        try:
            assert a is not b  # rebuilt after close
        finally:
            await pg_pool.close_pg_pool()

    asyncio.run(go())


class _FakePool:
    """Stand-in for AsyncConnectionPool: records that a pooled
    connection was acquired and yields a sentinel."""

    def __init__(self, sentinel):
        self.sentinel = sentinel
        self.acquired = 0

    @asynccontextmanager
    async def connection(self):
        self.acquired += 1
        yield self.sentinel


def test_async_postgres_default_dsn_uses_pool(monkeypatch):
    sentinel = object()
    fake = _FakePool(sentinel)

    async def _fake_get_pool():
        return fake

    monkeypatch.setattr("src.storage.postgres.get_pg_pool", _fake_get_pool)

    async def go():
        async with AsyncPostgres()._conn() as conn:
            assert conn is sentinel
        assert fake.acquired == 1

    asyncio.run(go())


def test_async_postgres_explicit_dsn_bypasses_pool(monkeypatch):
    """An explicit dsn must NOT route through the shared pool."""
    called = {"pool": False}

    async def _boom():
        called["pool"] = True
        raise AssertionError("explicit dsn should not use the pool")

    monkeypatch.setattr("src.storage.postgres.get_pg_pool", _boom)

    pg = AsyncPostgres(dsn="postgresql://u:p@localhost:5432/other")
    assert pg._dsn == "postgresql://u:p@localhost:5432/other"
    assert called["pool"] is False  # constructing it didn't touch the pool


def test_ingest_metrics_default_dsn_uses_pool(monkeypatch):
    sentinel = object()
    fake = _FakePool(sentinel)

    async def _fake_get_pool():
        return fake

    monkeypatch.setattr("src.storage.ingest_metrics.get_pg_pool", _fake_get_pool)

    async def go():
        async with AsyncIngestMetrics()._conn() as conn:
            assert conn is sentinel
        assert fake.acquired == 1

    asyncio.run(go())
