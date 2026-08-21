"""The Postgres entity-table mirror: fail-soft, keyed on vid.

Same honest-stub approach as test_er_verdict_cache_postgres.py's
`_StubPool` — no live database, but a real cursor/connection shape so a
call the implementation makes wrong (wrong method, wrong context-manager
protocol) fails the test instead of silently no-oping.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field

import pytest

import src.storage.pg_sync_pool as pg_sync_pool_mod
from src.graph.entity_table import mirror_entities

# ── stubs ────────────────────────────────────────────────────────────


@dataclass
class _StubCursor:
    executed: list[tuple] = field(default_factory=list)
    raise_on_execute: bool = False

    def execute(self, sql, params=None):
        if self.raise_on_execute:
            raise RuntimeError("boom")
        self.executed.append((sql, params))

    def executemany(self, sql, params_seq):
        if self.raise_on_execute:
            raise RuntimeError("boom")
        self.executed.append((sql, list(params_seq)))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@dataclass
class _StubConn:
    raise_on_execute: bool = False
    cursors: list[_StubCursor] = field(default_factory=list)

    def cursor(self, row_factory=None):
        cur = _StubCursor(raise_on_execute=self.raise_on_execute)
        self.cursors.append(cur)
        return cur

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _StubPool:
    def __init__(self, raise_on_execute: bool = False) -> None:
        self.conn = _StubConn(raise_on_execute=raise_on_execute)

    @contextmanager
    def connection(self):
        yield self.conn

    @property
    def executed(self) -> list[tuple]:
        return [e for cur in self.conn.cursors for e in cur.executed]


@pytest.fixture
def mirror_with_pool(monkeypatch):
    def _make(raise_on_execute=False):
        pool = _StubPool(raise_on_execute=raise_on_execute)
        monkeypatch.setattr(pg_sync_pool_mod, "get_pg_sync_pool", lambda: pool)
        return mirror_entities, pool

    return _make


# ── tests ────────────────────────────────────────────────────────────


def test_upsert_is_keyed_on_vid(mirror_with_pool):
    fn, pool = mirror_with_pool()
    fn([{"vid": "v1", "name": "Украина", "label": "Location",
         "description": "государство", "mention_count": 5}])
    sql, _params = pool.executed[0]
    assert "INSERT INTO entity" in sql
    assert "ON CONFLICT (vid) DO UPDATE" in sql
    assert "name = EXCLUDED.name" in sql
    assert "updated_at = now()" in sql


def test_empty_rows_is_a_noop(mirror_with_pool):
    fn, pool = mirror_with_pool()
    fn([])
    assert pool.executed == []


def test_a_postgres_error_is_swallowed(mirror_with_pool):
    """FAIL-SOFT: the mirror must never break the graph write."""
    fn, _pool = mirror_with_pool(raise_on_execute=True)
    fn([{"vid": "v1", "name": "n", "label": "", "description": "", "mention_count": 1}])
    # no exception propagated
