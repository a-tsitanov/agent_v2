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
    def __init__(self, raise_on_execute: bool = False, raise_on_connection: bool = False) -> None:
        self.conn = _StubConn(raise_on_execute=raise_on_execute)
        self.raise_on_connection = raise_on_connection
        # every timeout= value connection() was called with, in call order.
        self.connection_timeouts: list[float | None] = []

    @contextmanager
    def connection(self, timeout=None):
        self.connection_timeouts.append(timeout)
        if self.raise_on_connection:
            raise RuntimeError("pool unavailable")
        yield self.conn

    @property
    def executed(self) -> list[tuple]:
        return [e for cur in self.conn.cursors for e in cur.executed]


@pytest.fixture
def mirror_with_pool(monkeypatch):
    def _make(raise_on_execute=False, raise_on_connection=False):
        pool = _StubPool(
            raise_on_execute=raise_on_execute, raise_on_connection=raise_on_connection,
        )
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


def test_mention_count_defaults_to_zero_to_match_the_graph_vertex(mirror_with_pool):
    """nebula_store.row() defaults mention_count to 0
    (`int(props.get('mention_count', 0) or 0)`) — the mirror must agree,
    or the two writes drift on every entity with no mention_count."""
    fn, pool = mirror_with_pool()
    fn([{"vid": "v1", "name": "n", "label": "", "description": ""}])
    _sql, params = pool.executed[0]
    assert params[0][4] == 0


def test_connection_uses_a_short_timeout_decoupled_from_the_pool_budget(mirror_with_pool):
    """FAIL-FAST: the mirror must not absorb the shared sync pool's 30s
    pool_timeout_s per chunk during a Postgres outage — it asks for a
    connection with its own short (1s) budget instead."""
    fn, pool = mirror_with_pool()
    fn([{"vid": "v1", "name": "n", "label": "", "description": "", "mention_count": 1}])
    assert pool.connection_timeouts == [1]


def test_an_unavailable_pool_is_swallowed_without_blocking(mirror_with_pool):
    """FAIL-SOFT + FAIL-FAST: connection() itself raising (pool exhausted,
    Postgres unreachable) must not propagate, and must still have been
    attempted with the short budget, not the pool's full 30s."""
    fn, pool = mirror_with_pool(raise_on_connection=True)
    fn([{"vid": "v1", "name": "n", "label": "", "description": "", "mention_count": 1}])
    assert pool.connection_timeouts == [1]
    assert pool.executed == []
