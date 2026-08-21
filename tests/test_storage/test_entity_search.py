from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import pytest
from psycopg.rows import dict_row

import src.storage.entity_search as entity_search_mod
from src.storage.entity_search import EntitySearchRepository, build_entity_search_query

# ── query builders ──────────────────────────────────────────────────


def test_exact_matches_the_whole_name():
    sql, params = build_entity_search_query("Украина", mode="exact", label=None, limit=10)
    assert "name = %s" in sql
    assert params[0] == "Украина"
    assert params[-1] == 10


def test_prefix_uses_ilike_anchored_left():
    sql, params = build_entity_search_query("Украин", mode="prefix", label=None, limit=10)
    assert "name ILIKE %s" in sql
    assert params[0] == "Украин%"


def test_substring_uses_trigram_and_orders_by_similarity():
    sql, _params = build_entity_search_query("Ромаш", mode="substring", label=None, limit=10)
    # `%%` is the psycopg-escaped `%` trigram operator.
    assert "name %% %s" in sql
    assert "similarity(name, %s)" in sql
    assert "ORDER BY" in sql


def test_label_filter_is_added_only_when_given():
    sql_no, _p_no = build_entity_search_query("x", mode="exact", label=None, limit=5)
    assert "label = %s" not in sql_no
    sql_yes, p_yes = build_entity_search_query("x", mode="exact", label="Person", limit=5)
    assert "label = %s" in sql_yes
    assert "Person" in p_yes


def test_mention_count_breaks_ties():
    """Frequent entities surface first among equal matches."""
    sql, _ = build_entity_search_query("x", mode="prefix", label=None, limit=5)
    assert "mention_count DESC" in sql


def test_unknown_mode_is_rejected():
    with pytest.raises(ValueError, match="unknown mode"):
        build_entity_search_query("x", mode="fuzzy", label=None, limit=5)


# ── stubs ────────────────────────────────────────────────────────────
# Same honest stub as tests/test_storage/test_stats_repository.py: rows
# come back as dicts only when `row_factory=dict_row` was actually
# requested, tuples otherwise — a repository read that forgets
# `row_factory=dict_row` must fail loudly here too, not just in prod.


@dataclass
class _StubCursor:
    rows: list[dict]
    row_factory: object = None
    executed: list[tuple] = field(default_factory=list)

    async def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def _shaped(self, row):
        if self.row_factory is dict_row:
            return row
        return tuple(row.values()) if isinstance(row, dict) else row

    async def fetchall(self):
        return [self._shaped(r) for r in self.rows]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@dataclass
class _StubConn:
    cur: _StubCursor

    def cursor(self, *a, **kw):
        self.cur.row_factory = kw.get("row_factory")
        return self.cur


def _repo_with(rows: list[dict]) -> tuple[EntitySearchRepository, _StubConn]:
    conn = _StubConn(cur=_StubCursor(rows=rows))
    repo = EntitySearchRepository()

    @asynccontextmanager
    async def _conn():
        yield conn

    repo._conn = _conn  # type: ignore[method-assign]
    return repo, conn


# ── EntitySearchRepository.search() ─────────────────────────────────


async def test_blank_query_returns_empty_without_touching_pool():
    repo, conn = _repo_with(rows=[{"vid": "e1"}])
    result = await repo.search("   ")
    assert result == []
    assert conn.cur.executed == []  # no cursor.execute() call at all


async def test_search_reads_rows_with_dict_row_factory():
    rows = [
        {"vid": "e1", "name": "Украина", "label": "Country",
         "description": "", "mention_count": 5},
    ]
    repo, conn = _repo_with(rows=rows)
    result = await repo.search("Украина", mode="exact")
    assert conn.cur.row_factory is dict_row
    # Honest stub: without dict_row this would come back as tuples and
    # break `row["name"]`-style access downstream.
    assert result == rows


class _StubPool:
    """Records every `timeout=` `.connection()` was called with."""

    def __init__(self, conn: _StubConn) -> None:
        self._conn = conn
        self.connection_timeouts: list[float | None] = []

    @asynccontextmanager
    async def connection(self, timeout=None):
        self.connection_timeouts.append(timeout)
        yield self._conn


async def test_conn_acquires_the_pool_connection_with_a_short_timeout(monkeypatch):
    """FAIL-FAST: a Postgres outage must not stall entity search behind
    the shared pool's full pool_timeout_s (~30s) — `_conn()` asks for a
    connection with its own short (1s) budget instead, so
    `_entity_table_names`'s fail-soft catch fires in ~1s. Mirrors
    entity_table.mirror_entities' `timeout=1` on the write side."""
    rows = [{"vid": "e1", "name": "x", "label": "", "description": "", "mention_count": 1}]
    conn = _StubConn(cur=_StubCursor(rows=rows))
    pool = _StubPool(conn)

    async def _get_pool():
        return pool

    monkeypatch.setattr(entity_search_mod, "get_pg_pool", _get_pool)
    repo = EntitySearchRepository()
    result = await repo.search("x", mode="exact")
    assert result == rows
    assert pool.connection_timeouts == [1]


async def test_search_passes_mode_label_limit_through_to_builder():
    rows = [
        {"vid": "e2", "name": "Ромашка", "label": "Person",
         "description": "d", "mention_count": 3},
    ]
    repo, conn = _repo_with(rows=rows)
    result = await repo.search("Ромаш", mode="substring", label="Person", limit=7)
    assert result == rows
    sql, params = conn.cur.executed[0]
    assert "name %% %s" in sql
    assert "label = %s" in sql
    assert params[-1] == 7
