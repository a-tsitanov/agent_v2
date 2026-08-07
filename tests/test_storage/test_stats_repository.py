"""Query-builder tests are exact-string; row mapping is tested against a
stub connection.  Same split as `_stats_by` in MCP-2: the validation
lives in thin functions so it stays testable without a live Postgres."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import date

import pytest

from src.storage.stats import (
    StatsRepository,
    build_search_query,
    build_series_query,
)

# ── stubs ────────────────────────────────────────────────────────────


@dataclass
class _StubCursor:
    rows: list[dict]
    executed: list[tuple] = field(default_factory=list)

    async def execute(self, sql, params=None):
        self.executed.append((sql, params))

    async def executemany(self, sql, params_seq):
        self.executed.append((sql, list(params_seq)))

    async def fetchall(self):
        return self.rows

    async def fetchone(self):
        return self.rows[0] if self.rows else None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@dataclass
class _StubConn:
    cur: _StubCursor
    committed: int = 0

    def cursor(self, *a, **kw):
        return self.cur

    async def commit(self):
        self.committed += 1


def _repo_with(rows: list[dict]) -> tuple[StatsRepository, _StubConn]:
    conn = _StubConn(cur=_StubCursor(rows=rows))
    repo = StatsRepository()

    @asynccontextmanager
    async def _conn():
        yield conn

    repo._conn = _conn  # type: ignore[method-assign]
    return repo, conn


# ── query builders ───────────────────────────────────────────────────


def test_series_query_defaults_to_latest_revision():
    sql, params = build_series_query(7, None, None, None, None)
    assert "DISTINCT ON (period_start, dims)" in sql
    assert "ORDER BY period_start, dims, revision DESC" in sql
    assert params == [7]


def test_series_query_pins_an_explicit_revision():
    sql, params = build_series_query(7, None, None, None, 2)
    assert "revision = %s" in sql
    assert params == [7, 2]


def test_series_query_applies_date_bounds_and_dims():
    sql, params = build_series_query(
        7, date(2026, 1, 1), date(2026, 6, 1), {"region": "Москва"}, None,
    )
    assert "period_start >= %s" in sql
    assert "period_start <= %s" in sql
    assert "dims @> %s" in sql
    assert params[0] == 7
    assert date(2026, 1, 1) in params
    assert date(2026, 6, 1) in params


def test_search_query_uses_trigram_similarity_and_caps_limit():
    sql, params = build_search_query("тревожность", None, 20)
    assert "similarity(" in sql
    assert "ORDER BY score DESC" in sql
    assert params[-1] == 20


def test_search_query_filters_by_source():
    sql, params = build_search_query("тревожность", "fom", 5)
    assert "source = %s" in sql
    assert "fom" in params


# ── repository behaviour ─────────────────────────────────────────────


async def test_series_normalises_rows():
    repo, _ = _repo_with([
        {"period_start": date(2026, 1, 5), "period_end": date(2026, 1, 11),
         "dims": {}, "value": 57.5, "sample_n": 1500, "revision": 0,
         "source_doc_id": None},
    ])
    rows = await repo.series(7)
    assert rows == [{
        "period_start": "2026-01-05", "period_end": "2026-01-11",
        "dims": {}, "value": 57.5, "sample_n": 1500, "revision": 0,
        "source_doc_id": None,
    }]


async def test_series_rejects_bad_dims_type():
    repo, _ = _repo_with([])
    with pytest.raises(ValueError, match="dims"):
        await repo.series(7, dims=["region"])  # type: ignore[arg-type]


async def test_upsert_observations_commits_once():
    repo, conn = _repo_with([])
    await repo.upsert_observations([
        {"indicator_id": 7, "period_start": date(2026, 1, 5),
         "period_end": date(2026, 1, 11), "dims": {}, "value": 57.5,
         "sample_n": 1500, "revision": 0, "source_doc_id": None},
    ])
    assert conn.committed == 1
    sql, _ = conn.cur.executed[0]
    assert "ON CONFLICT (indicator_id, period_start, dims, revision)" in sql
    assert "DO UPDATE" in sql
