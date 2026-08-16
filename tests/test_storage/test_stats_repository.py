"""Query-builder tests are exact-string; row mapping is tested against a
stub connection.  Same split as `_stats_by` in MCP-2: the validation
lives in thin functions so it stays testable without a live Postgres."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import date

import pytest
from psycopg.rows import dict_row

from src.storage.stats import (
    StatsRepository,
    build_indicators_query,
    build_search_query,
    build_series_query,
    build_sources_query,
)

# ── stubs ────────────────────────────────────────────────────────────


@dataclass
class _StubCursor:
    rows: list[dict]
    row_factory: object = None
    executed: list[tuple] = field(default_factory=list)

    async def execute(self, sql, params=None):
        self.executed.append((sql, params))

    async def executemany(self, sql, params_seq):
        self.executed.append((sql, list(params_seq)))

    def _shaped(self, row):
        # Honour `row_factory` like the real pool does: dict rows only
        # when `dict_row` was actually asked for, tuples otherwise — a
        # cursor opened without it on live psycopg3 gets plain tuples,
        # so a read that forgets `row_factory=dict_row` must not
        # silently keep working here either.
        if self.row_factory is dict_row:
            return row
        return tuple(row.values()) if isinstance(row, dict) else row

    async def fetchall(self):
        return [self._shaped(r) for r in self.rows]

    async def fetchone(self):
        return self._shaped(self.rows[0]) if self.rows else None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@dataclass
class _StubConn:
    cur: _StubCursor
    committed: int = 0

    def cursor(self, *a, **kw):
        self.cur.row_factory = kw.get("row_factory")
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


async def test_forgetting_dict_row_must_not_silently_work():
    """Pins the stub's honesty.  On live psycopg3 the pool sets no row
    factory by default, so a cursor opened without `row_factory=dict_row`
    gets plain tuples back and `row["score"]` raises `TypeError`.  Before
    this fix `_StubConn.cursor()` swallowed kwargs and always handed back
    dict rows regardless of what was asked for, so deleting
    `row_factory=dict_row` from a real read in `src/storage/stats.py`
    broke nothing here — it would only blow up in production.
    """
    conn = _StubConn(cur=_StubCursor(rows=[{"id": 1, "score": 0.5}]))

    async with conn.cursor() as cur:  # no row_factory — the bug this pins
        await cur.execute("SELECT id, score FROM stat_indicator")
        rows = await cur.fetchall()
    assert rows == [(1, 0.5)]
    with pytest.raises(TypeError):
        _ = rows[0]["score"]

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT id, score FROM stat_indicator")
        rows = await cur.fetchall()
    assert rows == [{"id": 1, "score": 0.5}]


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


def test_series_query_empty_dims_means_strictly_undimensioned():
    """`dims={}` is "the rows carrying NO dimensions", not "any row".

    Containment (`@>`) with an empty object matches EVERY row, so an
    explicit empty cut used to be no filter at all: DISTINCT ON returned
    one row per regional cut and `resample` averaged them into a single
    number presented as exact.
    """
    sql, params = build_series_query(7, None, None, {}, None)
    assert "dims = %s" in sql
    assert "dims @> %s" not in sql
    assert params == [7, "{}"]


def test_series_query_omits_the_dims_filter_only_when_dims_is_none():
    sql, params = build_series_query(7, None, None, None, None)
    assert "dims = %s" not in sql
    assert "dims @> %s" not in sql
    assert params == [7]


def test_series_query_non_empty_dims_still_uses_containment():
    """A partial cut must stay a containment match — asking for
    `{"region": "Москва"}` should not require naming every other
    dimension the indicator happens to carry."""
    sql, params = build_series_query(7, None, None, {"region": "Москва"}, None)
    assert "dims @> %s" in sql
    assert "dims = %s" not in sql
    assert params[-1] == json.dumps({"region": "Москва"})


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


# ── registry discovery ───────────────────────────────────────────────


def test_sources_query_rolls_up_indicator_count_and_period_bounds():
    sql, params = build_sources_query()
    assert "LEFT JOIN stat_observation" in sql
    assert "count(DISTINCT i.id) AS indicators" in sql
    assert "min(o.period_start) AS earliest" in sql
    assert "max(o.period_start) AS latest" in sql
    assert "GROUP BY i.source" in sql
    assert params == []


def test_indicators_query_without_source_has_no_where_clause():
    sql, params = build_indicators_query(None, 100)
    assert "WHERE" not in sql
    assert params == [100]


def test_indicators_query_filters_by_source():
    sql, params = build_indicators_query("fom", 50)
    assert "WHERE source = %s" in sql
    assert params == ["fom", 50]


async def test_list_sources_isoformats_period_bounds():
    repo, _ = _repo_with([
        {"source": "fom", "indicators": 3,
         "earliest": date(2026, 1, 5), "latest": date(2026, 6, 1)},
    ])
    assert await repo.list_sources() == [
        {"source": "fom", "indicators": 3,
         "earliest": "2026-01-05", "latest": "2026-06-01"},
    ]


async def test_list_sources_survives_a_source_with_no_observations():
    """A registered indicator with no rows yet must still be listed —
    otherwise a freshly seeded source looks like it does not exist."""
    repo, _ = _repo_with([
        {"source": "rosstat", "indicators": 1, "earliest": None, "latest": None},
    ])
    assert await repo.list_sources() == [
        {"source": "rosstat", "indicators": 1, "earliest": None, "latest": None},
    ]


async def test_list_indicators_returns_the_registry_rows():
    """`build_indicators_query` was covered as a pure builder, but nothing
    ever called `StatsRepository.list_indicators()` end to end — so the
    honest stub had no way to notice a `row_factory=dict_row` regression
    here: unlike `list_sources`/`series`, this method does no internal
    `r["..."]` access to crash on; the only thing that catches a dropped
    `row_factory` is a test asserting the dict-shaped return value, which
    did not exist for this read. Mirrors
    `test_list_sources_isoformats_period_bounds`.
    """
    repo, _ = _repo_with([
        {"id": 1, "source": "fom", "code": "anxiety", "title": "Тревожность",
         "question_text": "", "unit": "%", "value_kind": "share",
         "granularity": "week", "dims_schema": {}, "entity_vid": None},
    ])
    assert await repo.list_indicators() == [
        {"id": 1, "source": "fom", "code": "anxiety", "title": "Тревожность",
         "question_text": "", "unit": "%", "value_kind": "share",
         "granularity": "week", "dims_schema": {}, "entity_vid": None},
    ]


async def test_upsert_indicator_rejects_unknown_value_kind():
    repo, conn = _repo_with([])
    with pytest.raises(ValueError, match="value_kind"):
        await repo.upsert_indicator(
            source="fom", code="x", title="T", unit="%",
            value_kind="ratio", granularity="week",
        )
    assert conn.cur.executed == []


async def test_upsert_indicator_rejects_unknown_granularity():
    repo, conn = _repo_with([])
    with pytest.raises(ValueError, match="granularity"):
        await repo.upsert_indicator(
            source="fom", code="x", title="T", unit="%",
            value_kind="share", granularity="fortnight",
        )
    assert conn.cur.executed == []


async def test_search_indicators_casts_score_to_float():
    repo, _ = _repo_with([
        {"id": 1, "source": "fom", "code": "anxiety", "title": "Тревожность",
         "question_text": "", "unit": "%", "value_kind": "share",
         "granularity": "week", "dims_schema": {}, "entity_vid": None,
         "score": 1},
    ])
    rows = await repo.search_indicators("тревожность")
    assert rows[0]["score"] == 1.0
    assert isinstance(rows[0]["score"], float)


async def test_get_indicator_returns_none_when_absent():
    repo, _ = _repo_with([])
    assert await repo.get_indicator(999) is None


async def test_get_indicator_returns_the_row_as_a_dict():
    """`get_indicator` has no builder function (inline SQL) and does no
    internal `r["..."]` access — it just returns `await cur.fetchone()`
    unprocessed, same shape as `list_indicators` before it got its own
    end-to-end test.  The absent-id test above proves nothing about
    `row_factory`: with no row to fetch, `fetchone()` returns `None`
    whether or not `dict_row` was asked for.  This one exercises an
    actual populated row instead.
    """
    repo, _ = _repo_with([
        {"id": 7, "source": "fom", "code": "anxiety", "title": "Тревожность",
         "question_text": "", "unit": "%", "value_kind": "share",
         "granularity": "week", "dims_schema": {}, "entity_vid": None},
    ])
    assert await repo.get_indicator(7) == {
        "id": 7, "source": "fom", "code": "anxiety", "title": "Тревожность",
        "question_text": "", "unit": "%", "value_kind": "share",
        "granularity": "week", "dims_schema": {}, "entity_vid": None,
    }


def test_trigram_operator_survives_psycopg_placeholder_expansion():
    """`%` is psycopg's placeholder marker, so the trigram operator has
    to be written `%%` — and that escaping is invisible until runtime.

    Asserted through psycopg's OWN converter rather than by eyeballing
    the string: it is the component that collapses `%%` to `%` and
    numbers the real placeholders, so this pins the query as actually
    sent.  Writing a single `%` instead passes every other test in this
    file and fails only against a live database.
    """
    from psycopg._queries import PostgresQuery
    from psycopg.adapt import Transformer

    sql, params = build_search_query("тревожность", None, 20)
    assert "title %% %s" in sql
    assert "question_text %% %s" in sql

    q = PostgresQuery(Transformer())
    q.convert(sql, params)
    sent = q.query.decode()
    # Collapsed to the single-character trigram operator...
    assert "(title % $3 OR question_text % $4)" in sent
    # ...and no doubled `%` survived into the wire query.
    assert "%%" not in sent
    # Exactly five real placeholders: two similarity() scores, two match
    # terms, LIMIT.  A `%` miscounted as one would shift the numbering.
    assert all(f"${i}" in sent for i in range(1, 6))
    assert "$6" not in sent


def test_search_query_single_percent_would_not_survive_conversion():
    """The counter-proof: with one `%` psycopg reads `% %s` as a broken
    placeholder, which is why the escape cannot be dropped."""
    import psycopg
    from psycopg._queries import PostgresQuery
    from psycopg.adapt import Transformer

    sql, params = build_search_query("тревожность", None, 20)
    broken = sql.replace("%%", "%")
    q = PostgresQuery(Transformer())
    with pytest.raises(psycopg.ProgrammingError, match="incomplete placeholder"):
        q.convert(broken, params)


async def test_upsert_indicator_returns_the_id_and_commits():
    """The success path: neither the RETURNING handling nor the commit
    was executed by any test, so an id read from the wrong column would
    not have been noticed."""
    repo, conn = _repo_with([{"id": 7}])
    got = await repo.upsert_indicator(
        source="fom", code="anxiety", title="Тревожность", unit="%",
        value_kind="share", granularity="week",
        question_text="Какое настроение преобладает?",
        dims_schema={"region": "str"}, entity_vid="ent-1",
    )
    assert got == 7
    assert conn.committed == 1

    sql, sent = conn.cur.executed[0]
    assert "INSERT INTO stat_indicator" in sql
    assert "ON CONFLICT (source, code) DO UPDATE SET" in sql
    assert "RETURNING id" in sql
    # Column order is positional here; a swapped pair would be silently
    # written to the wrong column.
    assert sent == (
        "fom", "anxiety", "Тревожность", "Какое настроение преобладает?",
        "%", "share", "week", json.dumps({"region": "str"}), "ent-1",
    )


async def test_upsert_indicator_reads_the_id_from_a_tuple_row_too():
    """`_conn()` yields a pooled connection whose default row factory is
    NOT dict_row, so the RETURNING row can arrive as a plain tuple."""
    repo, conn = _repo_with([(11,)])
    assert await repo.upsert_indicator(
        source="rosstat", code="cpi", title="ИПЦ", unit="index",
        value_kind="index", granularity="month",
    ) == 11
    assert conn.committed == 1


async def test_upsert_indicator_defaults_question_text_and_dims_schema():
    repo, conn = _repo_with([{"id": 3}])
    await repo.upsert_indicator(
        source="fom", code="x", title="T", unit="%",
        value_kind="share", granularity="week",
    )
    _, sent = conn.cur.executed[0]
    assert sent[3] == ""          # question_text
    assert sent[7] == "{}"        # dims_schema
    assert sent[8] is None        # entity_vid
