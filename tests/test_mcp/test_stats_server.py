"""Helper-level tests — FastMCP tool invocation internals are not
exercised, same convention as the other MCP tests."""

from __future__ import annotations

import json

import pytest

from src.mcp.stats_server import _align_tool, _indicators_search, _series

_UNSET = object()

_DEFAULT_INDICATOR = {
    "value_kind": "share", "unit": "%", "granularity": "week",
    "title": "t", "source": "fom", "code": "c", "question_text": "",
    "dims_schema": {}, "entity_vid": None,
}


class _StubRepo:
    def __init__(self, rows=None, indicators=None, sources=None,
                 indicator=_UNSET):
        self.rows = rows or []
        self.indicators = indicators or []
        self.sources = sources or []
        # `indicator=None` simulates an id that does not exist.
        self.indicator = (
            _DEFAULT_INDICATOR if indicator is _UNSET else indicator
        )
        self.calls: list[tuple] = []

    async def search_indicators(self, query, *, source=None, limit=20):
        self.calls.append(("search", query, source, limit))
        return self.indicators

    async def list_sources(self):
        self.calls.append(("list_sources",))
        return self.sources

    async def list_indicators(self, *, source=None, limit=100):
        self.calls.append(("list_indicators", source, limit))
        return self.indicators

    async def series(self, indicator_id, *, since=None, until=None,
                     dims=None, revision=None):
        self.calls.append(("series", indicator_id, since, until, dims, revision))
        return self.rows

    async def get_indicator(self, indicator_id):
        if self.indicator is None:
            return None
        return {**self.indicator, "id": indicator_id}


async def test_series_rejects_bad_date():
    out = await _series(_StubRepo(), 1, "not-a-date", None, None)
    assert "error" in out
    assert "YYYY-MM-DD" in out["error"]


async def test_series_errors_on_an_unknown_indicator():
    """An unknown id must say so.  Returning an empty series instead
    would read as "this indicator has no data", which is a different
    and much more misleading answer."""
    repo = _StubRepo(rows=[{"period_start": "2026-01-05", "value": 1.0}],
                     indicator=None)
    out = await _series(repo, 999, None, None, None)
    assert out == {"error": "no indicator with id 999"}
    assert not any(c[0] == "series" for c in repo.calls)


async def test_series_returns_rows_with_indicator_metadata():
    repo = _StubRepo(rows=[{"period_start": "2026-01-05", "value": 57.5}])
    out = await _series(repo, 1, None, None, None)
    assert out["rows"] == [{"period_start": "2026-01-05", "value": 57.5}]
    assert out["indicator"]["unit"] == "%"


async def test_series_warns_when_the_result_spans_several_dims_cuts():
    """Three regional cuts on one period are three numbers, not one.

    Without the warning a caller hands them to `stat_align`, which
    averages the cuts inside the bucket and reports the mean as an exact
    value for the whole indicator.
    """
    repo = _StubRepo(rows=[
        {"period_start": "2026-01-05", "value": 10.0, "dims": {"region": "Москва"}},
        {"period_start": "2026-01-05", "value": 20.0, "dims": {"region": "СПб"}},
        {"period_start": "2026-01-05", "value": 60.0, "dims": {"region": "Урал"}},
    ])
    out = await _series(repo, 1, None, None, None)
    assert out["warnings"] == ["multiple_dims_cuts"]


async def test_series_does_not_warn_on_a_single_cut():
    repo = _StubRepo(rows=[
        {"period_start": "2026-01-05", "value": 10.0, "dims": {"region": "Москва"}},
        {"period_start": "2026-01-12", "value": 20.0, "dims": {"region": "Москва"}},
    ])
    out = await _series(repo, 1, None, None, None)
    assert out["warnings"] == []


async def test_series_does_not_warn_on_undimensioned_rows():
    repo = _StubRepo(rows=[
        {"period_start": "2026-01-05", "value": 10.0, "dims": {}},
        {"period_start": "2026-01-12", "value": 20.0, "dims": {}},
    ])
    out = await _series(repo, 1, None, None, None)
    assert out["warnings"] == []


async def test_series_dims_key_order_is_not_a_second_cut():
    """`jsonb` normalises key order, so two rows differing only in the
    order they were written are the SAME cut and must not warn."""
    repo = _StubRepo(rows=[
        {"period_start": "2026-01-05", "value": 1.0,
         "dims": {"region": "Москва", "age": "18-30"}},
        {"period_start": "2026-01-12", "value": 2.0,
         "dims": {"age": "18-30", "region": "Москва"}},
    ])
    out = await _series(repo, 1, None, None, None)
    assert out["warnings"] == []


async def test_indicators_search_without_query_returns_the_catalogue():
    """No query and no source means the caller does not yet know what
    exists — answer with the catalogue rather than an error, or the
    caller has to guess a search term to learn anything."""
    repo = _StubRepo(sources=[
        {"source": "fom", "indicators": 3,
         "earliest": "2026-01-05", "latest": "2026-06-01"},
    ])
    out = await _indicators_search(repo, None, None, 20)
    assert out["sources"] == repo.sources
    assert repo.calls == [("list_sources",)]
    assert "error" not in out


async def test_indicators_search_blank_query_is_treated_as_absent():
    repo = _StubRepo(sources=[])
    out = await _indicators_search(repo, "   ", None, 20)
    assert "sources" in out
    assert repo.calls == [("list_sources",)]


async def test_indicators_search_with_source_only_lists_that_source():
    repo = _StubRepo(indicators=[{"id": 1, "source": "fom"}])
    out = await _indicators_search(repo, None, "fom", 20)
    assert out["source"] == "fom"
    assert out["indicators"] == repo.indicators
    assert repo.calls == [("list_indicators", "fom", 20)]


async def test_indicators_search_with_query_runs_the_trigram_search():
    repo = _StubRepo(indicators=[{"id": 1}])
    out = await _indicators_search(repo, "тревожность", None, 20)
    assert out["query"] == "тревожность"
    assert repo.calls[0][0] == "search"


async def test_indicators_search_caps_limit():
    """`== 100`, not `<= 100`: the old assertion held for any value the
    cap might have produced, including 1 or 0, so a broken cap that
    returned nothing would have passed."""
    repo = _StubRepo(indicators=[])
    await _indicators_search(repo, "тревожность", None, 10_000)
    assert repo.calls[0] == ("search", "тревожность", None, 100)


async def test_indicators_search_caps_limit_on_the_list_branch_too():
    """The `source`-only branch takes a different path to the repo and
    was not covered at all."""
    repo = _StubRepo(indicators=[])
    await _indicators_search(repo, None, "fom", 10_000)
    assert repo.calls[0] == ("list_indicators", "fom", 100)


async def test_indicators_search_floors_a_nonsense_limit_at_one():
    """A zero or negative limit must not become "return nothing" —
    an empty result is read as "no such data"."""
    repo = _StubRepo(indicators=[])
    await _indicators_search(repo, "тревожность", None, 0)
    assert repo.calls[0] == ("search", "тревожность", None, 1)

    repo = _StubRepo(indicators=[])
    await _indicators_search(repo, None, "fom", -5)
    assert repo.calls[0] == ("list_indicators", "fom", 1)


async def test_indicators_search_passes_a_reasonable_limit_through():
    repo = _StubRepo(indicators=[])
    await _indicators_search(repo, "тревожность", None, 20)
    assert repo.calls[0] == ("search", "тревожность", None, 20)


def test_align_tool_is_json_safe_and_reports_warnings():
    a = [{"period_start": f"2026-01-{d:02d}", "value": float(d)}
         for d in (5, 12, 19, 26)]
    out = _align_tool(a, a, "week", "share", "share", 0)
    assert out["divergence"] == pytest.approx(0.0)
    assert all(isinstance(g, str) for g in out["grid"])
    assert "low_overlap:4<8" in out["warnings"]


def test_align_tool_rejects_malformed_points():
    out = _align_tool([{"value": 1.0}], [], "week", "share", "share", 0)
    assert "error" in out


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_align_tool_rejects_non_finite_values(bad: float):
    """`float("nan")` parses fine and then silently poisons the result:
    `divergence` comes back as `nan`, no warning is raised, and `nan` is
    not valid JSON so the payload cannot even be serialised."""
    a = [{"period_start": "2026-01-05", "value": bad},
         {"period_start": "2026-01-12", "value": 2.0}]
    out = _align_tool(a, a, "week", "share", "share", 0)
    assert "error" in out
    assert "series_a[0]" in out["error"]
    assert "finite" in out["error"]


def test_align_tool_output_is_strictly_json_serialisable():
    """`json.dumps(..., allow_nan=False)` is what a strict JSON encoder
    on the wire does; if it raises, the tool cannot answer at all."""
    a = [{"period_start": f"2026-01-{d:02d}", "value": float(d)}
         for d in (5, 12, 19, 26)]
    out = _align_tool(a, a, "week", "share", "share", 0)
    assert json.dumps(out, allow_nan=False)


def test_align_tool_rejects_unknown_granularity():
    out = _align_tool([], [], "fortnight", "share", "share", 0)
    assert "error" in out
