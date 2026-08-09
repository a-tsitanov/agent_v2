"""Helper-level tests — FastMCP tool invocation internals are not
exercised, same convention as the other MCP tests."""

from __future__ import annotations

import pytest

from src.mcp.stats_server import _align_tool, _indicators_search, _series


class _StubRepo:
    def __init__(self, rows=None, indicators=None, sources=None):
        self.rows = rows or []
        self.indicators = indicators or []
        self.sources = sources or []
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
        return {"id": indicator_id, "value_kind": "share", "unit": "%",
                "granularity": "week", "title": "t", "source": "fom",
                "code": "c", "question_text": "", "dims_schema": {},
                "entity_vid": None}


async def test_series_rejects_bad_date():
    out = await _series(_StubRepo(), 1, "not-a-date", None, None)
    assert "error" in out
    assert "YYYY-MM-DD" in out["error"]


async def test_series_returns_rows_with_indicator_metadata():
    repo = _StubRepo(rows=[{"period_start": "2026-01-05", "value": 57.5}])
    out = await _series(repo, 1, None, None, None)
    assert out["rows"] == [{"period_start": "2026-01-05", "value": 57.5}]
    assert out["indicator"]["unit"] == "%"


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
    repo = _StubRepo(indicators=[])
    await _indicators_search(repo, "тревожность", None, 10_000)
    assert repo.calls[0][3] <= 100


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


def test_align_tool_rejects_unknown_granularity():
    out = _align_tool([], [], "fortnight", "share", "share", 0)
    assert "error" in out
