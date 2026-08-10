"""MCP-3: exact-statistics tools.

Returns data, never prose.  No LLM call exists anywhere in this
server's path, and it reads neither the graph nor Milvus — exact
numbers have a different contract from the semantic contour, where a
worse answer is still an acceptable answer.

The comparison tool takes BOTH series as arguments and reads nothing:
the client fetches the channel-side series from MCP-2 and the
indicator series from here, then hands both over.  That keeps the
boundary clean and keeps the arithmetic away from the model.

Run::

    uv run python -m src.mcp.stats_server --transport stdio
    uv run python -m src.mcp.stats_server --transport http --port 9003
"""

from __future__ import annotations

import json
from datetime import date
from math import isfinite
from typing import Any

from fastmcp import FastMCP

from src.config import settings
from src.mcp._shared import (
    assert_api_key_env_set,
    build_sse_auth,
    log_banner,
    parse_args,
)
from src.stats.align import GRANULARITIES, VALUE_KINDS, align
from src.storage.stats import StatsRepository

mcp = FastMCP(
    name="kb-llamaindex-stats",
    instructions=(
        "Exact external statistics (polls, official series).  Every tool "
        "returns data, never a written answer.  "
        "START by calling stat_indicators_search with NO arguments — it "
        "returns the catalogue of sources and what each covers.  Do not "
        "guess a search term before you have seen it: matching is "
        "trigram-based, so a wrong guess returns nothing and looks "
        "exactly like the data not existing.  "
        "Then stat_series for one indicator's values, and stat_align to "
        "compare it against a channel-side series fetched from the MCP-2 "
        "server (topic_trend / polarity_evolution).  stat_align is the "
        "arithmetic — do not compute gaps, correlations or lags yourself."
    ),
    auth=build_sse_auth(),
)

_MAX_SEARCH_LIMIT = 100


def _repo() -> StatsRepository:
    return StatsRepository()


def _parse_date(value: str | None, field: str) -> tuple[date | None, str | None]:
    if value is None:
        return None, None
    try:
        return date.fromisoformat(value), None
    except ValueError:
        return None, f"{field} must be ISO YYYY-MM-DD, got {value!r}"


def _points(raw: list[dict[str, Any]], label: str) -> tuple[list, str | None]:
    out: list[tuple[date, float]] = []
    for i, p in enumerate(raw):
        if not isinstance(p, dict) or "period_start" not in p or "value" not in p:
            return [], (
                f"{label}[{i}] must be an object with 'period_start' and 'value'"
            )
        try:
            d = date.fromisoformat(str(p["period_start"]))
            v = float(p["value"])
        except (ValueError, TypeError) as exc:
            return [], f"{label}[{i}] is malformed: {exc}"
        # NaN/Infinity parse as floats and then propagate silently: one of
        # them makes `divergence` NaN with no warning, and NaN is not valid
        # JSON, so the answer could not be returned even if it meant
        # anything.  Refuse the input instead of returning a broken number.
        if not isfinite(v):
            return [], f"{label}[{i}] value must be a finite number, got {v!r}"
        out.append((d, v))
    return out, None


async def _indicators_search(
    repo: Any, query: str | None, source: str | None, limit: int,
) -> dict[str, Any]:
    """Three modes, deliberately behind one tool.

    No query and no source is not an error — it is a caller that does
    not yet know what exists.  Answering it with the catalogue is the
    only way such a caller can learn anything: trigram matching finds
    spelling variants but not synonyms, so a wrong guess is
    indistinguishable from "no such data" and would be reported as the
    statistic not existing.
    """
    capped = max(1, min(int(limit), _MAX_SEARCH_LIMIT))
    blank = not query or not query.strip()
    if blank and source is None:
        return {"sources": await repo.list_sources()}
    if blank:
        rows = await repo.list_indicators(source=source, limit=capped)
        return {"source": source, "indicators": rows}
    rows = await repo.search_indicators(
        query.strip(), source=source, limit=capped,
    )
    return {"query": query.strip(), "source": source, "indicators": rows}


def _dims_cuts(rows: list[dict[str, Any]]) -> int:
    """How many distinct `dims` values the rows span.

    Serialised with sorted keys because `jsonb` normalises key order:
    two rows written `{a,b}` and `{b,a}` are the SAME cut, and counting
    them as two would raise a warning nobody can act on.
    """
    return len({
        json.dumps(r.get("dims") or {}, sort_keys=True) for r in rows
    })


async def _series(
    repo: Any,
    indicator_id: int,
    since: str | None,
    until: str | None,
    dims: dict[str, Any] | None,
) -> dict[str, Any]:
    s, err = _parse_date(since, "since")
    if err:
        return {"error": err}
    u, err = _parse_date(until, "until")
    if err:
        return {"error": err}
    if dims is not None and not isinstance(dims, dict):
        return {"error": "dims must be an object mapping name → value"}
    indicator = await repo.get_indicator(indicator_id)
    if indicator is None:
        return {"error": f"no indicator with id {indicator_id}"}
    rows = await repo.series(indicator_id, since=s, until=u, dims=dims)
    # Several cuts mean several numbers per period, and `stat_align`
    # averages within a bucket — so an unnarrowed panel would be reported
    # as one exact value for the whole indicator.  Say so instead.
    warnings = ["multiple_dims_cuts"] if _dims_cuts(rows) > 1 else []
    return {"indicator": indicator, "rows": rows, "warnings": warnings}


def _align_tool(
    series_a: list[dict[str, Any]],
    series_b: list[dict[str, Any]],
    granularity: str,
    value_kind_a: str,
    value_kind_b: str,
    max_lag: int,
) -> dict[str, Any]:
    if granularity not in GRANULARITIES:
        return {"error": f"granularity must be one of {sorted(GRANULARITIES)}"}
    for name, kind in (("value_kind_a", value_kind_a), ("value_kind_b", value_kind_b)):
        if kind not in VALUE_KINDS:
            return {"error": f"{name} must be one of {sorted(VALUE_KINDS)}"}
    if max_lag < 0:
        return {"error": "max_lag must be >= 0"}
    pa, err = _points(series_a, "series_a")
    if err:
        return {"error": err}
    pb, err = _points(series_b, "series_b")
    if err:
        return {"error": err}
    res = align(
        pa, pb, granularity=granularity,
        value_kind_a=value_kind_a, value_kind_b=value_kind_b,
        max_lag=max_lag, min_overlap=settings.stats.min_overlap,
    )
    return {
        "grid": [d.isoformat() for d in res.grid],
        "a": res.a, "b": res.b,
        "a_norm": res.a_norm, "b_norm": res.b_norm,
        "gap": res.gap,
        "divergence": res.divergence,
        "best_lag": res.best_lag,
        "correlation": res.correlation,
        "warnings": res.warnings,
    }


@mcp.tool(timeout=120)
async def stat_indicators_search(
    query: str | None = None, source: str | None = None, limit: int = 20,
) -> dict[str, Any]:
    """Discover what external statistics exist, then narrow to one indicator.

    CALL THIS FIRST, WITH NO ARGUMENTS — you get the catalogue: every
    source, how many indicators it holds, and the period it covers.
    Then either pass `source` to list that provider's indicators, or
    pass `query` to search by name and poll wording.

    Do not guess a `query` before you have seen the catalogue.  Matching
    is trigram-based: it finds spelling variants but NOT synonyms, so a
    wrong guess returns nothing and is indistinguishable from the data
    not existing.

    Every indicator carries `unit`, `value_kind` and `granularity` —
    that is what tells you whether two series are comparable at all, and
    it is what `stat_align` needs.
    NOT FOR: values (use `stat_series`) or document text (use MCP-2
    `vector_search`)."""
    return await _indicators_search(_repo(), query, source, limit)


@mcp.tool(timeout=120)
async def stat_series(
    indicator_id: int,
    since: str | None = None,
    until: str | None = None,
    dims: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Values of one indicator over time, newest revision per period.

    USE FOR: "как менялась тревожность", fetching the poll side before
    a comparison.  `since` / `until` are ISO `YYYY-MM-DD` bounds on
    `period_start`.  Each row carries `source_doc_id` — the ingested
    bulletin the number came from, so a claim can be traced back.

    `dims` picks a panel cut: `{"region": "Москва"}` matches every row
    carrying that region, and `{}` means specifically the rows with NO
    dimensions at all.  Omitting `dims` returns EVERY cut, which for a
    dimensioned indicator is several numbers per period, not a series —
    then `warnings` contains `multiple_dims_cuts` and you must narrow
    `dims` before aligning.  READ `warnings`.
    NEXT STEP: to compare against channel attention, fetch a series
    with MCP-2 `topic_trend` and pass both to `stat_align`."""
    return await _series(_repo(), indicator_id, since, until, dims)


@mcp.tool(timeout=120)
async def stat_align(
    series_a: list[dict[str, Any]],
    series_b: list[dict[str, Any]],
    granularity: str = "week",
    value_kind_a: str = "share",
    value_kind_b: str = "share",
    max_lag: int = 4,
) -> dict[str, Any]:
    """Put two series on a common grid and measure how far apart they run.

    USE FOR: comparing channel attention against a poll indicator.
    Each series is a list of `{"period_start": "YYYY-MM-DD", "value":
    <number>}`.  Both are resampled DOWN to `granularity` (never
    interpolated up), z-scored so different units are comparable, and
    correlated across shifts in `[-max_lag, +max_lag]`; the best-fitting
    shift is reported as `best_lag`.  Returns per-period `gap`, a scalar
    `divergence` (mean absolute gap), and `warnings` — read them:
    `sparse:*` means one side had missing buckets, `low_overlap:*` means
    too few common periods for the correlation to mean anything.
    Do NOT compute these numbers yourself; this tool is the arithmetic."""
    return _align_tool(
        series_a, series_b, granularity, value_kind_a, value_kind_b, max_lag,
    )


def main() -> None:
    args = parse_args()
    assert_api_key_env_set()
    log_banner(
        "kb-llamaindex-stats",
        transport=args["transport"], host=args["host"], port=args["port"],
    )
    if args["transport"] == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp.run(transport="http", host=args["host"], port=args["port"])


if __name__ == "__main__":
    main()
