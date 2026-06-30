"""P3 domain rollups — issue/resolution + communication intensity primitives."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n
from src.analytics.store_query import run_rows


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


# ── issue_resolution_stats ────────────────────────────────────────────────────

_ISSUE_STATS = (
    "MATCH (i:__Entity__:Issue) "
    "OPTIONAL MATCH (i)-[rr:RESOLVED_BY]-(r:__Entity__:Resolution) "
    "WHERE rr.polarity IS NULL OR rr.polarity <> 'negated' "
    "WITH i, count(r) AS res "
    "RETURN count(i) AS total, sum(CASE WHEN res = 0 THEN 1 ELSE 0 END) AS unresolved"
)


class IssueResolutionStatsParams(_Params):
    pass


async def issue_resolution_stats(store: Any | None) -> PrimitiveResult:
    rows = await run_rows(store, _ISSUE_STATS, {})
    agg = rows[0] if rows else {}
    total = int(agg.get("total", 0) or 0)
    unresolved = int(agg.get("unresolved", 0) or 0)
    resolved = total - unresolved
    rate = round(resolved / total, 4) if total else 0.0
    out = [
        {
            "total_issues": total,
            "resolved": resolved,
            "unresolved": unresolved,
            "resolution_rate": rate,
        }
    ]
    return PrimitiveResult(cypher=_ISSUE_STATS, params={}, rows=out)


# ── communication_stats ───────────────────────────────────────────────────────

# NOTE: spans both RESPONDED_TO (Person→Person) and CONTACT (Person→Phone/Email)
# per the Wave-2 plan, so "pairs" include person↔contact-method rows, not only
# person↔person. The `rel` column lets callers separate the two. To restrict to
# person-to-person communication, drop CONTACT and/or label-constrain b.
_COMMS = (
    "MATCH (a:__Entity__)-[r:CONTACT|RESPONDED_TO]-(b:__Entity__) "
    "WHERE a.name < b.name "
    "AND ($name IS NULL OR a.name = $name OR b.name = $name) "
    "AND (r.polarity IS NULL OR r.polarity <> 'negated') "
    "RETURN a.name AS a, b.name AS b, type(r) AS rel, count(*) AS interactions "
    "ORDER BY interactions DESC LIMIT $top_n"
)


class CommunicationStatsParams(_Params):
    name: str | None = None
    top_n: int = 20


async def communication_stats(
    store: Any | None,
    *,
    name: str | None = None,
    top_n: int = 20,
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=20)
    params = {"name": name, "top_n": top_n}
    return PrimitiveResult(
        cypher=_COMMS,
        params=params,
        rows=await run_rows(store, _COMMS, params),
    )


register(
    Primitive(
        "issue_resolution_stats",
        issue_resolution_stats,
        IssueResolutionStatsParams,
        "Issue/Resolution rollup: total, resolved, unresolved, resolution rate (RESOLVED_BY).",
    )
)
register(
    Primitive(
        "communication_stats",
        communication_stats,
        CommunicationStatsParams,
        "Who-talks-to-whom intensity over CONTACT/RESPONDED_TO, count per pair, busiest first.",
    )
)
