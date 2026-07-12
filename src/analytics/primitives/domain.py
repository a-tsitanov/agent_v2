"""P3 domain rollups — issue/resolution + communication intensity primitives."""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n
from src.graph.domain_graph_ops import build_domain_graph_ops


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


# ── issue_resolution_stats ────────────────────────────────────────────────────


class IssueResolutionStatsParams(_Params):
    pass


async def issue_resolution_stats(store: Any | None) -> PrimitiveResult:
    cypher = "domain_graph_ops.issue_resolution_stats"
    rows = (
        await asyncio.to_thread(build_domain_graph_ops(store).issue_resolution_stats)
        if store is not None
        else []
    )
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
    return PrimitiveResult(cypher=cypher, params={}, rows=out)


# ── communication_stats ───────────────────────────────────────────────────────

# NOTE: spans both RESPONDED_TO (Person→Person) and CONTACT (Person→Phone/Email)
# per the Wave-2 plan, so "pairs" include person↔contact-method rows, not only
# person↔person. The `rel` column lets callers separate the two. To restrict to
# person-to-person communication, drop CONTACT and/or label-constrain b.


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
    cypher = "domain_graph_ops.communication_stats"
    params = {"name": name, "top_n": top_n}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(build_domain_graph_ops(store).communication_stats, name, top_n)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


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
