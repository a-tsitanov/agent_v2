"""P2 — composite, decision-ready signals & queues (read materialized scores + compose Wave-0 primitives)."""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n
from src.graph.signals_graph_ops import build_signals_graph_ops


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class RiskScoreParams(_Params):
    name: str | None = None
    band: str | None = None
    top_n: int = 20


async def risk_score(
    store: Any | None,
    *,
    name: str | None = None,
    band: str | None = None,
    top_n: int = 20,
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    cypher = "signals_graph_ops.risk_score"
    params = {"name": name, "band": band, "top_n": top_n}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(build_signals_graph_ops(store).risk_score, name, band, top_n)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


register(
    Primitive(
        "risk_score",
        risk_score,
        RiskScoreParams,
        "Per-entity composite risk_score + band (low/medium/high) with firing components — "
        "a transparent triage heuristic, not ground truth. Reads materialized scores.",
        tier="offline-mat",
    )
)


class InvestigateNextParams(_Params):
    top_n: int = 20


async def investigate_next(
    store: Any | None,
    *,
    top_n: int = 20,
) -> PrimitiveResult:
    """High risk_score × low completeness — who deserves attention and is under-documented."""
    top_n = clamp_top_n(top_n)
    cypher = "signals_graph_ops.investigate_next"
    params = {"top_n": top_n}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(build_signals_graph_ops(store).investigate_next, top_n)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


register(
    Primitive(
        "investigate_next",
        investigate_next,
        InvestigateNextParams,
        "Ranked lead list: entities ranked by risk_score (and completeness_score once materialized"
        " — currently risk-dominant) — who deserves attention and is under-documented."
        " Reads materialized scores.",
        tier="offline-mat",
    )
)


class RecommendedMergesParams(_Params):
    top_n: int = 50


async def recommended_merges(
    store: Any | None,
    *,
    top_n: int = 50,
) -> PrimitiveResult:
    """Duplicate-display-name groups — a recommended-merge queue."""
    top_n = clamp_top_n(top_n, default=50)
    cypher = "signals_graph_ops.recommended_merges"
    params = {"top_n": top_n}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(build_signals_graph_ops(store).recommended_merges, top_n)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


register(
    Primitive(
        "recommended_merges",
        recommended_merges,
        RecommendedMergesParams,
        "Ranked duplicate-display-name groups — a recommended-merge queue.",
    )
)


class ReviewQueueParams(_Params):
    top_n: int = 50


async def review_queue(
    store: Any | None,
    *,
    top_n: int = 50,
) -> PrimitiveResult:
    """Shell-signal organizations (only identifier links) — the cheapest structural red flag for the queue."""
    top_n = clamp_top_n(top_n, default=50)
    cypher = "signals_graph_ops.review_queue"
    params = {"top_n": top_n}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(build_signals_graph_ops(store).review_queue, top_n)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


register(
    Primitive(
        "review_queue",
        review_queue,
        ReviewQueueParams,
        "Structural red-flag queue: organizations whose only links are identifiers (shell signal).",
    )
)


class CircularOwnershipParams(_Params):
    top_n: int = 20


async def circular_ownership(
    store: Any | None,
    *,
    top_n: int = 20,
) -> PrimitiveResult:
    """Ownership cycles (A owns … owns A) — a circular-ownership red flag."""
    top_n = clamp_top_n(top_n)
    cypher = "signals_graph_ops.circular_ownership"
    params = {"top_n": top_n}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(build_signals_graph_ops(store).circular_ownership, top_n)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


register(
    Primitive(
        "circular_ownership",
        circular_ownership,
        CircularOwnershipParams,
        "Ownership cycles (A owns … owns A) — a circular-ownership red flag.",
    )
)
