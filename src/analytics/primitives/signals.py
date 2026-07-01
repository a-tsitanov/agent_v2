"""P2 — composite, decision-ready signals & queues (read materialized scores + compose Wave-0 primitives)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import ID_TYPES, clamp_top_n
from src.analytics.store_query import run_rows


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
    cypher = (
        "MATCH (e:__Entity__) WHERE e.risk_score IS NOT NULL "
        "AND ($name IS NULL OR e.name=$name) AND ($band IS NULL OR e.risk_band=$band) "
        "RETURN e.name AS name, e.risk_score AS score, e.risk_band AS band, "
        "e.risk_components AS components ORDER BY e.risk_score DESC LIMIT $top_n"
    )
    params = {"name": name, "band": band, "top_n": top_n}
    return PrimitiveResult(
        cypher=cypher,
        params=params,
        rows=await run_rows(store, cypher, params),
    )


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
    cypher = (
        "MATCH (e:__Entity__) WHERE e.risk_score IS NOT NULL "
        "RETURN e.name AS name, e.risk_score AS risk, "
        "coalesce(e.completeness_score, 0.0) AS completeness "
        "ORDER BY e.risk_score DESC, coalesce(e.completeness_score, 0.0) ASC LIMIT $top_n"
    )
    params = {"top_n": top_n}
    return PrimitiveResult(
        cypher=cypher,
        params=params,
        rows=await run_rows(store, cypher, params),
    )


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
    cypher = (
        "MATCH (e:__Entity__) WHERE NONE(l IN labels(e) WHERE l IN $id_types) "
        "WITH toLower(trim(e.name)) AS key, collect(e.name) AS names, count(e) AS count "
        "WHERE count > 1 RETURN key, names, count ORDER BY count DESC LIMIT $top_n"
    )
    params = {"top_n": top_n, "id_types": ID_TYPES}
    return PrimitiveResult(
        cypher=cypher,
        params=params,
        rows=await run_rows(store, cypher, params),
    )


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
    cypher = (
        "MATCH (e:__Entity__:Organization) "
        "OPTIONAL MATCH (e)-[]-(n:__Entity__) "
        "WITH e, count(n) AS deg, "
        "sum(CASE WHEN any(l IN labels(n) WHERE l IN $id_types) THEN 1 ELSE 0 END) "
        "AS id_links "
        "WHERE deg > 0 AND deg = id_links "
        "RETURN e.name AS name, deg AS degree, 'shell_signal' AS flag ORDER BY deg DESC LIMIT $top_n"
    )
    params = {"top_n": top_n, "id_types": ID_TYPES}
    return PrimitiveResult(
        cypher=cypher,
        params=params,
        rows=await run_rows(store, cypher, params),
    )


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
    cypher = (
        "MATCH p=(a:__Entity__)-[:OWNS*2..6]->(a) "
        "RETURN [n IN nodes(p) | n.name] AS cycle ORDER BY size(cycle) DESC LIMIT $top_n"
    )
    params = {"top_n": top_n}
    return PrimitiveResult(
        cypher=cypher,
        params=params,
        rows=await run_rows(store, cypher, params),
    )


register(
    Primitive(
        "circular_ownership",
        circular_ownership,
        CircularOwnershipParams,
        "Ownership cycles (A owns … owns A) — a circular-ownership red flag.",
    )
)
