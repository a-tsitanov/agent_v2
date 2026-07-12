"""P1 — knowledge-quality flags (online, read-only)."""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n
from src.config import settings
from src.graph.quality_graph_ops import build_quality_graph_ops


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class ContradictionsParams(_Params):
    top_n: int = 50


async def contradictions(store: Any | None, *, top_n: int = 50) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=50)
    # Flag affirmed+negated of the SAME (a,type,b) only when their validity windows
    # overlap (contemporaneous). A null window is treated as open/overlapping.
    cypher = "quality_graph_ops.contradictions"
    params = {"top_n": top_n}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(build_quality_graph_ops(store).contradictions, top_n)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class OrphansParams(_Params):
    min_degree: int | None = None
    top_n: int = 50


async def orphans(
    store: Any | None, *, min_degree: int | None = None, top_n: int = 50
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=50)
    floor = settings.signals.orphan_min_degree if min_degree is None else int(min_degree)
    cypher = "quality_graph_ops.orphans"
    params = {"min_degree": floor, "top_n": top_n}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(build_quality_graph_ops(store).orphans, floor, top_n)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


register(
    Primitive(
        "contradictions",
        contradictions,
        ContradictionsParams,
        "Facts asserted AND denied via two contemporaneous edges on the same pair. "
        "NOTE: current ingest merges each (entity,relation,entity) to one edge with majority "
        "polarity, so this surfaces results only when duplicate opposite-polarity edges exist; "
        "merge-time contradiction detection is a planned follow-up.",
    )
)
register(
    Primitive(
        "orphans",
        orphans,
        OrphansParams,
        "Under-connected entities (degree below a floor) — noise or under-documented.",
    )
)


class IncompleteEntitiesParams(_Params):
    type: str = "Organization"
    top_n: int = 50


async def incomplete_entities(
    store: Any | None, *, type: str = "Organization", top_n: int = 50
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=50)
    expected = settings.signals.expected_attrs.get(type, [])
    cypher = "quality_graph_ops.incomplete_entities"
    params = {"type": type, "expected": expected, "top_n": top_n}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(
        build_quality_graph_ops(store).incomplete_entities, type, expected, top_n
    )
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class MergeCandidatesParams(_Params):
    top_n: int = 50


async def merge_candidates(store: Any | None, *, top_n: int = 50) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=50)
    # Duplicate display-name groups (case/space-insensitive). ER-similarity upgrade
    # deferred to Wave 1 (P2). Identifier-keys are excluded.
    cypher = "quality_graph_ops.merge_candidates"
    params = {"top_n": top_n}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(build_quality_graph_ops(store).merge_candidates, top_n)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


register(
    Primitive(
        "incomplete_entities",
        incomplete_entities,
        IncompleteEntitiesParams,
        "Entities missing expected identifier attributes for their type (completeness).",
    )
)
register(
    Primitive(
        "merge_candidates",
        merge_candidates,
        MergeCandidatesParams,
        "Duplicate display-name groups — a ranked recommended-merge queue.",
    )
)
