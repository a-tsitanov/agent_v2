"""P1 — knowledge-quality flags (online, read-only)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import ID_TYPES, clamp_top_n
from src.analytics.store_query import run_rows
from src.config import settings


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class ContradictionsParams(_Params):
    top_n: int = 50


async def contradictions(store: Any | None, *, top_n: int = 50) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=50)
    # Flag affirmed+negated of the SAME (a,type,b) only when their validity windows
    # overlap (contemporaneous). A null window is treated as open/overlapping.
    cypher = (
        "MATCH (a:__Entity__)-[r1]->(b:__Entity__), (a)-[r2]->(b) "
        "WHERE type(r1)=type(r2) AND r1.polarity='affirmed' AND r2.polarity='negated' "
        "AND id(r1)<id(r2) "
        "AND (r1.valid_from IS NULL OR r2.valid_to IS NULL OR "
        "r1.valid_from <= r2.valid_to) "
        "AND (r2.valid_from IS NULL OR r1.valid_to IS NULL OR "
        "r2.valid_from <= r1.valid_to) "
        "RETURN a.name AS a, type(r1) AS rel, b.name AS b, "
        "r1.source_chunks AS affirmed_chunks, r2.source_chunks AS negated_chunks "
        "LIMIT $top_n"
    )
    params = {"top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class OrphansParams(_Params):
    min_degree: int | None = None
    top_n: int = 50


async def orphans(
    store: Any | None, *, min_degree: int | None = None, top_n: int = 50
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=50)
    floor = settings.signals.orphan_min_degree if min_degree is None else int(min_degree)
    cypher = (
        "MATCH (e:__Entity__) WHERE NONE(l IN labels(e) WHERE l IN $id_types) "
        "OPTIONAL MATCH (e)-[r]-(:__Entity__) "
        "WITH e, count(r) AS degree WHERE degree < $min_degree "
        "RETURN e.name AS name, degree, "
        "[l IN labels(e) WHERE l<>'__Entity__'][0] AS type "
        "ORDER BY degree ASC LIMIT $top_n"
    )
    params = {"min_degree": floor, "top_n": top_n, "id_types": ID_TYPES}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


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
    cypher = (
        "MATCH (e:__Entity__) WHERE $type IN labels(e) "
        "OPTIONAL MATCH (e)-[]-(id:__Entity__) WHERE any(l IN labels(id) WHERE l IN $expected) "
        "WITH e, collect(DISTINCT [l IN labels(id) WHERE l IN $expected][0]) AS have "
        "RETURN e.name AS name, [x IN $expected WHERE NOT x IN have] AS missing, have "
        "ORDER BY size([x IN $expected WHERE NOT x IN have]) DESC LIMIT $top_n"
    )
    params = {"type": type, "expected": expected, "top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class MergeCandidatesParams(_Params):
    top_n: int = 50


async def merge_candidates(store: Any | None, *, top_n: int = 50) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=50)
    # Duplicate display-name groups (case/space-insensitive). ER-similarity upgrade
    # deferred to Wave 1 (P2). Identifier-keys are excluded.
    cypher = (
        "MATCH (e:__Entity__) WHERE NONE(l IN labels(e) WHERE l IN $id_types) "
        "WITH toLower(trim(e.name)) AS key, count(e) AS count, collect(e.name) AS names "
        "WHERE count > 1 "
        "RETURN key, names, count ORDER BY count DESC LIMIT $top_n"
    )
    params = {"top_n": top_n, "id_types": ID_TYPES}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


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
