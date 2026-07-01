"""Family 1 — aggregations & rankings (online, read-only)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import ID_TYPES, clamp_top_n
from src.analytics.store_query import run_rows


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class CountEntitiesParams(_Params):
    type: str | None = None
    exclude_identifiers: bool = True


async def count_entities(
    store: Any | None,
    *,
    type: str | None = None,
    exclude_identifiers: bool = True,
) -> PrimitiveResult:
    cypher = (
        "MATCH (e:__Entity__) "
        "WHERE ($type IS NULL OR $type IN labels(e)) "
        "AND ($exclude_ids = false OR NONE(l IN labels(e) WHERE l IN $id_types)) "
        "RETURN count(e) AS n"
    )
    params = {"type": type, "exclude_ids": exclude_identifiers, "id_types": ID_TYPES}
    rows = await run_rows(store, cypher, params)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class CountRelationshipsParams(_Params):
    rel_type: str | None = None
    polarity: str | None = None


async def count_relationships(
    store: Any | None,
    *,
    rel_type: str | None = None,
    polarity: str | None = None,
) -> PrimitiveResult:
    cypher = (
        "MATCH (:__Entity__)-[r]->(:__Entity__) "
        "WHERE ($rel_type IS NULL OR type(r) = $rel_type) "
        "AND ($polarity IS NULL OR r.polarity = $polarity) "
        "RETURN count(r) AS n"
    )
    params = {"rel_type": rel_type, "polarity": polarity}
    rows = await run_rows(store, cypher, params)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class DistributionByTypeParams(_Params):
    exclude_identifiers: bool = False


async def distribution_by_type(
    store: Any | None, *, exclude_identifiers: bool = False
) -> PrimitiveResult:
    cypher = (
        "MATCH (e:__Entity__) "
        "WHERE ($exclude_ids = false OR NONE(l IN labels(e) WHERE l IN $id_types)) "
        "WITH [l IN labels(e) WHERE l <> '__Entity__'][0] AS type "
        "RETURN type, count(*) AS n ORDER BY n DESC"
    )
    params = {"exclude_ids": exclude_identifiers, "id_types": ID_TYPES}
    rows = await run_rows(store, cypher, params)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class _NoParams(_Params):
    pass


async def distribution_by_relation_type(store: Any | None) -> PrimitiveResult:
    cypher = "MATCH (:__Entity__)-[r]->(:__Entity__) RETURN type(r) AS rel, count(*) AS n ORDER BY n DESC"
    rows = await run_rows(store, cypher, {})
    return PrimitiveResult(cypher=cypher, params={}, rows=rows)


class DistributionByPolarityParams(_Params):
    rel_type: str | None = None


async def distribution_by_polarity(
    store: Any | None, *, rel_type: str | None = None
) -> PrimitiveResult:
    cypher = (
        "MATCH (:__Entity__)-[r]->(:__Entity__) WHERE ($rel_type IS NULL OR type(r) = $rel_type) "
        "RETURN r.polarity AS polarity, count(*) AS n ORDER BY n DESC"
    )
    params = {"rel_type": rel_type}
    rows = await run_rows(store, cypher, params)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class TopByMentionsParams(_Params):
    type: str | None = None
    top_n: int = 20
    exclude_identifiers: bool = True


async def top_entities_by_mentions(
    store: Any | None,
    *,
    type: str | None = None,
    top_n: int = 20,
    exclude_identifiers: bool = True,
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    cypher = (
        "MATCH (e:__Entity__) "
        "WHERE ($type IS NULL OR $type IN labels(e)) "
        "AND ($exclude_ids = false OR NONE(l IN labels(e) WHERE l IN $id_types)) "
        "AND e.mention_count IS NOT NULL "
        "RETURN e.name AS name, e.mention_count AS mentions "
        "ORDER BY e.mention_count DESC LIMIT $top_n"
    )
    params = {
        "type": type,
        "exclude_ids": exclude_identifiers,
        "id_types": ID_TYPES,
        "top_n": top_n,
    }
    rows = await run_rows(store, cypher, params)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows, truncated=len(rows) >= top_n)


class TopByDegreeParams(_Params):
    type: str | None = None
    top_n: int = 20


async def top_entities_by_degree(
    store: Any | None, *, type: str | None = None, top_n: int = 20
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    cypher = (
        "MATCH (e:__Entity__) WHERE ($type IS NULL OR $type IN labels(e)) "
        "OPTIONAL MATCH (e)-[r]-(:__Entity__) WHERE (r.polarity IS NULL OR r.polarity <> 'negated') "
        "WITH e, count(r) AS degree "
        "RETURN e.name AS name, degree ORDER BY degree DESC LIMIT $top_n"
    )
    params = {"type": type, "top_n": top_n}
    rows = await run_rows(store, cypher, params)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows, truncated=len(rows) >= top_n)


register(
    Primitive(
        "count_entities",
        count_entities,
        CountEntitiesParams,
        "Count entities, optionally by type; excludes identifier nodes by default.",
    )
)
register(
    Primitive(
        "count_relationships",
        count_relationships,
        CountRelationshipsParams,
        "Count relationships, optionally by relation type and/or polarity.",
    )
)
register(
    Primitive(
        "distribution_by_type",
        distribution_by_type,
        DistributionByTypeParams,
        "Histogram of entities by type.",
    )
)
register(
    Primitive(
        "distribution_by_relation_type",
        distribution_by_relation_type,
        _NoParams,
        "Histogram of relationships by relation type.",
    )
)
register(
    Primitive(
        "distribution_by_polarity",
        distribution_by_polarity,
        DistributionByPolarityParams,
        "Share of affirmed/negated/uncertain relationships (contentiousness).",
    )
)
register(
    Primitive(
        "top_entities_by_mentions",
        top_entities_by_mentions,
        TopByMentionsParams,
        "Top entities by mention frequency (importance by how often discussed).",
    )
)
register(
    Primitive(
        "top_entities_by_degree",
        top_entities_by_degree,
        TopByDegreeParams,
        "Top entities by connection count (degree), ignoring negated edges.",
    )
)
