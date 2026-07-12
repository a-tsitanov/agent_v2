"""Family 1 — aggregations & rankings (online, read-only)."""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import ID_TYPES, clamp_top_n
from src.graph.aggregations_graph_ops import build_aggregations_graph_ops


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
    cypher = "aggregations_graph_ops.count_entities"
    params = {"type": type, "exclude_ids": exclude_identifiers, "id_types": ID_TYPES}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(
        build_aggregations_graph_ops(store).count_entities, type, exclude_identifiers
    )
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
    cypher = "aggregations_graph_ops.count_relationships"
    params = {"rel_type": rel_type, "polarity": polarity}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(
        build_aggregations_graph_ops(store).count_relationships, rel_type, polarity
    )
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class DistributionByTypeParams(_Params):
    exclude_identifiers: bool = False


async def distribution_by_type(
    store: Any | None, *, exclude_identifiers: bool = False
) -> PrimitiveResult:
    cypher = "aggregations_graph_ops.distribution_by_type"
    params = {"exclude_ids": exclude_identifiers, "id_types": ID_TYPES}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(
        build_aggregations_graph_ops(store).distribution_by_type, exclude_identifiers
    )
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class _NoParams(_Params):
    pass


async def distribution_by_relation_type(store: Any | None) -> PrimitiveResult:
    cypher = "aggregations_graph_ops.distribution_by_relation_type"
    if store is None:
        return PrimitiveResult(cypher=cypher, params={}, rows=[])
    rows = await asyncio.to_thread(
        build_aggregations_graph_ops(store).distribution_by_relation_type
    )
    return PrimitiveResult(cypher=cypher, params={}, rows=rows)


class DistributionByPolarityParams(_Params):
    rel_type: str | None = None


async def distribution_by_polarity(
    store: Any | None, *, rel_type: str | None = None
) -> PrimitiveResult:
    cypher = "aggregations_graph_ops.distribution_by_polarity"
    params = {"rel_type": rel_type}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(
        build_aggregations_graph_ops(store).distribution_by_polarity, rel_type
    )
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
    cypher = "aggregations_graph_ops.top_entities_by_mentions"
    params = {
        "type": type,
        "exclude_ids": exclude_identifiers,
        "id_types": ID_TYPES,
        "top_n": top_n,
    }
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(
        build_aggregations_graph_ops(store).top_entities_by_mentions,
        type, top_n, exclude_identifiers,
    )
    return PrimitiveResult(cypher=cypher, params=params, rows=rows, truncated=len(rows) >= top_n)


class TopByDegreeParams(_Params):
    type: str | None = None
    top_n: int = 20


async def top_entities_by_degree(
    store: Any | None, *, type: str | None = None, top_n: int = 20
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    cypher = "aggregations_graph_ops.top_entities_by_degree"
    params = {"type": type, "top_n": top_n}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(
        build_aggregations_graph_ops(store).top_entities_by_degree, type, top_n
    )
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
