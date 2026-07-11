"""Family 2 — connections & co-occurrence (online, read-only)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import ID_TYPES, clamp_top_n
from src.graph.analytics_graph_ops import build_analytics_graph_ops


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class EntityDossierParams(_Params):
    name: str
    top_n: int = 25


async def entity_dossier(store: Any | None, *, name: str, top_n: int = 25) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    params = {"name": name, "top_n": top_n, "id_types": ID_TYPES}
    cypher = " ;; ".join([
        "analytics_graph_ops.entity_core",
        "analytics_graph_ops.entity_neighbors",
        "analytics_graph_ops.entity_identifiers",
        "analytics_graph_ops.entity_communities",
    ])
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    ops = build_analytics_graph_ops(store)
    core = ops.entity_core(name)
    if not core:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    neighbors = ops.entity_neighbors(name, top_n)
    identifiers = ops.entity_identifiers(name, ID_TYPES, top_n)
    communities = ops.entity_communities(name)
    row = {
        "core": core[0],
        "connections": neighbors,
        "identifiers": identifiers,
        "communities": communities,
    }
    return PrimitiveResult(cypher=cypher, params=params, rows=[row])


class NeighborsByRelationParams(_Params):
    name: str
    rel_type: str
    polarity: str | None = None
    top_n: int = 25


async def neighbors_by_relation(
    store: Any | None,
    *,
    name: str,
    rel_type: str,
    polarity: str | None = None,
    top_n: int = 25,
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    params = {"name": name, "rel_type": rel_type, "polarity": polarity, "top_n": top_n}
    cypher = "analytics_graph_ops.neighbors_by_relation"
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = build_analytics_graph_ops(store).neighbors_by_relation(
        name, rel_type, polarity, top_n
    )
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class CooccurrenceParams(_Params):
    name: str
    top_n: int = 25


async def cooccurrence(store: Any | None, *, name: str, top_n: int = 25) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    params = {"name": name, "top_n": top_n}
    cypher = "analytics_graph_ops.cooccurrence"
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = build_analytics_graph_ops(store).cooccurrence(name, top_n)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class CommonConnectionsParams(_Params):
    a: str
    b: str
    top_n: int = 25


async def common_connections(
    store: Any | None, *, a: str, b: str, top_n: int = 25
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    params = {"a": a, "b": b, "top_n": top_n}
    cypher = "analytics_graph_ops.common_connections"
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = build_analytics_graph_ops(store).common_connections(a, b, top_n)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class ConnectionPathParams(_Params):
    source: str
    target: str
    max_hops: int = 6


async def connection_path(
    store: Any | None, *, source: str, target: str, max_hops: int = 6
) -> PrimitiveResult:
    hops = max(1, min(int(max_hops or 6), 12))  # clamp; inlined into pattern
    params = {"source": source, "target": target, "max_hops": hops}
    cypher = "analytics_graph_ops.connection_path"
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = build_analytics_graph_ops(store).connection_path(source, target, hops)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class SharedIdentifierParams(_Params):
    id_type: str | None = None
    min_owners: int = 2
    top_n: int = 25


async def shared_identifier_entities(
    store: Any | None,
    *,
    id_type: str | None = None,
    min_owners: int = 2,
    top_n: int = 25,
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    params = {
        "id_type": id_type,
        "min_owners": int(min_owners),
        "top_n": top_n,
        "id_types": ID_TYPES,
    }
    cypher = "analytics_graph_ops.shared_identifier_entities"
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    # NOTE: the seam's shared_identifier_entities does not take min_owners
    # (it always uses its own default of 2) — see
    # src/graph/analytics_graph_ops.py's _DEFAULT_MIN_OWNERS comment. The
    # primitive param is preserved above (params reporting) but the caller
    # value is not currently threaded through when it differs from 2.
    rows = build_analytics_graph_ops(store).shared_identifier_entities(id_type, top_n)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class IdentifierLookupParams(_Params):
    value: str


async def identifier_lookup(store: Any | None, *, value: str) -> PrimitiveResult:
    params = {"value": value, "id_types": ID_TYPES}
    cypher = "analytics_graph_ops.identifier_lookup"
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = build_analytics_graph_ops(store).identifier_lookup(value)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


register(
    Primitive(
        "entity_dossier",
        entity_dossier,
        EntityDossierParams,
        "Full portrait of one entity: core, top neighbors, attached identifiers, communities.",
    )
)
register(
    Primitive(
        "neighbors_by_relation",
        neighbors_by_relation,
        NeighborsByRelationParams,
        "Entities linked to a named entity by a specific relation type.",
    )
)
register(
    Primitive(
        "cooccurrence",
        cooccurrence,
        CooccurrenceParams,
        "Entities most often mentioned together with a named entity (shared chunks).",
    )
)
register(
    Primitive(
        "common_connections",
        common_connections,
        CommonConnectionsParams,
        "What/who two named entities share (common neighbors).",
    )
)
register(
    Primitive(
        "connection_path",
        connection_path,
        ConnectionPathParams,
        "Shortest path (chain of relations) between two named entities.",
    )
)
register(
    Primitive(
        "shared_identifier_entities",
        shared_identifier_entities,
        SharedIdentifierParams,
        "Distinct entities sharing one identifier (phone/INN/account) — affiliation/dedup/risk.",
    )
)
register(
    Primitive(
        "identifier_lookup",
        identifier_lookup,
        IdentifierLookupParams,
        "Who owns this identifier value (phone/INN/email).",
    )
)
