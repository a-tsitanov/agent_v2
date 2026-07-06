"""Family 2 — connections & co-occurrence (online, read-only)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import ID_TYPES, clamp_top_n
from src.analytics.store_query import run_rows


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


_CORE = (
    "MATCH (e:__Entity__ {name:$name}) "
    "RETURN e.name AS name, e.description AS description, labels(e) AS labels, "
    "e.mention_count AS mention_count"
)
_NEIGHBORS = (
    "MATCH (e:__Entity__ {name:$name})-[r]-(n:__Entity__) "
    "WHERE (r.polarity IS NULL OR r.polarity <> 'negated') AND NONE(l IN labels(n) WHERE l IN $id_types) "
    "RETURN type(r) AS rel, n.name AS name, "
    "[l IN labels(n) WHERE l <> '__Entity__' AND l <> '__Node__'][0] AS ntype, r.weight AS w "
    "ORDER BY r.weight DESC LIMIT $top_n"
)
_IDENTIFIERS = (
    "MATCH (e:__Entity__ {name:$name})-[]-(id:__Entity__) "
    "WHERE any(l IN labels(id) WHERE l IN $id_types) "
    "RETURN [l IN labels(id) WHERE l IN $id_types][0] AS id_type, id.name AS value "
    "LIMIT $top_n"
)
_COMMUNITIES = (
    "MATCH (e:__Entity__ {name:$name})-[:IN_COMMUNITY]->(c:Community) "
    "RETURN c.level AS level, c.title AS title"
)


class EntityDossierParams(_Params):
    name: str
    top_n: int = 25


async def entity_dossier(store: Any | None, *, name: str, top_n: int = 25) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    params = {"name": name, "top_n": top_n, "id_types": ID_TYPES}
    if store is None:
        return PrimitiveResult(cypher=_CORE, params=params, rows=[])
    core = await run_rows(store, _CORE, params)
    if not core:
        return PrimitiveResult(cypher=_CORE, params=params, rows=[])
    neighbors = await run_rows(store, _NEIGHBORS, params)
    identifiers = await run_rows(store, _IDENTIFIERS, params)
    communities = await run_rows(store, _COMMUNITIES, params)
    row = {
        "core": core[0],
        "connections": neighbors,
        "identifiers": identifiers,
        "communities": communities,
    }
    cypher = " ;; ".join([_CORE, _NEIGHBORS, _IDENTIFIERS, _COMMUNITIES])
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
    cypher = (
        "MATCH (e:__Entity__ {name:$name})-[r]-(n:__Entity__) "
        "WHERE type(r)=$rel_type AND ($polarity IS NULL OR r.polarity=$polarity) "
        "RETURN n.name AS name, r.weight AS w, r.valid_from AS valid_from, "
        "r.valid_to AS valid_to "
        "ORDER BY r.weight DESC LIMIT $top_n"
    )
    params = {"name": name, "rel_type": rel_type, "polarity": polarity, "top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class CooccurrenceParams(_Params):
    name: str
    top_n: int = 25


async def cooccurrence(store: Any | None, *, name: str, top_n: int = 25) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    cypher = (
        "MATCH (e:__Entity__ {name:$name})<-[:MENTIONS]-(c:Chunk)-[:MENTIONS]->"
        "(other:__Entity__) "
        "WHERE other <> e "
        "RETURN other.name AS name, count(DISTINCT c) AS shared ORDER BY shared DESC "
        "LIMIT $top_n"
    )
    params = {"name": name, "top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class CommonConnectionsParams(_Params):
    a: str
    b: str
    top_n: int = 25


async def common_connections(
    store: Any | None, *, a: str, b: str, top_n: int = 25
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    cypher = (
        "MATCH (x:__Entity__ {name:$a})-[r1]-(m:__Entity__)-[r2]-"
        "(y:__Entity__ {name:$b}) "
        "WHERE (r1.polarity IS NULL OR r1.polarity<>'negated') AND (r2.polarity IS NULL OR r2.polarity<>'negated') "
        "RETURN m.name AS name, [l IN labels(m) WHERE l<>'__Entity__' AND l<>'__Node__'][0] AS type, "
        "collect(DISTINCT type(r1))+collect(DISTINCT type(r2)) AS via "
        "ORDER BY size(via) DESC LIMIT $top_n"
    )
    params = {"a": a, "b": b, "top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class ConnectionPathParams(_Params):
    source: str
    target: str
    max_hops: int = 6


async def connection_path(
    store: Any | None, *, source: str, target: str, max_hops: int = 6
) -> PrimitiveResult:
    hops = max(1, min(int(max_hops or 6), 12))  # clamp; inlined into pattern
    cypher = (
        "MATCH (a:__Entity__ {name:$source}),(b:__Entity__ {name:$target}) "
        f"MATCH p = shortestPath((a)-[*..{hops}]-(b)) "
        "RETURN [n IN nodes(p)|n.name] AS path, [r IN relationships(p)|type(r)] AS "
        "rels, length(p) AS hops"
    )
    params = {"source": source, "target": target, "max_hops": hops}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


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
    cypher = (
        "MATCH (id:__Entity__) WHERE any(l IN labels(id) WHERE l IN $id_types) "
        "AND ($id_type IS NULL OR $id_type IN labels(id)) "
        "MATCH (id)-[]-(owner:__Entity__) "
        "WHERE NONE(l IN labels(owner) WHERE l IN $id_types) "
        "WITH id, [l IN labels(id) WHERE l IN $id_types][0] AS id_type, "
        "collect(DISTINCT owner.name) AS owners "
        "WHERE size(owners) >= $min_owners "
        "RETURN id.name AS value, id_type, owners ORDER BY size(owners) DESC "
        "LIMIT $top_n"
    )
    params = {
        "id_type": id_type,
        "min_owners": int(min_owners),
        "top_n": top_n,
        "id_types": ID_TYPES,
    }
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class IdentifierLookupParams(_Params):
    value: str


async def identifier_lookup(store: Any | None, *, value: str) -> PrimitiveResult:
    cypher = (
        "MATCH (id:__Entity__ {name:$value})-[r]-(e:__Entity__) "
        "WHERE any(l IN labels(id) WHERE l IN $id_types) "
        "AND NONE(l IN labels(e) WHERE l IN $id_types) "
        "RETURN e.name AS name, labels(e) AS labels, type(r) AS rel"
    )
    params = {"value": value, "id_types": ID_TYPES}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


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
