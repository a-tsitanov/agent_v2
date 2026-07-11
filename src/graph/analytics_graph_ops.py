"""Backend-dispatched analytics "connections" graph ops (read-only,
fail-soft neighbourhood reads).

``Neo4jAnalyticsGraphOps`` wraps the existing Cypher constants/inline
strings verbatim (default path, byte-for-byte unchanged; the constants
and query strings were MOVED here from
``analytics/primitives/connections.py``'s ``_CORE``/``_NEIGHBORS``/
``_IDENTIFIERS``/``_COMMUNITIES`` and the inline Cypher built in
``neighbors_by_relation``/``cooccurrence``/``common_connections``/
``connection_path``/``shared_identifier_entities``/``identifier_lookup``.
``connections.py`` still holds its own (transitionally duplicated) copies
pending Task 2's rewire.

Each method preserves the fail-soft behaviour of
``analytics/store_query.py::run_rows`` (``try/except Exception -> []``,
same warning log) — the seam replaces the raw ``run_rows(store, cypher,
params)`` call inside each primitive, not the fail-soft wrapper itself.

``NebulaAnalyticsGraphOps`` is a stub for Task 2 (nGQL neighbourhood
reads via GO/FETCH/FIND PATH); every method raises ``NotImplementedError``.
"""
from __future__ import annotations

from typing import Any, Protocol

from loguru import logger

from src.analytics.ids import ID_TYPES
from src.config import settings

# ── connections Cypher (moved verbatim from
# analytics/primitives/connections.py) ────────────────────────────────

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

# ``shared_identifier_entities``'s Protocol signature is
# ``(id_types, top_n)`` — it does not expose ``min_owners`` because the
# primitive never varies it from its default (2), so mirroring the default
# here is byte-for-byte. (Unlike top_n/polarity, which ARE caller-settable
# on their primitives and so ARE exposed on the seam signatures below.)
_DEFAULT_MIN_OWNERS = 2


class AnalyticsGraphOps(Protocol):
    def entity_core(self, name: str) -> list[dict]: ...

    def entity_neighbors(self, name: str, top_n: int) -> list[dict]: ...

    def entity_identifiers(self, name: str, id_types: list[str], top_n: int) -> list[dict]: ...

    def entity_communities(self, name: str) -> list[dict]: ...

    def neighbors_by_relation(
        self, name: str, rel: str, polarity: str | None, top_n: int
    ) -> list[dict]: ...

    def common_connections(self, a: str, b: str, top_n: int) -> list[dict]: ...

    def identifier_lookup(self, value: str) -> list[dict]: ...

    def shared_identifier_entities(self, id_types: str | None, top_n: int) -> list[dict]: ...

    def connection_path(self, source: str, target: str, hops: int) -> list[dict]: ...

    def cooccurrence(self, name: str, top_n: int) -> list[dict]: ...


class Neo4jAnalyticsGraphOps:
    """Runs the historical connections Cypher verbatim — zero behaviour
    change from the pre-seam ``analytics/primitives/connections.py``
    implementation. Fail-soft per method: mirrors
    ``analytics/store_query.py::run_rows`` (``try/except -> []``, same
    warning log)."""

    def __init__(self, store: Any):
        self._store = store

    def _rows(self, cypher: str, params: dict) -> list[dict]:
        try:
            rows = self._store.structured_query(cypher, param_map=params)
            return list(rows or [])
        except Exception as exc:  # fail-soft, like store_query.run_rows
            logger.warning("analytics query failed: {e}", e=exc)
            return []

    def entity_core(self, name: str) -> list[dict]:
        return self._rows(_CORE, {"name": name})

    def entity_neighbors(self, name: str, top_n: int) -> list[dict]:
        return self._rows(_NEIGHBORS, {"name": name, "top_n": top_n, "id_types": ID_TYPES})

    def entity_identifiers(self, name: str, id_types: list[str], top_n: int) -> list[dict]:
        return self._rows(
            _IDENTIFIERS, {"name": name, "id_types": id_types, "top_n": top_n}
        )

    def entity_communities(self, name: str) -> list[dict]:
        return self._rows(_COMMUNITIES, {"name": name})

    def neighbors_by_relation(
        self, name: str, rel: str, polarity: str | None, top_n: int
    ) -> list[dict]:
        cypher = (
            "MATCH (e:__Entity__ {name:$name})-[r]-(n:__Entity__) "
            "WHERE type(r)=$rel_type AND ($polarity IS NULL OR r.polarity=$polarity) "
            "RETURN n.name AS name, r.weight AS w, r.valid_from AS valid_from, "
            "r.valid_to AS valid_to "
            "ORDER BY r.weight DESC LIMIT $top_n"
        )
        params = {
            "name": name,
            "rel_type": rel,
            "polarity": polarity,
            "top_n": top_n,
        }
        return self._rows(cypher, params)

    def common_connections(self, a: str, b: str, top_n: int) -> list[dict]:
        cypher = (
            "MATCH (x:__Entity__ {name:$a})-[r1]-(m:__Entity__)-[r2]-"
            "(y:__Entity__ {name:$b}) "
            "WHERE (r1.polarity IS NULL OR r1.polarity<>'negated') AND (r2.polarity IS NULL OR r2.polarity<>'negated') "
            "RETURN m.name AS name, [l IN labels(m) WHERE l<>'__Entity__' AND l<>'__Node__'][0] AS type, "
            "collect(DISTINCT type(r1))+collect(DISTINCT type(r2)) AS via "
            "ORDER BY size(via) DESC LIMIT $top_n"
        )
        params = {"a": a, "b": b, "top_n": top_n}
        return self._rows(cypher, params)

    def identifier_lookup(self, value: str) -> list[dict]:
        cypher = (
            "MATCH (id:__Entity__ {name:$value})-[r]-(e:__Entity__) "
            "WHERE any(l IN labels(id) WHERE l IN $id_types) "
            "AND NONE(l IN labels(e) WHERE l IN $id_types) "
            "RETURN e.name AS name, labels(e) AS labels, type(r) AS rel"
        )
        params = {"value": value, "id_types": ID_TYPES}
        return self._rows(cypher, params)

    def shared_identifier_entities(self, id_types: str | None, top_n: int) -> list[dict]:
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
            "id_type": id_types,
            "min_owners": _DEFAULT_MIN_OWNERS,
            "top_n": top_n,
            "id_types": ID_TYPES,
        }
        return self._rows(cypher, params)

    def connection_path(self, source: str, target: str, hops: int) -> list[dict]:
        cypher = (
            "MATCH (a:__Entity__ {name:$source}),(b:__Entity__ {name:$target}) "
            f"MATCH p = shortestPath((a)-[*..{hops}]-(b)) "
            "RETURN [n IN nodes(p)|n.name] AS path, [r IN relationships(p)|type(r)] AS "
            "rels, length(p) AS hops"
        )
        params = {"source": source, "target": target, "max_hops": hops}
        return self._rows(cypher, params)

    def cooccurrence(self, name: str, top_n: int) -> list[dict]:
        cypher = (
            "MATCH (e:__Entity__ {name:$name})<-[:MENTIONS]-(c:Chunk)-[:MENTIONS]->"
            "(other:__Entity__) "
            "WHERE other <> e "
            "RETURN other.name AS name, count(DISTINCT c) AS shared ORDER BY shared DESC "
            "LIMIT $top_n"
        )
        params = {"name": name, "top_n": top_n}
        return self._rows(cypher, params)


class NebulaAnalyticsGraphOps:
    """Stub for Task 2 (nGQL connections reads: GO/FETCH/FIND SHORTEST
    PATH). Every method raises ``NotImplementedError``."""

    def __init__(self, store: Any):
        self._store = store

    def entity_core(self, name: str) -> list[dict]:
        raise NotImplementedError("NebulaAnalyticsGraphOps.entity_core (Task 2)")

    def entity_neighbors(self, name: str, top_n: int) -> list[dict]:
        raise NotImplementedError("NebulaAnalyticsGraphOps.entity_neighbors (Task 2)")

    def entity_identifiers(self, name: str, id_types: list[str], top_n: int) -> list[dict]:
        raise NotImplementedError("NebulaAnalyticsGraphOps.entity_identifiers (Task 2)")

    def entity_communities(self, name: str) -> list[dict]:
        raise NotImplementedError("NebulaAnalyticsGraphOps.entity_communities (Task 2)")

    def neighbors_by_relation(
        self, name: str, rel: str, polarity: str | None, top_n: int
    ) -> list[dict]:
        raise NotImplementedError("NebulaAnalyticsGraphOps.neighbors_by_relation (Task 2)")

    def common_connections(self, a: str, b: str, top_n: int) -> list[dict]:
        raise NotImplementedError("NebulaAnalyticsGraphOps.common_connections (Task 2)")

    def identifier_lookup(self, value: str) -> list[dict]:
        raise NotImplementedError("NebulaAnalyticsGraphOps.identifier_lookup (Task 2)")

    def shared_identifier_entities(self, id_types: str | None, top_n: int) -> list[dict]:
        raise NotImplementedError("NebulaAnalyticsGraphOps.shared_identifier_entities (Task 2)")

    def connection_path(self, source: str, target: str, hops: int) -> list[dict]:
        raise NotImplementedError("NebulaAnalyticsGraphOps.connection_path (Task 2)")

    def cooccurrence(self, name: str, top_n: int) -> list[dict]:
        raise NotImplementedError("NebulaAnalyticsGraphOps.cooccurrence (Task 2)")


def build_analytics_graph_ops(store: Any) -> AnalyticsGraphOps:
    if settings.graph.backend == "nebula":
        return NebulaAnalyticsGraphOps(store)
    return Neo4jAnalyticsGraphOps(store)
