"""Backend-dispatched analytics "aggregations" graph ops (read-only,
fail-soft counts/histograms/top-N reads).

``Neo4jAggregationsGraphOps`` wraps the existing Cypher constants verbatim
(default path, byte-for-byte unchanged; the constants were MOVED here from
``analytics/primitives/aggregations.py``'s inline ``cypher`` strings). Each
method preserves the fail-soft behaviour of
``analytics/store_query.py::run_rows`` (``try/except Exception -> []``, same
warning log) — the seam replaces the raw ``run_rows(store, cypher, params)``
call inside each primitive, not the fail-soft wrapper itself.

``NebulaAggregationsGraphOps`` translates the same reads to nGQL per
``docs/superpowers/nebula-analytics-ngql-rules-2026-07-11.md``: plain
``MATCH``+aggregation (nebula 3.8's openCypher subset supports this
natively, unlike ``connections``'s VID point-lookups which needed
GO/FETCH). Values are inline-quoted via ``_q``/list-literal (nebula binds
no params). Every method is fail-soft via ``_nebula_fail_soft``.
"""
from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, Protocol

from loguru import logger

from src.analytics.ids import ID_TYPES
from src.config import settings

# ── aggregations Cypher (moved verbatim from
# analytics/primitives/aggregations.py) ────────────────────────────────

_COUNT_ENTITIES = (
    "MATCH (e:__Entity__) "
    "WHERE ($type IS NULL OR $type IN labels(e)) "
    "AND ($exclude_ids = false OR NONE(l IN labels(e) WHERE l IN $id_types)) "
    "RETURN count(e) AS n"
)
_COUNT_RELATIONSHIPS = (
    "MATCH (:__Entity__)-[r]->(:__Entity__) "
    "WHERE ($rel_type IS NULL OR type(r) = $rel_type) "
    "AND ($polarity IS NULL OR r.polarity = $polarity) "
    "RETURN count(r) AS n"
)
_DISTRIBUTION_BY_TYPE = (
    "MATCH (e:__Entity__) "
    "WHERE ($exclude_ids = false OR NONE(l IN labels(e) WHERE l IN $id_types)) "
    "WITH [l IN labels(e) WHERE l <> '__Entity__' AND l <> '__Node__'][0] AS type "
    "RETURN type, count(*) AS n ORDER BY n DESC"
)
_DISTRIBUTION_BY_RELATION_TYPE = (
    "MATCH (:__Entity__)-[r]->(:__Entity__) RETURN type(r) AS rel, count(*) AS n ORDER BY n DESC"
)
_DISTRIBUTION_BY_POLARITY = (
    "MATCH (:__Entity__)-[r]->(:__Entity__) WHERE ($rel_type IS NULL OR type(r) = $rel_type) "
    "RETURN r.polarity AS polarity, count(*) AS n ORDER BY n DESC"
)
_TOP_ENTITIES_BY_MENTIONS = (
    "MATCH (e:__Entity__) "
    "WHERE ($type IS NULL OR $type IN labels(e)) "
    "AND ($exclude_ids = false OR NONE(l IN labels(e) WHERE l IN $id_types)) "
    "AND e.mention_count IS NOT NULL "
    "RETURN e.name AS name, e.mention_count AS mentions "
    "ORDER BY e.mention_count DESC LIMIT $top_n"
)
_TOP_ENTITIES_BY_DEGREE = (
    "MATCH (e:__Entity__) WHERE ($type IS NULL OR $type IN labels(e)) "
    "OPTIONAL MATCH (e)-[r]-(:__Entity__) WHERE (r.polarity IS NULL OR r.polarity <> 'negated') "
    "WITH e, count(r) AS degree "
    "RETURN e.name AS name, degree ORDER BY degree DESC LIMIT $top_n"
)


class AggregationsGraphOps(Protocol):
    def count_entities(self, type: str | None, exclude_identifiers: bool) -> list[dict]: ...

    def count_relationships(
        self, rel_type: str | None, polarity: str | None
    ) -> list[dict]: ...

    def distribution_by_type(self, exclude_identifiers: bool) -> list[dict]: ...

    def distribution_by_relation_type(self) -> list[dict]: ...

    def distribution_by_polarity(self, rel_type: str | None) -> list[dict]: ...

    def top_entities_by_mentions(
        self, type: str | None, top_n: int, exclude_identifiers: bool
    ) -> list[dict]: ...

    def top_entities_by_degree(self, type: str | None, top_n: int) -> list[dict]: ...


class Neo4jAggregationsGraphOps:
    """Runs the historical aggregations Cypher verbatim — zero behaviour
    change from the pre-seam ``analytics/primitives/aggregations.py``
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

    def count_entities(self, type: str | None, exclude_identifiers: bool) -> list[dict]:
        params = {"type": type, "exclude_ids": exclude_identifiers, "id_types": ID_TYPES}
        return self._rows(_COUNT_ENTITIES, params)

    def count_relationships(
        self, rel_type: str | None, polarity: str | None
    ) -> list[dict]:
        params = {"rel_type": rel_type, "polarity": polarity}
        return self._rows(_COUNT_RELATIONSHIPS, params)

    def distribution_by_type(self, exclude_identifiers: bool) -> list[dict]:
        params = {"exclude_ids": exclude_identifiers, "id_types": ID_TYPES}
        return self._rows(_DISTRIBUTION_BY_TYPE, params)

    def distribution_by_relation_type(self) -> list[dict]:
        return self._rows(_DISTRIBUTION_BY_RELATION_TYPE, {})

    def distribution_by_polarity(self, rel_type: str | None) -> list[dict]:
        params = {"rel_type": rel_type}
        return self._rows(_DISTRIBUTION_BY_POLARITY, params)

    def top_entities_by_mentions(
        self, type: str | None, top_n: int, exclude_identifiers: bool
    ) -> list[dict]:
        params = {
            "type": type,
            "exclude_ids": exclude_identifiers,
            "id_types": ID_TYPES,
            "top_n": top_n,
        }
        return self._rows(_TOP_ENTITIES_BY_MENTIONS, params)

    def top_entities_by_degree(self, type: str | None, top_n: int) -> list[dict]:
        params = {"type": type, "top_n": top_n}
        return self._rows(_TOP_ENTITIES_BY_DEGREE, params)


def _nebula_fail_soft(method: Callable[..., list[dict]]) -> Callable[..., list[dict]]:
    """Mirrors ``Neo4jAggregationsGraphOps._rows``'s ``try/except -> []``
    (same warning log) at the method level."""

    @functools.wraps(method)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> list[dict]:
        try:
            return method(self, *args, **kwargs)
        except Exception as exc:  # fail-soft, like store_query.run_rows
            logger.warning("analytics query failed: {e}", e=exc)
            return []

    return wrapper


def _list_literal(items: list[str]) -> str:
    """nGQL list literal for a ``NOT IN``/``IN`` filter (inline-quoted, no
    param_map — mirrors ``_q``)."""
    from src.graph.nebula_store import _q

    return "[" + ", ".join(_q(v) for v in items) + "]"


class NebulaAggregationsGraphOps:
    """nGQL aggregations graph ops: plain ``MATCH``+aggregation (nebula
    3.8's openCypher subset supports aggregation directly in ``MATCH``, so
    these port near-verbatim from the neo4j Cypher — see the rules doc).
    Values are inline-quoted (nebula binds no params — ``_q`` from
    ``nebula_store``). Every method is fail-soft via ``_nebula_fail_soft``."""

    def __init__(self, store: Any):
        self._store = store

    def _exec(self, stmt: str) -> list[dict]:
        return list(self._store.structured_query(stmt) or [])

    @_nebula_fail_soft
    def count_entities(self, type: str | None, exclude_identifiers: bool) -> list[dict]:
        from src.graph.nebula_store import _q

        clauses = []
        if type:
            clauses.append(f"e.`Entity`.label == {_q(type)}")
        if exclude_identifiers:
            clauses.append(f"e.`Entity`.label NOT IN {_list_literal(ID_TYPES)}")
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        stmt = f"MATCH (e:`Entity`){where} RETURN count(e) AS n;"
        return self._exec(stmt)

    @_nebula_fail_soft
    def count_relationships(
        self, rel_type: str | None, polarity: str | None
    ) -> list[dict]:
        # Unfiltered: a direct edge count works (no WHERE, so no index needed).
        if rel_type is None and polarity is None:
            stmt = "MATCH (:`Entity`)-[r:`RELATED`]->(:`Entity`) RETURN count(r) AS n;"
            return self._exec(stmt)
        # Filtered: nebula rejects an edge-property WHERE on an unanchored full
        # edge scan ("IndexNotFound — No valid index found"), so scan the
        # rel_type/polarity of every edge and count matches client-side. Scale
        # note: O(E) — a filtered edge-count index is a Tier-B concern; this
        # stays correct-but-unindexed, like the rest of the graph-wide analytics.
        stmt = (
            "MATCH (:`Entity`)-[r:`RELATED`]->(:`Entity`) "
            "RETURN r.rel_type AS rel_type, r.polarity AS polarity;"
        )
        rows = self._exec(stmt)
        n = sum(
            1
            for row in rows
            if (rel_type is None or row.get("rel_type") == rel_type)
            and (polarity is None or row.get("polarity") == polarity)
        )
        return [{"n": n}]

    @_nebula_fail_soft
    def distribution_by_type(self, exclude_identifiers: bool) -> list[dict]:
        where = ""
        if exclude_identifiers:
            where = f" WHERE e.`Entity`.label NOT IN {_list_literal(ID_TYPES)}"
        stmt = (
            f"MATCH (e:`Entity`){where} "
            "RETURN e.`Entity`.label AS type, count(*) AS n ORDER BY n DESC;"
        )
        return self._exec(stmt)

    @_nebula_fail_soft
    def distribution_by_relation_type(self) -> list[dict]:
        stmt = (
            "MATCH (:`Entity`)-[r:`RELATED`]->(:`Entity`) "
            "RETURN r.rel_type AS rel, count(*) AS n ORDER BY n DESC;"
        )
        return self._exec(stmt)

    @_nebula_fail_soft
    def distribution_by_polarity(self, rel_type: str | None) -> list[dict]:
        # Unfiltered: group-by-polarity works on the anonymous-endpoint scan.
        if not rel_type:
            stmt = (
                "MATCH (:`Entity`)-[r:`RELATED`]->(:`Entity`) "
                "RETURN r.polarity AS polarity, count(*) AS n ORDER BY n DESC;"
            )
            return self._exec(stmt)
        # Filtered: an edge-property WHERE on the anonymous-endpoint scan
        # IndexNotFounds (same as count_relationships) — scan rel_type/polarity
        # and group client-side. O(E), Tier-B index deferred.
        rows = self._exec(
            "MATCH (:`Entity`)-[r:`RELATED`]->(:`Entity`) "
            "RETURN r.rel_type AS rel_type, r.polarity AS polarity;"
        )
        counts: dict[Any, int] = {}
        for row in rows:
            if row.get("rel_type") == rel_type:
                pol = row.get("polarity")
                counts[pol] = counts.get(pol, 0) + 1
        out = [{"polarity": pol, "n": n} for pol, n in counts.items()]
        out.sort(key=lambda r: r["n"], reverse=True)
        return out

    @_nebula_fail_soft
    def top_entities_by_mentions(
        self, type: str | None, top_n: int, exclude_identifiers: bool
    ) -> list[dict]:
        from src.graph.nebula_store import _q

        clauses = []
        if type:
            clauses.append(f"e.`Entity`.label == {_q(type)}")
        if exclude_identifiers:
            clauses.append(f"e.`Entity`.label NOT IN {_list_literal(ID_TYPES)}")
        clauses.append("e.`Entity`.mention_count IS NOT NULL")
        where = f" WHERE {' AND '.join(clauses)}"
        stmt = (
            f"MATCH (e:`Entity`){where} "
            "RETURN e.`Entity`.name AS name, e.`Entity`.mention_count AS mentions "
            f"ORDER BY mentions DESC LIMIT {int(top_n)};"
        )
        return self._exec(stmt)

    @_nebula_fail_soft
    def top_entities_by_degree(self, type: str | None, top_n: int) -> list[dict]:
        from src.graph.nebula_store import _q

        # nebula rejects a WHERE inside OPTIONAL MATCH ("Where clause in optional
        # match is not supported"), so use a plain MATCH with the non-negated
        # filter. Divergence from neo4j's OPTIONAL MATCH: entities with zero
        # non-negated edges are dropped — immaterial for top-N-by-degree (a
        # zero-degree entity never ranks).
        clauses = []
        if type:
            clauses.append(f"e.`Entity`.label == {_q(type)}")
        clauses.append("(r.polarity IS NULL OR r.polarity != 'negated')")
        where = " WHERE " + " AND ".join(clauses)
        stmt = (
            f"MATCH (e:`Entity`)-[r:`RELATED`]-(:`Entity`){where} "
            "RETURN e.`Entity`.name AS name, count(r) AS degree "
            f"ORDER BY degree DESC LIMIT {int(top_n)};"
        )
        return self._exec(stmt)


def build_aggregations_graph_ops(store: Any) -> AggregationsGraphOps:
    if settings.graph.backend == "nebula":
        return NebulaAggregationsGraphOps(store)
    return Neo4jAggregationsGraphOps(store)
