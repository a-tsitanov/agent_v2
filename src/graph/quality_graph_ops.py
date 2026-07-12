"""Backend-dispatched analytics "quality" graph ops (read-only, fail-soft
knowledge-quality flags: contradictions / orphans / incomplete / merge).

``Neo4jQualityGraphOps`` runs the existing Cypher verbatim (constants MOVED
here from ``analytics/primitives/quality.py``; default path byte-for-byte
unchanged), fail-soft per ``analytics/store_query.py::run_rows``.

``NebulaQualityGraphOps`` translates to nGQL per
``docs/superpowers/nebula-analytics-ngql-rules-2026-07-11.md``. Two reads
(contradictions, orphans) port near-verbatim as ``MATCH`` (nebula 3.8 supports
OPTIONAL MATCH degree-0 retention, ``WITH ... WHERE`` post-aggregation filters,
two-MATCH patterns, and ``collect`` — all cluster-verified). Two reads
(incomplete_entities, merge_candidates) fetch a simple projection and do the
grouping/completeness math in Python (nebula MATCH has no ``toLower``/``trim``
and rejects a WHERE inside OPTIONAL MATCH). Every method is fail-soft.

Divergences under nebula (documented, minor):
- ``contradictions``: ``RELATED`` has no ``source_chunks`` column, so
  ``affirmed_chunks``/``negated_chunks`` come back empty (detection still
  works); neo4j ``valid_from IS NULL`` (open window) maps to ``== 0`` since
  nebula edge props are non-nullable; the ``id(r1)<id(r2)`` ordering guard is
  dropped (the affirmed/negated polarity asymmetry already prevents the
  self/duplicate pairing).
- ``incomplete_entities``/``merge_candidates`` pull the type-scoped / non-id
  entity set and aggregate client-side (O(N) — a Tier-B concern).
"""
from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, Protocol

from loguru import logger

from src.analytics.ids import ID_TYPES
from src.config import settings

# ── quality Cypher (moved verbatim from analytics/primitives/quality.py) ──

_CONTRADICTIONS = (
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
_ORPHANS = (
    "MATCH (e:__Entity__) WHERE NONE(l IN labels(e) WHERE l IN $id_types) "
    "OPTIONAL MATCH (e)-[r]-(:__Entity__) "
    "WITH e, count(r) AS degree WHERE degree < $min_degree "
    "RETURN e.name AS name, degree, "
    "[l IN labels(e) WHERE l<>'__Entity__' AND l<>'__Node__'][0] AS type "
    "ORDER BY degree ASC LIMIT $top_n"
)
_INCOMPLETE = (
    "MATCH (e:__Entity__) WHERE $type IN labels(e) "
    "OPTIONAL MATCH (e)-[]-(id:__Entity__) WHERE any(l IN labels(id) WHERE l IN $expected) "
    "WITH e, collect(DISTINCT [l IN labels(id) WHERE l IN $expected][0]) AS have "
    "RETURN e.name AS name, [x IN $expected WHERE NOT x IN have] AS missing, have "
    "ORDER BY size([x IN $expected WHERE NOT x IN have]) DESC LIMIT $top_n"
)
_MERGE_CANDIDATES = (
    "MATCH (e:__Entity__) WHERE NONE(l IN labels(e) WHERE l IN $id_types) "
    "WITH toLower(trim(e.name)) AS key, count(e) AS count, collect(e.name) AS names "
    "WHERE count > 1 "
    "RETURN key, names, count ORDER BY count DESC LIMIT $top_n"
)


class QualityGraphOps(Protocol):
    def contradictions(self, top_n: int) -> list[dict]: ...

    def orphans(self, min_degree: int, top_n: int) -> list[dict]: ...

    def incomplete_entities(self, type: str, expected: list[str], top_n: int) -> list[dict]: ...

    def merge_candidates(self, top_n: int) -> list[dict]: ...


class Neo4jQualityGraphOps:
    """Runs the historical quality Cypher verbatim — zero behaviour change.
    Fail-soft per method (mirrors ``store_query.run_rows``)."""

    def __init__(self, store: Any):
        self._store = store

    def _rows(self, cypher: str, params: dict) -> list[dict]:
        try:
            rows = self._store.structured_query(cypher, param_map=params)
            return list(rows or [])
        except Exception as exc:  # fail-soft, like store_query.run_rows
            logger.warning("analytics query failed: {e}", e=exc)
            return []

    def contradictions(self, top_n: int) -> list[dict]:
        return self._rows(_CONTRADICTIONS, {"top_n": top_n})

    def orphans(self, min_degree: int, top_n: int) -> list[dict]:
        params = {"min_degree": min_degree, "top_n": top_n, "id_types": ID_TYPES}
        return self._rows(_ORPHANS, params)

    def incomplete_entities(self, type: str, expected: list[str], top_n: int) -> list[dict]:
        params = {"type": type, "expected": expected, "top_n": top_n}
        return self._rows(_INCOMPLETE, params)

    def merge_candidates(self, top_n: int) -> list[dict]:
        return self._rows(_MERGE_CANDIDATES, {"top_n": top_n, "id_types": ID_TYPES})


def _nebula_fail_soft(method: Callable[..., list[dict]]) -> Callable[..., list[dict]]:
    @functools.wraps(method)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> list[dict]:
        try:
            return method(self, *args, **kwargs)
        except Exception as exc:  # fail-soft, like store_query.run_rows
            logger.warning("analytics query failed: {e}", e=exc)
            return []

    return wrapper


def _list_literal(items: list[str]) -> str:
    from src.graph.nebula_store import _q

    return "[" + ", ".join(_q(v) for v in items) + "]"


class NebulaQualityGraphOps:
    """nGQL quality graph ops — MATCH for contradictions/orphans, query+Python
    for incomplete/merge (see module docstring). Fail-soft per method."""

    def __init__(self, store: Any):
        self._store = store

    def _exec(self, stmt: str) -> list[dict]:
        return list(self._store.structured_query(stmt) or [])

    @_nebula_fail_soft
    def contradictions(self, top_n: int) -> list[dict]:
        # Two-MATCH detection of affirmed+negated same-(a,rel,b) with overlapping
        # validity windows. `source_chunks` is absent on nebula RELATED → chunks
        # come back empty; `IS NULL` (open window) → `== 0`; `id(r1)<id(r2)`
        # dropped (polarity asymmetry already prevents self/duplicate pairing).
        stmt = (
            "MATCH (a:`Entity`)-[r1:`RELATED`]->(b:`Entity`) "
            "MATCH (a)-[r2:`RELATED`]->(b) "
            "WHERE r1.rel_type == r2.rel_type "
            "AND r1.polarity == 'affirmed' AND r2.polarity == 'negated' "
            "AND (r1.valid_from == 0 OR r2.valid_to == 0 OR r1.valid_from <= r2.valid_to) "
            "AND (r2.valid_from == 0 OR r1.valid_to == 0 OR r2.valid_from <= r1.valid_to) "
            "RETURN a.`Entity`.name AS a, r1.rel_type AS rel, b.`Entity`.name AS b "
            f"LIMIT {int(top_n)};"
        )
        rows = self._exec(stmt)
        for row in rows:
            row["affirmed_chunks"] = []
            row["negated_chunks"] = []
        return rows

    @_nebula_fail_soft
    def orphans(self, min_degree: int, top_n: int) -> list[dict]:
        stmt = (
            f"MATCH (e:`Entity`) WHERE e.`Entity`.label NOT IN {_list_literal(ID_TYPES)} "
            "OPTIONAL MATCH (e)-[r:`RELATED`]-(:`Entity`) "
            f"WITH e, count(r) AS degree WHERE degree < {int(min_degree)} "
            "RETURN e.`Entity`.name AS name, degree, e.`Entity`.label AS type "
            f"ORDER BY degree ASC LIMIT {int(top_n)};"
        )
        return self._exec(stmt)

    @_nebula_fail_soft
    def incomplete_entities(self, type: str, expected: list[str], top_n: int) -> list[dict]:
        from src.graph.nebula_store import _q

        if not expected:
            return []
        # Collect every neighbour label (nebula rejects a WHERE inside OPTIONAL
        # MATCH), then compute have/missing against `expected` in Python.
        stmt = (
            f"MATCH (e:`Entity`) WHERE e.`Entity`.label == {_q(type)} "
            "OPTIONAL MATCH (e)-[:`RELATED`]-(idn:`Entity`) "
            "RETURN e.`Entity`.name AS name, collect(idn.`Entity`.label) AS have;"
        )
        expected_set = set(expected)
        out = []
        for row in self._exec(stmt):
            labels = [lbl for lbl in (row.get("have") or []) if lbl in expected_set]
            have = sorted(set(labels))
            missing = [x for x in expected if x not in have]
            out.append({"name": row.get("name"), "missing": missing, "have": have})
        out.sort(key=lambda r: len(r["missing"]), reverse=True)
        return out[:top_n]

    @_nebula_fail_soft
    def merge_candidates(self, top_n: int) -> list[dict]:
        # Group non-identifier entities by case/space-insensitive display name in
        # Python (nebula MATCH has no toLower/trim). O(N) over non-id entities.
        stmt = (
            f"MATCH (e:`Entity`) WHERE e.`Entity`.label NOT IN {_list_literal(ID_TYPES)} "
            "RETURN e.`Entity`.name AS name;"
        )
        groups: dict[str, list[str]] = {}
        for row in self._exec(stmt):
            name = row.get("name")
            if name is None:
                continue
            groups.setdefault(name.strip().lower(), []).append(name)
        out = [
            {"key": key, "names": names, "count": len(names)}
            for key, names in groups.items()
            if len(names) > 1
        ]
        out.sort(key=lambda r: r["count"], reverse=True)
        return out[:top_n]


def build_quality_graph_ops(store: Any) -> QualityGraphOps:
    if settings.graph.backend == "nebula":
        return NebulaQualityGraphOps(store)
    return Neo4jQualityGraphOps(store)
