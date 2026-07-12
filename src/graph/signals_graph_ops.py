"""Backend-dispatched analytics "signals" graph ops (P2 decision-ready queues,
read-only, fail-soft).

``Neo4jSignalsGraphOps`` runs the existing Cypher verbatim (constants MOVED here
from ``analytics/primitives/signals.py``). ``NebulaSignalsGraphOps``:
- ``recommended_merges``: fetch non-id entity names, group by case/space-
  insensitive display name in Python (nebula MATCH has no toLower/trim).
- ``review_queue``: fetch each Organization's neighbour labels, classify shell
  orgs (deg>0 and every neighbour is an identifier) in Python.
- ``circular_ownership``: var-len ``RELATED*2..6`` with ``all(rel.rel_type ==
  'OWNS')`` (cluster-verified), sort by cycle length in Python.
- ``risk_score`` / ``investigate_next`` → ``[]``: no ``risk_score`` / ``risk_band``
  / ``completeness_score`` columns on the nebula Entity tag (Tier-B risk
  materialize). Already ``[]`` via run_rows fail-soft; made explicit here.
Every method is fail-soft.
"""
from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, Protocol

from loguru import logger

from src.analytics.ids import ID_TYPES
from src.config import settings

# defensive cap on var-len path enumeration under nebula (neo4j has none; a
# circular-ownership red flag is rare, so this only guards pathological graphs).
_CYCLE_SCAN_CAP = 10000

_RISK_SCORE = (
    "MATCH (e:__Entity__) WHERE e.risk_score IS NOT NULL "
    "AND ($name IS NULL OR e.name=$name) AND ($band IS NULL OR e.risk_band=$band) "
    "RETURN e.name AS name, e.risk_score AS score, e.risk_band AS band, "
    "e.risk_components AS components ORDER BY e.risk_score DESC LIMIT $top_n"
)
_INVESTIGATE_NEXT = (
    "MATCH (e:__Entity__) WHERE e.risk_score IS NOT NULL "
    "RETURN e.name AS name, e.risk_score AS risk, "
    "coalesce(e.completeness_score, 0.0) AS completeness "
    "ORDER BY e.risk_score DESC, coalesce(e.completeness_score, 0.0) ASC LIMIT $top_n"
)
_RECOMMENDED_MERGES = (
    "MATCH (e:__Entity__) WHERE NONE(l IN labels(e) WHERE l IN $id_types) "
    "WITH toLower(trim(e.name)) AS key, collect(e.name) AS names, count(e) AS count "
    "WHERE count > 1 RETURN key, names, count ORDER BY count DESC LIMIT $top_n"
)
_REVIEW_QUEUE = (
    "MATCH (e:__Entity__:Organization) "
    "OPTIONAL MATCH (e)-[]-(n:__Entity__) "
    "WITH e, count(n) AS deg, "
    "sum(CASE WHEN any(l IN labels(n) WHERE l IN $id_types) THEN 1 ELSE 0 END) "
    "AS id_links "
    "WHERE deg > 0 AND deg = id_links "
    "RETURN e.name AS name, deg AS degree, 'shell_signal' AS flag ORDER BY deg DESC LIMIT $top_n"
)
_CIRCULAR_OWNERSHIP = (
    "MATCH p=(a:__Entity__)-[:OWNS*2..6]->(a) "
    "RETURN [n IN nodes(p) | n.name] AS cycle ORDER BY size(cycle) DESC LIMIT $top_n"
)


class SignalsGraphOps(Protocol):
    def risk_score(self, name: str | None, band: str | None, top_n: int) -> list[dict]: ...

    def investigate_next(self, top_n: int) -> list[dict]: ...

    def recommended_merges(self, top_n: int) -> list[dict]: ...

    def review_queue(self, top_n: int) -> list[dict]: ...

    def circular_ownership(self, top_n: int) -> list[dict]: ...


class Neo4jSignalsGraphOps:
    def __init__(self, store: Any):
        self._store = store

    def _rows(self, cypher: str, params: dict) -> list[dict]:
        try:
            rows = self._store.structured_query(cypher, param_map=params)
            return list(rows or [])
        except Exception as exc:  # fail-soft, like store_query.run_rows
            logger.warning("analytics query failed: {e}", e=exc)
            return []

    def risk_score(self, name: str | None, band: str | None, top_n: int) -> list[dict]:
        return self._rows(_RISK_SCORE, {"name": name, "band": band, "top_n": top_n})

    def investigate_next(self, top_n: int) -> list[dict]:
        return self._rows(_INVESTIGATE_NEXT, {"top_n": top_n})

    def recommended_merges(self, top_n: int) -> list[dict]:
        return self._rows(_RECOMMENDED_MERGES, {"top_n": top_n, "id_types": ID_TYPES})

    def review_queue(self, top_n: int) -> list[dict]:
        return self._rows(_REVIEW_QUEUE, {"top_n": top_n, "id_types": ID_TYPES})

    def circular_ownership(self, top_n: int) -> list[dict]:
        return self._rows(_CIRCULAR_OWNERSHIP, {"top_n": top_n})


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


class NebulaSignalsGraphOps:
    def __init__(self, store: Any):
        self._store = store

    def _exec(self, stmt: str) -> list[dict]:
        return list(self._store.structured_query(stmt) or [])

    @_nebula_fail_soft
    def risk_score(self, name: str | None, band: str | None, top_n: int) -> list[dict]:
        # No risk_score/risk_band columns on the nebula Entity tag (Tier-B risk
        # materialize) — cannot be surfaced. Documented degrade.
        return []

    @_nebula_fail_soft
    def investigate_next(self, top_n: int) -> list[dict]:
        # No risk_score/completeness_score columns (Tier-B). Documented degrade.
        return []

    @_nebula_fail_soft
    def recommended_merges(self, top_n: int) -> list[dict]:
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

    @_nebula_fail_soft
    def review_queue(self, top_n: int) -> list[dict]:
        # Shell orgs: every neighbour is an identifier. Fetch neighbour labels
        # (nebula rejects the sum(CASE ...) + WHERE-in-OPTIONAL) and classify in
        # Python.
        stmt = (
            "MATCH (e:`Entity`) WHERE e.`Entity`.label == 'Organization' "
            "OPTIONAL MATCH (e)-[:`RELATED`]-(n:`Entity`) "
            "RETURN e.`Entity`.name AS name, collect(n.`Entity`.label) AS neighbor_labels;"
        )
        id_set = set(ID_TYPES)
        out = []
        for row in self._exec(stmt):
            labels = [lbl for lbl in (row.get("neighbor_labels") or []) if lbl is not None]
            deg = len(labels)
            id_links = sum(1 for lbl in labels if lbl in id_set)
            if deg > 0 and deg == id_links:
                out.append({"name": row.get("name"), "degree": deg, "flag": "shell_signal"})
        out.sort(key=lambda r: r["degree"], reverse=True)
        return out[:top_n]

    @_nebula_fail_soft
    def circular_ownership(self, top_n: int) -> list[dict]:
        # var-len OWNS cycle (rel_type property, not an edge type); ORDER BY
        # size(cycle) is a computed expr nebula can't sort on, so sort in Python.
        stmt = (
            "MATCH p=(a:`Entity`)-[e:`RELATED`*2..6]->(a) "
            "WHERE all(rel IN e WHERE rel.rel_type == 'OWNS') "
            "RETURN [n IN nodes(p) | n.`Entity`.name] AS cycle "
            f"LIMIT {_CYCLE_SCAN_CAP};"
        )
        rows = [{"cycle": row.get("cycle") or []} for row in self._exec(stmt)]
        rows.sort(key=lambda r: len(r["cycle"]), reverse=True)
        return rows[:top_n]


def build_signals_graph_ops(store: Any) -> SignalsGraphOps:
    if settings.graph.backend == "nebula":
        return NebulaSignalsGraphOps(store)
    return Neo4jSignalsGraphOps(store)
