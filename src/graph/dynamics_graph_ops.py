"""Backend-dispatched analytics "dynamics" graph ops (temporal validity reads,
read-only, fail-soft): relationship_timeline / polarity_evolution / whats_changed.

topic_trend / entity_activity are NOT here — they traverse (:Chunk), which isn't a
nebula graph node, so they degrade to [] via run_rows fail-soft (no nebula-specific
logic to add).

``Neo4jDynamicsGraphOps`` runs the existing Cypher verbatim (moved from
``analytics/primitives/dynamics.py``). ``NebulaDynamicsGraphOps`` translates to
nGQL now that RELATED.valid_from/valid_to are STRING (see nebula_schema): near-
verbatim MATCH with ``substr`` (neo4j ``substring``), ``r.rel_type`` (neo4j
``type(r)``), and string range compares on the ISO validity window.

Divergence (documented): whats_changed's third match branch — edges with NO
validity window, surfaced by the E1 first-seen ``created_at`` fallback — cannot
fire under nebula because RELATED has no ``created_at`` column yet (deferred REL
first-seen). So nebula whats_changed surfaces only edges with a validity window in
range (the 'appeared'/'ended' cases), never the 'first_seen' case.
"""
from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, Protocol

from loguru import logger

from src.config import settings

_RELATIONSHIP_TIMELINE = (
    "MATCH (e:__Entity__ {name:$name})-[r]-(n:__Entity__) "
    "WHERE r.valid_from IS NOT NULL AND ($rel_type IS NULL OR type(r)=$rel_type) "
    "RETURN substring(r.valid_from,0,7) AS period, type(r) AS rel, n.name AS name, "
    "r.polarity AS polarity "
    "ORDER BY period"
)
_POLARITY_EVOLUTION = (
    "MATCH (e:__Entity__)-[r]-(:__Entity__) "
    "WHERE r.valid_from IS NOT NULL AND ($name IS NULL OR e.name=$name) "
    "AND ($rel_type IS NULL OR type(r)=$rel_type) "
    "RETURN substring(r.valid_from,0,7) AS period, r.polarity AS polarity, count(*) AS n "
    "ORDER BY period"
)
_WHATS_CHANGED = (
    "MATCH (e:__Entity__)-[r]-(n:__Entity__) "
    "WHERE type(r) <> 'LIKELY_LINK' AND ($entity IS NULL OR e.name=$entity) AND "
    "((r.valid_from >= $from AND r.valid_from <= $to) OR "
    "(r.valid_to >= $from AND r.valid_to <= $to) OR "
    "(r.valid_from IS NULL AND r.valid_to IS NULL AND $from_epoch IS NOT NULL "
    "AND r.created_at >= $from_epoch AND r.created_at <= $to_epoch)) "
    "RETURN e.name AS name, type(r) AS rel, n.name AS other, r.polarity AS polarity, "
    "r.valid_from AS valid_from, r.valid_to AS valid_to, r.created_at AS created_at, "
    "CASE WHEN r.valid_from >= $from AND r.valid_from <= $to THEN 'appeared' "
    "WHEN r.valid_to >= $from AND r.valid_to <= $to THEN 'ended' "
    "ELSE 'first_seen' END AS change "
    "ORDER BY coalesce(r.valid_from,r.valid_to) LIMIT $top_n"
)


class DynamicsGraphOps(Protocol):
    def relationship_timeline(self, name: str, rel_type: str | None) -> list[dict]: ...

    def polarity_evolution(self, name: str | None, rel_type: str | None) -> list[dict]: ...

    def whats_changed(
        self,
        date_from: str,
        date_to: str,
        entity: str | None,
        from_epoch: int | None,
        to_epoch: int | None,
        top_n: int,
    ) -> list[dict]: ...


class Neo4jDynamicsGraphOps:
    def __init__(self, store: Any):
        self._store = store

    def _rows(self, cypher: str, params: dict) -> list[dict]:
        try:
            rows = self._store.structured_query(cypher, param_map=params)
            return list(rows or [])
        except Exception as exc:  # fail-soft, like store_query.run_rows
            logger.warning("analytics query failed: {e}", e=exc)
            return []

    def relationship_timeline(self, name: str, rel_type: str | None) -> list[dict]:
        return self._rows(_RELATIONSHIP_TIMELINE, {"name": name, "rel_type": rel_type})

    def polarity_evolution(self, name: str | None, rel_type: str | None) -> list[dict]:
        return self._rows(_POLARITY_EVOLUTION, {"name": name, "rel_type": rel_type})

    def whats_changed(
        self,
        date_from: str,
        date_to: str,
        entity: str | None,
        from_epoch: int | None,
        to_epoch: int | None,
        top_n: int,
    ) -> list[dict]:
        return self._rows(
            _WHATS_CHANGED,
            {
                "from": date_from,
                "to": date_to,
                "from_epoch": from_epoch,
                "to_epoch": to_epoch,
                "entity": entity,
                "top_n": top_n,
            },
        )


def _nebula_fail_soft(method: Callable[..., list[dict]]) -> Callable[..., list[dict]]:
    @functools.wraps(method)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> list[dict]:
        try:
            return method(self, *args, **kwargs)
        except Exception as exc:  # fail-soft, like store_query.run_rows
            logger.warning("analytics query failed: {e}", e=exc)
            return []

    return wrapper


class NebulaDynamicsGraphOps:
    def __init__(self, store: Any):
        self._store = store

    def _exec(self, stmt: str) -> list[dict]:
        return list(self._store.structured_query(stmt) or [])

    @_nebula_fail_soft
    def relationship_timeline(self, name: str, rel_type: str | None) -> list[dict]:
        from src.graph.nebula_store import _q

        rel_clause = f" AND r.rel_type == {_q(rel_type)}" if rel_type else ""
        stmt = (
            f"MATCH (e:`Entity`)-[r:`RELATED`]-(n:`Entity`) "
            f"WHERE e.`Entity`.name == {_q(name)} AND r.valid_from != ''{rel_clause} "
            "RETURN substr(r.valid_from,0,7) AS period, r.rel_type AS rel, "
            "n.`Entity`.name AS name, r.polarity AS polarity "
            "ORDER BY period;"
        )
        return self._exec(stmt)

    @_nebula_fail_soft
    def polarity_evolution(self, name: str | None, rel_type: str | None) -> list[dict]:
        from src.graph.nebula_store import _q

        clauses = ["r.valid_from != ''"]
        if name:
            clauses.append(f"e.`Entity`.name == {_q(name)}")
        if rel_type:
            clauses.append(f"r.rel_type == {_q(rel_type)}")
        where = " AND ".join(clauses)
        stmt = (
            f"MATCH (e:`Entity`)-[r:`RELATED`]-(:`Entity`) WHERE {where} "
            "RETURN substr(r.valid_from,0,7) AS period, r.polarity AS polarity, "
            "count(*) AS n ORDER BY period;"
        )
        return self._exec(stmt)

    @_nebula_fail_soft
    def whats_changed(
        self,
        date_from: str,
        date_to: str,
        entity: str | None,
        from_epoch: int | None,
        to_epoch: int | None,
        top_n: int,
    ) -> list[dict]:
        from src.graph.nebula_store import _q

        # The created_at-fallback branch (windowless edges surfaced by first-seen)
        # can't fire: RELATED has no created_at under nebula (deferred REL
        # first-seen). Match only edges whose validity window overlaps [from,to];
        # classify appeared/ended + sort in Python (nebula ORDER BY can't take
        # coalesce, and CASE stays simpler in Python).
        clauses = ["r.rel_type != 'LIKELY_LINK'"]
        if entity:
            clauses.append(f"e.`Entity`.name == {_q(entity)}")
        f, t = _q(date_from), _q(date_to)
        clauses.append(
            f"((r.valid_from >= {f} AND r.valid_from <= {t}) "
            f"OR (r.valid_to >= {f} AND r.valid_to <= {t}))"
        )
        where = " AND ".join(clauses)
        stmt = (
            f"MATCH (e:`Entity`)-[r:`RELATED`]-(n:`Entity`) WHERE {where} "
            "RETURN e.`Entity`.name AS name, r.rel_type AS rel, n.`Entity`.name AS other, "
            "r.polarity AS polarity, r.valid_from AS valid_from, r.valid_to AS valid_to;"
        )
        rows = self._exec(stmt)
        out = []
        for row in rows:
            vf = row.get("valid_from") or ""
            vt = row.get("valid_to") or ""
            if date_from <= vf <= date_to:
                change = "appeared"
            elif date_from <= vt <= date_to:
                change = "ended"
            else:
                continue  # only window-overlapping edges reach here
            out.append({
                "name": row.get("name"),
                "rel": row.get("rel"),
                "other": row.get("other"),
                "polarity": row.get("polarity"),
                "valid_from": vf,
                "valid_to": vt,
                "created_at": None,  # absent on nebula RELATED (row-shape parity)
                "change": change,
            })
        out.sort(key=lambda r: r["valid_from"] or r["valid_to"])
        return out[:top_n]


def build_dynamics_graph_ops(store: Any) -> DynamicsGraphOps:
    if settings.graph.backend == "nebula":
        return NebulaDynamicsGraphOps(store)
    return Neo4jDynamicsGraphOps(store)
