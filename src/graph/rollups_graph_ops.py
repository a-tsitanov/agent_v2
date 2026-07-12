"""Backend-dispatched analytics "rollups" graph op (numeric Amount rollup,
read-only, fail-soft). The amount PARSING + aggregation stays in the primitive;
the seam only returns the raw (counterparty, amount) edge rows.

``Neo4jRollupsGraphOps`` runs the existing Cypher verbatim (moved from
``analytics/primitives/rollups.py``). ``NebulaRollupsGraphOps`` = MATCH neighbour
with ``label == 'Amount'`` (near-verbatim). Fail-soft per method.
"""
from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, Protocol

from loguru import logger

from src.config import settings

_AMOUNT_EDGES = (
    "MATCH (e:__Entity__)-[]-(a:__Entity__:Amount) "
    "WHERE ($cp IS NULL OR e.name=$cp) "
    "RETURN e.name AS counterparty, a.name AS amount"
)


class RollupsGraphOps(Protocol):
    def amount_edges(self, counterparty: str | None) -> list[dict]: ...


class Neo4jRollupsGraphOps:
    def __init__(self, store: Any):
        self._store = store

    def amount_edges(self, counterparty: str | None) -> list[dict]:
        try:
            rows = self._store.structured_query(_AMOUNT_EDGES, param_map={"cp": counterparty})
            return list(rows or [])
        except Exception as exc:  # fail-soft, like store_query.run_rows
            logger.warning("analytics query failed: {e}", e=exc)
            return []


def _nebula_fail_soft(method: Callable[..., list[dict]]) -> Callable[..., list[dict]]:
    @functools.wraps(method)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> list[dict]:
        try:
            return method(self, *args, **kwargs)
        except Exception as exc:  # fail-soft, like store_query.run_rows
            logger.warning("analytics query failed: {e}", e=exc)
            return []

    return wrapper


class NebulaRollupsGraphOps:
    def __init__(self, store: Any):
        self._store = store

    @_nebula_fail_soft
    def amount_edges(self, counterparty: str | None) -> list[dict]:
        from src.graph.nebula_store import _q

        where = "a.`Entity`.label == 'Amount'"
        if counterparty:
            where += f" AND e.`Entity`.name == {_q(counterparty)}"
        stmt = (
            f"MATCH (e:`Entity`)-[:`RELATED`]-(a:`Entity`) WHERE {where} "
            "RETURN e.`Entity`.name AS counterparty, a.`Entity`.name AS amount;"
        )
        return list(self._store.structured_query(stmt) or [])


def build_rollups_graph_ops(store: Any) -> RollupsGraphOps:
    if settings.graph.backend == "nebula":
        return NebulaRollupsGraphOps(store)
    return Neo4jRollupsGraphOps(store)
