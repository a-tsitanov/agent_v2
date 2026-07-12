"""Backend-dispatched analytics "centrality" graph ops (offline-materialized
reads: top-by-metric + link prediction, read-only, fail-soft).

``Neo4jCentralityGraphOps`` runs the existing Cypher verbatim (moved from
analytics/primitives/centrality.py). ``NebulaCentralityGraphOps`` reads the
centrality columns now materialized on Entity (analytics/materialize.py writes
them in-worker under nebula). link_prediction → [] under nebula: there is no
LIKELY_LINK edge (gds.nodeSimilarity has no in-worker port yet).
"""
from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, Protocol

from loguru import logger

from src.config import settings

_METRICS = ("pagerank", "betweenness", "eigenvector")


class CentralityGraphOps(Protocol):
    def top_central(self, metric: str, type: str | None, top_n: int) -> list[dict]: ...

    def link_prediction(self, name: str, top_n: int) -> list[dict]: ...


class Neo4jCentralityGraphOps:
    def __init__(self, store: Any):
        self._store = store

    def _rows(self, cypher: str, params: dict) -> list[dict]:
        try:
            rows = self._store.structured_query(cypher, param_map=params)
            return list(rows or [])
        except Exception as exc:  # fail-soft, like store_query.run_rows
            logger.warning("analytics query failed: {e}", e=exc)
            return []

    def top_central(self, metric: str, type: str | None, top_n: int) -> list[dict]:
        # metric is allowlist-validated by the caller (centrality.py) before here.
        cypher = (
            f"MATCH (e:__Entity__) WHERE e.{metric} IS NOT NULL "
            "AND ($type IS NULL OR $type IN labels(e)) "
            f"RETURN e.name AS name, e.{metric} AS score ORDER BY e.{metric} DESC LIMIT $top_n"
        )
        return self._rows(cypher, {"type": type, "top_n": top_n, "metric": metric})

    def link_prediction(self, name: str, top_n: int) -> list[dict]:
        cypher = (
            "MATCH (e:__Entity__ {name:$name})-[l:LIKELY_LINK]->(m:__Entity__) "
            "RETURN m.name AS name, l.score AS score ORDER BY l.score DESC LIMIT $top_n"
        )
        return self._rows(cypher, {"name": name, "top_n": top_n})


def _nebula_fail_soft(method: Callable[..., list[dict]]) -> Callable[..., list[dict]]:
    @functools.wraps(method)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> list[dict]:
        try:
            return method(self, *args, **kwargs)
        except Exception as exc:  # fail-soft, like store_query.run_rows
            logger.warning("analytics query failed: {e}", e=exc)
            return []

    return wrapper


class NebulaCentralityGraphOps:
    def __init__(self, store: Any):
        self._store = store

    def _exec(self, stmt: str) -> list[dict]:
        return list(self._store.structured_query(stmt) or [])

    @_nebula_fail_soft
    def top_central(self, metric: str, type: str | None, top_n: int) -> list[dict]:
        from src.graph.nebula_store import _q

        if metric not in _METRICS:  # guard the inlined column name
            return []
        # nebula default is 0 (unset) — treat > 0 as materialised (neo4j uses
        # IS NOT NULL; a 0-centrality entity never ranks in the top anyway).
        where = f"e.`Entity`.{metric} > 0"
        if type:
            where += f" AND e.`Entity`.label == {_q(type)}"
        stmt = (
            f"MATCH (e:`Entity`) WHERE {where} "
            f"RETURN e.`Entity`.name AS name, e.`Entity`.{metric} AS score "
            f"ORDER BY score DESC LIMIT {int(top_n)};"
        )
        return self._exec(stmt)

    @_nebula_fail_soft
    def link_prediction(self, name: str, top_n: int) -> list[dict]:
        # No LIKELY_LINK edge under nebula (gds.nodeSimilarity has no in-worker
        # port yet). Documented degrade.
        return []


def build_centrality_graph_ops(store: Any) -> CentralityGraphOps:
    if settings.graph.backend == "nebula":
        return NebulaCentralityGraphOps(store)
    return Neo4jCentralityGraphOps(store)
