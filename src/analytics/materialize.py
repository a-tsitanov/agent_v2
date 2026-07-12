"""Offline GDS compute + write-back into Neo4j. Mirrors src/graph/communities.py.

Under GRAPH_BACKEND=nebula there is no GDS: centrality is computed in-worker via
igraph (analytics/centrality_compute.py, off the edge-export seam) and written
back with nGQL UPDATE VERTEX. Link-prediction (gds.nodeSimilarity) has no in-worker
port yet — it is a documented no-op under nebula (returns 0)."""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

from src.config import settings
from src.graph.communities import _drop_cypher, _new_graph_name, _project_cypher  # noqa: F401

# metric -> GDS stream cypher (graph name f-substituted; weighted where applicable)
_CENTRALITY_STREAM: dict[str, str] = {
    "pagerank": (
        "CALL gds.pageRank.stream('{g}', {{relationshipWeightProperty:'weight'}}) "
        "YIELD nodeId, score RETURN gds.util.asNode(nodeId).name AS name, score"
    ),
    "betweenness": (
        "CALL gds.betweenness.stream('{g}') "
        "YIELD nodeId, score RETURN gds.util.asNode(nodeId).name AS name, score"
    ),
    "eigenvector": (
        "CALL gds.eigenvector.stream('{g}', {{relationshipWeightProperty:'weight'}}) "
        "YIELD nodeId, score RETURN gds.util.asNode(nodeId).name AS name, score"
    ),
}


def _run_query(store: Any, cypher: str, params: dict | None = None) -> list[dict]:
    return list(store.structured_query(cypher, param_map=params or {}))


async def _run(store: Any, cypher: str, params: dict | None = None) -> list[dict]:
    return await asyncio.to_thread(_run_query, store, cypher, params)


async def write_centrality(store: Any | None, graph_name: str, metric: str) -> int:
    """Run the GDS centrality stream for ``metric`` and write scores back to __Entity__ nodes.

    ``metric`` must be one of ``{pagerank, betweenness, eigenvector}`` — validated
    against the fixed allowlist BEFORE being inlined into the write-back Cypher.
    Returns the number of rows written, or 0 on None store.  Propagates GDS errors.
    """
    if store is None:
        return 0
    if metric not in _CENTRALITY_STREAM:
        raise ValueError(f"unknown centrality metric: {metric!r}")
    if settings.graph.backend == "nebula":
        return await _write_centrality_nebula(store, metric)
    rows = await _run(store, _CENTRALITY_STREAM[metric].format(g=graph_name))
    if not rows:
        return 0
    # metric is from the fixed allowlist above — safe to inline as a property key
    write = f"UNWIND $rows AS r MATCH (e:__Entity__ {{name: r.name}}) SET e.{metric} = r.score"
    await _run(store, write, {"rows": rows})
    return len(rows)


def _write_centrality_nebula_sync(store: Any, metric: str) -> int:
    """Compute ``metric`` in-worker (igraph over the edge-export graph) and write
    it to Entity vertices via nGQL UPDATE VERTEX. metric is allowlist-validated,
    safe to inline as a column. Fail-soft per vertex (a missing/ER-merged vertex
    must not abort the rest). Returns rows written."""
    from src.analytics.centrality_compute import compute_all
    from src.graph.nebula_store import entity_vid

    scores = compute_all(store).get(metric, {})
    if not scores:
        return 0
    written = 0
    for name, score in scores.items():
        stmt = (
            f'UPDATE VERTEX ON `Entity` "{entity_vid(name)}" '
            f"SET {metric} = {float(score)};"
        )
        try:
            store.structured_query(stmt)
            written += 1
        except Exception as exc:  # one missing vertex must not stop the rest
            logger.debug("centrality write skipped for {n}: {e}", n=name, e=exc)
    return written


async def _write_centrality_nebula(store: Any, metric: str) -> int:
    return await asyncio.to_thread(_write_centrality_nebula_sync, store, metric)


async def write_link_prediction(
    store: Any | None,
    graph_name: str,
    *,
    top_k: int,
    min_score: float,
) -> int:
    """Full-refresh :LIKELY_LINK edges via gds.nodeSimilarity.stream.

    Deletes ALL existing :LIKELY_LINK relationships, streams nodeSimilarity
    (topK), and writes MERGE pairs whose score >= ``min_score``.
    Returns the number of pairs written, or 0 on None store / any error (fail-soft).
    """
    if store is None:
        return 0
    if settings.graph.backend == "nebula":
        # gds.nodeSimilarity has no in-worker port yet; no LIKELY_LINK edge type
        # under nebula. Documented no-op — link_prediction reads return [].
        logger.debug("link_prediction is a no-op under nebula (no in-worker port)")
        return 0
    await _run(store, "MATCH ()-[l:LIKELY_LINK]->() DELETE l")  # full refresh
    rows = await _run(
        store,
        f"CALL gds.nodeSimilarity.stream('{graph_name}', {{topK: $k}}) "
        "YIELD node1, node2, similarity "
        "RETURN gds.util.asNode(node1).name AS a, gds.util.asNode(node2).name AS b, "
        "similarity AS score",
        {"k": int(top_k)},
    )
    pairs = [r for r in rows if float(r.get("score", 0.0)) >= min_score]
    if pairs:
        await _run(
            store,
            "UNWIND $pairs AS p "
            "MATCH (a:__Entity__ {name:p.a}), (b:__Entity__ {name:p.b}) "
            "MERGE (a)-[l:LIKELY_LINK]->(b) SET l.score = p.score",
            {"pairs": pairs},
        )
    return len(pairs)
