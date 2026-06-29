"""Offline GDS compute + write-back into Neo4j. Mirrors src/graph/communities.py."""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

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
    Returns the number of rows written, or 0 on None store / any error (fail-soft).
    """
    if store is None:
        return 0
    if metric not in _CENTRALITY_STREAM:
        raise ValueError(f"unknown centrality metric: {metric!r}")
    try:
        rows = await _run(store, _CENTRALITY_STREAM[metric].format(g=graph_name))
        if not rows:
            return 0
        # metric is from the fixed allowlist above — safe to inline as a property key
        write = f"UNWIND $rows AS r MATCH (e:__Entity__ {{name: r.name}}) SET e.{metric} = r.score"
        await _run(store, write, {"rows": rows})
        return len(rows)
    except Exception as exc:
        logger.warning("write_centrality {m} failed: {e}", m=metric, e=exc)
        return 0


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
    try:
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
    except Exception as exc:
        logger.warning("write_link_prediction failed: {e}", e=exc)
        return 0
