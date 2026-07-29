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


# Max chars in ONE nGQL request.  Nebula rejects anything over
# `max_allowed_query_size` (default 4 MiB) with `SyntaxError: Query is too
# large` — the same cap that broke the community write-back.  Batches are
# bounded by RENDERED SIZE, not vertex count.  Module-level so tests can
# monkeypatch it small.
_MAX_STMT_CHARS = 1_000_000


def _centrality_update_stmt(vid: str, per_metric: dict[str, float]) -> str:
    """ONE ``UPDATE VERTEX`` carrying EVERY metric for this vertex.

    Metric names come from the ``_CENTRALITY_STREAM`` allowlist (validated by
    the callers below), so they are safe to inline as column names."""
    sets = ", ".join(f"{k} = {float(v)}" for k, v in per_metric.items())
    return f'UPDATE VERTEX ON `Entity` "{vid}" SET {sets};'


def _flush_centrality_batch(store: Any, stmts: list[str]) -> int:
    """Send ``stmts`` as ONE multi-statement request; on failure retry them one
    at a time.

    Batching is what removes the round-trip per vertex.  The per-statement
    fallback preserves the fail-soft contract: an ER-merged or deleted vertex
    raises ``Storage Error: Vertex or edge not found`` and must not take the
    rest of its batch down with it."""
    if not stmts:
        return 0
    try:
        store.structured_query("\n".join(stmts))
        return len(stmts)
    except Exception:
        written = 0
        for stmt in stmts:
            try:
                store.structured_query(stmt)
                written += 1
            except Exception as exc:  # one missing vertex must not stop the rest
                logger.debug("centrality write skipped: {e}", e=exc)
        return written


def _write_centrality_nebula_all_sync(store: Any, metrics: list[str]) -> int:
    """Compute EVERY metric from ONE export + ONE igraph build, write batched.

    ``compute_all`` computes all three metrics per call, so calling it once per
    metric (the old per-metric loop) recomputed betweenness — the O(V*E)
    dominant cost, measured 1877s at V=78829 against 0.8s for pagerank — once
    per metric and discarded 2/3 of each result.

    Returns the number of VERTICES written (each carries every metric), not
    metric-rows."""
    from src.analytics.centrality_compute import compute_all
    from src.graph.nebula_store import entity_vid

    all_scores = compute_all(store)
    # transpose {metric -> {name -> score}} into {name -> {metric -> score}}
    # so each vertex is touched by exactly one UPDATE.
    per_vertex: dict[str, dict[str, float]] = {}
    for metric in metrics:
        for name, score in (all_scores.get(metric) or {}).items():
            per_vertex.setdefault(name, {})[metric] = score
    if not per_vertex:
        return 0

    written = 0
    batch: list[str] = []
    used = 0
    for name, per_metric in per_vertex.items():
        stmt = _centrality_update_stmt(entity_vid(name), per_metric)
        add = len(stmt) + (1 if batch else 0)       # '\n' joiner
        if batch and used + add > _MAX_STMT_CHARS:
            written += _flush_centrality_batch(store, batch)
            batch, used, add = [], 0, len(stmt)
        batch.append(stmt)
        used += add
    return written + _flush_centrality_batch(store, batch)


async def write_centrality_all(store: Any | None, metrics: list[str]) -> int:
    """Nebula path: every metric from ONE compute, written in batches.

    Prefer this over looping ``write_centrality`` per metric — that loop is
    what made betweenness run once per metric."""
    if store is None:
        return 0
    for metric in metrics:
        if metric not in _CENTRALITY_STREAM:
            raise ValueError(f"unknown centrality metric: {metric!r}")
    return await asyncio.to_thread(
        _write_centrality_nebula_all_sync, store, list(metrics),
    )


async def _write_centrality_nebula(store: Any, metric: str) -> int:
    # Single-metric entry point keeps ONE write implementation.  compute_all
    # still computes all three internally; callers wanting more than one metric
    # must use write_centrality_all so that cost is paid once.
    return await asyncio.to_thread(
        _write_centrality_nebula_all_sync, store, [metric],
    )


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
