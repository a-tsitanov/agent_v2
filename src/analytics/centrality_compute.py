"""In-worker centrality (igraph) over the exported __Entity__ graph.

Mirrors src/graph/community_leiden.py: stream the graph out via the
GraphEdgeExport seam, build a weighted undirected igraph, and compute the
centrality metric in this worker process — off Neo4j's GDS heap, and usable
under nebula which has no in-graph compute. Selected when
``settings.graph.backend == "nebula"`` (see analytics/materialize.py).

Fail-open: any error → ``{}`` (the same sentinel discipline as
community_graphscope — a non-empty score dict is never mistaken for a
failure, so a fail-open never wipes existing materialized scores).
"""
from __future__ import annotations

from typing import Any

from loguru import logger

_METRICS = ("pagerank", "betweenness", "eigenvector")


def compute_centrality(
    edges: list[tuple[str, str, float]],
    node_names: list[str],
    metric: str,
) -> dict[str, float]:
    """Return ``{entity_name -> score}`` for ``metric`` over the weighted
    undirected graph. Empty graph or any error → ``{}`` (fail-open)."""
    if metric not in _METRICS:
        raise ValueError(f"unknown centrality metric: {metric!r}")
    try:
        # reuse community_leiden's exact graph-build (dedup names + endpoints,
        # weighted, simplified) so centrality and community see the same graph.
        from src.graph.community_leiden import build_graph

        g, names = build_graph(edges, node_names)
        if g.vcount() == 0:
            return {}
        weights = g.es["weight"] if "weight" in g.es.attributes() else None
        if metric == "pagerank":
            scores = g.pagerank(weights=weights, directed=False)
        elif metric == "betweenness":
            scores = g.betweenness(weights=weights, directed=False)
        else:  # eigenvector
            scores = g.eigenvector_centrality(weights=weights, directed=False)
        return {names[i]: float(scores[i]) for i in range(len(names))}
    except Exception as exc:  # fail-open, never wipe existing scores
        logger.warning("centrality_compute({m}) failed (non-fatal): {e}", m=metric, e=exc)
        return {}


def compute_all(
    store: Any, *, batch_size: int = 50_000,
) -> dict[str, dict[str, float]]:
    """Stream the graph once, compute every metric. Returns
    ``{metric -> {name -> score}}`` ({} per metric on failure)."""
    from src.graph.community_leiden import extract_entity_edges

    try:
        edges, names = extract_entity_edges(store, batch_size=batch_size)
    except Exception as exc:  # fail-open
        logger.warning("centrality_compute: edge export failed (non-fatal): {e}", e=exc)
        return {m: {} for m in _METRICS}
    return {m: compute_centrality(edges, names, m) for m in _METRICS}
