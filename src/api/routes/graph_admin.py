"""Read-only graph-analysis admin endpoints (Track 7b).

Thin glue over ``src.graph.analysis`` — runs GDS/Cypher analysis on the
live ``__Entity__`` graph for operators.  Read-only, API-key gated, and
fail-soft (the analysis layer returns safe empties rather than raising),
so a GDS hiccup yields an empty result + a worker log, not a 500.

The Neo4j store is built once (cached) — these are low-frequency admin
calls, not the query hot path.
"""

from __future__ import annotations

import functools
from typing import Any

from fastapi import APIRouter, Depends

from src.analytics import temporal
from src.api.auth import require_api_key
from src.graph import analysis

router = APIRouter(prefix="/admin/graph", tags=["admin"])


@functools.cache
def _store() -> Any:
    from src.graph.store import build_neo4j_graph_store

    return build_neo4j_graph_store()


@router.post("/stats", dependencies=[Depends(require_api_key)])
async def graph_stats() -> dict:
    """Operational snapshot: entity/relationship counts, degree
    distribution, duplicate-name groups, community count."""
    return await analysis.graph_stats(_store())


@router.post("/pagerank", dependencies=[Depends(require_api_key)])
async def graph_pagerank(top_n: int = 20) -> dict:
    """Top-N most central entities by weighted PageRank."""
    return {"top": await analysis.pagerank(_store(), top_n=top_n)}


@router.post("/personalized-pagerank", dependencies=[Depends(require_api_key)])
async def graph_personalized_pagerank(
    seeds: list[str], top_n: int = 20,
) -> dict:
    """Top-N entities by PageRank biased toward the given seed entities
    (relevance/centrality *relative to* the seeds)."""
    return {"top": await analysis.personalized_pagerank(
        _store(), seeds, top_n=top_n,
    )}


@router.post("/components", dependencies=[Depends(require_api_key)])
async def graph_components() -> dict:
    """Weakly-connected-component count + size distribution."""
    return await analysis.components(_store())


@router.post("/shortest-path", dependencies=[Depends(require_api_key)])
async def graph_shortest_path(
    source: str, target: str, max_hops: int = 6,
) -> dict:
    """Shortest path between two entities by exact name."""
    return await analysis.shortest_path(
        _store(), source, target, max_hops=max_hops,
    )


@router.post("/snapshot", dependencies=[Depends(require_api_key)])
async def graph_snapshot(as_of: str, include_untimed: bool = True) -> dict:
    """Bitemporal graph state alive at ``as_of`` (ISO date/datetime).

    Returns edges + nodes + ``coverage`` (how much rests on real dates vs
    the ingest-time fallback). ``include_untimed`` keeps relations that
    have no temporal information at all (default on)."""
    return await temporal.snapshot(
        _store(), as_of, include_untimed=include_untimed,
    )


@router.post("/diff", dependencies=[Depends(require_api_key)])
async def graph_diff(t1: str, t2: str, include_untimed: bool = True) -> dict:
    """What changed between two moments: added / removed / persisted edges
    (``snapshot(t2) ⊖ snapshot(t1)``), with both coverages."""
    return await temporal.diff(
        _store(), t1, t2, include_untimed=include_untimed,
    )
