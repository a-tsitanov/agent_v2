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
import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from src.api.auth import require_api_key
from src.config import settings
from src.graph import analysis

router = APIRouter(prefix="/admin/graph", tags=["admin"])


@functools.cache
def _store() -> Any:
    from src.graph.store import build_graph_store

    return build_graph_store()


@router.post("/stats", dependencies=[Depends(require_api_key)])
async def graph_stats() -> dict:
    """Operational snapshot: entity/relationship counts, degree
    distribution, duplicate-name groups, community count.

    An unmeasurable field is `null` with the reason under `errors` — it
    is never reported as 0."""
    return await analysis.graph_stats(_store())


@router.post("/pagerank", dependencies=[Depends(require_api_key)])
async def graph_pagerank(top_n: int = 20) -> dict:
    """Top-N most central entities by weighted PageRank."""
    return {"top": await analysis.pagerank(_store(), top_n=top_n)}


@router.post("/personalized-pagerank", dependencies=[Depends(require_api_key)])
async def graph_personalized_pagerank(
    seeds: list[str],
    top_n: int = 20,
) -> dict:
    """Top-N entities by PageRank biased toward the given seed entities
    (relevance/centrality *relative to* the seeds)."""
    return {
        "top": await analysis.personalized_pagerank(
            _store(),
            seeds,
            top_n=top_n,
        )
    }


@router.post("/components", dependencies=[Depends(require_api_key)])
async def graph_components() -> dict:
    """Weakly-connected-component count + size distribution."""
    return await analysis.components(_store())


@router.post("/shortest-path", dependencies=[Depends(require_api_key)])
async def graph_shortest_path(
    source: str,
    target: str,
    max_hops: int = 6,
) -> dict:
    """Shortest path between two entities by exact name."""
    return await analysis.shortest_path(
        _store(),
        source,
        target,
        max_hops=max_hops,
    )


@router.post(
    "/materialize",
    dependencies=[Depends(require_api_key)],
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger offline analytics materialization (GDS centrality + link-prediction + risk)",
)
async def materialize() -> dict[str, str]:
    """Fire-and-forget: start AnalyticsMaterializeWorkflow on the graph-build queue."""
    from temporalio.common import WorkflowIDReusePolicy

    from src.analytics.contracts import MaterializeParams
    from src.workflow.analytics.materialize_workflow import AnalyticsMaterializeWorkflow
    from src.workflow.client import get_temporal_client

    request_id = uuid.uuid4().hex
    try:
        client = await get_temporal_client()
        handle = await client.start_workflow(
            AnalyticsMaterializeWorkflow.run,
            MaterializeParams(),
            id=f"analytics-materialize-{request_id}",
            task_queue=settings.temporal.graph_build_task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        )
        return {"workflow_id": handle.id, "status": "started"}
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"materialize failed to start: {exc}",
        ) from exc
