"""Admin operations: trigger wiki/monitor sweeps and monitor watchlist."""

from __future__ import annotations

from fastapi import APIRouter
from temporalio.common import WorkflowIDReusePolicy

from src.config import settings
from src.graph.alerts import mark_watched
from src.graph.store import build_neo4j_graph_store
from src.workflow.client import get_temporal_client
from src.workflow.monitor.workflow import MonitorSweepWorkflow
from src.workflow.wiki.wiki_sweep import WikiSweepWorkflow

router = APIRouter(prefix="/admin/wiki", tags=["admin"])


@router.post("/rebuild")
async def wiki_rebuild(all: bool = False) -> dict:
    if not settings.wiki.enabled:
        return {"status": "disabled"}
    if all:
        build_neo4j_graph_store().structured_query(
            "MATCH (e:__Entity__) SET e.wiki_dirty = true, e.wiki_dirty_at = datetime()"
        )
    client = await get_temporal_client()
    handle = await client.start_workflow(
        WikiSweepWorkflow.run,
        id="wiki-sweep-manual",
        task_queue=settings.wiki.task_queue,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    )
    return {"status": "started", "workflow_id": handle.id}


monitor_router = APIRouter(prefix="/admin/monitor", tags=["admin"])


@monitor_router.post("/sweep")
async def monitor_sweep() -> dict:
    if not settings.monitor.enabled:
        return {"status": "disabled"}
    client = await get_temporal_client()
    handle = await client.start_workflow(
        MonitorSweepWorkflow.run,
        id="monitor-sweep-manual",
        task_queue=settings.monitor.task_queue,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    )
    return {"status": "started", "workflow_id": handle.id}


@monitor_router.post("/watch")
async def monitor_watch(names: list[str], watched: bool = True) -> dict:
    # Intentionally NOT gated on settings.monitor.enabled (unlike /sweep): the
    # watchlist is the input the sweep consumes, so operators pre-seed it while
    # monitoring is still dark, then flip MONITOR_ENABLED on. `names` binds to
    # the JSON body (a string array); `watched` is a query param.
    mark_watched(build_neo4j_graph_store(), names, watched)
    return {"status": "ok", "count": len(names), "watched": watched}
