"""Admin operations: trigger a wiki sweep (and optional full rebuild)."""
from __future__ import annotations

from fastapi import APIRouter
from temporalio.common import WorkflowIDReusePolicy

from src.config import settings
from src.workflow.client import get_temporal_client
from src.workflow.wiki.wiki_sweep import WikiSweepWorkflow

router = APIRouter(prefix="/admin/wiki", tags=["admin"])


@router.post("/rebuild")
async def wiki_rebuild(all: bool = False) -> dict:
    if not settings.wiki.enabled:
        return {"status": "disabled"}
    if all:
        from src.graph.store import build_neo4j_graph_store
        build_neo4j_graph_store().structured_query(
            "MATCH (e:__Entity__) SET e.wiki_dirty = true, "
            "e.wiki_dirty_at = datetime()")
    client = await get_temporal_client()
    handle = await client.start_workflow(
        WikiSweepWorkflow.run, id="wiki-sweep-manual",
        task_queue=settings.wiki.task_queue,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE)
    return {"status": "started", "workflow_id": handle.id}
