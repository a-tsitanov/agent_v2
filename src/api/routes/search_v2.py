"""`POST /api/v1/search/local` — plan-execute-synthesize search (R2).

Submits ``SearchOrchestratorWorkflow``: decompose → parallel per-sub-
question retrieval → merge/dedup → single large-model synthesis.  No
ReAct loop.  Reuses the legacy ``SearchRequest`` / ``SearchResponse``
shapes so existing clients work unchanged — only the underlying flow
differs.

Lives alongside the legacy ``/api/v1/search`` (ReAct ``SearchWorkflow``)
behind the parity window; cutover happens in a later phase.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger
from temporalio.common import WorkflowIDReusePolicy

from src.api.auth import require_api_key
from src.config import settings
from src.models.search import SearchRequest, SearchResponse, SourceCitation
from src.observability.trace import trace_request
from src.workflow.client import get_temporal_client
from src.workflow.contracts import OrchestratorParams
from src.workflow.search.orchestrator import SearchOrchestratorWorkflow

router = APIRouter(tags=["search"])


@router.post(
    "/search/local",
    response_model=SearchResponse,
    dependencies=[Depends(require_api_key)],
    summary="Plan-execute-synthesize search (decompose → parallel retrieve → synth)",
)
async def search_local(req: SearchRequest) -> SearchResponse:
    request_id = uuid.uuid4().hex
    try:
        with trace_request("search_local", req.query):
            client = await get_temporal_client()
            handle = await client.start_workflow(
                SearchOrchestratorWorkflow.run,
                OrchestratorParams(
                    query=req.query,
                    max_subqueries=settings.agent.max_subqueries,
                    top_k=req.top_k,
                    request_id=request_id,
                    version_tag=settings.analytics.default_version_tag,
                    env=settings.analytics.env_name,
                ),
                id=f"search-local-{request_id}",
                task_queue=settings.temporal.search_task_queue,
                id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            )
            outcome = await handle.result()
        return SearchResponse(
            query=outcome.query,
            answer=outcome.answer,
            mode="local",
            sources=[
                SourceCitation(
                    doc_id=str(n.metadata.get("doc_id")
                               or n.metadata.get("file_path") or ""),
                    chunk_id=n.chunk_id,
                    content=n.text,
                    score=n.score,
                )
                for n in outcome.sources
            ],
            latency_ms=outcome.latency_ms,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("search_local failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {exc}",
        ) from exc
