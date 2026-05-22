"""`POST /api/v1/search` — plain hybrid retrieve + single synthesize.

Submits ``SearchWorkflow`` with ``mode="simple"`` — one
vector_search → one synthesize.  Same response shape as before
(``SearchResponse``); the route just gained durability + visibility
in Temporal UI + cancel-on-disconnect propagation.

For tool-using ReAct, use ``/api/v1/agent``; for reflective
synthesis with [NEED]-marker loop, use ``/api/v1/selfrag``.
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
from src.workflow.contracts import SearchParams
from src.workflow.search_workflow import SearchWorkflow

router = APIRouter(tags=["search"])


@router.post(
    "/search",
    response_model=SearchResponse,
    dependencies=[Depends(require_api_key)],
    summary="Hybrid retrieve + single synthesize (no agent, no judge)",
)
async def search(req: SearchRequest) -> SearchResponse:
    request_id = uuid.uuid4().hex
    try:
        with trace_request("search", req.query):
            client = await get_temporal_client()
            handle = await client.start_workflow(
                SearchWorkflow.run,
                SearchParams(
                    query=req.query,
                    mode="simple",
                    request_id=request_id,
                    version_tag=settings.analytics.default_version_tag,
                    env=settings.analytics.env_name,
                ),
                id=f"search-{request_id}",
                task_queue=settings.temporal.search_task_queue,
                id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            )
            outcome = await handle.result()
        return SearchResponse(
            query=outcome.query,
            answer=outcome.answer,
            mode=req.mode,
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
    except Exception as exc:  # noqa: BLE001
        logger.exception("search failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {exc}",
        ) from exc
