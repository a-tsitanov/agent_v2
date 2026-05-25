"""`POST /api/v1/selfrag` — ReAct agent + reflective synthesis.

Submits ``SearchWorkflow`` with ``mode="selfrag"``.  Workflow runs
the same ReAct loop as ``/agent`` then calls reflective_synthesize
inside the ``synthesize_answer`` activity instead of plain synth.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger
from temporalio.common import WorkflowIDReusePolicy

from src.api.auth import require_api_key
from src.config import settings
from src.models.search import (
    AgenticStepStat, ReflectiveAnswerDetail, ReflectiveCitation,
    ReflectiveUncertainty, SearchResponse, SelfRAGSearchRequest,
    SourceCitation,
)
from src.observability.trace import trace_request
from src.workflow.client import get_temporal_client
from src.workflow.contracts import SearchParams
from src.workflow.search_workflow import SearchWorkflow

router = APIRouter(tags=["search"])


@router.post(
    "/selfrag",
    response_model=SearchResponse,
    dependencies=[Depends(require_api_key)],
    summary="ReAct + reflective synthesis (Self-RAG inspired)",
)
async def search_selfrag(req: SelfRAGSearchRequest) -> SearchResponse:
    request_id = uuid.uuid4().hex
    try:
        with trace_request("selfrag", req.query):
            client = await get_temporal_client()
            handle = await client.start_workflow(
                SearchWorkflow.run,
                SearchParams(
                    query=req.query,
                    mode="selfrag",
                    max_iterations=req.max_iterations,
                    max_refinements=req.max_refinements,
                    request_id=request_id,
                    version_tag=settings.analytics.default_version_tag,
                    env=settings.analytics.env_name,
                    distill_enabled=settings.agent.distill_enabled,
                    distill_min_chars=settings.agent.distill_min_chars,
                    observation_max_chars=settings.agent.observation_max_chars,
                ),
                id=f"search-{request_id}",
                task_queue=settings.temporal.search_task_queue,
                id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            )
            outcome = await handle.result()
        return SearchResponse(
            query=outcome.query,
            answer=outcome.answer,
            mode="selfrag",
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
            agentic_step_stats=[
                AgenticStepStat(
                    step=s.step, tool_name=s.tool_name,
                    tool_args=s.tool_args,
                    observation_summary=s.observation_summary,
                )
                for s in outcome.step_stats
            ],
            answer_detail=ReflectiveAnswerDetail(
                citations=[
                    ReflectiveCitation(claim=c.claim, chunk_id=c.chunk_id)
                    for c in outcome.citations
                ],
                uncertainties=[
                    ReflectiveUncertainty(topic=u.topic, reason=u.reason)
                    for u in outcome.uncertainties
                ],
                refinement_rounds=outcome.refinement_rounds,
            ),
        )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("selfrag search failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Selfrag search failed: {exc}",
        ) from exc
