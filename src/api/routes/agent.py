"""`POST /api/v1/agent` — ReAct agent submitted as Temporal workflow.

Thin wrapper: validates input, submits ``SearchWorkflow`` with
``mode="agent"``, awaits the result, maps ``SearchOutcome`` →
``SearchResponse``.  All the ReAct loop logic now lives in
``src/workflow/search_workflow.py`` + the three activities under
``src/workflow/activities/{agent_reasoning,tool_execution,synthesize_answer}.py``.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger
from temporalio.common import WorkflowIDReusePolicy

from src.api.auth import require_api_key
from src.config import settings
from src.models.search import (
    AgenticStepStat, AgentSearchRequest, SearchResponse, SourceCitation,
)
from src.observability.trace import trace_request
from src.workflow.client import get_temporal_client
from src.workflow.contracts import SearchParams
from src.workflow.search_workflow import SearchWorkflow

router = APIRouter(tags=["search"])


@router.post(
    "/agent",
    response_model=SearchResponse,
    dependencies=[Depends(require_api_key)],
    summary="ReAct agent (tool calls + tool-decided termination)",
)
async def search_agent(req: AgentSearchRequest) -> SearchResponse:
    request_id = uuid.uuid4().hex
    try:
        with trace_request("agent", req.query):
            client = await get_temporal_client()
            handle = await client.start_workflow(
                SearchWorkflow.run,
                SearchParams(
                    query=req.query,
                    mode="agent",
                    max_iterations=req.max_iterations,
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
            mode="agent",
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
        )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("agent search failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Agent search failed: {exc}",
        ) from exc
