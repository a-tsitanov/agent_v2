"""`POST /api/v1/search/local` — plan-execute-synthesize search (R2).

Submits ``SearchOrchestratorWorkflow``: decompose → parallel per-sub-
question retrieval → merge/dedup → single large-model synthesis.  No
ReAct loop.  Reuses the ``SearchRequest`` / ``SearchResponse`` shapes so
existing clients work unchanged — only the underlying flow differs.

R7b cutover: this module is the SOLE search surface
(``/api/v1/search/{local,global,drift,auto}`` + the community-rebuild
admin trigger).  The legacy ReAct ``/api/v1/search``, ``/agent`` and
``/selfrag`` endpoints (and the ``SearchWorkflow`` behind them) were
removed.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger
from temporalio.common import WorkflowIDReusePolicy

from src.api.auth import require_api_key
from src.config import settings
from src.models.search import DocumentRef, SearchRequest, SearchResponse, SourceCitation
from src.observability.trace import trace_request
from src.workflow.client import get_temporal_client
from src.workflow.contracts import (
    ConversationTurnDict,
    DetectCommunitiesParams,
    GlobalSearchParams,
    OrchestratorParams,
    SearchOutcome,
)
from src.workflow.search.community_wf import CommunityBuildWorkflow
from src.workflow.search.global_wf import GlobalSearchWorkflow
from src.workflow.search.orchestrator import SearchOrchestratorWorkflow
from src.workflow.search.router_wf import AutoSearchWorkflow, DriftSearchWorkflow

router = APIRouter(tags=["search"])


def _local_params(req: SearchRequest) -> OrchestratorParams:
    """Build the local plan-execute workflow input from the request."""
    return OrchestratorParams(
        query=req.query,
        max_subqueries=settings.agent.max_subqueries,
        top_k=req.top_k,
        coverage_check_enabled=settings.agent.coverage_check_enabled,
        max_coverage_rounds=settings.agent.max_coverage_rounds,
        history=[
            ConversationTurnDict(role=t.role, content=t.content)
            for t in req.history
        ],
        contextualize_enabled=settings.agent.conversation_history_enabled,
    )


def _global_params(req: SearchRequest, *, drift_mode: bool = False) -> GlobalSearchParams:
    """Build the GraphRAG global map-reduce workflow input."""
    return GlobalSearchParams(
        query=req.query,
        level=0,
        max_communities=settings.agent.global_max_communities,
        map_parallelism=settings.agent.global_map_parallelism,
        drift_mode=drift_mode,
        history=[
            ConversationTurnDict(role=t.role, content=t.content)
            for t in req.history
        ],
        contextualize_enabled=settings.agent.conversation_history_enabled,
    )


def to_document_refs(doc_ids: list[str]) -> list[DocumentRef]:
    """doc_id list → relative download links (preserves order)."""
    return [DocumentRef(doc_id=d, url=f"/api/v1/documents/{d}") for d in doc_ids]


def _outcome_to_response(outcome: SearchOutcome) -> SearchResponse:
    """Map the workflow's ``SearchOutcome`` onto the shared response shape
    (identical projection across local/global/drift/auto)."""
    return SearchResponse(
        query=outcome.query,
        answer=outcome.answer,
        mode=outcome.mode,
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
        documents=to_document_refs(outcome.documents),
        latency_ms=outcome.latency_ms,
    )


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
                _local_params(req),
                id=f"search-local-{request_id}",
                task_queue=settings.temporal.search_task_queue,
                id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            )
            outcome = await handle.result()
        return _outcome_to_response(outcome)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("search_local failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {exc}",
        ) from exc


@router.post(
    "/search/global",
    response_model=SearchResponse,
    dependencies=[Depends(require_api_key)],
    summary="GraphRAG global search (map-reduce over community summaries)",
)
async def search_global(req: SearchRequest) -> SearchResponse:
    """Run ``GlobalSearchWorkflow`` — map-reduce over the R6 community
    summaries for corpus-level / thematic questions.  Orchestration runs
    on ``kb-search-small``; the REDUCE synthesis is pinned to the large
    queue inside the workflow."""
    request_id = uuid.uuid4().hex
    try:
        with trace_request("search_global", req.query):
            client = await get_temporal_client()
            handle = await client.start_workflow(
                GlobalSearchWorkflow.run,
                _global_params(req),
                id=f"search-global-{request_id}",
                task_queue=settings.temporal.search_task_queue,
                id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            )
            outcome = await handle.result()
        return _outcome_to_response(outcome)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("search_global failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Global search failed: {exc}",
        ) from exc


@router.post(
    "/search/drift",
    response_model=SearchResponse,
    dependencies=[Depends(require_api_key)],
    summary="Drift search (local plan-execute, then global community expansion)",
)
async def search_drift(req: SearchRequest) -> SearchResponse:
    """Run ``DriftSearchWorkflow`` — local plan-execute pass first, then
    expand with community context via a drift-mode global map-reduce
    (local sources seed the REDUCE).  Both passes orchestrate on
    ``kb-search-small``; synthesis is pinned large inside the global pass."""
    request_id = uuid.uuid4().hex
    try:
        with trace_request("search_drift", req.query):
            client = await get_temporal_client()
            handle = await client.start_workflow(
                DriftSearchWorkflow.run,
                args=[_local_params(req), _global_params(req, drift_mode=True)],
                id=f"search-drift-{request_id}",
                task_queue=settings.temporal.search_task_queue,
                id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            )
            outcome = await handle.result()
        return _outcome_to_response(outcome)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("search_drift failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Drift search failed: {exc}",
        ) from exc


@router.post(
    "/search/auto",
    response_model=SearchResponse,
    dependencies=[Depends(require_api_key)],
    summary="Auto search (route_query → dispatch local/global/drift)",
)
async def search_auto(req: SearchRequest) -> SearchResponse:
    """Run ``AutoSearchWorkflow`` — ``route_query`` (small tier) classifies
    the question, then dispatches to the local / global / drift flow.
    Fail-safe: a flaky router defaults to the local flow.  Orchestration
    on ``kb-search-small``; synthesis pinned large in the chosen flow."""
    request_id = uuid.uuid4().hex
    try:
        with trace_request("search_auto", req.query):
            client = await get_temporal_client()
            handle = await client.start_workflow(
                AutoSearchWorkflow.run,
                args=[_local_params(req), _global_params(req)],
                id=f"search-auto-{request_id}",
                task_queue=settings.temporal.search_task_queue,
                id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            )
            outcome = await handle.result()
        return _outcome_to_response(outcome)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("search_auto failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Auto search failed: {exc}",
        ) from exc


@router.post(
    "/admin/communities/rebuild",
    dependencies=[Depends(require_api_key)],
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger offline graph-community build (GDS Leiden + summaries)",
)
async def rebuild_communities() -> dict[str, str]:
    """Start the OFFLINE ``CommunityBuildWorkflow`` on ``kb-graph-build``.

    Fully decoupled from the query path: detects graph communities (GDS
    Leiden) and generates a small-tier batch summary per community.  Fire-
    and-forget — returns the workflow id immediately (the build runs for
    minutes).  Idempotent: re-running refreshes summaries / membership on
    the existing ``:Community`` nodes rather than duplicating them.
    """
    request_id = uuid.uuid4().hex
    try:
        client = await get_temporal_client()
        handle = await client.start_workflow(
            CommunityBuildWorkflow.run,
            DetectCommunitiesParams(
                min_size=settings.temporal.community_min_size,
                level=0,
            ),
            id=f"community-build-{request_id}",
            task_queue=settings.temporal.graph_build_task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        )
        return {"workflow_id": handle.id, "status": "started"}
    except Exception as exc:
        logger.exception("rebuild_communities failed to start")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Community build failed to start: {exc}",
        ) from exc
