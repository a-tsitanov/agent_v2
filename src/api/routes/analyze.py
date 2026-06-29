"""`POST /api/v1/analyze` — plan → compute → synthesize analytical Q&A."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from temporalio.common import WorkflowIDReusePolicy

from src.analytics.contracts import AnalyzeParams
from src.api.auth import require_api_key
from src.config import settings
from src.models.analyze import AnalyzeRequest, AnalyzeResponse
from src.retrieval.date_filters import iso_to_epoch_days
from src.workflow.analytics.workflow import AnalyticalQueryWorkflow
from src.workflow.client import get_temporal_client

router = APIRouter(tags=["analytics"])


def _to_params(req: AnalyzeRequest) -> AnalyzeParams:
    """Map AnalyzeRequest → AnalyzeParams (ISO dates → epoch-days)."""
    return AnalyzeParams(
        query=req.query,
        top_n=req.top_n,
        date_from_epoch=iso_to_epoch_days(req.date_from) if req.date_from else None,
        date_to_epoch=iso_to_epoch_days(req.date_to) if req.date_to else None,
    )


@router.post(
    "/analyze",
    response_model=AnalyzeResponse,
    dependencies=[Depends(require_api_key)],
    summary="Plan→compute→synthesize analytical Q&A over the knowledge graph",
)
async def analyze(req: AnalyzeRequest) -> AnalyzeResponse:
    request_id = uuid.uuid4().hex
    try:
        client = await get_temporal_client()
        handle = await client.start_workflow(
            AnalyticalQueryWorkflow.run,
            _to_params(req),
            id=f"analyze-{request_id}",
            task_queue=settings.temporal.search_task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        )
        outcome = await handle.result()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Analyze failed: {exc}",
        ) from exc
    return AnalyzeResponse(
        query=outcome.query,
        answer=outcome.answer,
        provenance=outcome.provenance,
        latency_ms=outcome.latency_ms,
    )
