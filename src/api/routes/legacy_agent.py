"""`POST /api/v1/legacy/agent` — legacy judge-based agentic search.

Kept as a comparative baseline for R9 answer-quality eval (see
`tests/eval/run_answer_eval.py --include-legacy`).  Mounted only
when `AGENT_ENABLE_LEGACY_AGENT=true` in env; otherwise the route
is never registered and any client request returns 404.

The handler delegates to `src/retrieval/agent.py:agentic_search` —
that module documents the judge-loop semantics in detail.
"""

from __future__ import annotations

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger

from src.api.auth import require_api_key
from src.models.search import AgentSearchRequest, SearchResponse
from src.observability.trace import trace_request
from src.retrieval.agent import (
    GraphRetrieverProtocol,
    JudgeProtocol,
    RetrieverProtocol,
    SynthesizerProtocol,
    agentic_search,
)

router = APIRouter(tags=["search"])


@router.post(
    "/legacy/agent",
    response_model=SearchResponse,
    dependencies=[Depends(require_api_key)],
    summary="LEGACY judge-based agentic search (baseline for R9 eval)",
)
@inject
async def search_legacy_agent(
    req: AgentSearchRequest,
    retriever: FromDishka[RetrieverProtocol],
    judge: FromDishka[JudgeProtocol],
    synthesizer: FromDishka[SynthesizerProtocol],
    graph_retriever: FromDishka[GraphRetrieverProtocol | None],
) -> SearchResponse:
    try:
        with trace_request("legacy", req.query):
            # legacy contract uses max_rounds (not max_iterations);
            # we map AgentSearchRequest's knob over.
            return await agentic_search(
                retriever=retriever,
                judge=judge,
                synthesizer=synthesizer,
                graph_retriever=graph_retriever,
                query=req.query,
                max_rounds=min(req.max_iterations, 5),
                mode="legacy",
            )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("legacy agent search failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Legacy agent search failed: {exc}",
        ) from exc
