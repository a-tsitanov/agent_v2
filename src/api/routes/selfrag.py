"""`POST /api/v1/selfrag` — ReAct agent + Reflective synthesis.

Same outer loop as `/api/v1/agent` (`agentic_react_search`), but
`submit_answer` triggers `reflective_synthesize` instead of the
plain `ResponseSynthesizer`.  The reflective synthesizer drafts
with inline self-reflection markers and re-retrieves as needed.
"""

from __future__ import annotations

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Depends, HTTPException, status
from llama_index.core.llms import LLM
from loguru import logger

from src.api.auth import require_api_key
from src.models.search import (
    ReflectiveAnswerDetail,
    SearchResponse,
    SelfRAGSearchRequest,
)
from src.observability.trace import trace_request
from src.retrieval.agent import GraphRetrieverProtocol, RetrieverProtocol
from src.retrieval.react_agent import agentic_react_search
from src.retrieval.reflective_synth import reflective_synthesize

router = APIRouter(tags=["search"])


@router.post(
    "/selfrag",
    response_model=SearchResponse,
    dependencies=[Depends(require_api_key)],
    summary="ReAct + reflective synthesis (Self-RAG inspired)",
)
@inject
async def search_selfrag(
    req: SelfRAGSearchRequest,
    llm: FromDishka[LLM],
    retriever: FromDishka[RetrieverProtocol],
    graph_retriever: FromDishka[GraphRetrieverProtocol | None],
) -> SearchResponse:
    last_reflective: dict = {"answer": None}

    async def synth(query: str, nodes):
        answer = await reflective_synthesize(
            llm=llm,
            query=query,
            context_nodes=nodes,
            retriever=retriever,
            max_refinements=req.max_refinements,
        )
        last_reflective["answer"] = answer
        return answer

    try:
        with trace_request("selfrag", req.query):
            result = await agentic_react_search(
                llm=llm,
                retriever=retriever,
                graph_retriever=graph_retriever,
                synthesize=synth,
                query=req.query,
                max_iterations=req.max_iterations,
                mode="selfrag",
            )
        ra = last_reflective["answer"]
        if ra is not None:
            result.answer_detail = ReflectiveAnswerDetail(
                citations=ra.citations,
                uncertainties=ra.uncertainties,
                refinement_rounds=ra.refinement_rounds,
            )
        return result
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("selfrag search failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Selfrag search failed: {exc}",
        ) from exc
