"""`POST /api/v1/agent` — ReAct agent with tool calls.

Outer loop is `agentic_react_search`; inner generator is the plain
project synthesizer (use `/api/v1/selfrag` for the reflective
variant — same outer loop, reflective synthesizer in place of plain).
"""

from __future__ import annotations

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Depends, HTTPException, status
from llama_index.core.llms import LLM
from loguru import logger

from src.api.auth import require_api_key
from src.models.search import AgentSearchRequest, SearchResponse
from src.retrieval.agent import (
    GraphRetrieverProtocol,
    RetrieverProtocol,
    SynthesizerProtocol,
)
from src.retrieval.react_agent import agentic_react_search

router = APIRouter(tags=["search"])


@router.post(
    "/agent",
    response_model=SearchResponse,
    dependencies=[Depends(require_api_key)],
    summary="ReAct agent (tool calls + tool-decided termination)",
)
@inject
async def search_agent(
    req: AgentSearchRequest,
    llm: FromDishka[LLM],
    retriever: FromDishka[RetrieverProtocol],
    synthesizer: FromDishka[SynthesizerProtocol],
    graph_retriever: FromDishka[GraphRetrieverProtocol | None],
) -> SearchResponse:
    async def synth(query: str, nodes):
        return await synthesizer.asynthesize(query=query, nodes=nodes)

    try:
        return await agentic_react_search(
            llm=llm,
            retriever=retriever,
            graph_retriever=graph_retriever,
            synthesize=synth,
            query=req.query,
            max_iterations=req.max_iterations,
            mode="agent",
        )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("agent search failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Agent search failed: {exc}",
        ) from exc
