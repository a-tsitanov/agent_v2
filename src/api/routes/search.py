"""`POST /api/v1/search` — plain hybrid retrieve + single synthesize.

No agentic loop, no judge, no reflective synthesis.  Use this
endpoint when you want a fast, deterministic-ish answer over the
vector index alone.  See `/api/v1/agent` (R7) for tool-using ReAct
agent and `/api/v1/selfrag` (R8) for ReAct + reflective synthesis.
"""

from __future__ import annotations

import time

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger

from src.api.auth import require_api_key
from src.models.search import SearchRequest, SearchResponse, SourceCitation
from src.observability.trace import record_timed, trace_request
from src.retrieval.agent import RetrieverProtocol, SynthesizerProtocol

router = APIRouter(tags=["search"])


@router.post(
    "/search",
    response_model=SearchResponse,
    dependencies=[Depends(require_api_key)],
    summary="Hybrid retrieve + single synthesize (no agent, no judge)",
)
@inject
async def search(
    req: SearchRequest,
    retriever: FromDishka[RetrieverProtocol],
    synthesizer: FromDishka[SynthesizerProtocol],
) -> SearchResponse:
    try:
        t0 = time.monotonic()
        with trace_request("search", req.query):
            with record_timed("tool_call", tool_name="vector_retrieve"):
                nodes = await retriever.aretrieve(req.query)
            # Inject a Russian-output instruction into the synthesizer
            # query.  LlamaIndex's default ResponseSynthesizer prompts
            # are English-leaning; without this it sometimes answers
            # in English when context chunks are English.  The graph
            # is normalised to Russian, queries are Russian, so the
            # answer must be Russian too.
            ru_query = (
                "Ответь на следующий вопрос на русском языке, "
                "сохраняя имена собственные и идентификаторы дословно "
                f"из исходного языка контекста: {req.query}"
            )
            with record_timed("synthesize", n_sources=len(nodes)):
                response = await synthesizer.asynthesize(
                    query=ru_query, nodes=nodes,
                )
        latency_ms = (time.monotonic() - t0) * 1000.0
        return SearchResponse(
            query=req.query,
            answer=getattr(response, "response", None) or str(response),
            mode=req.mode,
            sources=[
                SourceCitation(
                    doc_id=str(
                        n.node.metadata.get("doc_id")
                        or n.node.metadata.get("file_path") or ""
                    ),
                    chunk_id=n.node.node_id,
                    content=n.node.get_content(),
                    score=float(n.score or 0.0),
                )
                for n in nodes
            ],
            latency_ms=latency_ms,
        )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 — surface as 500
        logger.exception("search failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {exc}",
        ) from exc
