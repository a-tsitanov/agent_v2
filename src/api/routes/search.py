"""Search endpoint — hybrid query engine for ``agentic=False``,
``agentic_search`` for ``agentic=True``.

Concrete retrieval / synthesis collaborators are wired through
dishka so tests can override them with stubs.
"""

from __future__ import annotations

import time

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger

from src.api.auth import require_api_key
from src.models.search import SearchRequest, SearchResponse, SourceCitation
from src.retrieval.agent import (
    GraphRetrieverProtocol,
    JudgeProtocol,
    RetrieverProtocol,
    SynthesizerProtocol,
    agentic_search,
)

router = APIRouter(tags=["search"])


@router.post(
    "/search",
    response_model=SearchResponse,
    dependencies=[Depends(require_api_key)],
    summary="Hybrid semantic search",
)
@inject
async def search(
    req: SearchRequest,
    retriever: FromDishka[RetrieverProtocol],
    judge: FromDishka[JudgeProtocol],
    synthesizer: FromDishka[SynthesizerProtocol],
    graph_retriever: FromDishka[GraphRetrieverProtocol | None],
) -> SearchResponse:
    try:
        if req.agentic:
            return await agentic_search(
                retriever=retriever,
                judge=judge,
                synthesizer=synthesizer,
                graph_retriever=graph_retriever,
                query=req.query,
                max_rounds=req.agentic_max_rounds,
                mode=req.mode,
            )

        # Non-agentic path: single-round retrieve + synthesize.  Same
        # retriever as agentic mode — keeps the comparison fair.
        t0 = time.monotonic()
        nodes = await retriever.aretrieve(req.query)
        response = await synthesizer.asynthesize(query=req.query, nodes=nodes)
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
