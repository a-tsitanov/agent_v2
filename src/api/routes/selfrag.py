"""`POST /api/v1/selfrag` — ReAct + Reflective synthesis (R8).

Skeleton handler. R8 fills in `reflective_synthesize` and wires
it as the `submit_answer` body of the same ReAct agent that
`/api/v1/agent` (R7) uses.  Until then returns 503.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from src.api.auth import require_api_key
from src.models.search import SearchResponse, SelfRAGSearchRequest

router = APIRouter(tags=["search"])


@router.post(
    "/selfrag",
    response_model=SearchResponse,
    dependencies=[Depends(require_api_key)],
    summary="ReAct + reflective synthesis (Self-RAG inspired)",
)
async def search_selfrag(req: SelfRAGSearchRequest) -> SearchResponse:
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=(
            "Self-RAG endpoint will be wired in R8. "
            "Use /api/v1/search for plain hybrid retrieve."
        ),
    )
