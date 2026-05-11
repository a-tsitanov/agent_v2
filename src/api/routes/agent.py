"""`POST /api/v1/agent` — ReAct agent with tool calls (R7).

Skeleton handler. R7 fills in `agentic_react_search` and wires it
through dishka.  Until then this endpoint returns 503 so the API
surface and contract are visible from day one.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from src.api.auth import require_api_key
from src.models.search import AgentSearchRequest, SearchResponse

router = APIRouter(tags=["search"])


@router.post(
    "/agent",
    response_model=SearchResponse,
    dependencies=[Depends(require_api_key)],
    summary="ReAct agent (tool calls + tool-decided termination)",
)
async def search_agent(req: AgentSearchRequest) -> SearchResponse:
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=(
            "ReAct agent endpoint will be wired in R7. "
            "Use /api/v1/search for plain hybrid retrieve, or "
            "/api/v1/selfrag for the full reflective stack "
            "(also pending R8)."
        ),
    )
