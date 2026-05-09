"""Liveness + dependency health endpoint."""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health", summary="Liveness + dependency health")
async def health() -> dict:
    """Returns 200 when the API process is alive.

    Dependency-health pings (Milvus / Neo4j / PG) are added in
    later iterations — early bring-up keeps it cheap so health
    polling doesn't crowd the LLM proxy.
    """
    return {"status": "ok"}
