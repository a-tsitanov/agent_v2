"""Request/response models for POST /api/v1/analyze."""

from __future__ import annotations

from pydantic import BaseModel

from src.analytics.contracts import Provenance


class AnalyzeRequest(BaseModel):
    query: str
    top_n: int = 20


class AnalyzeResponse(BaseModel):
    query: str
    answer: str
    provenance: Provenance
    latency_ms: int = 0
