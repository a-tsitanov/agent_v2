"""Request/response models for POST /api/v1/analyze."""

from __future__ import annotations

from pydantic import BaseModel, field_validator

from src.analytics.contracts import Provenance
from src.retrieval.date_filters import iso_to_epoch_days


class AnalyzeRequest(BaseModel):
    query: str
    date_from: str | None = None
    date_to: str | None = None
    top_n: int = 20

    @field_validator("date_from", "date_to")
    @classmethod
    def _valid_iso(cls, v: str | None) -> str | None:
        if v is None:
            return v
        iso_to_epoch_days(v)  # raises ValueError → 422
        return v


class AnalyzeResponse(BaseModel):
    query: str
    answer: str
    provenance: Provenance
    latency_ms: int = 0
