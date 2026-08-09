"""Write path for the external-statistics subsystem.

One endpoint: `POST /api/v1/statistics/load` takes an indicator and its
observations and upserts both.  Row volumes are small, so there is no
batching protocol, no file upload and no per-source adapter — a caller
posts JSON.

The prefix is `/statistics`, not `/stats`: the latter already means
ingest-pipeline statistics over the `documents` table
(`src/api/routes/stats.py`), which is a different thing entirely.

Reads do NOT live here — they are served by MCP-3
(`src/mcp/stats_server.py`), which is the surface agents talk to.
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, field_validator, model_validator

from src.api.auth import require_api_key
from src.stats.align import GRANULARITIES, VALUE_KINDS
from src.storage.stats import StatsRepository

router = APIRouter(
    prefix="/statistics",
    tags=["statistics"],
    dependencies=[Depends(require_api_key)],
)

# Small by design: the subsystem takes curated series, not bulk dumps.
_MAX_OBSERVATIONS = 1000


class IndicatorIn(BaseModel):
    source: str = Field(min_length=1)
    code: str = Field(min_length=1)
    title: str = Field(min_length=1)
    unit: str = Field(min_length=1)
    value_kind: str
    granularity: str
    question_text: str = ""
    dims_schema: dict[str, Any] = Field(default_factory=dict)
    entity_vid: str | None = None

    @field_validator("value_kind")
    @classmethod
    def _known_value_kind(cls, v: str) -> str:
        if v not in VALUE_KINDS:
            raise ValueError(f"value_kind must be one of {sorted(VALUE_KINDS)}")
        return v

    @field_validator("granularity")
    @classmethod
    def _known_granularity(cls, v: str) -> str:
        if v not in GRANULARITIES:
            raise ValueError(f"granularity must be one of {sorted(GRANULARITIES)}")
        return v


class ObservationIn(BaseModel):
    period_start: date
    period_end: date
    value: float
    dims: dict[str, Any] = Field(default_factory=dict)
    sample_n: int | None = None
    revision: int = Field(default=0, ge=0)
    source_doc_id: uuid.UUID | None = None

    @model_validator(mode="after")
    def _ordered_period(self) -> ObservationIn:
        if self.period_end < self.period_start:
            raise ValueError("period_end must not precede period_start")
        return self


class LoadRequest(BaseModel):
    indicator: IndicatorIn
    observations: list[ObservationIn] = Field(
        default_factory=list, max_length=_MAX_OBSERVATIONS,
    )


class LoadResponse(BaseModel):
    indicator_id: int
    observations: int


@router.post(
    "/load",
    response_model=LoadResponse,
    status_code=status.HTTP_200_OK,
    summary="Upsert one indicator and its observations",
)
async def load_statistics(req: LoadRequest) -> LoadResponse:
    """Idempotent: re-posting the same payload changes nothing.

    Values are stored exactly as supplied — alignment and normalisation
    are computed on read, so changing the normalisation method never
    requires reloading a source.
    """
    repo = StatsRepository()
    ind = req.indicator
    try:
        indicator_id = await repo.upsert_indicator(
            source=ind.source,
            code=ind.code,
            title=ind.title,
            unit=ind.unit,
            value_kind=ind.value_kind,
            granularity=ind.granularity,
            question_text=ind.question_text,
            dims_schema=ind.dims_schema,
            entity_vid=ind.entity_vid,
        )
    except ValueError as exc:  # defence in depth; pydantic catches this first
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc),
        ) from exc

    rows = [
        {
            "indicator_id": indicator_id,
            "period_start": o.period_start,
            "period_end": o.period_end,
            "dims": o.dims,
            "value": o.value,
            "sample_n": o.sample_n,
            "revision": o.revision,
            "source_doc_id": o.source_doc_id,
        }
        for o in req.observations
    ]
    written = await repo.upsert_observations(rows)
    return LoadResponse(indicator_id=indicator_id, observations=written)
