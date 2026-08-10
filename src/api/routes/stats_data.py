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
from math import isfinite
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


def _first_non_finite_float(obj: Any) -> float | None:
    """DFS through nested dicts/lists for the first non-finite float.

    `dims` / `dims_schema` are `dict[str, Any]` — pydantic never looks
    inside `Any`, so a `NaN`/`Infinity` can hide at any depth (a value,
    or nested inside a dict/list value) and still reach
    `json.dumps(...)` in `StatsRepository`, producing a bare `NaN` token
    that Postgres rejects as invalid jsonb.
    """
    if isinstance(obj, float):
        return obj if not isfinite(obj) else None
    if isinstance(obj, dict):
        for v in obj.values():
            found = _first_non_finite_float(v)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = _first_non_finite_float(v)
            if found is not None:
                return found
    return None


def _non_finite_anywhere_is_not_a_value(v: Any) -> Any:
    """Same rationale as `ObservationIn._non_finite_is_not_a_value`, applied
    to a whole `dict[str, Any]` rather than a single `float` field.

    A NaN/Infinity nested anywhere inside `dims` / `dims_schema` reaches
    `json.dumps(...)` in `StatsRepository` and produces a bare `NaN` /
    `Infinity` token, which Postgres rejects as invalid jsonb — a 500,
    not a 422.  Swapping the whole dict for text (instead of raising with
    the original dict as `input`) makes pydantic's own dict-type check
    reject it naturally, which keeps the 422 body serialisable: the
    offending float never reaches the error payload.
    """
    if isinstance(v, dict):
        bad = _first_non_finite_float(v)
        if bad is not None:
            return f"non-finite value ({bad!r}) is not allowed anywhere in this object"
    return v


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

    @field_validator("dims_schema", mode="before")
    @classmethod
    def _non_finite_dims_schema_is_not_a_value(cls, v: Any) -> Any:
        return _non_finite_anywhere_is_not_a_value(v)


class ObservationIn(BaseModel):
    period_start: date
    period_end: date
    # `NaN` / `Infinity` are not JSON, but `json.loads` — which is what
    # Starlette calls — accepts the literals, and a plain `float` accepts
    # what it hands over.  Stored, they poison the read side silently:
    # `align` returns `divergence: NaN` with no warning, and `nan` cannot
    # be serialised back out as JSON at all.  This subsystem exists to
    # keep numbers exact; a non-number is not a value.
    value: float = Field(allow_inf_nan=False)
    dims: dict[str, Any] = Field(default_factory=dict)
    # A respondent count.  Negative is not a small sample, it is corrupt
    # input; zero is legitimate (a published cut with nobody in it).
    sample_n: int | None = Field(default=None, ge=0)
    revision: int = Field(default=0, ge=0)
    source_doc_id: uuid.UUID | None = None

    @field_validator("value", mode="before")
    @classmethod
    def _non_finite_is_not_a_value(cls, v: Any) -> Any:
        """Swap a non-finite float for text BEFORE pydantic records it.

        `allow_inf_nan=False` above is what rejects it, but pydantic
        echoes the offending input back inside the 422 body, and
        Starlette's `JSONResponse.render` serialises with
        `allow_nan=False` — so rendering the validation error would
        itself raise and the caller would get a 500, not a rejection.
        Replacing the value with a string keeps the refusal serialisable
        and puts the reason where the caller reads it.
        """
        if isinstance(v, float) and not isfinite(v):
            return f"non-finite value ({v!r}) is not a number"
        return v

    @field_validator("dims", mode="before")
    @classmethod
    def _non_finite_dims_is_not_a_value(cls, v: Any) -> Any:
        return _non_finite_anywhere_is_not_a_value(v)

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
            status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc),
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
