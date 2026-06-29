"""Frozen wire types for the analytical layer.

Mirrors the style of src/workflow/contracts.py (frozen BaseModel,
Field(default_factory=...) for collections). Serialized by Temporal's
pydantic data converter.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True)


class PrimitiveCall(_Frozen):
    primitive: str
    params: dict[str, Any] = Field(default_factory=dict)


class AnalysisPlan(_Frozen):
    route: Literal["catalog", "cypher"] = "catalog"
    steps: list[PrimitiveCall] = Field(default_factory=list)
    reason: str = ""


class StepResult(_Frozen):
    primitive: str
    params: dict[str, Any] = Field(default_factory=dict)
    cypher: str = ""
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    source_chunks: list[str] = Field(default_factory=list)
    truncated: bool = False


class Provenance(_Frozen):
    route: str = "catalog"
    plan_reason: str = ""
    steps: list[StepResult] = Field(default_factory=list)
    elapsed_ms: int = 0


class AnalyzeParams(_Frozen):
    """AnalyticalQueryWorkflow input (epoch-day bounds, like OrchestratorParams)."""

    query: str
    top_n: int = 20
    date_from_epoch: int | None = None
    date_to_epoch: int | None = None


class AnalyticsOutcome(_Frozen):
    query: str
    answer: str
    provenance: Provenance
    latency_ms: int = 0
