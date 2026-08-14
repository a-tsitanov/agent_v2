"""Deterministic provenance assembly (no LLM)."""

from __future__ import annotations

from src.analytics.catalog import PrimitiveResult
from src.analytics.contracts import AnalysisPlan, PrimitiveCall, Provenance, StepResult


def step_from_primitive(
    call: PrimitiveCall, result: PrimitiveResult, *, error: str = "", error_detail: str = ""
) -> StepResult:
    return StepResult(
        primitive=call.primitive,
        params=call.params,
        cypher=result.cypher,
        rows=result.rows,
        row_count=len(result.rows),
        source_chunks=list(result.source_chunks),
        truncated=result.truncated,
        error=error,
        error_detail=error_detail,
    )


def assemble_provenance(plan: AnalysisPlan, steps: list[StepResult], elapsed_ms: int) -> Provenance:
    return Provenance(
        route=plan.route,
        plan_reason=plan.reason,
        steps=list(steps),
        elapsed_ms=elapsed_ms,
    )
