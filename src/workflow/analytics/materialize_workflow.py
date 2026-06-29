"""AnalyticsMaterializeWorkflow — offline GDS materialization pipeline.

Sequence: centrality → link_prediction (optional) → risk (optional).
All activities run on the DEFAULT task queue (kb-graph-build).
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from src.analytics.contracts import (
        CentralityIn,
        LinkPredictionIn,
        MaterializeParams,
        MaterializeResult,
        RiskIn,
        StageResult,
    )

_RETRY = RetryPolicy(
    maximum_attempts=2,
    initial_interval=timedelta(seconds=2),
    maximum_interval=timedelta(seconds=30),
)
_START = timedelta(hours=1)
_S2C = timedelta(hours=3)
_HB = timedelta(minutes=2)


@workflow.defn
class AnalyticsMaterializeWorkflow:
    """Offline materialization: centrality → link prediction → risk."""

    def __init__(self) -> None:
        self._state: dict = {"phase": "init"}

    @workflow.query
    def get_state(self) -> dict:
        """Progress snapshot."""
        return dict(self._state)

    async def _stage(self, name: str, params: object) -> StageResult:
        return await workflow.execute_activity(
            name,
            params,
            result_type=StageResult,
            start_to_close_timeout=_START,
            schedule_to_close_timeout=_S2C,
            heartbeat_timeout=_HB,
            retry_policy=_RETRY,
        )

    @workflow.run
    async def run(self, params: MaterializeParams) -> MaterializeResult:
        errors: list[str] = []

        # ── 1. centrality (always) ───────────────────────────────────
        self._state["phase"] = "centrality"
        c = await self._stage("materialize_centrality", CentralityIn(metrics=params.metrics))
        if c.error:
            errors.append(f"centrality: {c.error}")

        # ── 2. link prediction (optional) ────────────────────────────
        links = StageResult()
        if params.link_prediction:
            self._state["phase"] = "link_prediction"
            links = await self._stage("materialize_link_prediction", LinkPredictionIn())
            if links.error:
                errors.append(f"link_prediction: {links.error}")

        # ── 3. risk (optional) ───────────────────────────────────────
        risk = StageResult()
        if params.risk:
            self._state["phase"] = "risk"
            risk = await self._stage("materialize_risk", RiskIn())
            if risk.error:
                errors.append(f"risk: {risk.error}")

        self._state["phase"] = "done"
        return MaterializeResult(
            centrality_written=c.written,
            links_written=links.written,
            risk_written=risk.written,
            errors=errors,
        )
