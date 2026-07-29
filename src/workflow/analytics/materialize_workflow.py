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
# Timeouts for the materialize stages.  `materialize_centrality` computes
# in-worker with python-igraph, and `g.betweenness` HOLDS THE GIL for its whole
# run — so the activity's `heartbeat_every(30.0)` pulse cannot fire, exactly as
# documented for leidenalg in `search/_retry.py`.  `asyncio.to_thread` does not
# help: the worker thread holds the GIL and the event loop never gets scheduled.
#
# Measured on the production graph (V=78829, E=123908): export 5.4s, pagerank
# 0.8s, eigenvector 0.5s, betweenness 1877.3s (31.3 min).  The old 2-minute
# heartbeat window meant the activity died at exactly 120s on EVERY run — three
# consecutive workflow failures, all `activity Heartbeat timeout`, never once
# reaching link-prediction or risk.
#
# Heartbeating THROUGH a single GIL-held C call is impossible, so the window
# must simply exceed the compute; start-to-close stays the real bound on a
# genuinely stuck run.  betweenness is O(V*E) — doubling the graph roughly
# quadruples it — hence the deliberately generous start-to-close headroom.
# The graph grew 78829 -> 91023 entities in ONE day (2026-07-28 -> 07-29).
# betweenness is O(V*E), so ~15% more nodes is ~32% more compute: the same call
# went from 1877s to roughly 2500s (~42min).  Against the previous 45min window
# that left 3 minutes of margin — one more growth step and the
# `activity Heartbeat timeout` failure comes back.  Size the window to cover
# DOUBLE the last measurement so ordinary growth between deploys is absorbed;
# start-to-close stays the real bound on a genuinely stuck run.
_START = timedelta(hours=3)
_S2C = timedelta(hours=7)
_HB = timedelta(hours=2)


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
