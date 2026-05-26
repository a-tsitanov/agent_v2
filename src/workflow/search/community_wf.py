"""``CommunityBuildWorkflow`` — offline graph-community build (Search R6).

DECOUPLED / OFFLINE — runs on the dedicated ``kb-graph-build`` queue,
triggered by the admin endpoint (or an optional Temporal Schedule), NEVER
on the query hot path.

Pipeline:

  1. ``detect_communities_activity`` — GDS Leiden over ``__Entity__``,
     materialise ``:Community`` nodes (idempotent MERGE).
  2. fan-out — one ``summarize_community_activity`` per detected community,
     run with BOUNDED parallelism (``community_summary_parallelism``) so a
     single rebuild doesn't flood the small-tier LLM proxy.
  3. done — counts returned; summaries live on ``:Community.summary``.

Idempotent / incremental: re-running UPDATES community summaries and
refreshes membership (the MERGE keys on ``(:Community {id, level})``) — it
never duplicates communities.

The summarize fan-out spec is extracted into the pure
``build_summarize_specs`` helper so the per-community batching logic is
unit-testable without a live Temporal env (see
[[kb-llamaindex-search-arch]] pure-helper convention).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from src.config import settings
    from src.workflow.contracts import (
        CommunityBuildResult,
        DetectCommunitiesParams,
        DetectCommunitiesResult,
        SummarizeCommunityParams,
        SummarizeCommunityResult,
    )
    from src.workflow.search._retry import FAST_RETRY


def build_summarize_specs(
    detect: DetectCommunitiesResult,
) -> list[SummarizeCommunityParams]:
    """Pure spec: detected communities → batchable summarize params.

    One ``SummarizeCommunityParams`` per community, preserving id/level/
    members.  Extracted so the fan-out shape is unit-testable outside
    Temporal.  Re-running over the same detection yields identical specs
    (idempotent) — the activity's MERGE then updates the same node.
    """
    return [
        SummarizeCommunityParams(
            community_id=c.community_id,
            level=c.level,
            members=list(c.members),
        )
        for c in detect.communities
    ]


@workflow.defn
class CommunityBuildWorkflow:
    """Offline detect → summarize community build."""

    def __init__(self) -> None:
        self._state: dict = {"phase": "init", "detected": 0, "summarized": 0}

    @workflow.query
    def get_state(self) -> dict:
        return dict(self._state)

    @workflow.run
    async def run(self, params: DetectCommunitiesParams) -> CommunityBuildResult:
        log = workflow.logger
        log.info(
            "community_build start  min_size=%d  level=%d",
            params.min_size, params.level,
        )

        # ── 1. detect (GDS Leiden + :Community MERGE) ───────────────
        self._state["phase"] = "detect"
        detect: DetectCommunitiesResult = await workflow.execute_activity(
            "detect_communities_activity",
            params,
            result_type=DetectCommunitiesResult,
            start_to_close_timeout=timedelta(minutes=20),
            schedule_to_close_timeout=timedelta(minutes=30),
            retry_policy=FAST_RETRY,
        )
        specs = build_summarize_specs(detect)
        self._state["detected"] = len(specs)
        log.info("community_build  detected %d communities", len(specs))

        # ── 2. summarize each (bounded parallelism) ─────────────────
        self._state["phase"] = "summarize"
        sem = asyncio.Semaphore(
            max(1, settings.temporal.community_summary_parallelism),
        )

        async def _summarize_one(
            spec: SummarizeCommunityParams,
        ) -> SummarizeCommunityResult:
            async with sem:
                return await workflow.execute_activity(
                    "summarize_community_activity",
                    spec,
                    result_type=SummarizeCommunityResult,
                    start_to_close_timeout=timedelta(minutes=5),
                    schedule_to_close_timeout=timedelta(minutes=15),
                    retry_policy=FAST_RETRY,
                )

        results: list[SummarizeCommunityResult] = await asyncio.gather(
            *[_summarize_one(s) for s in specs],
        )
        summarized = sum(1 for r in results if r.persisted)
        self._state["summarized"] = summarized
        self._state["phase"] = "done"
        log.info(
            "community_build done  detected=%d  summarized=%d",
            len(specs), summarized,
        )
        return CommunityBuildResult(detected=len(specs), summarized=summarized)
