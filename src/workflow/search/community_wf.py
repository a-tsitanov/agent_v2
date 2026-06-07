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
    from src.workflow.search._retry import (
        FAST_RETRY, LLM_SCHEDULE_TO_CLOSE, LLM_START_TO_CLOSE,
    )


def build_summarize_specs(
    detect: DetectCommunitiesResult,
) -> list[SummarizeCommunityParams]:
    """Pure spec: detected communities → batchable summarize params.

    One ``SummarizeCommunityParams`` per community that NEEDS a report,
    preserving id/level/members.  Communities carried over unchanged from a
    prior build (``needs_report=False``, set by ``detect_hierarchy``) are
    skipped — their report is already persisted.  Extracted so the fan-out
    shape is unit-testable outside Temporal.  Re-running over the same
    detection yields identical specs (idempotent) — the activity's MERGE
    then updates the same node.
    """
    return [
        SummarizeCommunityParams(
            community_id=c.community_id,
            level=c.level,
            members=list(c.members),
        )
        for c in detect.communities
        if getattr(c, "needs_report", True)
    ]


def group_specs_by_level(
    specs: list[SummarizeCommunityParams],
) -> list[list[SummarizeCommunityParams]]:
    """Group summarize specs by ``level``, FINEST-first.

    Returns a list of per-level groups ordered so the FINEST level (highest
    ``level`` number) comes first and the COARSEST (level 0) last.  The build
    workflow processes groups in this order so a coarser parent's child
    reports are already persisted before it runs (``_CHILD_REPORTS_CYPHER``
    reads only children that already have a report).  Pure / unit-testable;
    within-group order is preserved from ``specs``.
    """
    by_level: dict[int, list[SummarizeCommunityParams]] = {}
    for spec in specs:
        by_level.setdefault(spec.level, []).append(spec)
    return [by_level[level] for level in sorted(by_level, reverse=True)]


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

        # ── 2. summarize level-by-level, FINEST-first ───────────────
        # A coarser parent's report is composed from its CHILD reports
        # (_CHILD_REPORTS_CYPHER reads children that already have a report),
        # so the finest level must be fully persisted before the next
        # coarser level starts.  Within a level there is no such dependency,
        # so it fans out with bounded parallelism.  Pure grouping +
        # execute_activity only → replay-deterministic (no clock/random).
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
                    start_to_close_timeout=LLM_START_TO_CLOSE,
                    schedule_to_close_timeout=LLM_SCHEDULE_TO_CLOSE,
                    retry_policy=FAST_RETRY,
                )

        summarized = 0
        for group in group_specs_by_level(specs):
            level = group[0].level if group else -1
            results: list[SummarizeCommunityResult] = await asyncio.gather(
                *[_summarize_one(s) for s in group],
            )
            summarized += sum(1 for r in results if r.persisted)
            self._state["summarized"] = summarized
            log.info(
                "community_build  level=%d summarized %d/%d (running total %d)",
                level, sum(1 for r in results if r.persisted), len(group),
                summarized,
            )
        self._state["phase"] = "done"
        log.info(
            "community_build done  detected=%d  summarized=%d",
            len(specs), summarized,
        )
        return CommunityBuildResult(detected=len(specs), summarized=summarized)
