"""``SearchOrchestratorWorkflow`` — plan-execute-synthesize (R2).

Thin coordinator replacing the open-ended ReAct loop with a fixed,
parallel pipeline:

  1. ``plan_subquestions``  — split the question into atomic sub-Qs
     (small planner model; ``[query]`` for atomic questions).
  2. fan-out — one ``SubQueryRetrievalWorkflow`` child per sub-question,
     run in PARALLEL via ``asyncio.gather`` over
     ``workflow.execute_child_workflow``.
  3. merge — union all children's sources, dedup by chunk_id.
  4. ``synthesize_answer`` — ONE final synthesis (large model) over the
     merged sources, returning the SAME ``SearchOutcome`` shape the
     legacy ``SearchWorkflow`` returns (so route handlers are reusable).

No "LLM picks next tool" step anywhere — the only LLM calls are the
up-front planner and the final synthesizer.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from src.config import settings
    from src.workflow.contracts import (
        AgenticStepStatDict,
        ContextualizeParams,
        ContextualizeResult,
        CoverageParams,
        CoverageResult,
        OrchestratorParams,
        PlanParams,
        PlanResult,
        RerankParams,
        RerankResult,
        SearchOutcome,
        SerializedNode,
        SubQueryParams,
        SubQueryResult,
        SynthesizeParams,
        SynthesizeResult,
    )
    from src.workflow.search._coverage import (
        build_evidence,
        should_run_coverage_round,
    )
    from src.workflow.search._merge import merge_subquery_sources
    from src.workflow.search._retry import (
        FAST_RETRY,
        LLM_HEARTBEAT_TIMEOUT,
        LLM_SCHEDULE_TO_CLOSE,
        LLM_START_TO_CLOSE,
    )

from src.workflow.search.subquery_wf import SubQueryRetrievalWorkflow


def build_synthesize_call(
    *,
    query: str,
    sources: list[SerializedNode],
    max_refinements: int,
    answer_template: str = "",
) -> tuple[str, SynthesizeParams]:
    """Pure spec for the final ``synthesize_answer`` schedule (R5).

    Returns ``(task_queue, params)`` so the workflow body just unpacks
    and forwards to ``execute_activity``.  Extracted so the large-tier
    routing (``large_task_queue`` + ``use_synthesis_llm=True``) is
    unit-testable WITHOUT a live Temporal env — the orchestrator's
    Temporal-gated body is otherwise hard to assert on.

    ``mode="simple"`` selects the plain ResponseSynthesizer branch in
    ``synthesize_answer``; the user-facing label is "local" (set on
    SearchOutcome in the workflow body).
    """
    params = SynthesizeParams(
        query=query,
        mode="simple",
        accumulated=sources,
        max_refinements=max_refinements,
        use_synthesis_llm=True,  # large tier — build_synthesis_llm()
        answer_template=answer_template,
    )
    return settings.temporal.large_task_queue, params


def cap_synth_sources(
    sources: list[SerializedNode], top_n: int,
) -> list[SerializedNode]:
    """Bound the pool fed to synthesis.  Rerank already trims to top_n;
    on the FAIL-OPEN fallback the workflow passes the raw merged pool —
    cap it here too so a flaky reranker can never blow up the synthesis
    prompt (and the 5-min start_to_close).  ``top_n<=0`` ⇒ no cap."""
    if top_n <= 0:
        return sources
    return sources[:top_n]


def distinct_doc_ids(sources: list[SerializedNode]) -> list[str]:
    """Distinct, order-preserving doc_ids from a source pool's metadata.
    Skips sources without a doc_id (e.g. community partials)."""
    seen: list[str] = []
    for s in sources:
        d = s.metadata.get("doc_id")
        if d and d not in seen:
            seen.append(str(d))
    return seen


@workflow.defn
class SearchOrchestratorWorkflow:
    """Plan-execute-synthesize search session (local mode)."""

    def __init__(self) -> None:
        self._state: dict = {
            "phase": "init",
            "n_subqueries": 0,
            "n_sources": 0,
        }

    @workflow.query
    def get_state(self) -> dict:
        """Progress snapshot (mirrors SearchWorkflow.get_state)."""
        return dict(self._state)

    @workflow.run
    async def run(self, params: OrchestratorParams) -> SearchOutcome:
        log = workflow.logger
        t_start = workflow.now()
        log.info(
            "orchestrator start  query=%s  max_sub=%d",
            params.query[:80], params.max_subqueries,
        )

        # ── 0. contextualise the query against conversation history ──
        # Only when history is present + the feature is on.  Rewrites the
        # follow-up into a standalone question; the model_copy makes the
        # WHOLE downstream pipeline use the standalone query with no other
        # edits.  Empty history ⇒ this block is skipped (back-compat).
        if params.history and params.contextualize_enabled:
            ctx = await workflow.execute_activity(
                "contextualize_query",
                ContextualizeParams(
                    query=params.query, history=list(params.history)),
                start_to_close_timeout=LLM_START_TO_CLOSE,
                schedule_to_close_timeout=LLM_SCHEDULE_TO_CLOSE,
                retry_policy=FAST_RETRY,
                result_type=ContextualizeResult,
            )
            params = params.model_copy(update={"query": ctx.query})

        # ── 1. plan ─────────────────────────────────────────────────
        self._state["phase"] = "plan"
        plan: PlanResult = await workflow.execute_activity(
            "plan_subquestions",
            PlanParams(query=params.query, max_subqueries=params.max_subqueries),
            result_type=PlanResult,
            start_to_close_timeout=LLM_START_TO_CLOSE,
            schedule_to_close_timeout=LLM_SCHEDULE_TO_CLOSE,
            retry_policy=FAST_RETRY,
        )
        subquestions = plan.subquestions or [params.query]
        self._state["n_subqueries"] = len(subquestions)
        log.info("orchestrator  planned %d sub-questions", len(subquestions))

        # ── 2. fan-out: one child workflow per sub-question, parallel ─
        self._state["phase"] = "retrieve"
        child_coros = [
            workflow.execute_child_workflow(
                SubQueryRetrievalWorkflow.run,
                SubQueryParams(
                    subquestion=sub,
                    top_k=params.top_k,
                    doc_date_after_epoch=params.doc_date_after_epoch,
                    doc_date_before_epoch=params.doc_date_before_epoch,
                    inserted_after_epoch=params.inserted_after_epoch,
                    inserted_before_epoch=params.inserted_before_epoch,
                    groups=params.groups,
                    exclude_groups=params.exclude_groups,
                ),
                id=f"{workflow.info().workflow_id}-sub-{i}",
                result_type=SubQueryResult,
            )
            for i, sub in enumerate(subquestions)
        ]
        child_results: list[SubQueryResult] = await asyncio.gather(*child_coros)

        # ── 3. merge + dedup by chunk_id across all sub-questions ────
        merged = merge_subquery_sources([r.sources for r in child_results])
        self._state["n_sources"] = len(merged)
        log.info(
            "orchestrator  merged %d sources from %d children",
            len(merged), len(child_results),
        )

        # Per-sub-question telemetry — reuse the legacy step-stats shape
        # so the response model maps unchanged (one entry per child).
        step_stats = [
            AgenticStepStatDict(
                step=i + 1,
                tool_name="subquery_retrieval",
                tool_args={"subquestion": r.subquestion},
                observation_summary=f"{len(r.sources)} sources",
            )
            for i, r in enumerate(child_results)
        ]

        # ── 3b. coverage gate (R4) ──────────────────────────────────
        # After merging all sub-question sources, ask the small-tier
        # coverage_check whether the evidence FULLY covers the question.
        # On a named gap (and round budget left) issue the gap as ONE
        # extra SubQueryRetrievalWorkflow, re-merge its sources into the
        # pool (dedup by chunk_id), and decrement the budget.  Bounded by
        # ``max_coverage_rounds``.  FAIL-OPEN: any error in the check or
        # the extra round → proceed straight to synthesis (never block
        # the answer).
        rounds_left = params.max_coverage_rounds
        cov_round = 0
        while params.coverage_check_enabled and rounds_left > 0:
            self._state["phase"] = f"coverage_check_{cov_round + 1}"
            try:
                cov: CoverageResult = await workflow.execute_activity(
                    "coverage_check",
                    CoverageParams(
                        query=params.query,
                        evidence=build_evidence(merged),
                    ),
                    result_type=CoverageResult,
                    start_to_close_timeout=timedelta(minutes=2),
                    schedule_to_close_timeout=timedelta(minutes=10),
                    retry_policy=FAST_RETRY,
                )
                gap = should_run_coverage_round(cov, rounds_left)
            except Exception as exc:
                # Fail-open: a flaky coverage call must never block the
                # answer — drop straight through to synthesis.
                log.warning(
                    "orchestrator  coverage_check err=%s — fail-open", exc,
                )
                break

            if gap is None:
                log.info("orchestrator  coverage complete — synthesize")
                break

            # Named gap → one extra sub-question round.
            cov_round += 1
            rounds_left -= 1
            self._state["phase"] = "retrieve_coverage"
            log.info(
                "orchestrator  coverage gap (round %d) → %s",
                cov_round, gap[:120],
            )
            try:
                extra: SubQueryResult = await workflow.execute_child_workflow(
                    SubQueryRetrievalWorkflow.run,
                    SubQueryParams(
                        subquestion=gap,
                        top_k=params.top_k,
                        doc_date_after_epoch=params.doc_date_after_epoch,
                        doc_date_before_epoch=params.doc_date_before_epoch,
                        inserted_after_epoch=params.inserted_after_epoch,
                        inserted_before_epoch=params.inserted_before_epoch,
                        groups=params.groups,
                        exclude_groups=params.exclude_groups,
                    ),
                    id=f"{workflow.info().workflow_id}-cov-{cov_round}",
                    result_type=SubQueryResult,
                )
            except Exception as exc:
                # Fail-open: extra retrieval failed → keep what we have.
                log.warning(
                    "orchestrator  coverage round err=%s — fail-open", exc,
                )
                break

            merged = merge_subquery_sources([merged, extra.sources])
            self._state["n_sources"] = len(merged)
            step_stats.append(AgenticStepStatDict(
                step=len(step_stats) + 1,
                tool_name="subquery_retrieval",
                tool_args={"subquestion": gap, "coverage_round": cov_round},
                observation_summary=f"{len(extra.sources)} sources (coverage)",
            ))
            log.info(
                "orchestrator  re-merged %d sources after coverage round %d",
                len(merged), cov_round,
            )

        # ── 3c. unified graph+vector rerank (R5) ─────────────────────
        # Co-rank the merged pool (graph-derived + vector chunks) in ONE
        # bge cross-encoder pass so the returned ordering reflects the
        # globally most relevant chunks rather than the raw union order.
        # Runs UNCONDITIONALLY now — its output is `SearchOutcome.sources`
        # on both paths, not just synthesis context, so it is no longer
        # discardable work when `synthesize=False`.  The cross-encoder
        # scores every chunk in the pool regardless of `top_n` (`top_n`
        # only trims the trailing slice), so asking for the WHOLE pool
        # (`top_n=len(merged)`) costs the same model work as the old
        # `rerank_top_n` request.  Runs on the small search queue (the
        # model is cheap relative to the large synthesis LLM).
        # FAIL-OPEN: any rerank error → fall back to the unranked merged
        # pool (never block the answer) — the status quo ante.
        self._state["phase"] = "rerank"
        ranked_sources = merged
        try:
            rr: RerankResult = await workflow.execute_activity(
                "rerank_sources",
                RerankParams(
                    query=params.query,
                    sources=merged,
                    top_n=len(merged),
                ),
                result_type=RerankResult,
                start_to_close_timeout=timedelta(minutes=3),
                schedule_to_close_timeout=timedelta(minutes=10),
                retry_policy=FAST_RETRY,
            )
            if rr.sources:
                ranked_sources = list(rr.sources)
                log.info(
                    "orchestrator  reranked %d → %d sources",
                    len(merged), len(ranked_sources),
                )
        except Exception as exc:
            # Fail-open: reranker unavailable → return the pool untouched,
            # i.e. the full merged pool in retrieval order.
            ranked_sources = merged
            log.warning(
                "orchestrator  rerank err=%s — fall back to unranked "
                "merged pool (%d sources)", exc, len(ranked_sources),
            )

        if params.synthesize:
            # ── 4. synthesize once (large model, dedicated large queue) ──
            # Pin synthesize_answer to ``large_task_queue`` so the heavyweight
            # synthesis model runs on its own low-concurrency worker pool
            # (TEMPORAL_LARGE_ACTIVITY_CONCURRENCY).  The call spec (queue +
            # use_synthesis_llm=True) comes from the pure ``build_synthesize_call``
            # helper so it's unit-testable outside Temporal.  Synthesis still
            # gets only the top ``rerank_top_n`` of the ranked pool — the
            # cap that used to bound the whole rerank now bounds just this.
            self._state["phase"] = "synthesize"
            synth_queue, synth_params = build_synthesize_call(
                query=params.query,
                sources=cap_synth_sources(
                    ranked_sources, settings.temporal.rerank_top_n,
                ),
                max_refinements=params.max_refinements,
                answer_template=params.answer_template,
            )
            synth: SynthesizeResult = await workflow.execute_activity(
                "synthesize_answer",
                synth_params,
                task_queue=synth_queue,
                result_type=SynthesizeResult,
                start_to_close_timeout=LLM_START_TO_CLOSE,
                schedule_to_close_timeout=LLM_SCHEDULE_TO_CLOSE,
                heartbeat_timeout=LLM_HEARTBEAT_TIMEOUT,
                retry_policy=FAST_RETRY,
            )
        else:
            # Retrieval-only: the caller composes its own answer from
            # `sources`, which is still the full ranked pool — reranked
            # above like the synthesize=True path, just not fed to an LLM.
            self._state["phase"] = "skip-synthesize"
            synth = SynthesizeResult(text="")

        self._state["phase"] = "done"
        latency_ms = int((workflow.now() - t_start).total_seconds() * 1000)
        return SearchOutcome(
            query=params.query,
            mode="local",
            answer=synth.text,
            sources=ranked_sources,
            documents=distinct_doc_ids(merged),
            step_stats=step_stats,
            citations=list(synth.citations),
            uncertainties=list(synth.uncertainties),
            refinement_rounds=synth.refinement_rounds,
            latency_ms=latency_ms,
        )
