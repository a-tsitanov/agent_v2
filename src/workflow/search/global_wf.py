"""``GlobalSearchWorkflow`` — GraphRAG global map-reduce (Search R7a).

Decision C (global search): for corpus-level / thematic questions, answer
by MAP-REDUCE over the community summaries built offline in R6 instead of
retrieving individual chunks:

  1. ``map_communities`` — read the relevant ``:Community.summary`` texts
     (small-tier, ranked + capped by ``max_communities``).
  2. MAP — fan out one ``map_community_partial`` per community (SMALL tier,
     bounded by ``map_parallelism``); off-topic communities self-drop.
  3. REDUCE — wrap the surviving partials as synthesis context and run the
     existing ``synthesize_answer`` ONCE, pinned to ``large_task_queue``
     with ``use_synthesis_llm=True`` (the R5 large-tier pattern).

Returns the SAME ``SearchOutcome`` shape as the local orchestrator so the
route handlers map it onto ``SearchResponse`` unchanged.

Drift support (``drift_mode``): the route handler passes ``drift_seed``
local sources; the workflow MERGES them ahead of the community partials in
the REDUCE context and labels the outcome ``"drift"``.  Plain global runs
with no seed and labels ``"global"``.

Map-spec assembly, the reduce-context builder and the synthesize call spec
are extracted into pure helpers so the map-reduce wiring is unit-testable
WITHOUT a live Temporal env (the pure-helper convention used across the R2
–R6 workflows).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from src.config import settings
    from src.workflow.contracts import (
        AgenticStepStatDict,
        CommunitySummaryRef,
        ContextualizeParams,
        ContextualizeResult,
        DocumentsForCommunitiesParams,
        DocumentsForCommunitiesResult,
        GlobalSearchParams,
        MapCommunitiesParams,
        MapCommunitiesResult,
        MapPartialParams,
        MapPartialResult,
        SearchMode,
        SearchOutcome,
        SerializedNode,
        SynthesizeParams,
        SynthesizeResult,
    )
    from src.workflow.search._merge import merge_subquery_sources
    from src.workflow.search._retry import (
        FAST_RETRY, LLM_SCHEDULE_TO_CLOSE, LLM_START_TO_CLOSE,
    )


def _coerce_global_params(
    params: GlobalSearchParams | dict,
) -> GlobalSearchParams:
    """Belt-and-suspenders: a misconfigured data converter (or an older
    temporalio whose default converter can't rebuild Pydantic v2 models)
    can hand workflow args back as a plain ``dict`` instead of the typed
    model.  Coerce so ``params.drift_mode`` never raises ``AttributeError``."""
    if isinstance(params, dict):
        return GlobalSearchParams(**params)
    return params


def build_map_specs(
    communities: list[CommunitySummaryRef], *, query: str,
) -> list[MapPartialParams]:
    """Pure spec: community summaries → per-community MAP params.

    One ``MapPartialParams`` per community.  Extracted so the fan-out
    shape is unit-testable outside Temporal."""
    return [
        MapPartialParams(
            query=query,
            community_id=c.community_id,
            summary=c.summary,
        )
        for c in communities
    ]


def surviving_community_ids(partials: list[MapPartialResult]) -> list[str]:
    """Community ids of partials that contributed to REDUCE (non-empty,
    score>0) — the communities behind the answer."""
    return [p.community_id for p in partials if p.partial and p.score > 0.0]


def partials_to_sources(
    partials: list[MapPartialResult],
) -> list[SerializedNode]:
    """Pure: surviving (scored) MAP partials → synthesis context nodes.

    Each relevant partial becomes a ``SerializedNode`` whose chunk_id
    encodes its community so REDUCE can cite the community of origin.
    Off-topic partials (empty text / score 0) are dropped.  Extracted so
    the reduce-context assembly is unit-testable outside Temporal."""
    out: list[SerializedNode] = []
    for p in partials:
        text = (p.partial or "").strip()
        if not text or p.score <= 0.0:
            continue
        out.append(SerializedNode(
            chunk_id=f"community:{p.community_id}",
            text=text,
            score=float(p.score),
            metadata={"community_id": p.community_id, "source": "community"},
        ))
    return out


def build_reduce_call(
    *,
    query: str,
    sources: list[SerializedNode],
    max_refinements: int,
) -> tuple[str, SynthesizeParams]:
    """Pure spec for the global REDUCE ``synthesize_answer`` schedule.

    Mirrors the orchestrator's ``build_synthesize_call`` (R5): pins the
    final synthesis to ``large_task_queue`` with ``use_synthesis_llm=True``
    so REDUCE runs on the heavyweight tier exactly like the local flow.
    Returns ``(task_queue, params)`` so the workflow body just forwards."""
    params = SynthesizeParams(
        query=query,
        mode="simple",
        accumulated=sources,
        max_refinements=max_refinements,
        use_synthesis_llm=True,  # large tier — build_synthesis_llm()
    )
    return settings.temporal.large_task_queue, params


@workflow.defn
class GlobalSearchWorkflow:
    """GraphRAG global search — map-reduce over community summaries."""

    def __init__(self) -> None:
        self._state: dict = {
            "phase": "init",
            "n_communities": 0,
            "n_partials": 0,
        }

    @workflow.query
    def get_state(self) -> dict:
        return dict(self._state)

    @workflow.run
    async def run(
        self,
        params: GlobalSearchParams,
        drift_seed: list[SerializedNode] | None = None,
    ) -> SearchOutcome:
        log = workflow.logger
        t_start = workflow.now()
        params = _coerce_global_params(params)
        if drift_seed and isinstance(drift_seed[0], dict):
            drift_seed = [SerializedNode(**n) for n in drift_seed]
        mode: SearchMode = "drift" if params.drift_mode else "global"
        log.info(
            "global_search start  mode=%s  query=%s  max_comm=%d",
            mode, params.query[:80], params.max_communities,
        )

        # ── 0. contextualise the query against conversation history ──
        # Only when history is present + the feature is on.  Rewrites the
        # follow-up into a standalone question so the whole map-reduce uses
        # the standalone query.  Empty history ⇒ skipped (back-compat).
        # NOTE: drift dispatches global as a child with history CLEARED, so
        # this block does NOT re-run for the drift path.
        if params.history and settings.agent.conversation_history_enabled:
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

        # ── 1. read community summaries (ranked + capped) ───────────
        self._state["phase"] = "map_communities"
        comm: MapCommunitiesResult = await workflow.execute_activity(
            "map_communities",
            MapCommunitiesParams(
                query=params.query,
                level=params.level,
                limit=params.max_communities,
            ),
            result_type=MapCommunitiesResult,
            start_to_close_timeout=timedelta(minutes=2),
            schedule_to_close_timeout=timedelta(minutes=10),
            retry_policy=FAST_RETRY,
        )
        self._state["n_communities"] = len(comm.communities)
        log.info("global_search  %d communities to map", len(comm.communities))

        # ── 2. MAP: per-community partial (small tier, bounded fan-out) ─
        self._state["phase"] = "map"
        specs = build_map_specs(comm.communities, query=params.query)
        sem = asyncio.Semaphore(max(1, params.map_parallelism))

        async def _map_one(spec: MapPartialParams) -> MapPartialResult:
            async with sem:
                return await workflow.execute_activity(
                    "map_community_partial",
                    spec,
                    result_type=MapPartialResult,
                    start_to_close_timeout=LLM_START_TO_CLOSE,
                    schedule_to_close_timeout=LLM_SCHEDULE_TO_CLOSE,
                    retry_policy=FAST_RETRY,
                )

        partials: list[MapPartialResult] = await asyncio.gather(
            *[_map_one(s) for s in specs],
        )
        partial_sources = partials_to_sources(partials)
        self._state["n_partials"] = len(partial_sources)
        log.info(
            "global_search  %d/%d communities produced a partial",
            len(partial_sources), len(specs),
        )

        # Drift: merge caller-supplied local sources AHEAD of the community
        # partials (local evidence leads, community context expands it),
        # dedup by chunk_id.  Plain global runs with the partials alone.
        reduce_sources = (
            merge_subquery_sources([list(drift_seed or []), partial_sources])
            if params.drift_mode
            else partial_sources
        )

        # Per-community telemetry — reuse the legacy step-stats shape so the
        # response model maps unchanged (one entry per surviving partial).
        step_stats = [
            AgenticStepStatDict(
                step=i + 1,
                tool_name="community_partial",
                tool_args={"community_id": p.community_id},
                observation_summary=p.partial[:120],
            )
            for i, p in enumerate(partials)
            if p.partial and p.score > 0.0
        ]

        docs_res: DocumentsForCommunitiesResult = await workflow.execute_activity(
            "documents_for_communities",
            DocumentsForCommunitiesParams(
                community_ids=surviving_community_ids(partials)),
            result_type=DocumentsForCommunitiesResult,
            start_to_close_timeout=timedelta(minutes=2),
            schedule_to_close_timeout=timedelta(minutes=10),
            retry_policy=FAST_RETRY,
        )

        # ── 3. REDUCE: single large-tier synthesis (pinned large queue) ─
        self._state["phase"] = "reduce"
        reduce_queue, reduce_params = build_reduce_call(
            query=params.query,
            sources=reduce_sources,
            max_refinements=params.max_refinements,
        )
        synth: SynthesizeResult = await workflow.execute_activity(
            "synthesize_answer",
            reduce_params,
            task_queue=reduce_queue,
            result_type=SynthesizeResult,
            start_to_close_timeout=LLM_START_TO_CLOSE,
            schedule_to_close_timeout=LLM_SCHEDULE_TO_CLOSE,
            retry_policy=FAST_RETRY,
        )

        self._state["phase"] = "done"
        latency_ms = int((workflow.now() - t_start).total_seconds() * 1000)
        return SearchOutcome(
            query=params.query,
            mode=mode,
            answer=synth.text,
            sources=reduce_sources,
            step_stats=step_stats,
            citations=list(synth.citations),
            uncertainties=list(synth.uncertainties),
            refinement_rounds=synth.refinement_rounds,
            latency_ms=latency_ms,
            documents=list(docs_res.doc_ids),
        )
