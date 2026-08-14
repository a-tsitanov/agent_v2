"""Temporal activities for the analytical layer.

Three activities:
  * analytical_plan      — NL → AnalysisPlan (LLM, "plan" role)
  * execute_step         — run one primitive from CATALOG against Neo4j
  * synthesize_analytical — AnalysisPlan results → verbalized answer (LLM, "synthesis" role)

Module-level lists for worker registration:
  * ANALYTICS_ACTIVITIES       = [analytical_plan, execute_step]
  * ANALYTICS_LARGE_ACTIVITIES = [synthesize_analytical]
"""

from __future__ import annotations

from temporalio import activity

import src.analytics.primitives  # noqa: F401 — triggers CATALOG registration at worker load
from src.analytics.catalog import CATALOG, PrimitiveResult
from src.analytics.contracts import (
    AnalysisPlan,
    ExecInput,
    PlanInput,
    StepResult,
    SynthInput,
    SynthResult,
)
from src.analytics.planner import plan_query
from src.analytics.provenance import step_from_primitive
from src.analytics.synthesis import build_synthesis_prompt
from src.graph.store import build_graph_store
from src.retrieval.llm_pool import get_llm_pool


@activity.defn
async def analytical_plan(p: PlanInput) -> AnalysisPlan:
    """Call the planner LLM and return a structured AnalysisPlan."""
    llm = get_llm_pool().get("plan")
    return await plan_query(p.query, llm, max_steps=p.max_steps)


@activity.defn
async def execute_step(p: ExecInput) -> StepResult:
    """Run a single primitive from CATALOG; fail-soft on unknown primitive or bad kwargs."""
    prim = CATALOG.get(p.call.primitive)
    if prim is None:
        return StepResult(
            primitive=p.call.primitive,
            params=p.call.params,
            rows=[],
            row_count=0,
        )

    store = build_graph_store()
    params = dict(p.call.params)

    # Inject default top_n when the primitive accepts it and the planner omitted it.
    if "top_n" in prim.param_model.model_fields and "top_n" not in params:
        params["top_n"] = p.top_n

    call = p.call.model_copy(update={"params": params})
    try:
        result: PrimitiveResult = await prim.fn(store, **params)
    except TypeError:
        # Planner produced params the fn doesn't accept → fail-soft empty.
        result = PrimitiveResult(cypher="", params=params, rows=[])
    except NotImplementedError as exc:
        # The backend structurally cannot run this query (e.g. Nebula refusing
        # a parameterised nGQL call) — report it, don't present it as a
        # computed zero. Unlike a transient query error this will not
        # self-heal on retry, so it's logged at error level (not warning) —
        # grep-able across a week of worker logs, which is the question
        # provenance on a single request can't answer.
        activity.logger.error(
            "execute_step  primitive=%s cannot run on this backend "
            "(structural, will not succeed on retry): %s",
            call.primitive,
            exc,
        )
        result = PrimitiveResult(cypher="", params=params, rows=[])
        return step_from_primitive(
            call,
            result,
            # Short and caller-safe — this reaches the synthesis prompt and,
            # from there, can reach the user-facing answer. The raw exception
            # (nGQL/param_map/Phase 2 internals) is operator detail only.
            error="this primitive is not supported by the current graph backend",
            error_detail=str(exc),
        )

    return step_from_primitive(call, result)


@activity.defn
async def synthesize_analytical(p: SynthInput) -> SynthResult:
    """Verbalize the step results into a natural-language answer."""
    if not any(s.rows for s in p.steps):
        return SynthResult(text="Не удалось вычислить ответ по доступным аналитическим примитивам.")
    llm = get_llm_pool().get("synthesis")
    resp = await llm.achat(build_synthesis_prompt(p.query, p.steps))
    return SynthResult(text=(resp.message.content or "").strip())


ANALYTICS_ACTIVITIES = [analytical_plan, execute_step]
ANALYTICS_LARGE_ACTIVITIES = [synthesize_analytical]
