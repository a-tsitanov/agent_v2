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
from src.graph.store import build_neo4j_graph_store
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

    store = build_neo4j_graph_store()
    params = dict(p.call.params)

    # Inject default top_n when the primitive accepts it and the planner omitted it.
    if "top_n" in prim.param_model.model_fields and "top_n" not in params:
        params["top_n"] = p.top_n

    try:
        result: PrimitiveResult = await prim.fn(store, **params)
    except TypeError:
        # Planner produced params the fn doesn't accept → fail-soft empty.
        result = PrimitiveResult(cypher="", params=params, rows=[])

    return step_from_primitive(p.call.model_copy(update={"params": params}), result)


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
