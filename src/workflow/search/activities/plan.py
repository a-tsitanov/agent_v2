"""``plan_subquestions`` activity — decompose a question (R2).

Wraps ``src.retrieval.query_planner.decompose`` behind the Temporal
activity boundary.  Builds the small-tier planner LLM (role ``plan``)
once per worker process and runs the decomposition.  Fail-safe by
construction — ``decompose`` returns ``[query]`` on any planner error,
so this activity never fails the orchestrating workflow.
"""

from __future__ import annotations

import time

from temporalio import activity

from src.workflow._search_plan_deps import get_plan_llm
from src.workflow.contracts import PlanParams, PlanResult


@activity.defn
async def plan_subquestions(params: PlanParams) -> PlanResult:
    """Split the query into atomic sub-questions (≥1)."""
    t0 = time.monotonic()
    activity.heartbeat({"stage": "init", "query": params.query[:80]})
    from src.retrieval.query_planner import decompose

    llm = await get_plan_llm()
    subs = await decompose(
        params.query, llm, max_subqueries=params.max_subqueries,
    )
    activity.logger.info(
        "plan_subquestions  n=%d  ms=%d",
        len(subs), int((time.monotonic() - t0) * 1000),
    )
    return PlanResult(subquestions=subs)
