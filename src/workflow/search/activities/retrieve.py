"""``retrieve_subquestion`` activity — deterministic retrieval (R2).

For ONE sub-question, runs a fixed pipeline — hybrid ``vector_search``
plus ``graph_search`` — by reusing ``atomic_tools.dispatch`` (the same
code path as the legacy ``tool_execution`` activity and the MCP server).
NO LLM tool selection / no ReAct: the plan-execute flow decides the
tools up front, so this activity is purely deterministic retrieval.

Sources from both tools are merged and deduped by chunk_id, then
serialised back across the Temporal boundary exactly as
``tool_execution`` does.  A failure in one tool is logged and does not
sink the activity — we still return whatever the other tool found.
"""

from __future__ import annotations

import time

from temporalio import activity

from src.retrieval import atomic_tools
from src.workflow._search_deps import get_graph_retriever, get_retriever
from src.workflow._search_serde import node_to_serialized
from src.workflow.contracts import RetrieveParams, RetrieveResult

# Deterministic tool pipeline for one sub-question.  Vector first
# (always available), then graph (skipped gracefully if Neo4j is down —
# atomic_tools.graph_search returns empty for a None retriever).
#
# graph_walk (bounded multi-hop, R3) is registered in atomic_tools and
# dispatchable on this path via the same graph_retriever DI — it is NOT
# in the default deterministic pipeline because it needs an explicit
# `start_entity` (a real entity name), which only an LLM tool-pick step
# can supply. When R3's connection-aware planner lands, add "graph_walk"
# to a per-sub-question pipeline keyed on questions classified as
# multi-hop/connection ('как связаны', 'через цепочку'); the dispatch
# wiring + caps are already in place here.
_PIPELINE = ("vector_search", "graph_search")

# Tools this deterministic activity is ALLOWED to dispatch (default
# pipeline + the explicitly-bounded multi-hop walk available for the
# connection-aware path). Kept as the contract surface for R3 wiring.
ALLOWED_TOOLS = ("vector_search", "graph_search", "graph_walk")


@activity.defn
async def retrieve_subquestion(params: RetrieveParams) -> RetrieveResult:
    """Run the deterministic retrieve pipeline for one sub-question."""
    t0 = time.monotonic()
    activity.heartbeat({"stage": "init", "sub": params.subquestion[:80]})

    retriever = await get_retriever()
    graph_retriever = await get_graph_retriever()

    collected = []  # list[NodeWithScore]
    seen: set[str] = set()
    errors: list[str] = []

    for tool_name in _PIPELINE:
        try:
            result = await atomic_tools.dispatch(
                tool_name,
                {"query": params.subquestion},
                retriever=retriever,
                graph_retriever=graph_retriever,
            )
        except Exception as exc:
            # One tool failing must not lose the other's results.
            activity.logger.warning(
                "retrieve_subquestion  tool=%s  err=%s", tool_name, exc,
            )
            errors.append(f"{tool_name}: {exc}")
            continue
        for n in result.sources:
            cid = n.node.node_id
            if cid in seen:
                continue
            seen.add(cid)
            collected.append(n)

    sources = [node_to_serialized(n) for n in collected]
    duration_ms = int((time.monotonic() - t0) * 1000)
    activity.logger.info(
        "retrieve_subquestion  sub=%s  sources=%d  ms=%d",
        params.subquestion[:60], len(sources), duration_ms,
    )
    return RetrieveResult(
        subquestion=params.subquestion,
        sources=sources,
        duration_ms=duration_ms,
        error="; ".join(errors),
    )
