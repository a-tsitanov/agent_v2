"""``tool_execution`` activity — dispatch one atomic tool call.

The workflow calls this each iteration after a successful
``agent_reasoning_step`` returns a non-submit_answer decision.
Activity body resolves dependencies (retriever / graph_retriever /
chunk_repository) once per worker and dispatches via
``src.retrieval.atomic_tools.dispatch`` — exactly the same code path
the MCP-2 server (Stage 4) and the legacy in-process ReAct loop use.

Result rebuilds the LlamaIndex ``NodeWithScore`` objects into
serialised dicts so they survive the Temporal payload boundary.
"""

from __future__ import annotations

import time

from temporalio import activity

from src.retrieval import atomic_tools
from src.workflow._search_deps import (
    get_chunk_repository, get_graph_retriever, get_retriever,
)
from src.workflow._search_serde import (
    node_to_serialized, serialized_to_node,
)
from src.workflow.contracts import ToolCallParams, ToolCallResult


@activity.defn
async def tool_execution(params: ToolCallParams) -> ToolCallResult:
    """Execute one atomic tool and return serialised results."""
    t0 = time.monotonic()
    activity.heartbeat({"stage": "init", "tool": params.tool_name})

    # Lazily resolve dependencies — graph_retriever may be None if
    # Neo4j unreachable; atomic_tools.* handle that path gracefully.
    retriever = await get_retriever()
    graph_retriever = await get_graph_retriever()
    chunk_repository = await get_chunk_repository()

    # filter_by_metadata is the only tool that needs the accumulator —
    # rehydrate just for that path.
    accumulated = (
        [serialized_to_node(n) for n in params.accumulated_sources]
        if params.tool_name == "filter_by_metadata"
        else []
    )

    try:
        result = await atomic_tools.dispatch(
            params.tool_name, params.tool_kwargs,
            retriever=retriever,
            graph_retriever=graph_retriever,
            chunk_repository=chunk_repository,
            accumulated_sources=accumulated,
        )
        sources_added = [node_to_serialized(n) for n in result.sources]
        duration_ms = int((time.monotonic() - t0) * 1000)
        activity.logger.info(
            "tool_execution  tool=%s  sources=%d  ms=%d",
            params.tool_name, len(sources_added), duration_ms,
        )
        return ToolCallResult(
            tool_name=params.tool_name,
            observation=result.observation,
            sources_added=sources_added,
            duration_ms=duration_ms,
        )
    except Exception as exc:  # noqa: BLE001
        # Best-effort: tool failures become observations the agent
        # sees ("error: ...") — but the workflow continues.
        msg = f"tool error: {exc}"
        activity.logger.warning(
            "tool_execution  tool=%s  err=%s", params.tool_name, exc,
        )
        return ToolCallResult(
            tool_name=params.tool_name,
            observation=msg,
            sources_added=[],
            duration_ms=int((time.monotonic() - t0) * 1000),
            error=str(exc),
        )
