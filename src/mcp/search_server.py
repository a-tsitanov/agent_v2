"""MCP-1: high-level search server.

Exposes one tool — ``kb_search(query, mode)`` — that submits the
project ``SearchWorkflow`` to Temporal and forwards progress back
to the client as MCP ``notifications/progress`` messages while it
runs.  Returns the synthesized answer + structured fields
(citations, uncertainties, refinement_rounds) when the workflow
completes.

Run::

    # Stdio — Claude Desktop / Cursor / Continue
    uv run python -m src.mcp.search_server --transport stdio

    # HTTP/SSE — OpenWebUI
    uv run python -m src.mcp.search_server --transport sse --port 9001
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any, Literal

from fastmcp import Context, FastMCP
from loguru import logger
from temporalio.common import WorkflowIDReusePolicy

from src.config import settings
from src.mcp._shared import (
    assert_api_key_env_set, log_banner, parse_args,
)
from src.workflow.client import get_temporal_client
from src.workflow.contracts import SearchOutcome, SearchParams
from src.workflow.search_workflow import SearchWorkflow


mcp = FastMCP(
    name="kb-llamaindex-search",
    instructions=(
        "High-level search over the project knowledge base.  The "
        "underlying ReAct agent reads the corpus through vector + "
        "graph retrievers and synthesises a Russian answer with "
        "citations.  Use `mode='selfrag'` for highest fidelity, "
        "`mode='agent'` for balanced default, `mode='simple'` for "
        "fast single-shot answers."
    ),
)


@mcp.tool()
async def kb_search(
    query: str,
    ctx: Context,
    mode: Literal["simple", "agent", "selfrag"] = "agent",
    max_iterations: int = 8,
    max_refinements: int = 3,
) -> dict[str, Any]:
    """Search the project knowledge base.

    Args:
      query: question in Russian (or the source-document language).
      mode:  simple = single retrieve+synth; agent = ReAct loop
             (recommended default); selfrag = reflective synthesis
             with [NEED]/[SUPPORTED]/[UNCERTAIN] markers.
      max_iterations: cap for the ReAct loop (modes agent/selfrag).
      max_refinements: cap for the reflective loop (mode selfrag).

    Returns:
      {
        "answer": str,
        "sources": [{chunk_id, doc_id, text, score}, ...],
        "citations": [...],         # selfrag only
        "uncertainties": [...],     # selfrag only
        "refinement_rounds": int,
        "step_stats": [...],
        "latency_ms": int,
      }
    """
    request_id = uuid.uuid4().hex
    workflow_id = f"mcp-search-{request_id}"
    client = await get_temporal_client()
    handle = await client.start_workflow(
        SearchWorkflow.run,
        SearchParams(
            query=query,
            mode=mode,
            max_iterations=max_iterations,
            max_refinements=max_refinements,
            request_id=request_id,
            version_tag=settings.analytics.default_version_tag,
            env=settings.analytics.env_name,
        ),
        id=workflow_id,
        task_queue=settings.temporal.search_task_queue,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    )

    # Poll workflow state for progress and forward to MCP client.
    # Loop steps + max_iterations give a coarse progress fraction;
    # use `last_tool` text for the message.
    result_task = asyncio.create_task(handle.result())
    try:
        while not result_task.done():
            try:
                state = await handle.query(SearchWorkflow.get_state)
                progress = min(
                    1.0,
                    (state.get("steps_completed") or 0) / max(1, max_iterations),
                )
                await ctx.report_progress(
                    progress=progress,
                    total=1.0,
                    message=(
                        f"{state.get('phase','init')}  "
                        f"step={state.get('steps_completed')}  "
                        f"last_tool={state.get('last_tool')}  "
                        f"sources={state.get('n_sources')}"
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "query state failed (transient): {e}", e=exc,
                )
            try:
                await asyncio.wait_for(asyncio.shield(result_task), 0.3)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                logger.info(
                    "mcp kb_search cancelled by client, cancelling workflow "
                    "{wid}", wid=workflow_id,
                )
                await handle.cancel()
                raise

        outcome: SearchOutcome = result_task.result()
    except asyncio.CancelledError:
        await handle.cancel()
        raise

    return {
        "answer": outcome.answer,
        "mode": outcome.mode,
        "query": outcome.query,
        "sources": [
            {
                "chunk_id": n.chunk_id,
                "doc_id": str(n.metadata.get("doc_id")
                              or n.metadata.get("file_path") or ""),
                "text": n.text,
                "score": n.score,
            }
            for n in outcome.sources
        ],
        "citations": [c.model_dump() for c in outcome.citations],
        "uncertainties": [u.model_dump() for u in outcome.uncertainties],
        "refinement_rounds": outcome.refinement_rounds,
        "step_stats": [s.model_dump() for s in outcome.step_stats],
        "latency_ms": outcome.latency_ms,
    }


def main() -> None:
    args = parse_args()
    assert_api_key_env_set()
    log_banner(
        "kb-llamaindex-search",
        transport=args["transport"], host=args["host"], port=args["port"],
    )
    if args["transport"] == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp.run(
            transport="sse",
            host=args["host"], port=args["port"],
        )


if __name__ == "__main__":
    main()
