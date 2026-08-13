"""MCP-1: high-level search server.

Exposes four orchestrated search tools, each submitting a Temporal
workflow and forwarding progress to the client as MCP
``notifications/progress`` while it runs, then returning a synthesized
answer + structured fields (citations, uncertainties, …):

  * ``kb_search``        — local plan-execute-synthesize
                           (``SearchOrchestratorWorkflow``)
  * ``kb_global_search`` — GraphRAG map-reduce over community summaries
                           (``GlobalSearchWorkflow``) for corpus-wide
                           thematic questions
  * ``kb_drift_search``  — local seed + global expansion
                           (``DriftSearchWorkflow``)
  * ``kb_auto_search``   — LLM router picks local / global / drift
                           (``AutoSearchWorkflow``)

For raw retrieval primitives (vector / graph / entity / chunk tools)
use the sibling MCP-2 server instead.

Run::

    # Stdio — Claude Desktop / Cursor / Continue
    uv run python -m src.mcp.search_server --transport stdio

    # Streamable HTTP (endpoint /mcp) — OpenWebUI etc.
    uv run python -m src.mcp.search_server --transport http --port 9001
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from fastmcp import Context, FastMCP
from loguru import logger
from temporalio.common import WorkflowIDReusePolicy

from src.config import settings
from src.mcp._shared import (
    assert_api_key_env_set,
    build_sse_auth,
    log_banner,
    parse_args,
)
from src.retrieval.date_filters import DateBounds, bounds_from_iso, iso_to_epoch_days
from src.workflow.client import get_temporal_client
from src.workflow.contracts import (
    GlobalSearchParams,
    OrchestratorParams,
    SearchOutcome,
)
from src.workflow.search.global_wf import GlobalSearchWorkflow
from src.workflow.search.orchestrator import SearchOrchestratorWorkflow
from src.workflow.search.router_wf import (
    AutoSearchWorkflow,
    DriftSearchWorkflow,
)

mcp = FastMCP(
    name="kb-llamaindex-search",
    instructions=(
        "High-level orchestrated search over the project knowledge base. "
        "Four tools: kb_search (local plan-execute), kb_global_search "
        "(corpus-wide GraphRAG themes), kb_drift_search (local seed + "
        "global), kb_auto_search (router picks the mode).  Each returns a "
        "synthesised Russian answer with citations.  For raw retrieval "
        "primitives use the sibling MCP-2 tools server instead."
    ),
    auth=build_sse_auth(),  # evaluated at import time; see build_sse_auth docstring
)


# ── shared helpers ─────────────────────────────────────────────────


def _local_params(
    query: str,
    max_refinements: int = 3,
    bounds: DateBounds | None = None,
    *,
    synthesize: bool = True,
) -> OrchestratorParams:
    """Build OrchestratorParams for the local plan-execute flow,
    mirroring the FastAPI ``/search/local`` route defaults.

    ``top_k`` is intentionally left at the OrchestratorParams default —
    the MCP tools don't expose a per-call retrieval-pool knob (the FastAPI
    route surfaces it via SearchRequest; both default to 10).

    ``bounds`` carries the epoch-day date filters (already converted from
    ISO strings by ``_bounds_or_error`` — see the date-filters spec); None
    means "no filter set" (all four epoch fields stay None).

    ``synthesize`` mirrors ``SearchRequest.synthesize`` / the FastAPI
    route's ``_local_params`` — False skips the large-model write-up and
    the tool returns retrieval only (``answer == ""``)."""
    b = bounds or DateBounds()
    return OrchestratorParams(
        query=query,
        max_subqueries=settings.agent.max_subqueries,
        max_refinements=max_refinements,
        coverage_check_enabled=settings.agent.coverage_check_enabled,
        max_coverage_rounds=settings.agent.max_coverage_rounds,
        doc_date_after_epoch=b.doc_after,
        doc_date_before_epoch=b.doc_before,
        inserted_after_epoch=b.ins_after,
        inserted_before_epoch=b.ins_before,
        synthesize=synthesize,
    )


def _bounds_or_error(
    doc_date_after: str | None,
    doc_date_before: str | None,
    created_after: str | None,
    created_before: str | None,
) -> DateBounds | dict[str, Any]:
    """Convert the four optional ISO (YYYY-MM-DD) date params to epoch-day
    bounds via ``bounds_from_iso`` — the same conversion the FastAPI
    ``/search/local`` route runs outside the Temporal sandbox (see
    ``src.api.routes.search_v2._local_params``).

    Unlike the REST route (whose ``SearchRequest`` field validator already
    rejects malformed dates with a 422 before this ever runs), the MCP
    tools have no such gate — so this never raises: on a ValueError it
    returns the tool's error-dict shape naming the offending field instead
    of crashing the tool call."""
    try:
        return bounds_from_iso(
            doc_after=doc_date_after, doc_before=doc_date_before,
            ins_after=created_after, ins_before=created_before,
        )
    except ValueError:
        for name, val in (
            ("doc_date_after", doc_date_after),
            ("doc_date_before", doc_date_before),
            ("created_after", created_after),
            ("created_before", created_before),
        ):
            if val is None:
                continue
            try:
                iso_to_epoch_days(val)
            except ValueError:
                return {
                    "error": "invalid date format, expected YYYY-MM-DD",
                    "field": name,
                }
        # Defensive — bounds_from_iso raised but no individual field did
        # (shouldn't happen; it converts the same four fields).
        return {"error": "invalid date format, expected YYYY-MM-DD", "field": "unknown"}


def _global_params(
    query: str, *, drift_mode: bool = False, synthesize: bool = True,
) -> GlobalSearchParams:
    """Build GlobalSearchParams, mirroring the FastAPI ``/search/global``
    and ``/search/drift`` route defaults.

    ``synthesize`` mirrors ``OrchestratorParams.synthesize`` — False skips
    the REDUCE step's large-model write-up (``answer == ""``)."""
    return GlobalSearchParams(
        query=query,
        level=0,
        max_communities=settings.agent.global_max_communities,
        drift_mode=drift_mode,
        synthesize=synthesize,
    )


def _outcome_to_dict(outcome: SearchOutcome) -> dict[str, Any]:
    """Map a SearchOutcome to the JSON-serialisable tool result shape
    shared by all four search tools."""
    return {
        "answer": outcome.answer,
        "mode": outcome.mode,
        "query": outcome.query,
        "sources": [
            {
                "chunk_id": n.chunk_id,
                "doc_id": str(n.metadata.get("doc_id") or n.metadata.get("file_path") or ""),
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


async def _stream_until_done(handle, ctx: Context, state_query) -> SearchOutcome:
    """Await the workflow result while forwarding coarse progress to the
    MCP client.  ``state_query`` is the workflow's ``get_state`` query
    method (or None to skip progress, e.g. workflows without one).  On
    client cancellation the underlying workflow is cancelled too."""
    result_task = asyncio.create_task(handle.result())
    try:
        while not result_task.done():
            if state_query is not None:
                try:
                    state = await handle.query(state_query)
                    msg = "  ".join(f"{k}={v}" for k, v in state.items())
                    await ctx.report_progress(
                        progress=0.0,
                        total=1.0,
                        message=msg or "…",
                    )
                except Exception as exc:
                    logger.debug("query state failed (transient): {e}", e=exc)
            try:
                await asyncio.wait_for(asyncio.shield(result_task), 0.3)
            except TimeoutError:
                continue
            except asyncio.CancelledError:
                logger.info("mcp search cancelled by client, cancelling workflow")
                await handle.cancel()
                raise
        return result_task.result()
    except asyncio.CancelledError:
        await handle.cancel()
        raise


@mcp.tool(timeout=1800)
async def kb_search(
    query: str,
    ctx: Context,
    max_refinements: int = 3,
    doc_date_after: str | None = None,
    doc_date_before: str | None = None,
    created_after: str | None = None,
    created_before: str | None = None,
    synthesize: bool = True,
) -> dict[str, Any]:
    """Orchestrated LOCAL deep-search over the knowledge base: decomposes
    the question into sub-questions, retrieves each over vector + graph in
    parallel, then synthesises ONE cited answer (with uncertainties).

    USE FOR: complex / multi-part questions you can't resolve in 2-3
    atomic MCP-2 tool calls, when you want a finished answer rather than
    raw primitives. This is the heavy escape hatch.
    COST: runs the full plan-execute-synthesize workflow (can take up to
    ~30 min); for simple lookups use the atomic tools instead.
    SCOPE: "local" mode — focused retrieval. For corpus-wide thematic
    aggregation use `kb_global_search`; for a mix use `kb_drift_search`;
    to let a router decide use `kb_auto_search`.

    Args:
      query: question in Russian (or the source-document language).
      max_refinements: cap for the reflective synthesis loop.
      doc_date_after / doc_date_before: ISO YYYY-MM-DD, inclusive bounds —
        filters by document date (the client-supplied date the document
        itself is about/dated, not when it was ingested).
      created_after / created_before: ISO YYYY-MM-DD, inclusive bounds —
        filters by insertion date (when the document was ingested into
        the knowledge base).
      synthesize: pass False to skip the final large-model write-up and
        return the retrieved sources with an empty answer — for a client
        that composes its own answer and does not want to pay for the
        model call.

    Returns:
      {
        "answer": str,
        "sources": [{chunk_id, doc_id, text, score}, ...],
        "citations": [...],
        "uncertainties": [...],
        "refinement_rounds": int,
        "step_stats": [...],
        "latency_ms": int,
      }
      On a malformed date param: {"error": str, "field": str} instead.
    """
    bounds = _bounds_or_error(doc_date_after, doc_date_before, created_after, created_before)
    if isinstance(bounds, dict):
        return bounds
    handle = await (await get_temporal_client()).start_workflow(
        SearchOrchestratorWorkflow.run,
        _local_params(query, max_refinements, bounds, synthesize=synthesize),
        id=f"mcp-search-{uuid.uuid4().hex}",
        task_queue=settings.temporal.search_task_queue,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    )
    outcome = await _stream_until_done(
        handle,
        ctx,
        SearchOrchestratorWorkflow.get_state,
    )
    return _outcome_to_dict(outcome)


@mcp.tool(timeout=1800)
async def kb_global_search(
    query: str,
    ctx: Context,
    synthesize: bool = True,
) -> dict[str, Any]:
    """Corpus-wide GraphRAG search: map-reduce over precomputed community
    summaries, then synthesise one answer.

    USE FOR: broad / thematic / aggregate questions about the WHOLE
    corpus — "what are the main risks across all contracts", "summarise
    recurring themes". It reasons over community-level summaries, not
    individual chunks.
    NOT FOR: a specific fact about one entity / document — use `kb_search`
    (local) or the atomic MCP-2 tools.
    COST: heavy map-reduce workflow (up to ~30 min).
    NOTE: date filters are not applicable to global mode — community
    summaries are undated, so this tool has no doc_date_*/created_*
    params (unlike `kb_search`, `kb_drift_search`, `kb_auto_search`).

    Args:
      synthesize: pass False to skip the REDUCE step's large-model
        write-up and return the retrieved sources with an empty answer —
        for a client that composes its own answer and does not want to
        pay for the model call.

    Returns the same shape as `kb_search` (answer + sources + citations +
    uncertainties + latency_ms; mode == "global").
    """
    handle = await (await get_temporal_client()).start_workflow(
        GlobalSearchWorkflow.run,
        _global_params(query, synthesize=synthesize),
        id=f"mcp-global-{uuid.uuid4().hex}",
        task_queue=settings.temporal.search_task_queue,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    )
    outcome = await _stream_until_done(
        handle,
        ctx,
        GlobalSearchWorkflow.get_state,
    )
    return _outcome_to_dict(outcome)


@mcp.tool(timeout=1800)
async def kb_drift_search(
    query: str,
    ctx: Context,
    doc_date_after: str | None = None,
    doc_date_before: str | None = None,
    created_after: str | None = None,
    created_before: str | None = None,
    synthesize: bool = True,
) -> dict[str, Any]:
    """Drift search: run the local plan-execute flow first, then expand
    with a global GraphRAG pass seeded by the local sources, and
    synthesise once.

    USE FOR: questions that need BOTH specific local detail AND corpus-
    wide context — "how does <specific clause> compare to our general
    practice". A middle ground between `kb_search` (local) and
    `kb_global_search` (global).
    COST: heaviest — runs local then global sequentially; may approach or
    hit the shared 1800s tool timeout. If it does, prefer `kb_search` or
    `kb_global_search` alone.

    Args:
      doc_date_after / doc_date_before: ISO YYYY-MM-DD, inclusive bounds —
        filters by document date. Applied to the LOCAL pass only (the
        global community-summary pass is undated, same as `kb_global_search`).
      created_after / created_before: ISO YYYY-MM-DD, inclusive bounds —
        filters by insertion date. Applied to the LOCAL pass only.
      synthesize: pass False to skip the final large-model write-up (both
        legs) and return the retrieved sources with an empty answer — for
        a client that composes its own answer and does not want to pay
        for the model call.

    Returns the same shape as `kb_search` (mode == "drift"); on a
    malformed date param: {"error": str, "field": str} instead.
    """
    bounds = _bounds_or_error(doc_date_after, doc_date_before, created_after, created_before)
    if isinstance(bounds, dict):
        return bounds
    handle = await (await get_temporal_client()).start_workflow(
        DriftSearchWorkflow.run,
        # drift_mode=True mirrors the FastAPI /search/drift route; the
        # workflow also forces it internally, so this is defensive.
        args=[
            _local_params(query, bounds=bounds, synthesize=synthesize),
            _global_params(query, drift_mode=True, synthesize=synthesize),
        ],
        id=f"mcp-drift-{uuid.uuid4().hex}",
        task_queue=settings.temporal.search_task_queue,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    )
    # DriftSearchWorkflow has no get_state query — skip progress polling.
    outcome = await _stream_until_done(handle, ctx, None)
    return _outcome_to_dict(outcome)


@mcp.tool(timeout=1800)
async def kb_auto_search(
    query: str,
    ctx: Context,
    doc_date_after: str | None = None,
    doc_date_before: str | None = None,
    created_after: str | None = None,
    created_before: str | None = None,
    synthesize: bool = True,
) -> dict[str, Any]:
    """Auto-routed search: a small-tier LLM router classifies the query
    and dispatches to local / global / drift, then returns that mode's
    answer.

    USE FOR: when you're unsure which mode fits and want the system to
    decide. Fail-safe: routing errors fall back to local.
    COST: a quick routing LLM call + the chosen workflow (up to ~30 min).

    Args:
      doc_date_after / doc_date_before: ISO YYYY-MM-DD, inclusive bounds —
        filters by document date. Only applied if the router picks local
        or drift (the global route is undated, same as `kb_global_search`).
      created_after / created_before: ISO YYYY-MM-DD, inclusive bounds —
        filters by insertion date. Same local/drift-only caveat.
      synthesize: pass False to skip the final large-model write-up
        (whichever mode is chosen) and return the retrieved sources with
        an empty answer — for a client that composes its own answer and
        does not want to pay for the model call.

    Returns the same shape as `kb_search`; `mode` reflects the chosen
    route ("local" / "global" / "drift"). On a malformed date param:
    {"error": str, "field": str} instead.
    """
    bounds = _bounds_or_error(doc_date_after, doc_date_before, created_after, created_before)
    if isinstance(bounds, dict):
        return bounds
    handle = await (await get_temporal_client()).start_workflow(
        AutoSearchWorkflow.run,
        args=[
            _local_params(query, bounds=bounds, synthesize=synthesize),
            _global_params(query, synthesize=synthesize),
        ],
        id=f"mcp-auto-{uuid.uuid4().hex}",
        task_queue=settings.temporal.search_task_queue,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    )
    outcome = await _stream_until_done(handle, ctx, AutoSearchWorkflow.get_state)
    return _outcome_to_dict(outcome)


@mcp.tool(timeout=1800)
async def kb_analyze(query: str, ctx: Context, top_n: int = 20) -> dict[str, Any]:
    """Analytical Q&A: computes counts/rankings/connections/centrality/temporal facts
    over the knowledge graph and returns an answer plus a deterministic provenance
    chain (which primitives ran, the rows, source chunks). USE FOR quantitative /
    structural / "how many / who is most central / how connected / what changed"
    questions. For 'what do the documents say', use kb_search instead."""
    from src.analytics.contracts import AnalyzeParams
    from src.workflow.analytics.workflow import AnalyticalQueryWorkflow

    handle = await (await get_temporal_client()).start_workflow(
        AnalyticalQueryWorkflow.run,
        AnalyzeParams(query=query, top_n=top_n),
        id=f"mcp-analyze-{uuid.uuid4().hex}",
        task_queue=settings.temporal.search_task_queue,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    )
    outcome = await handle.result()
    return {
        "query": outcome.query,
        "answer": outcome.answer,
        "provenance": outcome.provenance.model_dump(),
        "latency_ms": outcome.latency_ms,
    }


def main() -> None:
    args = parse_args()
    assert_api_key_env_set()
    log_banner(
        "kb-llamaindex-search",
        transport=args["transport"],
        host=args["host"],
        port=args["port"],
    )
    if args["transport"] == "stdio":
        mcp.run(transport="stdio")
    else:
        # Streamable HTTP (modern MCP transport; endpoint `/mcp`).  Replaces
        # the legacy SSE transport for MCP-1 (matches MCP-2 tools_server).
        mcp.run(
            transport="http",
            host=args["host"],
            port=args["port"],
        )


if __name__ == "__main__":
    main()
