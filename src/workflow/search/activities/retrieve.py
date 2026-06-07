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

import json
import time

from temporalio import activity

from src.config import settings
from src.retrieval import atomic_tools
from src.workflow._search_deps import get_graph_retriever, get_retriever
from src.workflow._search_serde import node_to_serialized
from src.workflow.contracts import RetrieveParams, RetrieveResult

# Deterministic tool pipeline for one sub-question.  Vector first
# (always available), then graph (skipped gracefully if Neo4j is down —
# atomic_tools.graph_search returns empty for a None retriever).
#
# graph_walk (bounded multi-hop, R3) is registered in atomic_tools and
# dispatchable on this path via the same graph_retriever DI. It is NOT
# in this fixed pipeline because it needs an explicit `start_entity`
# (a real entity name). R3b ACTIVATES it deterministically WITHOUT an
# LLM tool-pick: after graph_search runs, the activity seeds graph_walk
# from the top graph_search entity AND (when ``graph_walk_dual_seed`` is
# on) the top find_entity_by_name entity (``top_entity_name`` +
# ``_walk_seeds`` in the activity body), flag-gated by
# ``settings.agent.graph_walk_enabled`` and fail-open.
_PIPELINE = ("vector_search", "graph_search", "find_entity_by_name")

# Tools this deterministic activity is ALLOWED to dispatch (default
# pipeline + the explicitly-bounded multi-hop walk available for the
# connection-aware path). Kept as the contract surface for R3 wiring.
ALLOWED_TOOLS = (
    "vector_search",
    "graph_search",
    "find_entity_by_name",
    "graph_walk",
)


def top_entity_name(observation: str) -> str | None:
    """Pick the top entity_name from a ``graph_search`` observation.

    PURE (no I/O) so it's unit-testable without dispatch. ``graph_search``
    serialises ``{"entities": [...], "relations": [...]}`` with entities
    in similarity-rank order, so the "top" entity is the first one with a
    non-blank ``entity_name``.

    Returns ``None`` for any non-conforming input — empty / missing
    entities, a list-shaped observation (vector_search), or garbled JSON —
    so the caller treats it as "no seed available" and skips the walk.
    """
    try:
        data = json.loads(observation)
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    for ent in data.get("entities") or []:
        if not isinstance(ent, dict):
            continue
        name = (ent.get("entity_name") or "").strip()
        if name:
            return name
    return None


def _walk_seeds(graph_search_obs: str, find_name_obs: str, *, dual: bool) -> list[str]:
    """Seed entity name(s) for graph_walk.

    Legacy (dual=False): graph_search's top entity, else fulltext's — one
    seed.  dual=True: the union of both (deduped, order: graph_search
    first) so a fulltext-matched entity also contributes its neighbourhood
    even when graph_search returned something."""
    gs = top_entity_name(graph_search_obs or "")
    fn = top_entity_name(find_name_obs or "")
    if not dual:
        return [s for s in (gs or fn,) if s]
    out: list[str] = []
    for s in (gs, fn):
        if s and s not in out:
            out.append(s)
    return out


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

    def _merge_sources(new_sources) -> None:
        """Append new NodeWithScore items, deduping by chunk_id."""
        for n in new_sources:
            cid = n.node.node_id
            if cid in seen:
                continue
            seen.add(cid)
            collected.append(n)

    graph_search_obs: str | None = None
    find_name_obs: str | None = None

    for tool_name in _PIPELINE:
        tool_args: dict = {"query": params.subquestion}
        if tool_name == "graph_search":
            # Operator-tunable neighbour depth (default 1 = current
            # behaviour); see settings.agent.graph_search_path_depth.
            tool_args["depth"] = settings.agent.graph_search_path_depth
        try:
            result = await atomic_tools.dispatch(
                tool_name,
                tool_args,
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
        if tool_name == "graph_search":
            graph_search_obs = result.observation
        if tool_name == "find_entity_by_name":
            find_name_obs = result.observation
        _merge_sources(result.sources)

    # R3b: deterministically seed the bounded multi-hop graph_walk from
    # the top graph_search entity (and, with dual-seed, the top fulltext
    # entity — see ``_walk_seeds``). FAIL-OPEN per seed — any error (parse
    # failure, store error, missing seed) just skips that walk and returns
    # the vector + graph_search results unchanged; never raises.
    if settings.agent.graph_walk_enabled:
        seeds = _walk_seeds(
            graph_search_obs or "", find_name_obs or "",
            dual=settings.agent.graph_walk_dual_seed,
        )
        for start in seeds:
            try:
                walk = await atomic_tools.dispatch(
                    "graph_walk",
                    {"start_entity": start, "hops": settings.agent.graph_walk_hops},
                    graph_retriever=graph_retriever,
                )
                _merge_sources(walk.sources)
            except Exception as exc:
                activity.logger.warning(
                    "retrieve_subquestion  graph_walk skipped  start=%s  err=%s",
                    start, exc,
                )
                errors.append(f"graph_walk: {exc}")

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
