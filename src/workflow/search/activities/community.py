"""Offline community-build activities (Search R6, decision C2).

Two activities, both run on the dedicated ``kb-graph-build`` queue — never
on the query hot path:

  * ``detect_communities_activity`` — wraps ``detect_communities`` (GDS
    Leiden) and returns the detected communities.
  * ``summarize_community_activity`` — for ONE community, summarise its
    members (+ their inter-member relations) via the SMALL-tier LLM
    (``build_llm("retrieve")`` → small tier) and persist the result on
    ``:Community.summary`` (idempotent MERGE).  Batchable: the workflow
    fans out one call per community with bounded parallelism.

Both are fail-safe by construction — a store/LLM error is logged and
returns an empty/non-persisted result rather than raising through the
Temporal boundary (the offline build must never crash; a partial rebuild
is fine and the next run reconciles).
"""

from __future__ import annotations

import asyncio
from typing import Any

from temporalio import activity

from src.workflow.contracts import (
    DetectCommunitiesParams,
    DetectCommunitiesResult,
    SummarizeCommunityParams,
    SummarizeCommunityResult,
)

# Read the members' names + descriptions for the summary prompt (and any
# inter-member relations to give the LLM relational context).
_MEMBER_CONTEXT_CYPHER = """
MATCH (e:__Entity__)
WHERE e.name IN $members
OPTIONAL MATCH (e)-[r]-(o:__Entity__)
WHERE o.name IN $members
RETURN e.name AS name,
       coalesce(e.description, '') AS description,
       collect(DISTINCT type(r))[..10] AS rel_types
ORDER BY name
"""

# Idempotent: re-running updates the summary on the SAME :Community node
# (keyed on id+level) rather than creating a new one.
_WRITE_SUMMARY_CYPHER = """
MERGE (c:Community {id: $community_id, level: $level})
SET c.summary = $summary, c.summarized_at = timestamp()
"""

_SUMMARY_SYSTEM = (
    "Ты аналитик графа знаний. Ниже — сущности одного сообщества "
    "(тесно связанная группа) и их описания. Напиши краткое связное "
    "резюме (3-5 предложений) на русском: что это за группа, какие "
    "сущности в неё входят и как они связаны. Без вступлений и "
    "маркированных списков — только резюме."
)


def _get_store() -> Any | None:
    """Build the Neo4j graph store (or ``None`` when unreachable).

    Indirected through a module-level fn so tests can monkeypatch it
    without touching the heavy Neo4j factory."""
    try:
        from src.graph.store import build_neo4j_graph_store

        return build_neo4j_graph_store()
    except Exception as exc:  # noqa: BLE001
        activity.logger.warning("community: graph store unavailable: %s", exc)
        return None


def _get_summary_llm() -> Any:
    """Small-tier LLM for community summaries.

    Uses the ``retrieve`` role (small tier per ``_DEFAULT_ROLE_TIERS``) so
    summaries NEVER occupy the large synthesis model.  Indirected for
    monkeypatching in tests."""
    from src.retrieval.llm import build_llm

    return build_llm("retrieve")


def _build_summary_prompt(rows: list[dict]) -> str:
    """Render the member context rows into a single LLM prompt body."""
    lines = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = row.get("name") or ""
        desc = (row.get("description") or "").replace("\n", " ")[:400]
        rels = ", ".join(str(t) for t in (row.get("rel_types") or []) if t)
        line = f"- {name}"
        if desc:
            line += f": {desc}"
        if rels:
            line += f"  (связи: {rels})"
        lines.append(line)
    return _SUMMARY_SYSTEM + "\n\nСущности сообщества:\n" + "\n".join(lines)


@activity.defn
async def detect_communities_activity(
    params: DetectCommunitiesParams,
) -> DetectCommunitiesResult:
    """Run GDS Leiden detection + materialise ``:Community`` nodes."""
    activity.heartbeat({"stage": "detect", "min_size": params.min_size})
    from src.graph.communities import detect_communities

    store = _get_store()
    communities = await detect_communities(
        store, min_size=params.min_size, level=params.level,
    )
    activity.logger.info(
        "detect_communities_activity  detected=%d  min_size=%d",
        len(communities), params.min_size,
    )
    return DetectCommunitiesResult(communities=communities)


@activity.defn
async def summarize_community_activity(
    params: SummarizeCommunityParams,
) -> SummarizeCommunityResult:
    """Summarise ONE community via the small LLM; persist on
    ``:Community.summary`` (idempotent).  Fail-safe — any error returns a
    non-persisted result."""
    activity.heartbeat({
        "stage": "summarize",
        "community_id": params.community_id,
        "members": len(params.members),
    })
    if not params.members:
        return SummarizeCommunityResult(
            community_id=params.community_id, summary="", persisted=False,
        )

    store = _get_store()
    if store is None:
        return SummarizeCommunityResult(
            community_id=params.community_id, summary="", persisted=False,
        )

    # 1. Gather member context (names, descriptions, inter-member rels).
    try:
        rows = await asyncio.to_thread(
            store.structured_query,
            _MEMBER_CONTEXT_CYPHER,
            {"members": list(params.members)},
        )
        rows = list(rows or [])
    except Exception as exc:  # noqa: BLE001
        activity.logger.warning(
            "summarize_community_activity  cid=%s  context fetch err=%s",
            params.community_id, exc,
        )
        rows = [{"name": m, "description": "", "rel_types": []} for m in params.members]

    # 2. Summarise (small tier).
    try:
        llm = _get_summary_llm()
        prompt = _build_summary_prompt(rows)
        resp = await llm.acomplete(prompt)
        summary = (getattr(resp, "text", None) or str(resp)).strip()
    except Exception as exc:  # noqa: BLE001
        activity.logger.warning(
            "summarize_community_activity  cid=%s  llm err=%s",
            params.community_id, exc,
        )
        return SummarizeCommunityResult(
            community_id=params.community_id, summary="", persisted=False,
        )

    if not summary:
        return SummarizeCommunityResult(
            community_id=params.community_id, summary="", persisted=False,
        )

    # 3. Persist on :Community.summary (idempotent MERGE).
    persisted = False
    try:
        await asyncio.to_thread(
            store.structured_query,
            _WRITE_SUMMARY_CYPHER,
            {
                "community_id": params.community_id,
                "level": params.level,
                "summary": summary,
            },
        )
        persisted = True
    except Exception as exc:  # noqa: BLE001
        activity.logger.warning(
            "summarize_community_activity  cid=%s  persist err=%s",
            params.community_id, exc,
        )

    activity.logger.info(
        "summarize_community_activity  cid=%s  chars=%d  persisted=%s",
        params.community_id, len(summary), persisted,
    )
    return SummarizeCommunityResult(
        community_id=params.community_id, summary=summary, persisted=persisted,
    )


__all__ = [
    "detect_communities_activity",
    "summarize_community_activity",
]
