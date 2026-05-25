"""GraphRAG global-search activities (Search R7a, decision C).

Two small-tier activities backing ``GlobalSearchWorkflow``'s map-reduce
over the community summaries built in R6:

  * ``map_communities`` — read the stored ``:Community.summary`` texts
    from Neo4j for a given level, bounded by ``limit``.  This is the set
    the MAP step fans out over.  Optionally ranked by lexical overlap with
    the query (cheap, deterministic) so the most relevant summaries map
    first when the corpus exceeds the limit.
  * ``map_community_partial`` — produce a per-community PARTIAL answer
    (small tier) for ONE community summary against the user query, with a
    self-rated relevance score so REDUCE can drop off-topic communities.

REDUCE is NOT here — it reuses the existing ``synthesize_answer`` activity
pinned to the large tier (the orchestrator's R5 pattern).

Both activities are fail-safe by construction (store / LLM error → empty
result, never raised through the Temporal boundary) so a partial graph or
flaky proxy degrades gracefully rather than failing the whole search.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

from temporalio import activity

from src.workflow.contracts import (
    CommunitySummaryRef,
    MapCommunitiesParams,
    MapCommunitiesResult,
    MapPartialParams,
    MapPartialResult,
)

# Read the stored community summaries for a level, largest first so the
# most informative communities lead when ``limit`` truncates.  Empty/
# unsummarised communities are skipped (summary IS NOT NULL / non-blank).
_READ_SUMMARIES_CYPHER = """
MATCH (c:Community {level: $level})
WHERE c.summary IS NOT NULL AND trim(c.summary) <> ''
RETURN c.id AS community_id, c.level AS level, c.summary AS summary,
       coalesce(c.member_count, 0) AS member_count
ORDER BY member_count DESC, community_id ASC
"""

_WORD_RE = re.compile(r"\w+", re.UNICODE)

_PARTIAL_SYSTEM = (
    "Ты отвечаешь на вопрос пользователя, опираясь ТОЛЬКО на резюме "
    "одного сообщества графа знаний ниже. Если резюме не относится к "
    "вопросу — ответь ровно словом НЕТ. Иначе кратко (1-3 предложения, "
    "на русском) изложи, что это резюме сообщает по вопросу. Без "
    "вступлений."
)


def _get_store() -> Any | None:
    """Build the Neo4j graph store (or ``None`` when unreachable).

    Indirected for monkeypatching (mirrors ``community._get_store``)."""
    try:
        from src.graph.store import build_neo4j_graph_store

        return build_neo4j_graph_store()
    except Exception as exc:
        activity.logger.warning("global_search: graph store unavailable: %s", exc)
        return None


def _get_map_llm() -> Any:
    """Small-tier LLM for the per-community MAP step (role ``route``/small
    tier — same cheap tier as routing/summaries; REDUCE uses the large
    synthesizer).  Indirected for monkeypatching."""
    from src.retrieval.llm import build_llm

    return build_llm("retrieve")


def _tokens(text: str) -> set[str]:
    return {t.lower() for t in _WORD_RE.findall(text or "")}


def rank_summaries(
    rows: list[dict], *, query: str, limit: int,
) -> list[CommunitySummaryRef]:
    """Pure: parse summary rows → ``CommunitySummaryRef``s ranked by
    lexical overlap with the query, capped at ``limit``.

    Deterministic, LLM-free (the small model is reserved for the MAP
    partials).  Ties / no-overlap fall back to the Cypher order (largest
    community first).  Extracted so the rank/cap logic is unit-testable
    outside Temporal."""
    q_tokens = _tokens(query)
    refs: list[tuple[int, int, CommunitySummaryRef]] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        cid = row.get("community_id")
        summary = (row.get("summary") or "").strip()
        if cid is None or not summary:
            continue
        overlap = len(q_tokens & _tokens(summary)) if q_tokens else 0
        ref = CommunitySummaryRef(
            community_id=int(cid),
            level=int(row.get("level") or 0),
            summary=summary,
        )
        # Sort key: more overlap first; preserve incoming order on ties.
        refs.append((overlap, i, ref))
    refs.sort(key=lambda t: (-t[0], t[1]))
    return [r for _, _, r in refs[: max(0, limit)]]


def is_relevant_partial(text: str) -> bool:
    """Pure: did the MAP model return a usable partial (not the 'НЕТ'
    refusal)?  Tolerant of trailing punctuation / casing."""
    cleaned = (text or "").strip().strip(".!?").lower()
    return bool(cleaned) and cleaned != "нет"


@activity.defn
async def map_communities(params: MapCommunitiesParams) -> MapCommunitiesResult:
    """Fetch + rank the community summaries to map over.  Fail-safe →
    empty list on any store error."""
    activity.heartbeat({"stage": "map_communities", "level": params.level})
    store = _get_store()
    if store is None:
        return MapCommunitiesResult(communities=[])
    try:
        rows = await asyncio.to_thread(
            store.structured_query,
            _READ_SUMMARIES_CYPHER,
            {"level": params.level},
        )
        rows = list(rows or [])
    except Exception as exc:
        activity.logger.warning("map_communities  read err=%s", exc)
        return MapCommunitiesResult(communities=[])

    refs = rank_summaries(rows, query=params.query, limit=params.limit)
    activity.logger.info(
        "map_communities  level=%d  read=%d  selected=%d",
        params.level, len(rows), len(refs),
    )
    return MapCommunitiesResult(communities=refs)


@activity.defn
async def map_community_partial(params: MapPartialParams) -> MapPartialResult:
    """Per-community PARTIAL answer (small tier).  Off-topic communities
    self-report 'НЕТ' → score 0.  Fail-safe → empty partial on error."""
    activity.heartbeat({
        "stage": "map_partial", "community_id": params.community_id,
    })
    if not (params.summary or "").strip():
        return MapPartialResult(community_id=params.community_id, score=0.0)
    try:
        llm = _get_map_llm()
        prompt = (
            f"{_PARTIAL_SYSTEM}\n\nВопрос: {params.query}\n\n"
            f"Резюме сообщества:\n{params.summary}"
        )
        resp = await llm.acomplete(prompt)
        text = (getattr(resp, "text", None) or str(resp)).strip()
    except Exception as exc:
        activity.logger.warning(
            "map_community_partial  cid=%d  llm err=%s",
            params.community_id, exc,
        )
        return MapPartialResult(community_id=params.community_id, score=0.0)

    if not is_relevant_partial(text):
        return MapPartialResult(community_id=params.community_id, score=0.0)
    return MapPartialResult(
        community_id=params.community_id, partial=text, score=1.0,
    )


__all__ = [
    "is_relevant_partial",
    "map_communities",
    "map_community_partial",
    "rank_summaries",
]
