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

import numpy as np
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

# Semantic selection (v1): kNN over the structured community report
# vectors (``c.report_vec``, indexed as ``community_report_vec``).
# ``queryNodes`` already returns nearest-first; ``ORDER BY score DESC`` is
# belt-and-suspenders.  Skip empty/unsummarised communities so the MAP
# step never fans out over a blank summary.
_SELECT_SEMANTIC_CYPHER = """
CALL db.index.vector.queryNodes('community_report_vec', $limit, $vec) YIELD node, score
WHERE node.level = $level AND node.summary IS NOT NULL AND trim(node.summary) <> ''
RETURN node.id AS community_id, node.level AS level, node.summary AS summary
ORDER BY score DESC
"""

# Hierarchy-descent selection (v2): start at the coarsest level (0) and
# greedily descend the ``PARENT_OF`` tree (coarse→fine) toward the most
# query-relevant communities, collecting the finest relevant communities.
# ``_DESCENT_ROOT_CYPHER`` seeds the level-0 frontier; ``_DESCENT_CHILDREN_
# CYPHER`` expands one community into its finer children.
_DESCENT_ROOT_CYPHER = """
MATCH (c:Community {level: 0})
WHERE c.report_vec IS NOT NULL
RETURN c.id AS community_id, c.level AS level, c.summary AS summary, c.report_vec AS report_vec
"""

_DESCENT_CHILDREN_CYPHER = """
MATCH (c:Community {id: $community_id, level: $level})-[:PARENT_OF]->(ch:Community)
WHERE ch.report_vec IS NOT NULL
RETURN ch.id AS community_id, ch.level AS level, ch.summary AS summary, ch.report_vec AS report_vec
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
        from src.graph.store import build_graph_store

        return build_graph_store()
    except Exception as exc:
        activity.logger.warning("global_search: graph store unavailable: %s", exc)
        return None


def _get_map_llm() -> Any:
    """Small-tier LLM for the per-community MAP step (role ``retrieve``,
    which maps to the small tier — same cheap tier as routing/summaries;
    REDUCE uses the large synthesizer).  Indirected for monkeypatching.
    Returns the pooled LLM via ``get_llm_pool().get('retrieve')`` so the
    global N semaphore counts MAP calls."""
    from src.retrieval.llm_pool import get_llm_pool

    return get_llm_pool().get("retrieve")


def _get_embed_model() -> Any:
    """Embedding model for the semantic community selection (same model as
    the report vectors were built with).  Indirected for monkeypatching
    (mirrors ``community._get_embed_model``)."""
    from src.ingestion.embeddings import build_embedding_model

    return build_embedding_model()


async def select_communities_semantic(
    store: Any, query_vec: list[float], *, level: int, limit: int,
) -> list[CommunitySummaryRef]:
    """kNN over the community report vectors → ``CommunitySummaryRef``s for
    ``level``, nearest-first, capped at ``limit`` (the queryNodes ``k``).

    Same output shape as ``rank_summaries``.  Fail-open: returns ``[]`` on
    ANY error so the caller can fall back to the lexical path."""
    try:
        rows = await asyncio.to_thread(
            store.structured_query,
            _SELECT_SEMANTIC_CYPHER,
            {"vec": query_vec, "level": level, "limit": max(0, limit)},
        )
        rows = list(rows or [])
    except Exception as exc:
        activity.logger.warning("select_communities_semantic  err=%s", exc)
        return []

    refs: list[CommunitySummaryRef] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        cid = row.get("community_id")
        summary = (row.get("summary") or "").strip()
        if cid is None or not summary:
            continue
        refs.append(CommunitySummaryRef(
            community_id=str(cid),
            level=int(row.get("level") or 0),
            summary=summary,
        ))
    return refs[: max(0, limit)]


def _cosine(a: list[float] | None, b: list[float] | None) -> float:
    """Cosine similarity of two vectors as a plain float.

    Pure / deterministic.  Empty, mismatched-length, or zero-norm inputs
    → ``0.0`` (so descent ranking never raises on a missing/blank vec)."""
    if not a or not b or len(a) != len(b):
        return 0.0
    va = np.asarray(a, dtype=float)
    vb = np.asarray(b, dtype=float)
    denom = float(np.linalg.norm(va) * np.linalg.norm(vb))
    if denom <= 0.0:
        return 0.0
    return float(np.dot(va, vb) / (denom + 1e-12))


async def select_communities_descent(
    store: Any, query_vec: list[float], *, budget: int,
) -> list[CommunitySummaryRef]:
    """v2 hierarchy descent: start at the coarsest level (0), keep the most
    query-relevant communities (cosine of ``query_vec`` vs ``report_vec``),
    descend via ``PARENT_OF`` into their finer children, and repeat —
    collecting the FINEST relevant communities, capped at ``budget``.

    This is the GraphRAG dynamic community-selection behaviour: coarse→fine
    pruning that spends the budget on the most relevant leaf communities.

    Deterministic (frontier ranked by cosine, no randomness) and guarded
    against cycles via a visited set.  Fail-open: returns ``[]`` on ANY
    store error so the caller can fall back to the lexical path."""
    try:
        root = await asyncio.to_thread(
            store.structured_query, _DESCENT_ROOT_CYPHER, {},
        )
        root = [r for r in (root or []) if isinstance(r, dict)]

        selected: list[dict] = []     # finest relevant communities
        frontier: list[dict] = root
        visited: set = set()          # community_ids, guard against cycles

        while frontier and len(selected) < budget:
            scored = sorted(
                frontier, key=lambda r: -_cosine(query_vec, r.get("report_vec")),
            )
            kept = scored[:budget]
            next_frontier: list[dict] = []
            for r in kept:
                cid = r.get("community_id")
                if cid in visited:
                    continue
                visited.add(cid)
                children = await asyncio.to_thread(
                    store.structured_query,
                    _DESCENT_CHILDREN_CYPHER,
                    {"community_id": cid, "level": r.get("level")},
                )
                children = [c for c in (children or []) if isinstance(c, dict)]
                if children:
                    next_frontier.extend(children)
                else:
                    selected.append(r)   # leaf relevant community → select
            frontier = next_frontier

        # Fallback: nothing reached a leaf (e.g. only level 0 exists) → take
        # the top-budget root communities by cosine.
        if not selected:
            selected = sorted(
                root, key=lambda r: -_cosine(query_vec, r.get("report_vec")),
            )[:budget]
    except Exception as exc:
        activity.logger.warning("select_communities_descent  err=%s", exc)
        return []

    return [
        CommunitySummaryRef(
            community_id=str(r.get("community_id")),
            level=int(r.get("level") or 0),
            summary=(r.get("summary") or "").strip(),
        )
        for r in selected[: max(0, budget)]
    ]


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
            community_id=str(cid),
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


async def _embed_query(query: str) -> list[float] | None:
    """Embed the query for vector-based selection (semantic / descent).
    Returns ``None`` on any embed error so the caller falls back to the
    lexical path."""
    try:
        return await _get_embed_model().aget_text_embedding(query)
    except Exception as exc:
        activity.logger.warning("map_communities  embed err=%s", exc)
        return None


async def _map_communities_lexical(
    store: Any, params: MapCommunitiesParams,
) -> MapCommunitiesResult:
    """LEXICAL path: read stored summaries + rank by query word-overlap.
    Fail-safe → empty list on any store error."""
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
        "map_communities  selection=lexical  level=%d  read=%d  selected=%d",
        params.level, len(rows), len(refs),
    )
    return MapCommunitiesResult(communities=refs)


@activity.defn
async def map_communities(params: MapCommunitiesParams) -> MapCommunitiesResult:
    """Select the community summaries to map over.

    Strategy switch on ``params.selection``:
      * ``"semantic"`` — kNN over the report vectors (v1),
      * ``"descent"`` — coarse→fine hierarchy descent over ``PARENT_OF``
        toward the finest query-relevant communities (v2, GraphRAG
        dynamic selection),
      * anything else — the lexical word-overlap path.
    Both vector strategies fall back to lexical on an empty result or any
    error.  Fail-safe → empty list on any store error (never raises
    through the Temporal boundary)."""
    activity.heartbeat({"stage": "map_communities", "level": params.level})
    store = _get_store()
    if store is None:
        return MapCommunitiesResult(communities=[])

    if params.selection == "semantic":
        try:
            query_vec = await _embed_query(params.query)
            if query_vec is not None:
                refs = await select_communities_semantic(
                    store, query_vec, level=params.level, limit=params.limit,
                )
                if refs:
                    activity.logger.info(
                        "map_communities  selection=semantic  level=%d  selected=%d",
                        params.level, len(refs),
                    )
                    return MapCommunitiesResult(communities=refs)
            activity.logger.info(
                "map_communities  selection=semantic empty → lexical fallback",
            )
        except Exception as exc:
            activity.logger.warning(
                "map_communities  semantic err=%s → lexical fallback", exc,
            )

    elif params.selection == "descent":
        try:
            query_vec = await _embed_query(params.query)
            if query_vec is not None:
                refs = await select_communities_descent(
                    store, query_vec, budget=params.limit,
                )
                if refs:
                    activity.logger.info(
                        "map_communities  selection=descent  selected=%d",
                        len(refs),
                    )
                    return MapCommunitiesResult(communities=refs)
            activity.logger.info(
                "map_communities  selection=descent empty → lexical fallback",
            )
        except Exception as exc:
            activity.logger.warning(
                "map_communities  descent err=%s → lexical fallback", exc,
            )

    return await _map_communities_lexical(store, params)


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
            "map_community_partial  cid=%s  llm err=%s",
            params.community_id, exc,
        )
        return MapPartialResult(community_id=params.community_id, score=0.0)

    if not is_relevant_partial(text):
        return MapPartialResult(community_id=params.community_id, score=0.0)
    return MapPartialResult(
        community_id=params.community_id, partial=text, score=1.0,
    )


__all__ = [
    "_cosine",
    "is_relevant_partial",
    "map_communities",
    "map_community_partial",
    "rank_summaries",
    "select_communities_descent",
    "select_communities_semantic",
]
