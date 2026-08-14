"""``rerank_sources`` activity — unified graph+vector rerank (R5).

Before the single large-tier ``synthesize_answer``, the orchestrator's
merged pool (graph-derived + vector chunks, already deduped by chunk_id
across sub-questions) is co-ranked in ONE bge cross-encoder pass. This
co-ranks the two retrieval modalities against each other so synthesis
sees the globally most-relevant top-N — not just the union order.

REUSES ``src/retrieval/reranker.py`` (the same ``BAAI/bge-reranker-v2-m3``
``SentenceTransformerRerank`` applied in ``hybrid.py``) and the repo's
node (de)serialization. The model is lazy-built/process-cached in
``_search_deps.get_reranker``. The pool-prep (dedup-before-rerank) is a
pure helper so it's unit-testable without loading the model.
"""

from __future__ import annotations

import asyncio
import math

from temporalio import activity

from src.config import settings
from src.workflow._search_deps import get_reranker
from src.workflow._search_serde import node_to_serialized, serialized_to_node
from src.workflow.contracts import RerankParams, RerankResult, SerializedNode
from src.workflow.search._merge import dedup_by_chunk_id


def prepare_rerank_pool(
    sources: list[SerializedNode],
) -> list[SerializedNode]:
    """Build the unified pool fed to the cross-encoder.

    A chunk may surface from BOTH graph and vector retrieval (or from
    overlapping sub-questions); dedup by chunk_id (first wins) so the
    reranker scores each unique chunk exactly once. Pure / Temporal-free
    — unit-tested directly.
    """
    return dedup_by_chunk_id(sources)


def _sigmoid(x: float) -> float:
    """Map an unbounded logit to (0, 1). Clamped against ``math.exp``
    overflow on pathological (very negative) inputs — at that tail the
    true value is indistinguishable from 0.0 anyway."""
    if x < -700:
        return 0.0
    return 1.0 / (1.0 + math.exp(-x))


def apply_group_weights(
    sources: list[SerializedNode], weights: dict[str, float],
) -> list[SerializedNode]:
    """Bias rerank order by channel group. The cross-encoder emits unbounded
    logits (often negative), so we sigmoid-normalize to (0,1) BEFORE applying
    the group multiplier — otherwise a boost on a negative logit would demote
    it (and a penalty promote it). Missing group / "" -> weight 1.0. Returns
    the pool re-sorted by weighted score desc. Pure."""
    def _weighted(s: SerializedNode) -> float:
        norm = _sigmoid(s.score)
        return norm * weights.get(s.metadata.get("doc_group", ""), 1.0)

    weighted = [s.model_copy(update={"score": _weighted(s)}) for s in sources]
    return sorted(weighted, key=lambda s: s.score, reverse=True)


def append_unranked_remainder(
    ranked: list[SerializedNode], pool: list[SerializedNode],
) -> list[SerializedNode]:
    """Nothing-lost safety net for the cached cross-encoder's fixed
    ``_RERANK_SCORE_CAP`` (see ``_search_deps.get_reranker``):
    ``postprocess_nodes`` never scores more than that cap, no matter what
    ``top_n`` is requested, so for a pool bigger than the cap ``ranked``
    can be missing members the model never touched at all — a real loss
    once the caller (not just synthesis) reads the result.

    Appends those pool members — matched by ``chunk_id``,
    ``prepare_rerank_pool``'s dedup key — after the ranked ones, in their
    ORIGINAL pool order: ranked chunks first (best-first), then the
    unranked remainder. A no-op when ``ranked`` already contains every
    pool member (pool at or under the cap). Pure."""
    ranked_ids = {n.chunk_id for n in ranked}
    remainder = [n for n in pool if n.chunk_id not in ranked_ids]
    return ranked + remainder


@activity.defn
async def rerank_sources(params: RerankParams) -> RerankResult:
    """Co-rank the merged graph+vector pool, return reranked top-N."""
    pool = prepare_rerank_pool(params.sources)
    activity.heartbeat({"stage": "init", "n_pool": len(pool)})

    # Empty pool → nothing to rank, skip the (heavy) model entirely.
    if not pool:
        return RerankResult(sources=[])

    reranker = await get_reranker(params.top_n)
    if reranker is None:
        # Reranker unavailable (missing torch/sentence-transformers) — degrade
        # gracefully: return the deduped pool truncated to top_n, un-reranked.
        activity.logger.warning(
            "rerank_sources: reranker unavailable, returning pool without rerank "
            "(pool=%d top_n=%d)", len(pool), params.top_n,
        )
        return RerankResult(sources=pool[: params.top_n])
    nodes = [serialized_to_node(s) for s in pool]
    # Cross-encoder inference is sync CPU/GPU — off the loop so it can't
    # freeze concurrent search/ingest activities in the shared process.
    reranked = await asyncio.to_thread(
        reranker.postprocess_nodes, nodes, query_str=params.query,
    )

    out = [node_to_serialized(n) for n in reranked]
    out = apply_group_weights(out, settings.agent.group_weights)
    # `postprocess_nodes` above only ever scores up to the cached
    # cross-encoder's fixed cap (`_RERANK_SCORE_CAP` inside
    # `get_reranker`) — `out` can be missing pool members the model never
    # touched at all when the pool exceeds that cap. Restore them,
    # unranked, after the ranked head, THEN apply `params.top_n` so the
    # caller's own "how many to return" request still bounds the
    # combined (ranked + remainder) list, same as before.
    out = append_unranked_remainder(out, pool)[: params.top_n]
    activity.logger.info(
        "rerank_sources  pool=%d  top_n=%d  out=%d",
        len(pool), params.top_n, len(out),
    )
    return RerankResult(sources=out)
