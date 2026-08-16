"""``rerank_sources`` activity — unified graph+vector rerank (R5).

The orchestrator's merged pool (graph-derived + vector chunks, already
deduped by chunk_id across sub-questions) is co-ranked in ONE bge
cross-encoder pass. This co-ranks the two retrieval modalities against
each other so the returned ordering reflects the globally most-relevant
chunks first — not just the union order.

The orchestrator requests the WHOLE pool BACK (``top_n=len(pool)``), not
just enough for synthesis. The output feeds TWO consumers: the synthesis
prompt (capped separately via the orchestrator's ``cap_synth_sources``)
AND ``SearchOutcome.sources`` returned to the caller, unconditionally —
including when ``synthesize=False``. It is no longer synthesis-only
input.

Returning the whole pool is NOT the same as scoring it. Cross-encoder
cost is linear in chunks scored and the pool grows with the corpus, so
scoring everything cannot fit a fixed activity timeout — in production
it stopped fitting, at a 139-chunk pool against a 3-minute bound.
``max_candidates`` bounds the scoring; ``select_rerank_candidates``
chooses WHICH chunks, drawing across the per-sub-question blocks rather
than off the front of the concatenation. Everything unscored still comes
back, appended in pool order.

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


def split_into_blocks(
    pool: list[SerializedNode], block_sizes: list[int],
) -> list[list[SerializedNode]]:
    """Cut ``pool`` back into the per-sub-question blocks it was merged
    from. Falls back to a single block covering the whole pool when the
    sizes are absent or do not describe this pool — a wrong split would
    silently distort selection, so anything inconsistent is treated as
    "structure unknown". Pure."""
    if not block_sizes or any(s < 0 for s in block_sizes):
        return [pool]
    if sum(block_sizes) != len(pool):
        return [pool]
    out: list[list[SerializedNode]] = []
    start = 0
    for size in block_sizes:
        if size:
            out.append(pool[start : start + size])
        start += size
    return out or [pool]


def select_rerank_candidates(
    pool: list[SerializedNode],
    block_sizes: list[int],
    max_candidates: int,
) -> list[SerializedNode]:
    """Pick the chunks the cross-encoder actually scores.

    Cross-encoder cost is linear in the number of chunks scored, and the
    pool grows with the corpus, so scoring all of it is an unbounded
    promise the fixed activity timeout cannot keep. This bounds it.

    Selection is round-robin BY RANK across blocks — each sub-question's
    best chunk, then each one's second-best, and so on — rather than the
    pool's leading slice. The pool is a concatenation of per-sub-question
    blocks whose scores are not comparable across blocks (that
    incomparability is exactly what the cross-encoder resolves), so its
    leading slice is just the first sub-question or two: on a measured
    139-chunk production pool, ``pool[:40]`` drew from 2 of 6 blocks and
    contained none of the pool's ten highest-scoring chunks, while this
    rule drew from all 6 and contained six of them. Round-robin only
    ever compares chunks with their own block-mates, which is the one
    comparison the retriever scores support.

    ``max_candidates <= 0``, or a pool already at/under the bound,
    returns the pool unchanged — the pre-existing "score everything"
    behaviour. Pure.
    """
    if max_candidates <= 0 or len(pool) <= max_candidates:
        return list(pool)
    blocks = split_into_blocks(pool, block_sizes)
    out: list[SerializedNode] = []
    for rank in range(max(len(b) for b in blocks)):
        for block in blocks:
            if rank >= len(block):
                continue
            out.append(block[rank])
            if len(out) == max_candidates:
                return out
    return out


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
    pool member (pool at or under the cap). Pure.

    NOTE: this is now the NORMAL case, not an edge one — ``max_candidates``
    bounds scoring well below both the pool size and
    ``_RERANK_SCORE_CAP``, so most of a large pool lands in the
    remainder. Two consequences, both by design:

    - The combined list is not monotonic in ``score``. The ranked head
      carries ``apply_group_weights``' output (sigmoid-normalized
      cross-encoder logit × group weight, in (0, 1)); the remainder
      still carries each chunk's raw retriever score. Ordering
      (best-first) is correct; the ``score`` field alone is not
      comparable across the boundary.
    - Group weighting only reaches the scored head. A boosted-group
      chunk sitting in the remainder is not promoted past the head —
      weighting biases the shortlist, it does not re-open it."""
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
    # Bound the model's work. Everything NOT selected is still returned —
    # `append_unranked_remainder` below puts it back, in pool order,
    # after the ranked head.
    candidates = select_rerank_candidates(
        pool, params.block_sizes, params.max_candidates,
    )
    activity.heartbeat(
        {"stage": "score", "n_pool": len(pool), "n_candidates": len(candidates)},
    )
    nodes = [serialized_to_node(s) for s in candidates]
    # Cross-encoder inference is sync CPU/GPU — off the loop so it can't
    # freeze concurrent search/ingest activities in the shared process.
    reranked = await asyncio.to_thread(
        reranker.postprocess_nodes, nodes, query_str=params.query,
    )

    out = [node_to_serialized(n) for n in reranked]
    out = apply_group_weights(out, settings.agent.group_weights)
    # `out` covers only the candidates just scored — plus, independently,
    # `postprocess_nodes` never scores past the cached cross-encoder's
    # fixed cap (`_RERANK_SCORE_CAP` inside `get_reranker`). Either way
    # the pool has members the model never touched. Restore them,
    # unranked, after the ranked head, THEN apply `params.top_n` so the
    # caller's own "how many to return" request still bounds the
    # combined (ranked + remainder) list, same as before.
    out = append_unranked_remainder(out, pool)[: params.top_n]
    activity.logger.info(
        "rerank_sources  pool=%d  scored=%d  top_n=%d  out=%d",
        len(pool), len(candidates), params.top_n, len(out),
    )
    return RerankResult(sources=out)
