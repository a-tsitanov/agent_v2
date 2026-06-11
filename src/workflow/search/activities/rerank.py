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

from temporalio import activity

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


@activity.defn
async def rerank_sources(params: RerankParams) -> RerankResult:
    """Co-rank the merged graph+vector pool, return reranked top-N."""
    pool = prepare_rerank_pool(params.sources)
    activity.heartbeat({"stage": "init", "n_pool": len(pool)})

    # Empty pool → nothing to rank, skip the (heavy) model entirely.
    if not pool:
        return RerankResult(sources=[])

    reranker = await get_reranker(params.top_n)
    nodes = [serialized_to_node(s) for s in pool]
    # Cross-encoder inference is sync CPU/GPU — off the loop so it can't
    # freeze concurrent search/ingest activities in the shared process.
    reranked = await asyncio.to_thread(
        reranker.postprocess_nodes, nodes, query_str=params.query,
    )

    out = [node_to_serialized(n) for n in reranked]
    activity.logger.info(
        "rerank_sources  pool=%d  top_n=%d  out=%d",
        len(pool), params.top_n, len(out),
    )
    return RerankResult(sources=out)
