"""Hybrid retrieval — BM25 + dense vector + RRF fusion.

The agent loop (``src/retrieval/agent.py``) doesn't change in
Stage 5 — only the retriever passed in.  This isolates the upgrade
to a single seam and makes A/B benchmarks straightforward
(``run_eval --retriever vector`` vs ``--retriever hybrid``).

Reranker is exposed as a separate factory because:
  * it pulls in ``sentence-transformers`` + downloads a model on
    first use, so heavy and unit-test-unfriendly;
  * for evaluation we want to compare retriever fusion with and
    without reranking.

Composition pattern (Stage 8 ties it together):

    retriever = build_hybrid_retriever(...)
    reranker  = build_reranker()              # optional
    nodes = retriever.retrieve(q)
    nodes = reranker.postprocess_nodes(nodes, query_str=q)
"""

from __future__ import annotations

from llama_index.core import VectorStoreIndex
from llama_index.core.llms import LLM
from llama_index.core.retrievers import BaseRetriever, QueryFusionRetriever
from llama_index.core.schema import BaseNode
from llama_index.retrievers.bm25 import BM25Retriever


def build_bm25_retriever(
    nodes: list[BaseNode], *, similarity_top_k: int = 10,
) -> BM25Retriever:
    """Pure-Python BM25 over ``nodes``.  Index built in-memory.

    For larger corpora pass nodes from the persisted ``docstore``
    of a ``StorageContext`` — same call shape, no API change.
    """
    return BM25Retriever.from_defaults(
        nodes=nodes,
        similarity_top_k=similarity_top_k,
    )


def build_hybrid_retriever(
    vector_index: VectorStoreIndex,
    bm25_nodes: list[BaseNode],
    *,
    similarity_top_k: int = 10,
    num_queries: int = 1,
    fusion_mode: str = "reciprocal_rerank",
    weights: list[float] | None = None,
    llm: LLM | None = None,
) -> BaseRetriever:
    """RRF-fuse the dense vector retriever and a BM25 retriever.

    ``num_queries=1`` disables LlamaIndex's built-in query
    expansion — the agent loop already handles its own multi-hop
    expansion via the LLM judge, two layers of expansion would just
    burn tokens.  Even with expansion off, ``QueryFusionRetriever``
    resolves an LLM at construction time via ``Settings.llm`` if
    ``llm`` is None — pass an explicit ``llm`` (real or
    ``MockLLM`` for tests) to avoid an OpenAI-dependency surprise.

    ``fusion_mode="reciprocal_rerank"`` is RRF; ``"relative_score"``
    is the alternative when retriever scores are calibrated (rare).
    """
    vector_retriever = vector_index.as_retriever(
        similarity_top_k=similarity_top_k,
    )
    bm25_retriever = build_bm25_retriever(
        bm25_nodes, similarity_top_k=similarity_top_k,
    )
    return QueryFusionRetriever(
        retrievers=[vector_retriever, bm25_retriever],
        similarity_top_k=similarity_top_k,
        num_queries=num_queries,
        mode=fusion_mode,
        use_async=True,
        retriever_weights=weights,
        llm=llm,
    )
