"""Cross-encoder reranker (BGE-reranker-v2-m3 by default).

Exposed as a separate factory because of its weight: pulls
``sentence-transformers``, downloads a model on first use, and runs
a transformer per chunk on inference.  Tests skip it; production
attaches it to the query engine as a ``NodePostprocessor``.
"""

from __future__ import annotations

from llama_index.postprocessor.sbert_rerank import SentenceTransformerRerank


def build_reranker(
    *,
    model_name: str = "BAAI/bge-reranker-v2-m3",
    top_n: int = 5,
) -> SentenceTransformerRerank:
    """Construct a cross-encoder reranker.

    First call downloads ``model_name`` (~1 GB for BGE-v2-m3) into
    the HuggingFace cache.  Air-gapped deploys should pre-cache.
    """
    return SentenceTransformerRerank(
        model=model_name,
        top_n=top_n,
    )
