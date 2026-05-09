"""Query-engine factories.

Stage 3 ships only the basic dense-vector query engine.  Stage 5
swaps the underlying retriever for a hybrid (vector + BM25 + rerank)
chain — the ``build_basic_query_engine`` API stays the same so
callers don't need to change.
"""

from __future__ import annotations

from llama_index.core import VectorStoreIndex
from llama_index.core.llms import LLM
from llama_index.core.query_engine import RetrieverQueryEngine
from llama_index.core.response_synthesizers import (
    ResponseMode,
    get_response_synthesizer,
)


def build_basic_query_engine(
    index: VectorStoreIndex,
    llm: LLM,
    *,
    similarity_top_k: int = 10,
    response_mode: ResponseMode = ResponseMode.COMPACT,
) -> RetrieverQueryEngine:
    """Dense-only retriever + LLM synthesis."""
    retriever = index.as_retriever(similarity_top_k=similarity_top_k)
    synthesizer = get_response_synthesizer(
        llm=llm,
        response_mode=response_mode,
    )
    return RetrieverQueryEngine(
        retriever=retriever,
        response_synthesizer=synthesizer,
    )
