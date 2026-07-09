"""Process-level cache for search-side dependencies inside Temporal
activities.

Activities re-enter Python on each invocation; building a Milvus
client + connecting to Neo4j + bootstrapping the BGE-reranker from
scratch on every ReAct step would multiply latency.  We memoise per
worker process — first activity in this worker builds them, all
subsequent calls reuse.

Lock-protected so concurrent activity workers don't race the lazy
init.  Reset hooks left out — workers restart cleanly enough; if
the underlying store config changes the operator restarts the
worker.

All builders obtain their LLM from the shared per-process ``LLMPool``
(``src/retrieval/llm_pool.py``) — still BoundedLLM-gated under the
hood — so concurrent activities share one GPU semaphore.
"""

from __future__ import annotations

import asyncio
from typing import Any

from llama_index.core.llms import LLM
from loguru import logger

from src.retrieval.atomic_tools import (
    GraphRetrieverProtocol,
    RetrieverProtocol,
)

_lock = asyncio.Lock()
_state: dict[str, Any] = {
    "llm": None, "retriever": None, "graph_retriever": None,
    "chunk_repository": None, "synthesizer": None,
    "synthesis_llm": None, "synthesis_synthesizer": None,
    "reranker": None,
}


async def _build_retriever_once():
    from src.ingestion.embeddings import build_embedding_model
    from src.retrieval.vector_index import build_vector_index, build_vector_store
    embed = build_embedding_model()
    store = build_vector_store()
    index = build_vector_index(store, embed)
    # Stash the index so per-request retrievers at a custom top_k (the date
    # over-fetch path) can reuse it without rebuilding the Milvus client.
    _state["_vector_index"] = index
    return index.as_retriever(similarity_top_k=10), embed


async def _build_graph_retriever_once(embed_model, llm):
    """Returns None when Neo4j unreachable — graph-tools handle this."""
    try:
        from src.config import settings
        from src.graph.index import (
            build_kg_extractor,
            build_property_graph_index,
            ensure_entity_fulltext_index,
            ensure_entity_lookup_indexes,
        )
        from src.graph.retriever import GraphRetriever
        from src.graph.store import build_graph_store
        gs = build_graph_store()
        pg = build_property_graph_index(
            graph_store=gs, embed_model=embed_model,
            extractor=build_kg_extractor(llm), nodes=None,
            llm=llm,  # local LiteLLM model for the retriever's synonym step
        )
        # Existing-graph coverage: create the entity-name full-text index
        # plus the range indexes (name / mention_count) once at bootstrap
        # (idempotent, fail-open).
        ensure_entity_fulltext_index(gs)
        ensure_entity_lookup_indexes(gs)
        return GraphRetriever(
            pg, similarity_top_k=settings.agent.graph_similarity_top_k,
            filter_polarity_temporal=(
                settings.agent.graph_walk_filter_polarity_temporal
            ),
        )
    except Exception as exc:
        logger.warning(
            "search_deps: graph_retriever disabled (Neo4j unreachable?): {e}",
            e=exc,
        )
        return None


async def _build_chunk_repo_once():
    from src.storage.chunk_repository import ChunkRepository
    from src.storage.postgres import AsyncPostgres
    return ChunkRepository(pg=AsyncPostgres())


async def _build_synthesizer_once(llm):
    from llama_index.core import get_response_synthesizer
    from llama_index.core.response_synthesizers.type import ResponseMode
    return get_response_synthesizer(
        llm=llm, response_mode=ResponseMode.COMPACT,
    )


async def get_search_llm() -> LLM:
    """Project search-role LLM from the shared per-process LLM pool."""
    from src.retrieval.llm_pool import get_llm_pool
    return get_llm_pool().get("search")


async def get_retriever() -> RetrieverProtocol:
    async with _lock:
        if _state["retriever"] is None:
            ret, embed = await _build_retriever_once()
            _state["retriever"] = ret
            _state["_embed_model"] = embed
    return _state["retriever"]


async def get_vector_retriever(top_k: int) -> RetrieverProtocol:
    """Per-request vector retriever at a custom ``similarity_top_k``.

    Used by the date-filter over-fetch path (``retrieve_subquestion``):
    fetch more candidates so post-filtering out-of-range chunks doesn't
    starve the in-range pool.  Reuses the cached vector index —
    ``as_retriever`` is an in-memory wrap (no Milvus rebuild), so building
    one per request is cheap."""
    async with _lock:
        if _state.get("_vector_index") is None:
            ret, embed = await _build_retriever_once()
            if _state["retriever"] is None:
                _state["retriever"] = ret
            _state["_embed_model"] = embed
        index = _state["_vector_index"]
    return index.as_retriever(similarity_top_k=top_k)


async def get_graph_retriever() -> GraphRetrieverProtocol | None:
    if _state["graph_retriever"] is not None or "graph_retriever_attempted" in _state:
        return _state["graph_retriever"]
    async with _lock:
        if "graph_retriever_attempted" in _state:
            return _state["graph_retriever"]
        # Need embed model + llm built first.
        if "_embed_model" not in _state:
            _, embed = await _build_retriever_once()
            _state["_embed_model"] = embed
        from src.retrieval.llm_pool import get_llm_pool
        llm = _state["llm"] or get_llm_pool().get("search")
        _state["llm"] = llm
        _state["graph_retriever"] = await _build_graph_retriever_once(
            _state["_embed_model"], llm,
        )
        _state["graph_retriever_attempted"] = True
    return _state["graph_retriever"]


async def get_chunk_repository():
    async with _lock:
        if _state["chunk_repository"] is None:
            _state["chunk_repository"] = await _build_chunk_repo_once()
    return _state["chunk_repository"]


async def get_synthesizer():
    # Resolve the LLM before taking _lock (pool.get is non-blocking).
    llm = await get_search_llm()
    async with _lock:
        if _state["synthesizer"] is None:
            _state["synthesizer"] = await _build_synthesizer_once(llm)
    return _state["synthesizer"]


async def get_synthesis_llm() -> LLM:
    """Large-tier final-synthesis LLM from the shared per-process pool."""
    from src.retrieval.llm_pool import get_llm_pool
    return get_llm_pool().get("synthesis")


async def get_synthesis_synthesizer():
    """ResponseSynthesizer bound to the large tier (R2)."""
    # Resolve the LLM before taking _lock (pool.get is non-blocking).
    llm = await get_synthesis_llm()
    async with _lock:
        if _state["synthesis_synthesizer"] is None:
            _state["synthesis_synthesizer"] = await _build_synthesizer_once(llm)
    return _state["synthesis_synthesizer"]


async def get_reranker(top_n: int | None = None):
    """Process-cached bge cross-encoder (Search R5 unified rerank).

    Heavy: pulls ``sentence-transformers`` and downloads
    ``BAAI/bge-reranker-v2-m3`` on first use — memoised per worker
    process so the unified rerank pass before synthesis doesn't reload
    it.  ``top_n`` defaults to ``TemporalSettings.rerank_top_n``; the
    cached instance is built once with that top_n (the rerank activity
    enforces top_n on its side too, so a config change between calls
    still truncates correctly)."""
    async with _lock:
        if _state["reranker"] is None:
            from src.config import settings
            from src.retrieval.reranker import build_reranker
            _state["reranker"] = build_reranker(
                top_n=top_n if top_n is not None
                else settings.temporal.rerank_top_n,
            )
    return _state["reranker"]


def reset_for_tests() -> None:
    """Test hook — clear all caches.  Production code never calls."""
    for k in list(_state):
        _state[k] = None if k in (
            "llm", "retriever", "graph_retriever", "chunk_repository",
            "synthesizer", "synthesis_llm", "synthesis_synthesizer",
            "reranker",
        ) else _state[k]
    _state.pop("graph_retriever_attempted", None)
    _state.pop("_embed_model", None)
    _state.pop("_vector_index", None)
