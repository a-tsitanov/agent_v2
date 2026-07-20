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
        if settings.graph.backend == "nebula":
            # Nebula: no LlamaIndex PropertyGraphStore; the retriever works
            # over nGQL (awalk/afind). Vector/synonym aretrieve is Phase 3.
            return GraphRetriever.for_store(
                gs,
                similarity_top_k=settings.agent.graph_similarity_top_k,
                filter_polarity_temporal=(
                    settings.agent.graph_walk_filter_polarity_temporal
                ),
            )
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


async def get_vector_retriever(
    top_k: int, filters: Any | None = None,
) -> RetrieverProtocol:
    """Per-request vector retriever at a custom ``similarity_top_k``.

    Used by the date-filter path (``retrieve_subquestion``). ``filters`` is
    a Milvus ``MetadataFilters`` push-down: WITHOUT it the vector search
    ranks the globally-most-similar chunks and a post-filter then drops the
    out-of-range ones — which starves the in-range pool when the corpus
    spans many dates (the relevant same-date chunks never enter the global
    top-k). Pushing the date bound INTO the Milvus search makes it rank the
    top-k among only in-range chunks. Over-fetch still applies so the merged
    pool (vector + graph + walk) has candidates for rerank. Reuses the
    cached vector index — ``as_retriever`` is an in-memory wrap (no Milvus
    rebuild), so building one per request is cheap."""
    async with _lock:
        if _state.get("_vector_index") is None:
            ret, embed = await _build_retriever_once()
            if _state["retriever"] is None:
                _state["retriever"] = ret
            _state["_embed_model"] = embed
        index = _state["_vector_index"]
    return index.as_retriever(similarity_top_k=top_k, filters=filters)


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
        # Fail-soft: the reranker pulls sentence-transformers + torch, which are
        # heavy and may be absent from a lean deploy image (e.g. no linux-arm64
        # torch wheel). Rerank is OPTIONAL — on a missing dep, cache the failure
        # (so we don't re-attempt the import every call) and return None; the
        # rerank activity then skips reranking and returns the pool as-is.
        if _state.get("reranker_failed"):
            return None
        if _state["reranker"] is None:
            from src.config import settings
            from src.retrieval.reranker import build_reranker
            try:
                _state["reranker"] = build_reranker(
                    top_n=top_n if top_n is not None
                    else settings.temporal.rerank_top_n,
                )
            except Exception as exc:  # ImportError (no torch) or model-load failure
                from loguru import logger
                logger.warning(
                    "reranker unavailable — search continues WITHOUT rerank: {e}", e=exc,
                )
                _state["reranker_failed"] = True
                return None
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
    _state.pop("reranker_failed", None)
    _state.pop("_embed_model", None)
    _state.pop("_vector_index", None)
