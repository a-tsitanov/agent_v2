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

All builders use the project's search-role LLM (BoundedLLM wrapper —
see ``src/retrieval/llm_semaphore.py``) so concurrent activities
share one GPU semaphore even when they run inside Temporal.
"""

from __future__ import annotations

import asyncio
from typing import Any

from llama_index.core.llms import LLM
from loguru import logger

from src.retrieval.atomic_tools import (
    GraphRetrieverProtocol, RetrieverProtocol,
)
from src.retrieval.llm import build_search_llm
from src.retrieval.llm_semaphore import wrap_if_needed


_lock = asyncio.Lock()
_state: dict[str, Any] = {
    "llm": None, "retriever": None, "graph_retriever": None,
    "chunk_repository": None, "synthesizer": None,
    "synthesis_llm": None, "synthesis_synthesizer": None,
}


async def _build_retriever_once():
    from src.ingestion.embeddings import build_embedding_model
    from src.retrieval.vector_index import build_vector_index, build_vector_store
    embed = build_embedding_model()
    store = build_vector_store()
    index = build_vector_index(store, embed)
    return index.as_retriever(similarity_top_k=10), embed


async def _build_graph_retriever_once(embed_model, llm):
    """Returns None when Neo4j unreachable — graph-tools handle this."""
    try:
        from src.graph.index import build_kg_extractor, build_property_graph_index
        from src.graph.retriever import GraphRetriever
        from src.graph.store import build_neo4j_graph_store
        gs = build_neo4j_graph_store()
        pg = build_property_graph_index(
            graph_store=gs, embed_model=embed_model,
            extractor=build_kg_extractor(llm), nodes=None,
        )
        return GraphRetriever(pg)
    except Exception as exc:  # noqa: BLE001
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
    """Project search-role LLM wrapped in BoundedLLM semaphore."""
    async with _lock:
        if _state["llm"] is None:
            from src.config import settings
            _state["llm"] = wrap_if_needed(
                build_search_llm(),
                max_concurrent=settings.agent.llm_max_concurrent,
            )
    return _state["llm"]


async def get_retriever() -> RetrieverProtocol:
    async with _lock:
        if _state["retriever"] is None:
            ret, embed = await _build_retriever_once()
            _state["retriever"] = ret
            _state["_embed_model"] = embed
    return _state["retriever"]


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
        llm = _state["llm"] or wrap_if_needed(build_search_llm(), max_concurrent=8)
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
    async with _lock:
        if _state["synthesizer"] is None:
            llm = await get_search_llm()
            _state["synthesizer"] = await _build_synthesizer_once(llm)
    return _state["synthesizer"]


async def get_synthesis_llm() -> LLM:
    """Large-tier final-synthesis LLM (R2 plan-execute flow), wrapped in
    the shared BoundedLLM semaphore."""
    async with _lock:
        if _state["synthesis_llm"] is None:
            from src.config import settings
            from src.retrieval.llm import build_synthesis_llm
            _state["synthesis_llm"] = wrap_if_needed(
                build_synthesis_llm(),
                max_concurrent=settings.agent.llm_max_concurrent,
            )
    return _state["synthesis_llm"]


async def get_synthesis_synthesizer():
    """ResponseSynthesizer bound to the large tier (R2)."""
    async with _lock:
        if _state["synthesis_synthesizer"] is None:
            llm = await get_synthesis_llm()
            _state["synthesis_synthesizer"] = await _build_synthesizer_once(llm)
    return _state["synthesis_synthesizer"]


def reset_for_tests() -> None:
    """Test hook — clear all caches.  Production code never calls."""
    for k in list(_state):
        _state[k] = None if k in (
            "llm", "retriever", "graph_retriever", "chunk_repository",
            "synthesizer", "synthesis_llm", "synthesis_synthesizer",
        ) else _state[k]
    _state.pop("graph_retriever_attempted", None)
    _state.pop("_embed_model", None)
