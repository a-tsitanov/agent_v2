"""MCP-2: atomic retrieval tools.

Exposes the 7 raw retrieval functions from
``src/retrieval/atomic_tools.py`` directly as MCP tools — no
Temporal workflow in between.  Used when the MCP client (an LLM
running in Claude Desktop / Cursor / OpenWebUI) wants to drive its
own ReAct loop and just needs primitives.

GPU/LLM protection: the project ``BoundedLLM`` semaphore (in DI,
``settings.agent.llm_max_concurrent``) gates every LLM call —
including those inside ``graph_search`` / ``find_*`` (which use
``LLMSynonymRetriever`` for query normalisation).  Concurrent MCP
clients automatically serialise behind it.

Run::

    # Stdio
    uv run python -m src.mcp.tools_server --transport stdio

    # HTTP/SSE — OpenWebUI etc.
    uv run python -m src.mcp.tools_server --transport sse --port 9002
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from fastmcp import FastMCP
from loguru import logger

from src.config import settings
from src.mcp._shared import (
    assert_api_key_env_set, log_banner, parse_args,
)
from src.retrieval import atomic_tools


mcp = FastMCP(
    name="kb-llamaindex-tools",
    instructions=(
        "Atomic retrieval tools over the project knowledge base.  "
        "Each tool returns a JSON-serialisable dict.  Compose them "
        "yourself in your own LLM loop.  For an already-orchestrated "
        "answer, use the sibling MCP-1 server (kb_search) instead."
    ),
)


# ── lazy DI bootstrap ──────────────────────────────────────────────


_lock = asyncio.Lock()
_deps: dict[str, Any] = {}


async def _init() -> None:
    """First-call init: build retriever / graph_retriever /
    chunk_repository / BoundedLLM-wrapped LLM via the same factories
    used by the FastAPI route handlers and the Temporal search-side
    activities (see ``src/workflow/_search_deps.py``)."""
    if _deps:
        return
    async with _lock:
        if _deps:
            return
        from src.ingestion.embeddings import build_embedding_model
        from src.retrieval.llm import build_search_llm
        from src.retrieval.llm_semaphore import wrap_if_needed
        from src.retrieval.vector_index import (
            build_vector_index, build_vector_store,
        )
        from src.storage.chunk_repository import ChunkRepository
        from src.storage.postgres import AsyncPostgres

        embed = build_embedding_model()
        store = build_vector_store()
        index = build_vector_index(store, embed)
        _deps["retriever"] = index.as_retriever(similarity_top_k=10)

        llm = wrap_if_needed(
            build_search_llm(),
            max_concurrent=settings.agent.llm_max_concurrent,
        )
        _deps["llm"] = llm

        try:
            from src.graph.index import (
                build_kg_extractor, build_property_graph_index,
            )
            from src.graph.retriever import GraphRetriever
            from src.graph.store import build_neo4j_graph_store
            gs = build_neo4j_graph_store()
            pg = build_property_graph_index(
                graph_store=gs, embed_model=embed,
                extractor=build_kg_extractor(llm), nodes=None,
                llm=llm,  # local LiteLLM model for the retriever's synonym step
            )
            _deps["graph"] = GraphRetriever(pg)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "MCP-2: graph_retriever disabled (Neo4j down?): {e}",
                e=exc,
            )
            _deps["graph"] = None

        _deps["chunks"] = ChunkRepository(pg=AsyncPostgres())


async def _r():
    await _init()
    return _deps["retriever"]


async def _g():
    await _init()
    return _deps["graph"]


async def _c():
    await _init()
    return _deps["chunks"]


# ── MCP tools (one per atomic_tools.* function) ────────────────────


@mcp.tool()
async def vector_search(query: str, top_k: int = 10) -> dict[str, Any]:
    """Hybrid (BM25 + dense vector + RRF) retrieval over the project
    corpus.  Returns the top_k matching chunks with text + metadata.
    """
    r = await atomic_tools.vector_search(await _r(), query=query, top_k=top_k)
    return {"sources": json.loads(r.observation)}


@mcp.tool()
async def graph_search(query: str, depth: int = 2) -> dict[str, Any]:
    """Walk the knowledge graph around the query.  Returns entities
    and relations found in the KG (and adds matched chunks to the
    in-process accumulator).  When Neo4j is unavailable returns
    empty lists rather than failing."""
    r = await atomic_tools.graph_search(await _g(), query=query, depth=depth)
    return json.loads(r.observation)


@mcp.tool()
async def find_entity_by_id(
    name: str, entity_type: str | None = None,
) -> dict[str, Any]:
    """Exact lookup by canonical name (E.164 phone, INN, email,
    SNILS, OGRN, …).  Use when you already know the identifier."""
    r = await atomic_tools.find_entity_by_id(
        await _g(), name=name, entity_type=entity_type,
    )
    return json.loads(r.observation)


@mcp.tool()
async def find_neighbours(
    entity_name: str, hops: int = 1,
) -> dict[str, Any]:
    """Walk the graph 1-2 hops around an entity.  Returns the
    entity's neighbours + relations."""
    r = await atomic_tools.find_neighbours(
        await _g(), entity_name=entity_name, hops=hops,
    )
    return json.loads(r.observation)


@mcp.tool()
async def get_chunks_by_doc_id(
    doc_id: str, limit: int = 50, offset: int = 0,
) -> dict[str, Any]:
    """Fetch all chunks of one document by `doc_id`, ordered by
    position.  Paginated via `limit`/`offset`.  Useful when a
    vector hit needs surrounding context within the same source."""
    r = await atomic_tools.get_chunks_by_doc_id(
        await _c(), doc_id=doc_id, limit=limit, offset=offset,
    )
    # Observation is either a JSON list of chunks or an error dict.
    try:
        return {"chunks": json.loads(r.observation)}
    except json.JSONDecodeError:
        return {"error": r.observation}


@mcp.tool()
async def read_full_document(
    doc_id: str, max_chars: int = 20000,
) -> dict[str, Any]:
    """Raw text of the original uploaded file (pre-chunk,
    pre-translation), capped at `max_chars`.  Use only when
    chunk-level retrieval can't surface what you need (tables,
    code, short docs)."""
    r = await atomic_tools.read_full_document(
        await _c(), doc_id=doc_id, max_chars=max_chars,
    )
    if r.observation.startswith("Error"):
        return {"error": r.observation}
    return {"text": r.observation}


# Note: filter_by_metadata is not exposed via MCP-2.  It only makes
# sense in the context of an existing accumulator, which atomic-MCP
# clients don't maintain — they pass each tool call as a fresh
# request and assemble context themselves.


def main() -> None:
    args = parse_args()
    assert_api_key_env_set()
    log_banner(
        "kb-llamaindex-tools",
        transport=args["transport"], host=args["host"], port=args["port"],
    )
    if args["transport"] == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp.run(
            transport="sse",
            host=args["host"], port=args["port"],
        )


if __name__ == "__main__":
    main()
