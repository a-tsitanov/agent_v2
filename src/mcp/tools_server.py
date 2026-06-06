"""MCP-2: atomic retrieval tools.

Exposes 8 of the raw retrieval functions from
``src/retrieval/atomic_tools.py`` directly as MCP tools — no
Temporal workflow in between (``filter_by_metadata`` is intentionally
omitted; it operates on an in-process accumulator that stateless
tool-call clients don't maintain).  Used when the MCP client (an LLM
running in Hermes Agent / Claude Desktop / Cursor / OpenWebUI) wants
to drive its own ReAct loop and just needs primitives.

Every tool carries an 1800s (30 min) execution timeout (FastMCP
``@mcp.tool(timeout=...)``, seconds) so a slow graph walk or cold
retriever can't hang a client indefinitely, while still letting the
heaviest legitimate calls finish.

GPU/LLM protection: every LLM call — including those inside
``graph_search`` / ``find_*`` (which use ``LLMSynonymRetriever`` for
query normalisation) — is served by the per-process ``LLMPool``
(``src/retrieval/llm_pool.py``, configured via ``settings.llm_pool``),
which keeps each role BoundedLLM-gated.  Concurrent MCP clients
automatically serialise behind it.

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
    assert_api_key_env_set, build_sse_auth, log_banner, parse_args,
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
    auth=build_sse_auth(),  # evaluated at import time; see build_sse_auth docstring
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
        from src.retrieval.vector_index import (
            build_vector_index, build_vector_store,
        )
        from src.storage.chunk_repository import ChunkRepository
        from src.storage.postgres import AsyncPostgres

        embed = build_embedding_model()
        store = build_vector_store()
        index = build_vector_index(store, embed)
        _deps["retriever"] = index.as_retriever(similarity_top_k=10)

        from src.retrieval.llm_pool import get_llm_pool
        llm = get_llm_pool().get("search")
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


@mcp.tool(timeout=1800)
async def vector_search(query: str, top_k: int = 10) -> dict[str, Any]:
    """Hybrid (BM25 + dense vector + RRF) semantic search over document
    chunks. Returns the top_k matching chunks with text + metadata.

    USE FOR: factual / "what / how / why" questions answered by the text
    of documents. This is the default for content questions.
    NOT FOR: entity lookup or relationships — use the graph tools
    (`graph_search`, `find_entity_by_*`, `find_neighbours`, `graph_walk`).
    NEXT STEP: to read more around a good hit, call `get_chunks_by_doc_id`
    with its `doc_id`.
    """
    r = await atomic_tools.vector_search(await _r(), query=query, top_k=top_k)
    return {"sources": json.loads(r.observation)}


@mcp.tool(timeout=1800)
async def graph_search(query: str, depth: int = 1) -> dict[str, Any]:
    """Similarity search over the knowledge graph from a free-text query.
    Returns matched entities + their neighbours up to `depth` triplet-hops
    + related chunks.

    `depth` controls neighbour expansion (1-3, default 1): 1 = matched
    nodes + immediate relations; raise for wider context (more nodes /
    tokens / latency).
    USE FOR: graph / relationship questions when you have NO specific
    entity pinned yet — discovery ("что в графе связано с темой X").
    PREFER INSTEAD: `find_entity_by_id` (you have an exact identifier),
    `find_entity_by_name` (you have a partial name), `find_neighbours`
    (you have a known entity, want 1 hop), `graph_walk` (known entity,
    multi-hop chains). Returns empty lists if Neo4j is unavailable."""
    r = await atomic_tools.graph_search(await _g(), query=query, depth=depth)
    return json.loads(r.observation)


@mcp.tool(timeout=1800)
async def graph_walk(
    start_entity: str, hops: int = 2, rel_filter: list[str] | None = None,
) -> dict[str, Any]:
    """Bounded MULTI-hop traversal following real relationships outward
    from a KNOWN start entity (hard-capped nodes / edges / hops).

    USE FOR: transitive connection / chain questions — "how is X linked
    to Y", "trace X's network N hops out". Optionally restrict to
    `rel_filter` relationship types. Returns entities + relations.
    NOT FOR: a single hop (use `find_neighbours`) or discovery without a
    start entity (use `graph_search` / `find_entity_by_name` first)."""
    r = await atomic_tools.graph_walk(
        await _g(), start_entity=start_entity, hops=hops,
        rel_filter=rel_filter,
    )
    return json.loads(r.observation)


@mcp.tool(timeout=1800)
async def find_entity_by_id(
    name: str, entity_type: str | None = None,
) -> dict[str, Any]:
    """Exact pinpoint lookup by a precise identifier or canonical name
    (E.164 phone, INN, OGRN, SNILS, email, exact entity name).

    USE FOR: "whose number is +7…", "find INN 7707…" — when the user
    gives an exact identifier. One entity expected.
    PREFER INSTEAD: `find_entity_by_name` when you only have a partial /
    approximate name; `graph_search` for non-name semantic queries."""
    r = await atomic_tools.find_entity_by_id(
        await _g(), name=name, entity_type=entity_type,
    )
    return json.loads(r.observation)


@mcp.tool(timeout=1800)
async def find_entity_by_name(
    query: str, limit: int = 10,
) -> dict[str, Any]:
    """Fuzzy / partial NAME search via the full-text index ("Иванов" →
    "Иванов Иван Иванович"; "Ромаш" → "ООО Ромашка").

    USE FOR: resolving an approximate / partial name into canonical
    entities — usually the first step before drilling in with
    `find_neighbours` or `graph_walk`.
    PREFER INSTEAD: `find_entity_by_id` for an exact identifier;
    `graph_search` for non-name semantic queries. Returns entity rows
    only (no chunks)."""
    r = await atomic_tools.find_entity_by_name(
        await _g(), query=query, limit=limit,
    )
    return json.loads(r.observation)


@mcp.tool(timeout=1800)
async def find_neighbours(
    entity_name: str, hops: int = 1,
) -> dict[str, Any]:
    """Neighbours + relations of a KNOWN entity, up to `hops` triplet-hops.

    `hops` controls neighbour depth (1-3, default 1 = immediate). Raising
    it widens context (more nodes / tokens / latency).
    USE FOR: "who / what is connected to X", and as the core of an entity
    dossier.
    PREFER INSTEAD: `graph_walk` for deterministic edge-following with
    rel-type filtering; `graph_search` if the entity isn't pinned down
    yet; `find_entity_*` to resolve the entity first."""
    r = await atomic_tools.find_neighbours(
        await _g(), entity_name=entity_name, hops=hops,
    )
    return json.loads(r.observation)


@mcp.tool(timeout=1800)
async def get_chunks_by_doc_id(
    doc_id: str, limit: int = 50, offset: int = 0,
) -> dict[str, Any]:
    """Fetch chunks of ONE document by `doc_id`, in position order,
    paginated via `limit` / `offset`.

    USE FOR: expanding context around a `vector_search` hit within the
    same source, or reading a document chunk-by-chunk.
    REQUIRES: a `doc_id` you already have (from `vector_search` or graph
    results). For the whole raw file at once, see `read_full_document`."""
    r = await atomic_tools.get_chunks_by_doc_id(
        await _c(), doc_id=doc_id, limit=limit, offset=offset,
    )
    # Observation is either a JSON list of chunks or an error dict.
    try:
        return {"chunks": json.loads(r.observation)}
    except json.JSONDecodeError:
        return {"error": r.observation}


@mcp.tool(timeout=1800)
async def read_full_document(
    doc_id: str, max_chars: int = 20000,
) -> dict[str, Any]:
    """Raw full text of the original uploaded file (pre-chunk,
    pre-translation), capped at `max_chars`.

    USE FOR: tables, code, or short documents where chunk-level
    retrieval splits content badly.
    REQUIRES: a `doc_id`. For large documents prefer
    `get_chunks_by_doc_id` (this returns one big blob, truncated at
    `max_chars`)."""
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
