"""Atomic retrieval tools as pure async functions.

Each function is a standalone unit: takes the underlying retriever /
graph_retriever / chunk_repository explicitly, returns a ``ToolResult``
with both an aggregate-friendly ``sources`` list and a JSON-string
``observation`` that an LLM can read directly.

Three consumers expected:

1. ``src/retrieval/react_agent.py`` — wraps each function in a
   FunctionTool with a closure over ``accumulated_sources``.  Backward
   compat — current ReAct loop unchanged.
2. ``src/workflow/activities/tool_execution.py`` (Stage 1 of the
   search-mcp plan) — dispatches ``ToolCallParams`` to the matching
   function; sources are serialised back through the workflow boundary.
3. ``src/mcp/tools_server.py`` (Stage 4) — exposes each function as
   its own MCP tool for external clients (Claude Desktop / OpenWebUI).

Pure functions = no closure-captured mutable state.  LLM calls inside
``graph_search`` / ``find_*`` (via ``LLMSynonymRetriever``) hit the
project-wide ``BoundedLLM`` semaphore through DI so concurrent callers
serialise on the GPU automatically.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Protocol

from llama_index.core.schema import NodeWithScore, TextNode
from loguru import logger

from src.retrieval._common import deduplicate_nodes


# ── protocols ────────────────────────────────────────────────────────


class RetrieverProtocol(Protocol):
    async def aretrieve(self, query: str) -> list[NodeWithScore]: ...


class GraphRetrieverProtocol(Protocol):
    async def aretrieve(self, query: str) -> Any: ...


class ChunkRepositoryProtocol(Protocol):
    async def aget_chunks_by_doc_id(
        self, doc_id: str, *, limit: int, offset: int,
    ) -> list[dict]: ...

    async def aread_document_text(
        self, doc_id: str, *, max_chars: int,
    ) -> str | None: ...


# ── result shape ────────────────────────────────────────────────────


@dataclass(frozen=True)
class ToolResult:
    """Single tool invocation outcome.

    * ``sources`` — list of new ``NodeWithScore`` items that should be
      appended to the accumulator (ReAct loop / synthesizer context).
      Empty when the tool doesn't produce retrieval results
      (``filter_by_metadata`` is filter-only).
    * ``observation`` — JSON string the LLM reads as the tool message
      content.  Kept short (truncated content) so the agent's chat
      history doesn't explode.
    """

    sources: list[NodeWithScore] = field(default_factory=list)
    observation: str = "{}"


# ── tools ────────────────────────────────────────────────────────────


async def vector_search(
    retriever: RetrieverProtocol,
    *,
    query: str,
    top_k: int = 10,
) -> ToolResult:
    """Hybrid (BM25 + dense) retrieval over corpus chunks."""
    nodes = await retriever.aretrieve(query)
    observation = json.dumps(
        [
            {
                "chunk_id": n.node.node_id,
                "text": n.node.get_content()[:500],
                "score": float(n.score or 0.0),
                "doc_id": (n.node.metadata or {}).get("doc_id", ""),
                "canonical_identifiers": (n.node.metadata or {}).get(
                    "canonical_identifiers", [],
                ),
            }
            for n in nodes[:top_k]
        ],
        ensure_ascii=False,
    )
    return ToolResult(sources=list(nodes), observation=observation)


async def graph_search(
    graph_retriever: GraphRetrieverProtocol | None,
    *,
    query: str,
    depth: int = 2,
) -> ToolResult:
    """Knowledge-graph traversal: entities + relations + related chunks."""
    if graph_retriever is None:
        return ToolResult(
            sources=[],
            observation=json.dumps({"entities": [], "relations": []}),
        )
    data = await graph_retriever.aretrieve(query)
    entities = getattr(data, "entities", []) or []
    relations = getattr(data, "relations", []) or []
    chunks = list(getattr(data, "chunks", []) or [])
    return ToolResult(
        sources=chunks,
        observation=json.dumps(
            {"entities": entities, "relations": relations},
            ensure_ascii=False,
        ),
    )


async def find_entity_by_id(
    graph_retriever: GraphRetrieverProtocol | None,
    *,
    name: str,
    entity_type: str | None = None,
) -> ToolResult:
    """Exact lookup by canonical name (E.164 phone, INN, email …)."""
    if graph_retriever is None:
        return ToolResult(
            sources=[], observation=json.dumps({"entities": []}),
        )
    data = await graph_retriever.aretrieve(name)
    entities = [
        e for e in (getattr(data, "entities", []) or [])
        if entity_type is None
        or (e.get("entity_type", "").lower() == entity_type.lower())
    ]
    return ToolResult(
        sources=[],
        observation=json.dumps({"entities": entities}, ensure_ascii=False),
    )


async def find_neighbours(
    graph_retriever: GraphRetrieverProtocol | None,
    *,
    entity_name: str,
    hops: int = 1,  # noqa: ARG001 — reserved for future multi-hop
) -> ToolResult:
    """Walk the graph around an entity (1-2 hops)."""
    if graph_retriever is None:
        return ToolResult(
            sources=[],
            observation=json.dumps({"entities": [], "relations": []}),
        )
    data = await graph_retriever.aretrieve(entity_name)
    return ToolResult(
        sources=[],
        observation=json.dumps(
            {
                "entities": getattr(data, "entities", []) or [],
                "relations": getattr(data, "relations", []) or [],
            },
            ensure_ascii=False,
        ),
    )


async def get_chunks_by_doc_id(
    chunk_repository: ChunkRepositoryProtocol | None,
    *,
    doc_id: str,
    limit: int = 50,
    offset: int = 0,
) -> ToolResult:
    """Fetch all chunks of one document in source order."""
    if chunk_repository is None:
        return ToolResult(
            sources=[],
            observation=json.dumps({"error": "chunk_repository unavailable"}),
        )
    try:
        chunks = await chunk_repository.aget_chunks_by_doc_id(
            doc_id, limit=limit, offset=offset,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "get_chunks_by_doc_id failed  doc={d}  err={e}",
            d=doc_id, e=exc,
        )
        return ToolResult(
            sources=[],
            observation=json.dumps({"error": str(exc), "chunks": []}),
        )
    sources = [
        NodeWithScore(
            node=TextNode(
                id_=c["chunk_id"] or f"{doc_id}#{c['position']}",
                text=c["text"],
                metadata={
                    "doc_id": c["doc_id"],
                    "file_path": c["file_path"],
                    "position": c["position"],
                },
            ),
            score=0.0,
        )
        for c in chunks
    ]
    observation = json.dumps(
        [
            {
                "chunk_id": c["chunk_id"],
                "position": c["position"],
                "text": c["text"][:400],
                "doc_id": c["doc_id"],
            }
            for c in chunks
        ],
        ensure_ascii=False,
    )
    return ToolResult(sources=sources, observation=observation)


async def read_full_document(
    chunk_repository: ChunkRepositoryProtocol | None,
    *,
    doc_id: str,
    max_chars: int = 20000,
) -> ToolResult:
    """Read raw source text of one document (pre-chunking, pre-translation)."""
    if chunk_repository is None:
        return ToolResult(
            sources=[],
            observation="Error: chunk_repository unavailable",
        )
    try:
        text = await chunk_repository.aread_document_text(
            doc_id, max_chars=max_chars,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "read_full_document failed  doc={d}  err={e}",
            d=doc_id, e=exc,
        )
        return ToolResult(sources=[], observation=f"Error: {exc}")
    if text is None:
        return ToolResult(
            sources=[], observation=f"Error: document {doc_id} not found",
        )
    return ToolResult(sources=[], observation=text)


def filter_by_metadata(
    accumulated_sources: list[NodeWithScore],
    *,
    doc_id: str | None = None,
    department: str | None = None,
    doc_type: str | None = None,
) -> ToolResult:
    """In-memory filter over already-accumulated sources.

    Pure / synchronous; doesn't talk to anything.  Returns the
    observation matching the original ReAct-tool contract (just chunk
    ids + doc_ids) so the agent's chat history sees the same shape.
    Sources list is left empty — we don't double-add what's already in
    the accumulator.
    """
    out = []
    for n in accumulated_sources:
        md = n.node.metadata or {}
        if doc_id and md.get("doc_id") != doc_id:
            continue
        if department and md.get("department") != department:
            continue
        if doc_type and md.get("doc_type") != doc_type:
            continue
        out.append(
            {
                "chunk_id": n.node.node_id,
                "doc_id": md.get("doc_id", ""),
            }
        )
    return ToolResult(
        sources=[], observation=json.dumps(out, ensure_ascii=False),
    )


# ── tool descriptions (single source of truth) ──────────────────────


TOOL_DESCRIPTIONS: dict[str, str] = {
    "vector_search": (
        "Semantic search over text chunks. Use this for questions "
        "where you don't know an exact entity name yet."
    ),
    "graph_search": (
        "Knowledge-graph traversal. Use when the question involves "
        "relations between people/organizations/topics/concepts."
    ),
    "find_entity_by_id": (
        "Exact lookup by canonical name (phone in E.164, INN, email). "
        "Use when you already know the ID."
    ),
    "find_neighbours": (
        "List entities connected to a known one in the graph "
        "(1-2 hops). Use for 'tell me everything about X' questions."
    ),
    "filter_by_metadata": (
        "Filter accumulated sources by doc_id / department / doc_type. "
        "Use to scope reasoning after a wide retrieve."
    ),
    "get_chunks_by_doc_id": (
        "Fetch ALL chunks of one document by doc_id, ordered by "
        "position. Use when a single chunk isn't enough and you need "
        "surrounding context from the same source."
    ),
    "read_full_document": (
        "Read the raw uploaded source file (pre-chunk, pre-translation) "
        "by doc_id, capped at max_chars. Use only when chunk-level "
        "retrieval can't surface what you need — table / code / short "
        "doc cases."
    ),
}


# ── dispatcher (used by Stage 1's tool_execution activity) ──────────


async def dispatch(
    tool_name: str,
    tool_kwargs: dict[str, Any],
    *,
    retriever: RetrieverProtocol | None = None,
    graph_retriever: GraphRetrieverProtocol | None = None,
    chunk_repository: ChunkRepositoryProtocol | None = None,
    accumulated_sources: list[NodeWithScore] | None = None,
) -> ToolResult:
    """Dispatch a tool call by name.  Used by the Temporal
    ``tool_execution`` activity (Stage 1 of the search-mcp plan) where
    we don't have closures, only string tool names.
    """
    if tool_name == "vector_search":
        if retriever is None:
            raise ValueError("vector_search needs a retriever")
        return await vector_search(retriever, **tool_kwargs)
    if tool_name == "graph_search":
        return await graph_search(graph_retriever, **tool_kwargs)
    if tool_name == "find_entity_by_id":
        return await find_entity_by_id(graph_retriever, **tool_kwargs)
    if tool_name == "find_neighbours":
        return await find_neighbours(graph_retriever, **tool_kwargs)
    if tool_name == "get_chunks_by_doc_id":
        return await get_chunks_by_doc_id(chunk_repository, **tool_kwargs)
    if tool_name == "read_full_document":
        return await read_full_document(chunk_repository, **tool_kwargs)
    if tool_name == "filter_by_metadata":
        return filter_by_metadata(
            accumulated_sources or [], **tool_kwargs,
        )
    raise ValueError(f"unknown tool: {tool_name}")


def deduplicate_sources(
    sources: list[NodeWithScore],
) -> list[NodeWithScore]:
    """Re-export from _common for callers that only import this module."""
    return deduplicate_nodes(sources)
