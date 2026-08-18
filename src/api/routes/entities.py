"""Entity name lookup over HTTP.

The same `find_entity_by_name` MCP-2 exposes, reachable by the Telegram
bot — which talks to this API and nothing else, so a capability that
lives only as an MCP tool cannot become a bot command.

Prefix matching over Nebula's `entity_name_idx`: fast (well under a
second) and, unlike everything else the bot can ask about content, not a
ninety-second search.
"""

from __future__ import annotations

import functools
import json
from typing import Any

from fastapi import APIRouter, Depends, Query

from src.api.auth import require_api_key
from src.config import settings
from src.retrieval import atomic_tools

router = APIRouter(prefix="/entities", tags=["entities"])


@functools.cache
def _retriever() -> Any:
    """Store-only graph retriever, built once.

    Mirrors `tools_server._init` and `_search_deps._get_graph_retriever`
    for the nebula backend: no LlamaIndex PropertyGraphStore exists for
    it, so the nGQL-only retriever is the whole thing.
    """
    from src.graph.retriever import GraphRetriever
    from src.graph.store import build_graph_store

    return GraphRetriever.for_store(
        build_graph_store(),
        similarity_top_k=settings.agent.graph_similarity_top_k,
    )


@router.get("", dependencies=[Depends(require_api_key)])
async def find_entities(
    query: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50),
) -> dict[str, Any]:
    """Entities whose name starts with any token of `query`.

    Returns `{"entities": [...]}`, plus an `error` key when the lookup
    could not run at all. An empty list WITHOUT `error` means the graph
    genuinely has no such name; the two must not be confused, and for a
    while they were — a memory refusal was reported as "no such entity".

    Prefix, not substring: "Иванов" finds "Иванов Иван Иванович", but
    "Ромаш" does not find "ООО Ромашка". Substring search needs an index
    built for it.
    """
    result = await atomic_tools.find_entity_by_name(
        _retriever(), query=query, limit=limit,
    )
    return json.loads(result.observation)
