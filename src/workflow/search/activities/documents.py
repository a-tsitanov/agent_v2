"""``documents_for_communities`` activity — map community ids to the
source documents their member entities were extracted from.

Graph path: (:Chunk)-[:MENTIONS]->(:__Entity__)-[:IN_COMMUNITY]->(:Community {level:0}).
The entity→community hop is pinned to level 0: member links now exist at
every hierarchy level, but community ``id`` is unique only per level, so the
``comm.id IN $ids`` filter must be scoped to one level to avoid collisions.
Fail-open: a missing store or any Cypher error → empty list (the answer
is never blocked on document provenance).
"""

from __future__ import annotations

import asyncio
from typing import Any

from temporalio import activity

from src.workflow.contracts import (
    DocumentsForCommunitiesParams,
    DocumentsForCommunitiesResult,
)

# NOTE: `c.doc_id` and the MENTIONS/IN_COMMUNITY traversal are written per
# the project graph model but UNVERIFIED against a live Neo4j store (same
# caution as the GDS Cypher in src/graph/communities.py). Verify the chunk
# doc_id property name on the live store before relying on it.
_DOCS_FOR_COMMUNITIES_CYPHER = """
MATCH (c:Chunk)-[:MENTIONS]->(:__Entity__)-[:IN_COMMUNITY]->(comm:Community {level: 0})
WHERE comm.id IN $ids
RETURN DISTINCT c.doc_id AS doc_id
"""


def _get_store() -> Any | None:
    """Neo4j store or None when unreachable (indirected for monkeypatch)."""
    try:
        from src.graph.store import build_neo4j_graph_store

        return build_neo4j_graph_store()
    except Exception as exc:
        activity.logger.warning("documents_for_communities: store unavailable: %s", exc)
        return None


@activity.defn
async def documents_for_communities(
    params: DocumentsForCommunitiesParams,
) -> DocumentsForCommunitiesResult:
    activity.heartbeat({"stage": "documents_for_communities",
                        "n_communities": len(params.community_ids)})
    if not params.community_ids:
        return DocumentsForCommunitiesResult(doc_ids=[])
    store = _get_store()
    if store is None:
        return DocumentsForCommunitiesResult(doc_ids=[])
    try:
        rows = await asyncio.to_thread(
            store.structured_query,
            _DOCS_FOR_COMMUNITIES_CYPHER,
            {"ids": list(params.community_ids)},
        )
    except Exception as exc:  # fail-open
        activity.logger.warning("documents_for_communities  err=%s", exc)
        return DocumentsForCommunitiesResult(doc_ids=[])

    doc_ids: list[str] = []
    for r in rows or []:
        d = (r or {}).get("doc_id")
        if d and d not in doc_ids:
            doc_ids.append(str(d))
    return DocumentsForCommunitiesResult(doc_ids=doc_ids)
