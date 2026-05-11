"""Shared helpers used by both legacy `agentic_search` and the new
ReAct / reflective implementations.

Kept here so the three retrieval entry points don't duplicate
node-to-citation conversion, dedup logic, etc.
"""

from __future__ import annotations

from llama_index.core.schema import NodeWithScore

from src.models.search import SourceCitation


def deduplicate_nodes(
    nodes: list[NodeWithScore],
) -> list[NodeWithScore]:
    """Keep first occurrence per `node.node_id`."""
    seen: set[str] = set()
    out: list[NodeWithScore] = []
    for n in nodes:
        if n.node.node_id in seen:
            continue
        seen.add(n.node.node_id)
        out.append(n)
    return out


def node_to_citation(n: NodeWithScore) -> SourceCitation:
    md = n.node.metadata or {}
    return SourceCitation(
        doc_id=str(md.get("doc_id") or md.get("file_path") or ""),
        chunk_id=n.node.node_id,
        position=int(md.get("position", 0) or 0),
        content=n.node.get_content(),
        score=float(n.score or 0.0),
        department=str(md.get("department", "") or ""),
        doc_type=str(md.get("doc_type", "") or md.get("file_type", "") or ""),
    )
