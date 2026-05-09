"""Graph-search wrapper for the agent loop.

Returns a ``RoundGraphData`` with structured ``entities`` and
``relations`` lists — same shape used by enterprise-kb's
``query_graph_data`` so dedup / accumulation logic ports
directly.

Implementation thin over LlamaIndex's PG retriever:
  * Uses ``LLMSynonymRetriever`` from PropertyGraphIndex by default —
    it normalises query terms via the LLM before traversing.
  * Returns three things the agent cares about:
      - entity dicts (``entity_name``, ``entity_type``,
        ``description``);
      - relationship dicts (``src_id``, ``tgt_id``, ``label``);
      - chunk nodes related to the matched graph elements.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from llama_index.core import PropertyGraphIndex
from llama_index.core.schema import NodeWithScore


@dataclass
class RoundGraphData:
    entities: list[dict] = field(default_factory=list)
    relations: list[dict] = field(default_factory=list)
    chunks: list[NodeWithScore] = field(default_factory=list)


class GraphRetriever:
    """Async wrapper over ``PropertyGraphIndex.as_retriever``."""

    def __init__(
        self,
        pg_index: PropertyGraphIndex,
        *,
        similarity_top_k: int = 10,
        path_depth: int = 1,
        include_text: bool = True,
    ) -> None:
        self._retriever = pg_index.as_retriever(
            similarity_top_k=similarity_top_k,
            path_depth=path_depth,
            include_text=include_text,
        )

    async def aretrieve(self, query: str) -> RoundGraphData:
        nodes = await self._retriever.aretrieve(query)
        out = RoundGraphData()
        for n in nodes:
            text = n.node.get_content() or ""
            md = n.node.metadata or {}
            # PG retriever interleaves three node kinds; classify by
            # node-class name so we don't depend on private fields.
            cls = type(n.node).__name__
            if cls in {"EntityNode", "ChunkNode"} and md.get("triplet_source_id"):
                # text-as-triplet snippet
                out.relations.append({
                    "src_id": md.get("subj") or md.get("src") or "",
                    "tgt_id": md.get("obj") or md.get("tgt") or "",
                    "label": md.get("rel_type") or md.get("label") or "",
                    "description": text,
                })
            elif cls == "EntityNode":
                out.entities.append({
                    "entity_name": md.get("name") or text,
                    "entity_type": md.get("label") or md.get("type") or "",
                    "description": text,
                })
            else:
                # plain content chunk attached for context
                out.chunks.append(n)
        return out
