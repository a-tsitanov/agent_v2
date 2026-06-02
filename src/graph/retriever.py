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

import asyncio
import re
from dataclasses import dataclass, field

from llama_index.core import PropertyGraphIndex
from llama_index.core.schema import NodeWithScore
from loguru import logger

# ── bounded multi-hop walk caps (R3) ─────────────────────────────────
# Hard ceilings so a deep/dense traversal can never blow up the agent's
# context. Enforced both in the Cypher LIMIT (server side) and again
# when mapping rows (defensive truncation).
GRAPH_WALK_MAX_HOPS = 3
GRAPH_WALK_NODE_CAP = 50
GRAPH_WALK_EDGE_CAP = 100


@dataclass
class RoundGraphData:
    entities: list[dict] = field(default_factory=list)
    relations: list[dict] = field(default_factory=list)
    chunks: list[NodeWithScore] = field(default_factory=list)


# Bounded N-hop traversal. ``$hops`` is interpolated into the
# variable-length pattern (Neo4j can't parametrise the upper bound), so
# it MUST be a clamped int — never user text. Everything else is a
# proper param. ``size($rel_filter)=0`` means "no relation filter".
_WALK_CYPHER = """
MATCH (e:__Entity__ {{name: $name}})
CALL {{
    WITH e
    MATCH path = (e)-[r*1..{hops}]-(m:__Entity__)
    WHERE all(rel IN r WHERE size($rel_filter) = 0
              OR type(rel) IN $rel_filter)
    RETURN m, r
    LIMIT $node_cap
}}
WITH collect(DISTINCT m) AS ms,
     [rel IN apoc.coll.flatten(collect(r)) | rel][0..$edge_cap] AS rels,
     e
RETURN
    [n IN ([e] + ms) | {{
        name: n.name,
        label: head([l IN labels(n) WHERE l <> '__Entity__'
                     AND l <> '__Node__']),
        description: coalesce(n.description, '')
    }}][0..$node_cap] AS entities,
    [rel IN rels | {{
        src: startNode(rel).name,
        tgt: endNode(rel).name,
        label: type(rel)
    }}] AS relations
"""

# Fallback without APOC (apoc.coll.flatten) — relation list is built
# per-path and flattened in Python instead.
_WALK_CYPHER_NO_APOC = """
MATCH (e:__Entity__ {{name: $name}})
MATCH path = (e)-[r*1..{hops}]-(m:__Entity__)
WHERE all(rel IN r WHERE size($rel_filter) = 0
          OR type(rel) IN $rel_filter)
WITH e, m, r
LIMIT $node_cap
RETURN
    e.name AS start_name,
    [l IN labels(e) WHERE l <> '__Entity__' AND l <> '__Node__'] AS start_labels,
    coalesce(e.description, '') AS start_description,
    m.name AS m_name,
    [l IN labels(m) WHERE l <> '__Entity__' AND l <> '__Node__'] AS m_labels,
    coalesce(m.description, '') AS m_description,
    [rel IN r | {{src: startNode(rel).name, tgt: endNode(rel).name,
                  label: type(rel)}}] AS rels
"""


# Lucene special chars that must be backslash-escaped inside a query term.
_LUCENE_SPECIAL = re.compile(r'([+\-!(){}\[\]^"~*?:\\/&|])')


def build_fulltext_query(query: str) -> str:
    """Build a Lucene OR-of-tokens query for the ``entity_name_fulltext``
    index: whitespace-split the input, escape Lucene special chars in each
    token, join with ``OR``.  Returns ``""`` for blank input so the caller
    short-circuits to empty results (never issues a bare/invalid query)."""
    tokens: list[str] = []
    for raw in (query or "").split():
        esc = _LUCENE_SPECIAL.sub(r"\\\1", raw).strip()
        if esc:
            tokens.append(esc)
    return " OR ".join(tokens)


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
        # Underlying store handle for the bounded N-hop ``awalk`` (R3).
        # ``structured_query`` is the generic Cypher entry on
        # Neo4jPropertyGraphStore (same one ER uses). None for stores
        # that don't expose it — ``awalk`` degrades to empty.
        self._graph_store = getattr(
            pg_index, "property_graph_store", None,
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

    async def awalk(
        self,
        start_entity: str,
        *,
        hops: int = 2,
        rel_filter: list[str] | None = None,
    ) -> RoundGraphData:
        """Bounded N-hop traversal from ``start_entity``.

        Distinct from ``aretrieve`` (which is similarity-based,
        path_depth=1 and UNCHANGED). This issues one bounded Cypher
        query against the underlying store:

          * ``hops`` is clamped to ``GRAPH_WALK_MAX_HOPS`` and
            interpolated into the variable-length pattern (Neo4j can't
            parametrise the bound — so it must be a clamped int).
          * ``rel_filter`` restricts to the given relationship types at
            the query level (empty ⇒ all types).
          * server-side ``LIMIT $node_cap`` plus Python-side truncation
            guarantee at most ``GRAPH_WALK_NODE_CAP`` nodes /
            ``GRAPH_WALK_EDGE_CAP`` edges.

        Returns the same ``RoundGraphData`` shape as ``aretrieve`` so
        callers serialise identically. Empty on any failure / missing
        store (best-effort — never raises through the tool boundary).
        """
        if self._graph_store is None:
            return RoundGraphData()

        safe_hops = max(1, min(int(hops), GRAPH_WALK_MAX_HOPS))
        params = {
            "name": start_entity,
            "hops": safe_hops,
            "rel_filter": list(rel_filter) if rel_filter else [],
            "node_cap": GRAPH_WALK_NODE_CAP,
            "edge_cap": GRAPH_WALK_EDGE_CAP,
        }
        try:
            rows = await asyncio.to_thread(
                self._graph_store.structured_query,
                _WALK_CYPHER.format(hops=safe_hops),
                params,
            )
        except Exception as exc:  # noqa: BLE001 — APOC missing / store down
            logger.warning(
                "graph_walk: APOC path failed, retrying without APOC: {e}",
                e=exc,
            )
            try:
                rows = await asyncio.to_thread(
                    self._graph_store.structured_query,
                    _WALK_CYPHER_NO_APOC.format(hops=safe_hops),
                    params,
                )
                return self._map_no_apoc_rows(rows)
            except Exception as exc2:  # noqa: BLE001
                logger.warning("graph_walk failed: {e}", e=exc2)
                return RoundGraphData()

        return self._map_walk_rows(rows)

    @staticmethod
    def _map_walk_rows(rows: list[dict] | None) -> RoundGraphData:
        """Map the APOC-path Cypher result into ``RoundGraphData``.

        Defensive caps re-applied here in case the store ignored the
        LIMIT (e.g. a non-Neo4j store).
        """
        out = RoundGraphData()
        for row in rows or []:
            for ent in (row.get("entities") or []):
                name = ent.get("name")
                if not name:
                    continue
                out.entities.append({
                    "entity_name": name,
                    "entity_type": ent.get("label") or "",
                    "description": ent.get("description") or "",
                })
            for rel in (row.get("relations") or []):
                out.relations.append({
                    "src_id": rel.get("src") or "",
                    "tgt_id": rel.get("tgt") or "",
                    "label": rel.get("label") or "",
                })
        out.entities = _dedupe_entities(out.entities)[:GRAPH_WALK_NODE_CAP]
        out.relations = _dedupe_relations(out.relations)[:GRAPH_WALK_EDGE_CAP]
        return out

    @staticmethod
    def _map_no_apoc_rows(rows: list[dict] | None) -> RoundGraphData:
        """Map the per-path (no-APOC) Cypher result into RoundGraphData."""
        out = RoundGraphData()
        for row in rows or []:
            start = row.get("start_name")
            if start:
                labels = list(row.get("start_labels") or [])
                out.entities.append({
                    "entity_name": start,
                    "entity_type": labels[0] if labels else "",
                    "description": row.get("start_description") or "",
                })
            m_name = row.get("m_name")
            if m_name:
                m_labels = list(row.get("m_labels") or [])
                out.entities.append({
                    "entity_name": m_name,
                    "entity_type": m_labels[0] if m_labels else "",
                    "description": row.get("m_description") or "",
                })
            for rel in (row.get("rels") or []):
                out.relations.append({
                    "src_id": rel.get("src") or "",
                    "tgt_id": rel.get("tgt") or "",
                    "label": rel.get("label") or "",
                })
        out.entities = _dedupe_entities(out.entities)[:GRAPH_WALK_NODE_CAP]
        out.relations = _dedupe_relations(out.relations)[:GRAPH_WALK_EDGE_CAP]
        return out


def _dedupe_entities(entities: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for e in entities:
        key = e["entity_name"]
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def _dedupe_relations(relations: list[dict]) -> list[dict]:
    seen: set[tuple] = set()
    out: list[dict] = []
    for r in relations:
        key = (r["src_id"], r["tgt_id"], r["label"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out
