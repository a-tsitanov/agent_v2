"""Cross-chunk consolidation for LightRAG-style extraction.

Inputs: a list of chunks (`BaseNode`) whose `KG_NODES_KEY` and
`KG_RELATIONS_KEY` metadata has been populated by
`LightRAGExtractor`.

Output: a deduplicated set of `EntityNode`s and `Relation`s where:

  * Entities are merged by normalised name.  Multiple per-chunk
    descriptions are either concatenated (small batches) or
    consolidated via a single LLM "summarize_entity_descriptions"
    call (large batches), matching LightRAG's algorithm exactly.
  * Relations are merged by the normalised, order-insensitive
    (source, target) pair.  Keywords from all occurrences are
    union'd, weight = max, description merged with the same logic
    as entities.
  * `source_chunks` / `mention_count` are tracked so retrievers can
    rank-by-frequency when LLM evidence ties.

The merger is deliberately storage-agnostic — it returns plain
LlamaIndex `EntityNode` / `Relation` objects ready for
`graph_store.upsert_nodes` / `upsert_relations`.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.graph_stores.types import (
    KG_NODES_KEY,
    KG_RELATIONS_KEY,
    EntityNode,
    Relation,
)
from llama_index.core.schema import BaseNode
from loguru import logger

from src.graph.lightrag_parse import (
    _cypher_safe_label,
    _first_keyword,
    _normalize_entity_name,
)
from src.graph.lightrag_prompts import (
    DEFAULT_FORCE_SUMMARY_ON_CHARS,
    DEFAULT_FORCE_SUMMARY_ON_COUNT,
    DEFAULT_SUMMARY_MAX_TOKENS,
    SUMMARIZE_ENTITY_DESCRIPTIONS,
)
from src.retrieval._common import strip_thinking


# ── intermediate aggregation shapes ─────────────────────────────────


@dataclass
class _EntityAgg:
    """Bucket of per-chunk extractions for one (normalised) entity name."""

    display_name: str           # first non-empty original-case name we saw
    descriptions: list[str] = field(default_factory=list)
    type_votes: Counter = field(default_factory=Counter)
    source_chunks: list[str] = field(default_factory=list)
    file_paths: set[str] = field(default_factory=set)
    raw_ids: list[str] = field(default_factory=list)  # original per-chunk EntityNode ids


@dataclass
class _RelationAgg:
    """Bucket of per-chunk extractions for one (src, tgt) pair."""

    display_src: str
    display_tgt: str
    descriptions: list[str] = field(default_factory=list)
    keywords: set[str] = field(default_factory=set)
    source_chunks: list[str] = field(default_factory=list)


def _id_to_name(nodes: list[BaseNode]) -> dict[str, str]:
    """Build a per-chunk lookup `EntityNode.id → normalised name`."""
    out: dict[str, str] = {}
    for n in nodes:
        for ent in n.metadata.get(KG_NODES_KEY) or []:
            if isinstance(ent, EntityNode):
                out[ent.id] = _normalize_entity_name(ent.name)
    return out


def _split_keywords(raw: str) -> list[str]:
    return [kw.strip() for kw in (raw or "").split(",") if kw.strip()]


# ── summary helper ─────────────────────────────────────────────────


async def _maybe_summarize_descriptions(
    *,
    llm: Any,
    description_name: str,
    description_type: str,    # "Entity" or "Relationship"
    descriptions: list[str],
    force_count: int,
    force_chars: int,
    summary_max_tokens: int,
    language: str,
) -> str:
    """Return one consolidated description for `descriptions`.

    Path A — concat: when there are few short descriptions, just
    join with `\\n---\\n`.  This costs zero LLM calls and preserves
    every source description verbatim.

    Path B — LLM summary: when descriptions cross the count or
    char threshold, fire the LightRAG `summarize_entity_descriptions`
    prompt with all descriptions.  Returns one cohesive paragraph.
    """
    unique = list(dict.fromkeys(d.strip() for d in descriptions if d and d.strip()))
    if not unique:
        return ""
    if len(unique) == 1:
        return unique[0]
    total_chars = sum(len(d) for d in unique)
    if len(unique) < force_count and total_chars < force_chars:
        return "\n---\n".join(unique)

    # LLM summary path.
    payload = "\n".join(
        json.dumps({"Description": d}, ensure_ascii=False) for d in unique
    )
    prompt = SUMMARIZE_ENTITY_DESCRIPTIONS.format(
        description_type=description_type,
        description_name=description_name,
        description_list=payload,
        summary_length=summary_max_tokens,
        language=language,
    )
    try:
        resp = await llm.achat([
            ChatMessage(role=MessageRole.USER, content=prompt),
        ])
        summary = strip_thinking(resp.message.content or "").strip()
        if summary:
            return summary
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "summary LLM call failed for {kind} {name}: {err}",
            kind=description_type, name=description_name, err=exc,
        )
    # Fallback: concat anyway so the entity still has a description.
    return "\n---\n".join(unique)


# ── main entry ──────────────────────────────────────────────────────


async def merge_kg_extraction(
    nodes: list[BaseNode],
    llm: Any,
    *,
    force_summary_on_count: int = DEFAULT_FORCE_SUMMARY_ON_COUNT,
    force_summary_on_chars: int = DEFAULT_FORCE_SUMMARY_ON_CHARS,
    summary_max_tokens: int = DEFAULT_SUMMARY_MAX_TOKENS,
    language: str = "Russian",
) -> tuple[list[EntityNode], list[Relation]]:
    """Merge per-chunk extraction into a deduplicated graph patch.

    Returns:
        `(entities, relations)` — plain LlamaIndex shapes ready for
        `graph_store.upsert_nodes(entities)` and
        `graph_store.upsert_relations(relations)`.

    Does NOT mutate input `nodes` — `PropertyGraphIndex` is expected
    to still consume their `KG_NODES_KEY` / `KG_RELATIONS_KEY`
    metadata for chunk↔entity `Chunk(:MENTIONS)→Entity` linkage.

    LLM-call cost (under defaults):
      * One summary call per entity whose mention count crosses
        `force_summary_on_count` (8) OR whose combined description
        length crosses `force_summary_on_chars` (12000 chars).
      * Same for relations.
    """

    # ── 1. Aggregate entities by normalised name ─────────────────────
    ent_agg: dict[str, _EntityAgg] = {}
    name_to_chunk_ids: dict[str, list[str]] = {}  # for orphan handling
    chunk_name_lookup = _id_to_name(nodes)         # raw_id → normalised name

    for n in nodes:
        chunk_id = n.node_id
        for ent in n.metadata.get(KG_NODES_KEY) or []:
            if not isinstance(ent, EntityNode):
                continue
            key = _normalize_entity_name(ent.name)
            if not key:
                continue
            agg = ent_agg.get(key)
            if agg is None:
                agg = _EntityAgg(display_name=ent.name)
                ent_agg[key] = agg
            desc = (ent.properties or {}).get("description", "")
            if desc:
                agg.descriptions.append(desc)
            agg.type_votes[ent.label or "Other"] += 1
            agg.source_chunks.append(chunk_id)
            fp = (ent.properties or {}).get("file_path") or ""
            if fp:
                agg.file_paths.add(fp)
            agg.raw_ids.append(ent.id)
            name_to_chunk_ids.setdefault(key, []).append(chunk_id)

    # ── 2. Aggregate relations by normalised (src, tgt) ──────────────
    rel_agg: dict[tuple[str, str], _RelationAgg] = {}
    for n in nodes:
        chunk_id = n.node_id
        for rel in n.metadata.get(KG_RELATIONS_KEY) or []:
            if not isinstance(rel, Relation):
                continue
            src_name = chunk_name_lookup.get(rel.source_id, "")
            tgt_name = chunk_name_lookup.get(rel.target_id, "")
            if not src_name or not tgt_name:
                # Endpoint not in this chunk's known entities — skip.
                # Orphan handling already happens in the extractor.
                continue
            key = tuple(sorted([src_name, tgt_name]))
            agg = rel_agg.get(key)
            if agg is None:
                display_src = ent_agg[src_name].display_name if src_name in ent_agg else src_name
                display_tgt = ent_agg[tgt_name].display_name if tgt_name in ent_agg else tgt_name
                agg = _RelationAgg(display_src=display_src, display_tgt=display_tgt)
                rel_agg[key] = agg
            desc = (rel.properties or {}).get("description", "")
            if desc:
                agg.descriptions.append(desc)
            agg.keywords.update(_split_keywords(
                (rel.properties or {}).get("keywords", "")
            ))
            agg.source_chunks.append(chunk_id)

    # ── 3. Materialise merged EntityNode list ────────────────────────
    merged_entities: list[EntityNode] = []
    name_to_merged_id: dict[str, str] = {}
    for key, agg in ent_agg.items():
        majority_type = (agg.type_votes.most_common(1)[0][0]
                         if agg.type_votes else "Other")
        merged_desc = await _maybe_summarize_descriptions(
            llm=llm,
            description_name=agg.display_name,
            description_type="Entity",
            descriptions=agg.descriptions,
            force_count=force_summary_on_count,
            force_chars=force_summary_on_chars,
            summary_max_tokens=summary_max_tokens,
            language=language,
        )
        ent = EntityNode(
            name=agg.display_name,
            label=majority_type,
            properties={
                "description": merged_desc,
                "source_chunks": list(dict.fromkeys(agg.source_chunks)),
                "file_paths": sorted(agg.file_paths),
                "mention_count": len(agg.source_chunks),
            },
        )
        merged_entities.append(ent)
        name_to_merged_id[key] = ent.id

    # ── 4. Materialise merged Relation list ──────────────────────────
    merged_relations: list[Relation] = []
    for (src_key, tgt_key), agg in rel_agg.items():
        src_id = name_to_merged_id.get(src_key)
        tgt_id = name_to_merged_id.get(tgt_key)
        if not src_id or not tgt_id:
            continue
        merged_desc = await _maybe_summarize_descriptions(
            llm=llm,
            description_name=f"{agg.display_src} ↔ {agg.display_tgt}",
            description_type="Relationship",
            descriptions=agg.descriptions,
            force_count=force_summary_on_count,
            force_chars=force_summary_on_chars,
            summary_max_tokens=summary_max_tokens,
            language=language,
        )
        primary_keyword = sorted(agg.keywords)[0] if agg.keywords else ""
        label = _cypher_safe_label(primary_keyword)
        tags = sorted(agg.keywords)
        distinct_chunks = list(dict.fromkeys(agg.source_chunks))
        mention_count = len(distinct_chunks)
        merged_relations.append(Relation(
            label=label,
            source_id=src_id,
            target_id=tgt_id,
            properties={
                "description": merged_desc,
                "keywords": ",".join(tags),
                # Tie strength = distinct co-occurrence count, so
                # weighted Leiden / ranking see meaningful weights
                # (ParsedRelation.weight is a constant 1.0 today).
                "weight": float(mention_count),
                # Discrete, per-element-filterable tags (vs the joined
                # `keywords` string) for graph analysis / edge filtering.
                "tags": tags,
                "source_chunks": distinct_chunks,
                "mention_count": mention_count,
            },
        ))

    logger.info(
        "kg merge done  entities={e}  relations={r}  summary_calls=N/A",
        e=len(merged_entities), r=len(merged_relations),
    )
    return merged_entities, merged_relations


__all__ = ["merge_kg_extraction"]
