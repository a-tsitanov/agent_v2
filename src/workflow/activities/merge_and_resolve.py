"""`merge_and_resolve` — cross-chunk dedup + phone consolidation + ER.

Pulls the post-KG nodes from staging, runs the three dedup passes
(LightRAG merge -> phone consolidation -> entity resolution), then
writes a tuple ``(entities, relations, nodes)`` to a fresh staging
blob.  Nodes are written too because phone/ER passes can rewrite
chunk-level `KG_NODES_KEY` metadata in-place.

For each dedup pass we surface "what was compared" to Temporal UI
via heartbeats:
  * `merged` — pre-merge entity total + top groups whose names
    appeared multiple times across chunks (these are exactly the
    candidates the merger collapsed).
  * `phone_consolidated` — alias → E.164 canonical map sample.
  * `resolved` — ER alias → survivor map sample (cross-language /
    multi-form dedup).
"""

from __future__ import annotations

from collections import defaultdict

from llama_index.core.graph_stores.types import KG_NODES_KEY
from loguru import logger
from temporalio import activity

from src.config import settings
from src.graph.entity_resolution import ERConfig, resolve_entities
from src.graph.merge import merge_kg_extraction
from src.graph.phone_consolidation import consolidate_phone_entities
from src.graph.store import build_neo4j_graph_store
from src.ingestion.embeddings import build_embedding_model
from src.retrieval.llm import build_judge_llm
from src.workflow.contracts import DuplicateGroup, KGExtracted, Merged
from src.workflow.staging import build_staging_store

_HEARTBEAT_SAMPLE_CAP = 20


def _premerge_groups(nodes) -> tuple[int, list[DuplicateGroup]]:
    """Group raw extracted entities by name across all chunks.

    Names that appear more than once are exactly what `merge_kg_extraction`
    will collapse into a single entity.  Returns:
      * total raw entity count (sum across all chunks)
      * up to `_HEARTBEAT_SAMPLE_CAP` groups with >1 occurrences,
        sorted by frequency desc.  Each group: name, count, labels seen.
    """
    by_name: dict[str, list[str]] = defaultdict(list)
    total = 0
    for n in nodes:
        md = getattr(n, "metadata", None)
        if not isinstance(md, dict):
            continue
        for ent in md.get(KG_NODES_KEY, []) or []:
            total += 1
            by_name[str(getattr(ent, "name", ""))].append(
                str(getattr(ent, "label", "") or ""),
            )
    dup_groups = [
        DuplicateGroup(
            name=name[:120],
            count=len(labels),
            labels=sorted({lab for lab in labels if lab})[:4],
        )
        for name, labels in by_name.items()
        if len(labels) > 1
    ]
    dup_groups.sort(key=lambda g: -g.count)
    return total, dup_groups[:_HEARTBEAT_SAMPLE_CAP]


def _sample_map(m: dict[str, str]) -> dict[str, str]:
    """First N entries of an alias->canonical map, truncated for
    heartbeat sanity."""
    out: dict[str, str] = {}
    for old, new in list(m.items())[:_HEARTBEAT_SAMPLE_CAP]:
        out[str(old)[:120]] = str(new)[:120]
    return out


@activity.defn
async def merge_and_resolve(kg: KGExtracted) -> Merged:
    activity.logger.info("merge_and_resolve start  doc=%s", kg.parsed.ctx.doc_id)
    activity.heartbeat({"stage": "init"})

    staging = build_staging_store()
    nodes = staging.read_pickle(kg.nodes_with_kg_uri)
    activity.heartbeat({"stage": "loaded", "chunks": len(nodes)})

    raw_entity_count, dup_groups = _premerge_groups(nodes)
    activity.heartbeat({
        "stage": "premerge",
        "raw_entities": raw_entity_count,
        "duplicate_groups": dup_groups,
    })

    llm = build_judge_llm()

    activity.logger.info(
        "merge_and_resolve merging  chunks=%d  raw_entities=%d  duplicate_groups=%d",
        len(nodes), raw_entity_count, len(dup_groups),
    )
    merged_entities, merged_relations = await merge_kg_extraction(
        nodes, llm, language="Russian",
    )
    activity.heartbeat({
        "stage": "merged",
        "raw_entities": raw_entity_count,
        "merged_entities": len(merged_entities),
        "collapsed": max(raw_entity_count - len(merged_entities), 0),
        "relations": len(merged_relations),
    })

    pre_phone_count = len(merged_entities)
    merged_entities, merged_relations, _phone_map = consolidate_phone_entities(
        merged_entities, merged_relations, nodes,
    )
    activity.heartbeat({
        "stage": "phone_consolidated",
        "entities_in": pre_phone_count,
        "entities_out": len(merged_entities),
        "phones_collapsed": len(_phone_map),
        "phone_alias_map": _sample_map(_phone_map),
    })

    er_map: dict[str, str] = {}
    if settings.agent.er_enabled:
        activity.logger.info("merge_and_resolve resolving entities (ER)")
        embed_model = build_embedding_model()
        graph_store = build_neo4j_graph_store()
        pre_er_count = len(merged_entities)
        merged_entities, merged_relations, er_map = await resolve_entities(
            merged_entities, merged_relations, nodes,
            llm=llm, embed_model=embed_model, graph_store=graph_store,
            config=ERConfig(
                language="Russian",
                judge_batch=settings.agent.er_judge_batch_size,
                name_token_min_overlap=0.1,
            ),
        )
        activity.heartbeat({
            "stage": "resolved",
            "entities_in": pre_er_count,
            "entities_out": len(merged_entities),
            "er_merged": len(er_map),
            "er_alias_map": _sample_map(er_map),
        })
    else:
        activity.heartbeat({"stage": "er_skipped"})

    uri = staging.write_pickle(
        kg.parsed.ctx.workflow_run_id, "merged",
        (merged_entities, merged_relations, nodes),
    )
    logger.info(
        "merge_and_resolve done  doc={d}  raw={raw}  merged={e}  relations={r}",
        d=kg.parsed.ctx.doc_id,
        raw=raw_entity_count,
        e=len(merged_entities), r=len(merged_relations),
    )
    return Merged(
        kg=kg,
        merged_entities_uri=uri,
        raw_entity_count=raw_entity_count,
        merged_entity_count=len(merged_entities),
        relation_count=len(merged_relations),
        duplicate_groups=dup_groups,
        phones_collapsed=len(_phone_map),
        phone_alias_map=_sample_map(_phone_map),
        er_merged=len(er_map),
        er_alias_map=_sample_map(er_map),
    )
