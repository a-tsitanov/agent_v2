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

import asyncio
from collections import defaultdict

from llama_index.core.graph_stores.types import KG_NODES_KEY, Relation
from loguru import logger
from temporalio import activity

from src.config import settings
from src.graph.entity_resolution import ERConfig, resolve_entities
from src.graph.event_merge import dedup_cross_channel_events, merge_events
from src.graph.merge import merge_kg_extraction
from src.graph.phone_consolidation import consolidate_phone_entities
from src.graph.store import build_graph_store
from src.ingestion.embeddings import build_embedding_model
from src.retrieval.llm_pool import get_llm_pool
from src.workflow.contracts import DuplicateGroup, KGExtracted, Merged
from src.workflow.heartbeat import heartbeat_every
from src.workflow.staging import build_staging_store

_HEARTBEAT_SAMPLE_CAP = 20
# Pulse interval while merge_kg_extraction runs (per-entity judge calls,
# no internal heartbeat).  Must stay well under the activity's
# heartbeat_timeout (15m in graph_build.py).
_HEARTBEAT_INTERVAL_S = 60.0


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


def _rewrite_endpoints(r: Relation, alias: dict[str, str]) -> Relation:
    """Repoint a relation's name-endpoints through ``alias`` (folded entity
    name -> surviving event name).  Returns the same object when untouched."""
    s = alias.get(r.source_id, r.source_id)
    t = alias.get(r.target_id, r.target_id)
    if s == r.source_id and t == r.target_id:
        return r
    return Relation(
        label=r.label, source_id=s, target_id=t, properties=dict(r.properties or {})
    )


@activity.defn
async def merge_and_resolve(kg: KGExtracted) -> Merged:
    activity.logger.info("merge_and_resolve start  doc=%s", kg.parsed.ctx.doc_id)
    activity.heartbeat({"stage": "init"})

    staging = build_staging_store()
    nodes = await asyncio.to_thread(staging.read_pickle, kg.nodes_with_kg_uri)
    activity.heartbeat({"stage": "loaded", "chunks": len(nodes)})

    raw_entity_count, dup_groups = _premerge_groups(nodes)
    activity.heartbeat(
        {
            "stage": "premerge",
            "raw_entities": raw_entity_count,
            "duplicate_groups": dup_groups,
        }
    )

    llm = get_llm_pool().get("judge")

    activity.logger.info(
        "merge_and_resolve merging  chunks=%d  raw_entities=%d  duplicate_groups=%d",
        len(nodes),
        raw_entity_count,
        len(dup_groups),
    )
    # merge_kg_extraction fans out per-entity/relation judge calls with no
    # internal heartbeat; pulse on a timer so a saturated proxy can't trip
    # the 15-min heartbeat_timeout mid-merge (-> cancel -> retry storm).
    async with heartbeat_every(_HEARTBEAT_INTERVAL_S, {"stage": "merging"}):
        merged_entities, merged_relations = await merge_kg_extraction(
            nodes,
            llm,
            language="Russian",
        )
    activity.heartbeat(
        {
            "stage": "merged",
            "raw_entities": raw_entity_count,
            "merged_entities": len(merged_entities),
            "collapsed": max(raw_entity_count - len(merged_entities), 0),
            "relations": len(merged_relations),
        }
    )

    pre_phone_count = len(merged_entities)
    # CPU-bound (libphonenumber parse over every entity) — off the loop.
    merged_entities, merged_relations, _phone_map = await asyncio.to_thread(
        consolidate_phone_entities,
        merged_entities,
        merged_relations,
        nodes,
    )
    activity.heartbeat(
        {
            "stage": "phone_consolidated",
            "entities_in": pre_phone_count,
            "entities_out": len(merged_entities),
            "phones_collapsed": len(_phone_map),
            "phone_alias_map": _sample_map(_phone_map),
        }
    )

    # ── Event de-duplication (gated, dark by default) ────────────────────
    # Split EventOrAction nodes out BEFORE ER so ER never touches them;
    # their dedup is handled deterministically by merge_events.  When the
    # feature is off the lists are passed through unchanged.
    #
    # The split is gated on the event-PIPELINE signature (label +
    # `trigger` property), not label alone: entity-extractor nodes can
    # also carry label="EventOrAction" but have no pipeline props (no
    # `trigger` -- only `events_to_graph` in src/graph/event_extract.py
    # writes it).  Routing those into merge_events stamps a spurious
    # `event_type='event'` default and mass-merges them under the shared
    # untimed dedup key.  Non-pipeline EventOrAction nodes must flow
    # through the regular entity path (ER etc.) untouched, exactly like
    # any other entity.
    _held_ev_nodes: list = []
    _held_ev_rels: list = []
    if settings.events.extraction_enabled:
        def _is_pipeline_event(e) -> bool:
            return e.label == "EventOrAction" and "trigger" in (e.properties or {})

        _event_names = {e.name for e in merged_entities if _is_pipeline_event(e)}
        _ev_in = [e for e in merged_entities if _is_pipeline_event(e)]
        _non_ev_entities = [e for e in merged_entities if not _is_pipeline_event(e)]
        _ev_rels = [
            r
            for r in merged_relations
            if r.source_id in _event_names or r.target_id in _event_names
        ]
        _non_ev_rels = [
            r
            for r in merged_relations
            if r.source_id not in _event_names and r.target_id not in _event_names
        ]
        _held_ev_nodes, _held_ev_rels = merge_events(_ev_in, _ev_rels)
        merged_entities = _non_ev_entities
        merged_relations = _non_ev_rels
        activity.heartbeat(
            {
                "stage": "event_deduped",
                "events_in": len(_ev_in),
                "events_out": len(_held_ev_nodes),
            }
        )

        # ── Cross-channel dedup (п.4, gated, dark by default) ────────────
        # Every event-heavy chunk is double-extracted: the ENTITY channel emits
        # a nominal EventOrAction and the EVENT channel a verbal one, for the
        # SAME happening, under different name-VIDs → two nodes. Fold the
        # entity-channel node into the same-chunk event node it paraphrases
        # (embedding cosine ≥ threshold); below threshold it is kept so an
        # event the event channel missed still survives (recall over aggression).
        if settings.events.cross_channel_dedup_enabled and _held_ev_nodes:
            _ent_events = [e for e in merged_entities if e.label == "EventOrAction"]
            if _ent_events:
                _embed = build_embedding_model()
                _names = list(
                    {e.name for e in _ent_events} | {e.name for e in _held_ev_nodes}
                )
                _batch = getattr(_embed, "aget_text_embedding_batch", None)
                if _batch is not None:
                    _vecs = await _batch(_names)
                else:
                    _vecs = await asyncio.gather(
                        *[_embed.aget_text_embedding(n) for n in _names]
                    )
                _emb = dict(zip(_names, _vecs, strict=False))
                _kept, _alias = dedup_cross_channel_events(
                    _ent_events,
                    _held_ev_nodes,
                    _emb,
                    threshold=settings.events.cross_channel_dedup_threshold,
                )
                if _alias:
                    _ev_by_name = {e.name: e for e in _held_ev_nodes}
                    for _src_name, _dst_name in _alias.items():
                        _src = next(
                            (e for e in _ent_events if e.name == _src_name), None
                        )
                        _dst = _ev_by_name.get(_dst_name)
                        # carry the (often fuller) entity-channel description onto
                        # the surviving event node when it has none of its own.
                        if _src and _dst and not (_dst.properties or {}).get("description"):
                            _dst.properties = {
                                **(_dst.properties or {}),
                                "description": (_src.properties or {}).get("description", ""),
                            }
                    _dropped = set(_alias)
                    merged_entities = [
                        e for e in merged_entities if e.name not in _dropped
                    ]
                    merged_relations = [
                        _rewrite_endpoints(r, _alias) for r in merged_relations
                    ]
                    _held_ev_rels = [
                        _rewrite_endpoints(r, _alias) for r in _held_ev_rels
                    ]
                logger.info(
                    "merge_and_resolve cross-channel dedup  entity_events={e}  "
                    "events={v}  folded={f}",
                    e=len(_ent_events), v=len(_held_ev_nodes), f=len(_alias),
                )
                activity.heartbeat(
                    {"stage": "cross_channel_deduped", "folded": len(_alias)}
                )

    er_map: dict[str, str] = {}
    if settings.agent.er_enabled:
        activity.logger.info("merge_and_resolve resolving entities (ER)")
        embed_model = build_embedding_model()
        graph_store = build_graph_store()
        from src.graph.entity_vector_store import (
            Neo4jEntityVectorStore,
            build_entity_vector_store,
        )
        # neo4j-native keeps the exact `_load_candidates_native` path (prod ER
        # byte-for-byte unchanged, no upsert). Only a Milvus-backed store
        # (GRAPH_BACKEND=nebula or opt-in AGENT_ER_VECTOR_BACKEND=milvus) routes
        # ER candidate-kNN + canonical upsert through the EntityVectorStore seam.
        _evs = build_entity_vector_store(graph_store)
        vector_store = None if isinstance(_evs, Neo4jEntityVectorStore) else _evs
        pre_er_count = len(merged_entities)
        # ER on a dense-entity doc issues thousands of LLM judge-pairs and can
        # run far past the 15-min heartbeat_timeout; resolve_entities emits no
        # heartbeat of its own, so pulse throughout — otherwise Temporal cancels
        # + retries it every 15 min, re-running the same explosion forever
        # (the 0b938ba5 wedge: attempt 9/50, 2385 judge-pairs, ~4.5h stuck).
        async with heartbeat_every(_HEARTBEAT_INTERVAL_S, {"stage": "resolving"}):
            merged_entities, merged_relations, er_map = await resolve_entities(
                merged_entities,
                merged_relations,
                nodes,
                llm=llm,
                embed_model=embed_model,
                graph_store=graph_store,
                config=ERConfig(
                    language="Russian",
                    judge_batch=settings.agent.er_judge_batch_size,
                    name_token_min_overlap=0.1,
                    verdict_cache_enabled=settings.agent.er_verdict_cache_enabled,
                    use_native_vector_knn=settings.agent.er_use_native_vector_knn,
                    vector_knn_k=settings.agent.er_vector_knn_k,
                ),
                # Reuse the Neo4j handle as the persistent verdict-cache
                # store (it exposes `structured_query`).  Cache stays
                # inactive when `er_verdict_cache_enabled` is off.
                er_store=graph_store,
                vector_store=vector_store,
            )
        activity.heartbeat(
            {
                "stage": "resolved",
                "entities_in": pre_er_count,
                "entities_out": len(merged_entities),
                "er_merged": len(er_map),
                "er_alias_map": _sample_map(er_map),
            }
        )
    else:
        activity.heartbeat({"stage": "er_skipped"})

    # Re-join event nodes/rels held aside during ER.
    if _held_ev_nodes or _held_ev_rels:
        merged_entities = merged_entities + _held_ev_nodes
        merged_relations = merged_relations + _held_ev_rels

    uri = await asyncio.to_thread(
        staging.write_pickle,
        kg.parsed.ctx.workflow_run_id,
        "merged",
        (merged_entities, merged_relations, nodes),
    )
    logger.info(
        "merge_and_resolve done  doc={d}  raw={raw}  merged={e}  relations={r}",
        d=kg.parsed.ctx.doc_id,
        raw=raw_entity_count,
        e=len(merged_entities),
        r=len(merged_relations),
    )
    # Task 8b: surface entity/relation names on the contract so the wiki
    # dirty-mark hook can read them without touching staging (sandbox-safe).
    _entity_names = [e.name for e in merged_entities]
    _relation_endpoints = list(
        dict.fromkeys(
            [r.source_id for r in merged_relations] + [r.target_id for r in merged_relations]
        )
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
        entity_names=_entity_names,
        relation_endpoints=_relation_endpoints,
    )
