"""`extract_kg` — LightRAG-style KG extraction (heaviest stage).

One LLM call per chunk produces KG_NODES_KEY / KG_RELATIONS_KEY
metadata on each node.  Output blob is pickled separately from the
parsed blob so a retry of merge_and_resolve can re-read it without
rerunning the extractor.

Surfaces what the extractor produced to Temporal UI:
  * entity + relation totals
  * top-10 labels by frequency
  * 20 sample entities (name + label)
  * 20 sample relations (source / target / label)
"""

from __future__ import annotations

import asyncio
from collections import Counter

from llama_index.core.graph_stores.types import KG_NODES_KEY, KG_RELATIONS_KEY
from loguru import logger
from temporalio import activity

from src.graph.index import build_kg_extractor
from src.retrieval.llm_pool import get_llm_pool
from src.workflow.contracts import (
    EntitySample,
    KGExtracted,
    Parsed,
    RelationSample,
)
from src.workflow.heartbeat import heartbeat_every
from src.workflow.staging import build_staging_store

_HEARTBEAT_SAMPLE_CAP = 20
_HEARTBEAT_LABEL_TOP = 10
# Pulse interval while extractor.acall runs.  Must stay well under the
# activity's heartbeat_timeout (15m in document_ingest.py) so a slow or
# saturated LLM proxy can't trip a mid-extraction timeout.
_HEARTBEAT_INTERVAL_S = 60.0


def _summarise_kg(nodes) -> dict:
    """Pull out small JSON-serialisable samples of what the extractor
    emitted.  These are returned as part of `KGExtracted` so they are
    visible on the `ActivityTaskCompleted` event in Temporal UI
    after the activity finishes (heartbeat details only survive
    while the activity is still running)."""
    entities = []
    relations = []
    for n in nodes:
        md = getattr(n, "metadata", None)
        if not isinstance(md, dict):
            continue
        entities.extend(md.get(KG_NODES_KEY, []) or [])
        relations.extend(md.get(KG_RELATIONS_KEY, []) or [])

    label_counts = Counter(str(getattr(e, "label", "") or "") for e in entities)
    rel_label_counts = Counter(str(getattr(r, "label", "") or "") for r in relations)

    return {
        "entity_count": len(entities),
        "relation_count": len(relations),
        "entity_labels_top": dict(label_counts.most_common(_HEARTBEAT_LABEL_TOP)),
        "relation_labels_top": dict(
            rel_label_counts.most_common(_HEARTBEAT_LABEL_TOP),
        ),
        "sample_entities": [
            EntitySample(
                name=str(e.name)[:120],
                label=str(getattr(e, "label", "") or "")[:60],
            )
            for e in entities[:_HEARTBEAT_SAMPLE_CAP]
        ],
        "sample_relations": [
            RelationSample(
                source=str(getattr(r, "source_id", ""))[:120],
                target=str(getattr(r, "target_id", ""))[:120],
                label=str(getattr(r, "label", "") or "")[:60],
            )
            for r in relations[:_HEARTBEAT_SAMPLE_CAP]
        ],
    }


@activity.defn
async def extract_kg(parsed: Parsed) -> KGExtracted:
    activity.logger.info(
        "extract_kg start  doc=%s  chunks=%d",
        parsed.ctx.doc_id, parsed.chunk_count,
    )
    activity.heartbeat({"stage": "init", "chunks": parsed.chunk_count})

    staging = build_staging_store()
    nodes = await asyncio.to_thread(staging.read_pickle, parsed.nodes_uri)
    activity.heartbeat({"stage": "loaded", "chunks": len(nodes)})

    llm = get_llm_pool().get("extraction")
    extractor = build_kg_extractor(llm, mode="lightrag")
    activity.logger.info("extract_kg invoking LLM extractor  chunks=%d", len(nodes))
    activity.heartbeat({"stage": "extracting", "chunks": len(nodes)})

    # extractor.acall fans out one LLM call per chunk with no internal
    # heartbeat — under a saturated proxy the whole call can outrun the
    # 15-min heartbeat_timeout, getting the activity cancelled + retried
    # (a retry storm that amplifies the very saturation that caused it).
    # Pulse on a timer so progressing-but-slow extraction stays alive.
    async with heartbeat_every(
        _HEARTBEAT_INTERVAL_S, {"stage": "extracting", "chunks": len(nodes)}
    ):
        nodes = await extractor.acall(nodes)

    summary = _summarise_kg(nodes)
    activity.logger.info(
        "extract_kg extracted  entities=%d  relations=%d  top_labels=%s",
        summary["entity_count"], summary["relation_count"],
        summary["entity_labels_top"],
    )
    # Heartbeat carries the samples while the activity is running so
    # they show up in the UI's Pending Activities panel.  The same data
    # is included in the return value below so it persists after the
    # activity completes.
    activity.heartbeat({
        "stage": "extracted",
        "entity_count": summary["entity_count"],
        "relation_count": summary["relation_count"],
        "entity_labels_top": summary["entity_labels_top"],
        "relation_labels_top": summary["relation_labels_top"],
    })

    uri = await asyncio.to_thread(
        staging.write_pickle, parsed.ctx.workflow_run_id, "kg", nodes,
    )
    logger.info(
        "extract_kg done  doc={d}  chunks={n}  entities={e}  relations={r}  "
        "uri={u}",
        d=parsed.ctx.doc_id, n=len(nodes),
        e=summary["entity_count"], r=summary["relation_count"], u=uri,
    )
    return KGExtracted(
        parsed=parsed,
        nodes_with_kg_uri=uri,
        **summary,
    )
