"""`extract_kg` — LightRAG-style KG extraction (heaviest stage).

One LLM call per chunk produces KG_NODES_KEY / KG_RELATIONS_KEY
metadata on each node.  Output blob is pickled separately from the
parsed blob so a retry of merge_and_resolve can re-read it without
rerunning the extractor.
"""

from __future__ import annotations

from loguru import logger
from temporalio import activity

from src.graph.index import build_kg_extractor
from src.retrieval.llm import build_llm
from src.workflow.contracts import KGExtracted, Parsed
from src.workflow.staging import build_staging_store


@activity.defn
async def extract_kg(parsed: Parsed) -> KGExtracted:
    activity.logger.info(
        "extract_kg start  doc=%s  chunks=%d",
        parsed.ctx.doc_id, parsed.chunk_count,
    )
    activity.heartbeat({"stage": "init", "chunks": parsed.chunk_count})

    staging = build_staging_store()
    nodes = staging.read_pickle(parsed.nodes_uri)
    activity.heartbeat({"stage": "loaded", "chunks": len(nodes)})

    llm = build_llm()
    extractor = build_kg_extractor(llm, mode="lightrag")
    activity.logger.info("extract_kg invoking LLM extractor  chunks=%d", len(nodes))
    activity.heartbeat({"stage": "extracting", "chunks": len(nodes)})

    nodes = await extractor.acall(nodes)
    activity.heartbeat({"stage": "extracted", "chunks": len(nodes)})

    uri = staging.write_pickle(parsed.ctx.workflow_run_id, "kg", nodes)
    activity.heartbeat({"stage": "staged", "uri": uri})
    logger.info(
        "extract_kg done  doc={d}  chunks={n}  uri={u}",
        d=parsed.ctx.doc_id, n=len(nodes), u=uri,
    )
    return KGExtracted(parsed=parsed, nodes_with_kg_uri=uri)
