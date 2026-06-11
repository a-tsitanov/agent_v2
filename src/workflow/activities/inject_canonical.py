"""`inject_canonical` — write canonical identifier entities to Neo4j."""

from __future__ import annotations

import asyncio

from loguru import logger
from temporalio import activity

from src.graph.store import build_neo4j_graph_store
from src.ingestion.identifier_transform import inject_canonical_entities
from src.workflow.contracts import Injected, Parsed
from src.workflow.staging import build_staging_store


@activity.defn
async def inject_canonical(parsed: Parsed) -> Injected:
    activity.logger.info("inject_canonical start  doc=%s", parsed.ctx.doc_id)
    activity.heartbeat({"stage": "init"})

    staging = build_staging_store()
    nodes = await asyncio.to_thread(staging.read_pickle, parsed.nodes_uri)
    activity.heartbeat({"stage": "loaded", "chunks": len(nodes)})

    graph_store = build_neo4j_graph_store()
    # Neo4j upsert is sync (blocking driver) — off the loop.
    await asyncio.to_thread(inject_canonical_entities, graph_store, nodes)
    activity.heartbeat({"stage": "injected", "chunks": len(nodes)})

    logger.info(
        "inject_canonical done  doc={d}  chunks={n}",
        d=parsed.ctx.doc_id, n=len(nodes),
    )
    return Injected(count=len(nodes))
