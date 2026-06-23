"""`inject_canonical` — write canonical identifier entities to Neo4j."""

from __future__ import annotations

import asyncio

from loguru import logger
from temporalio import activity

from src.graph.store import build_neo4j_graph_store
from src.graph.write_retry import write_with_retry
from src.ingestion.identifier_transform import inject_canonical_entities
from src.workflow.contracts import Injected, Parsed
from src.workflow.heartbeat import heartbeat_every
from src.workflow.staging import build_staging_store

# Pulse interval for the blocking Neo4j upsert. Must stay well under the
# activity's heartbeat_timeout (2m in document_ingest.py).
_HEARTBEAT_INTERVAL_S = 60.0


@activity.defn
async def inject_canonical(parsed: Parsed) -> Injected:
    activity.logger.info("inject_canonical start  doc=%s", parsed.ctx.doc_id)
    activity.heartbeat({"stage": "init"})

    staging = build_staging_store()
    nodes = await asyncio.to_thread(staging.read_pickle, parsed.nodes_uri)
    activity.heartbeat({"stage": "loaded", "chunks": len(nodes)})

    # Pulse on a timer throughout: hub-node lock contention can make the
    # write slow, and without an internal heartbeat a stuck connection
    # would outrun the heartbeat_timeout and tie up the admission slot.
    # BOTH the store construction AND the upsert run off the event loop
    # via to_thread: build_neo4j_graph_store() does blocking driver I/O
    # (schema refresh + index DDL) and takes a process-global lock — doing
    # that ON the loop froze the heartbeater under contention, so the
    # pulse stopped and the activity hit timeout_type_heartbeat.
    async with heartbeat_every(
        _HEARTBEAT_INTERVAL_S, {"stage": "injecting", "chunks": len(nodes)}
    ):
        graph_store = await asyncio.to_thread(build_neo4j_graph_store)
        # Retry transient hub-node lock/deadlock errors (see write_retry).
        await asyncio.to_thread(
            write_with_retry, inject_canonical_entities, graph_store, nodes
        )
    activity.heartbeat({"stage": "injected", "chunks": len(nodes)})

    logger.info(
        "inject_canonical done  doc={d}  chunks={n}",
        d=parsed.ctx.doc_id, n=len(nodes),
    )
    return Injected(count=len(nodes))
