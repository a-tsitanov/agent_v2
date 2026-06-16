"""`parse_and_chunk` — read + split + identifier-canon + translate.

Runs the LlamaIndex ingestion pipeline (reader + splitter + identifier
canonicalisation + translation) over the downloaded document.  Output
is the list of LlamaIndex `BaseNode` objects, pickled to MinIO under
`{run_id}/parsed.pkl`.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from loguru import logger
from temporalio import activity

from src.ingestion.embeddings import build_embedding_model
from src.ingestion.pipeline import build_ingestion_pipeline, read_documents
from src.ingestion.translate_transform import (
    FULL_TRANSLATED_TEXT_KEY,
    ORIGINAL_DOC_LENGTH_KEY,
)
from src.retrieval.llm_pool import get_llm_pool
from src.workflow.contracts import Ctx, Parsed
from src.workflow.staging import build_staging_store


def _scrub(md: dict | None) -> None:
    if not md:
        return
    md.pop(FULL_TRANSLATED_TEXT_KEY, None)
    md.pop(ORIGINAL_DOC_LENGTH_KEY, None)


@activity.defn
async def parse_and_chunk(ctx: Ctx) -> Parsed:
    target = Path(ctx.local_path)
    activity.logger.info("parse_and_chunk start  target=%s", target)
    activity.heartbeat({"stage": "init", "target": str(target)})

    llm = get_llm_pool().get("extraction")
    embed_model = build_embedding_model()
    pipeline = build_ingestion_pipeline(
        embed_model=embed_model,
        translator_llm=llm,
    )

    docs = await asyncio.to_thread(read_documents, target.parent, recursive=False)
    docs = [d for d in docs if d.metadata.get("file_path") == str(target)]
    if not docs:
        raise FileNotFoundError(f"file not in reader output: {target}")
    activity.logger.info("parse_and_chunk read  docs=%d", len(docs))
    activity.heartbeat({"stage": "read", "docs": len(docs)})

    # Force the source Document id to the *application* doc_id (Postgres
    # documents.id / ingest job id).  Every chunk inherits this as its
    # `ref_doc_id`, and MilvusVectorStore writes ref_doc_id into the
    # scalar `doc_id` column — so `get_chunks_by_doc_id(app_doc_id)`
    # actually matches.  Without this the scalar holds LlamaIndex's
    # auto-generated Document uuid and the lookup returns 0 rows.
    # (KG provenance keys on per-chunk node_id / entity ids, not the
    # parent Document id_, so this is safe.)
    for d in docs:
        d.id_ = ctx.doc_id

    nodes = await pipeline.arun(documents=docs)
    activity.logger.info("parse_and_chunk pipeline  chunks=%d", len(nodes))
    activity.heartbeat({"stage": "pipeline", "chunks": len(nodes)})

    # Stamp source-order position + the app doc_id on each chunk so the
    # chunk store can return them ordered and queryable by doc_id.  The
    # splitter emits nodes in document order; enumerate preserves it.
    for i, n in enumerate(nodes):
        md = getattr(n, "metadata", None)
        if md is None:
            md = n.metadata = {}
        md["position"] = i
        md["doc_id"] = ctx.doc_id

    # Scrub doc-translation scaffolding so it never reaches downstream
    # stores.
    for n in nodes:
        _scrub(getattr(n, "metadata", None))
        for rel in (getattr(n, "relationships", {}) or {}).values():
            _scrub(getattr(rel, "metadata", None))

    staging = build_staging_store()
    uri = await asyncio.to_thread(
        staging.write_pickle, ctx.workflow_run_id, "parsed", nodes,
    )
    activity.heartbeat({"stage": "staged", "chunks": len(nodes), "uri": uri})
    logger.info(
        "parse_and_chunk done  doc={d}  chunks={n}  uri={u}",
        d=ctx.doc_id, n=len(nodes), u=uri,
    )
    return Parsed(ctx=ctx, nodes_uri=uri, chunk_count=len(nodes))
