"""`parse_and_chunk` — read + split + identifier-canon + translate.

Mirrors the first half of `src.ingestion.tasks.process_document`'s
pipeline section.  Output is the list of LlamaIndex `BaseNode`
objects, pickled to MinIO under `{run_id}/parsed.pkl`.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger
from temporalio import activity

from src.ingestion.embeddings import build_embedding_model
from src.ingestion.pipeline import build_ingestion_pipeline, read_documents
from src.ingestion.translate_transform import (
    FULL_TRANSLATED_TEXT_KEY,
    ORIGINAL_DOC_LENGTH_KEY,
)
from src.retrieval.llm import build_llm
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
    llm = build_llm()
    embed_model = build_embedding_model()
    pipeline = build_ingestion_pipeline(
        embed_model=embed_model,
        translator_llm=llm,
    )

    docs = read_documents(target.parent, recursive=False)
    docs = [d for d in docs if d.metadata.get("file_path") == str(target)]
    if not docs:
        raise FileNotFoundError(f"file not in reader output: {target}")

    nodes = await pipeline.arun(documents=docs)

    # Scrub doc-translation scaffolding so it never reaches downstream
    # stores.  Same logic as the legacy taskiq task.
    for n in nodes:
        _scrub(getattr(n, "metadata", None))
        for rel in (getattr(n, "relationships", {}) or {}).values():
            _scrub(getattr(rel, "metadata", None))

    activity.heartbeat({"chunks": len(nodes)})

    staging = build_staging_store()
    uri = staging.write_pickle(ctx.workflow_run_id, "parsed", nodes)
    logger.info(
        "parse_and_chunk done  doc={d}  chunks={n}  uri={u}",
        d=ctx.doc_id, n=len(nodes), u=uri,
    )
    return Parsed(ctx=ctx, nodes_uri=uri, chunk_count=len(nodes))
