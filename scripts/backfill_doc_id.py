"""Backfill the `doc_id` metadata field on legacy Milvus chunks.

Chunks indexed before `index_vector` started tagging every node with its
`doc_id` have no `doc_id` in Milvus, so the agent tool
`get_chunks_by_doc_id` / `read_full_document` can't fetch them.  This
one-shot job resolves each legacy chunk's `doc_id` from its stored
`file_path` (via the Postgres `documents` table) and re-inserts the
chunk through the normal LlamaIndex write path so the row stays
schema-consistent (upsert by node id — no duplicates).

It reconstructs each row's text + embedding from the existing Milvus
fields, so NO re-embedding and NO re-parsing happen — the chunk body and
vector are preserved exactly; only `doc_id` is added.

Usage::

    python -m scripts.backfill_doc_id                # dry-run (counts only)
    python -m scripts.backfill_doc_id --no-dry-run   # apply
    python -m scripts.backfill_doc_id --no-dry-run --batch-size 2000

Apply only after a Milvus backup / on a verifiable dataset — validate the
counts in a dry-run first.  Idempotent: re-running only touches chunks
that still lack a `doc_id`.  Chunks whose `file_path` isn't registered in
Postgres are left untouched and reported as `unresolved`.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger
from pymilvus import MilvusClient

from src.config import settings
from src.retrieval.vector_index import build_vector_store
from src.storage.backfill import (
    BackfillStats,
    build_path_index,
    plan_doc_id_backfill,
    row_to_node,
)
from src.storage.postgres import AsyncPostgres
from src.utils.logging import configure_logging

# Milvus output fields: everything (metadata/_node_content/dynamic) plus
# the vector + text fields so reconstruction loses nothing on re-insert.
_OUTPUT_FIELDS = ["*"]


def _iter_rows(client: MilvusClient, collection: str, batch_size: int):
    """Yield Milvus rows in batches via query_iterator (offset-paging has
    a 16 384 window cap — the iterator streams past it)."""
    it = client.query_iterator(
        collection_name=collection,
        filter="",
        output_fields=_OUTPUT_FIELDS,
        batch_size=batch_size,
    )
    try:
        while True:
            page = it.next()
            if not page:
                break
            yield page
    finally:
        it.close()


async def _load_path_index() -> dict[str, str]:
    pg = AsyncPostgres()
    rows = await pg.list_id_path()
    return build_path_index(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--no-dry-run", dest="dry_run", action="store_false",
        help="actually re-insert chunks with doc_id (default: dry-run)",
    )
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.set_defaults(dry_run=True)
    args = parser.parse_args(argv)

    configure_logging()

    path_to_doc = asyncio.run(_load_path_index())
    logger.info("backfill doc_id: {n} registered documents", n=len(path_to_doc))

    client = MilvusClient(uri=settings.milvus.uri, timeout=settings.milvus.timeout_s)
    collection = settings.milvus.collection
    store = build_vector_store()

    totals = BackfillStats()
    for page in _iter_rows(client, collection, args.batch_size):
        nodes = [row_to_node(r) for r in page]
        to_update, stats = plan_doc_id_backfill(nodes, path_to_doc)
        totals.total += stats.total
        totals.already += stats.already
        totals.resolved += stats.resolved
        totals.unresolved += stats.unresolved

        if to_update and not args.dry_run:
            store.add(to_update)  # upsert_mode → overwrites by node id

    logger.info(
        "backfill doc_id {mode}: total={t} already={a} resolved={r} "
        "unresolved={u}",
        mode="DRY-RUN" if args.dry_run else "APPLIED",
        t=totals.total, a=totals.already, r=totals.resolved, u=totals.unresolved,
    )
    if args.dry_run:
        logger.info("DRY-RUN — no writes.  Re-run with --no-dry-run to apply.")
    if totals.unresolved:
        logger.warning(
            "{u} chunk(s) had no doc_id and no matching file_path in Postgres "
            "— left untouched (orphaned uploads or path drift).",
            u=totals.unresolved,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
