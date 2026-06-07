"""Backfill ``__Entity__.er_vec`` (native vector list) from the legacy
``er_embedding`` JSON string, and build the ER vector index.

Prerequisite for enabling ``ERConfig.use_native_vector_knn`` (env
``ER_USE_NATIVE_VECTOR_KNN=true``): the opt-in path does a per-entity
nearest-neighbour lookup over the ER vector index instead of loading a
bounded 5000-entity window — but it can only find canonicals that have
``er_vec`` populated.  This one-shot job parses each existing entity's
``er_embedding`` JSON into a native list and creates the index.  No
re-extraction, no re-embedding.

Usage::

    python -m scripts.backfill_er_vector                 # dry-run (counts only)
    python -m scripts.backfill_er_vector --no-dry-run    # apply
    python -m scripts.backfill_er_vector --no-dry-run --batch-size 2000

Apply only after a Neo4j backup.  Idempotent: re-running only touches
entities that still lack ``er_vec``.  Requires APOC (already a project
dependency).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger  # noqa: E402

from src.config import settings  # noqa: E402
from src.graph.index import ensure_er_vector_index  # noqa: E402
from src.graph.store import build_neo4j_graph_store  # noqa: E402
from src.utils.logging import configure_logging  # noqa: E402


_COUNT_CYPHER = (
    "MATCH (e:__Entity__) "
    "WHERE e.er_embedding IS NOT NULL AND e.er_vec IS NULL "
    "RETURN count(e) AS pending"
)

_TOTAL_CYPHER = (
    "MATCH (e:__Entity__) WHERE e.er_embedding IS NOT NULL "
    "RETURN count(e) AS total"
)


def _backfill_cypher(batch_size: int) -> str:
    # batch_size is a trusted int from argparse → safe to inline (APOC's
    # config map does not accept query parameters).
    return (
        "CALL apoc.periodic.iterate("
        "  'MATCH (e:__Entity__) WHERE e.er_embedding IS NOT NULL "
        "AND e.er_vec IS NULL RETURN e', "
        "  'SET e.er_vec = apoc.convert.fromJsonList(e.er_embedding)', "
        f"  {{batchSize: {int(batch_size)}, parallel: false}}"
        ") YIELD batches, total, errorMessages "
        "RETURN batches, total, errorMessages"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--no-dry-run", dest="dry_run", action="store_false",
        help="actually write er_vec + build the index (default: dry-run)",
    )
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.set_defaults(dry_run=True)
    args = parser.parse_args(argv)

    configure_logging()
    store = build_neo4j_graph_store()

    total = (store.structured_query(_TOTAL_CYPHER) or [{}])[0].get("total", 0)
    pending = (store.structured_query(_COUNT_CYPHER) or [{}])[0].get("pending", 0)
    logger.info(
        "ER vector backfill: {p} of {t} entities need er_vec (dim={d})",
        p=pending, t=total, d=settings.milvus.dim,
    )

    if args.dry_run:
        logger.info("DRY-RUN — no writes.  Re-run with --no-dry-run to apply.")
        return 0

    if pending:
        res = store.structured_query(_backfill_cypher(args.batch_size))
        row = (res or [{}])[0]
        logger.info(
            "backfill done: batches={b} total={t} errors={e}",
            b=row.get("batches"), t=row.get("total"),
            e=row.get("errorMessages"),
        )

    ok = ensure_er_vector_index(store, settings.milvus.dim)
    logger.info("ER vector index ensured: {ok}", ok=ok)
    remaining = (store.structured_query(_COUNT_CYPHER) or [{}])[0].get("pending", 0)
    logger.info("remaining without er_vec: {r}", r=remaining)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
