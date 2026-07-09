"""One-time E1 backfill: stamp a sentinel ``created_at`` on pre-existing
graph elements so they are never mis-flagged as "new". Run BEFORE enabling
``EVENTS_FIRST_SEEN_ENABLED``.

Backfill form chosen: plain ``MATCH … SET`` (no ``CALL IN TRANSACTIONS``).

Rationale: ``Neo4jPropertyGraphStore.structured_query`` calls
``driver.execute_query`` first (managed/explicit transaction) then falls back
to ``session.run`` (implicit transaction) when the server rejects the query
with a "needs implicit transaction" error.  That fallback makes
``CALL { … } IN TRANSACTIONS`` technically runnable, but it means the first
call always errors — noisy, non-obvious, and fragile across driver versions.
For a one-shot admin script on a modest graph (< 1 M entities) a plain
``MATCH … SET`` in a single managed transaction is safer, easier to audit,
and sufficient.  If the graph is too large to fit in one TX, re-run with a
smaller window by wrapping in a Python loop with ``SKIP/LIMIT``; that
complexity is not needed yet.

Usage::

    python -m scripts.backfill_first_seen [--dry-run]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger

from src.config import settings
from src.graph.index import ensure_first_seen_indexes
from src.graph.store import build_graph_store
from src.utils.logging import configure_logging

_COUNT_ENT_CYPHER = "MATCH (e:__Entity__) WHERE e.created_at IS NULL RETURN count(e) AS pending"
_COUNT_REL_CYPHER = "MATCH ()-[r]->() WHERE r.created_at IS NULL RETURN count(r) AS pending"
_SET_ENT_CYPHER = "MATCH (e:__Entity__) WHERE e.created_at IS NULL SET e.created_at = $sentinel"
_SET_REL_CYPHER = "MATCH ()-[r]->() WHERE r.created_at IS NULL SET r.created_at = $sentinel"


def _pending(store, cypher: str) -> int:
    rows = store.structured_query(cypher) or [{}]
    return int(rows[0].get("pending", 0))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="count pending nodes/rels but do not write (default: apply)",
    )
    args = parser.parse_args(argv)

    configure_logging()
    store = build_graph_store()
    sentinel = settings.events.backfill_sentinel

    ent_pending = _pending(store, _COUNT_ENT_CYPHER)
    rel_pending = _pending(store, _COUNT_REL_CYPHER)
    logger.info(
        "backfill_first_seen: {e} entities + {r} rels need created_at (sentinel={s})",
        e=ent_pending,
        r=rel_pending,
        s=sentinel,
    )

    if args.dry_run:
        logger.info("DRY-RUN — no writes.  Re-run without --dry-run to apply.")
        return 0

    ensure_first_seen_indexes(store)

    if ent_pending:
        store.structured_query(_SET_ENT_CYPHER, param_map={"sentinel": sentinel})
        logger.info("entities stamped with created_at={s}", s=sentinel)

    if rel_pending:
        store.structured_query(_SET_REL_CYPHER, param_map={"sentinel": sentinel})
        logger.info("relations stamped with created_at={s}", s=sentinel)

    logger.info("backfill_first_seen complete (sentinel={s})", s=sentinel)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
