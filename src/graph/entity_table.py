"""Fail-soft mirror of graph entities into the Postgres `entity` table.

Called from the same place the graph write happens. The mirror is a
search accelerator, never a critical path: any Postgres error is logged
and swallowed so the graph write still succeeds. A drifted mirror is
re-fillable (scripts/backfill_entity_table.py).
"""

from __future__ import annotations

from typing import Any

from loguru import logger

_UPSERT = (
    "INSERT INTO entity (vid, name, label, description, mention_count) "
    "VALUES (%s, %s, %s, %s, %s) "
    "ON CONFLICT (vid) DO UPDATE SET "
    "name = EXCLUDED.name, label = EXCLUDED.label, "
    "description = EXCLUDED.description, "
    "mention_count = EXCLUDED.mention_count, updated_at = now()"
)


def node_to_row(n: Any, vid: str) -> dict[str, Any]:
    """Map a write-path node (as ``upsert_nodes`` iterates it: ``.name``,
    ``.label``, ``.properties``) to a mirror row dict.

    ``mention_count`` defaults to 0 here to match nebula_store's own
    vertex write (``int(props.get('mention_count', 0) or 0)``) — this
    helper is the one place both the graft in ``upsert_nodes`` and its
    test go through, so the two writes can no longer drift on the default.
    """
    props = getattr(n, "properties", {}) or {}
    return {
        "vid": vid,
        "name": getattr(n, "name", ""),
        "label": getattr(n, "label", "") or "",
        "description": props.get("description", ""),
        "mention_count": props.get("mention_count", 0),
    }


def mirror_entities(rows: list[dict[str, Any]]) -> None:
    """Upsert entity rows into Postgres. Fail-soft, fail-FAST.

    ``timeout=1`` decouples connection acquisition from the shared sync
    pool's ``pool_timeout_s`` (30s) — during a Postgres outage, a graph
    write must not absorb up to 30s per chunk waiting on a search-only
    mirror. The 1s budget still goes through the same try/except, so a
    timeout is just another swallowed error.
    """
    if not rows:
        return
    try:
        from src.storage.pg_sync_pool import get_pg_sync_pool

        values = [
            (r["vid"], r["name"], r.get("label") or "",
             r.get("description") or "", int(r.get("mention_count") or 0))
            for r in rows
        ]
        with get_pg_sync_pool().connection(timeout=1) as conn, conn.cursor() as cur:
            cur.executemany(_UPSERT, values)
    except Exception as exc:
        logger.warning("entity mirror upsert failed (search only): {e}", e=exc)


__all__ = ["mirror_entities", "node_to_row"]
