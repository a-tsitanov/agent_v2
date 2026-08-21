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


def mirror_entities(rows: list[dict[str, Any]]) -> None:
    """Upsert entity rows into Postgres. Fail-soft."""
    if not rows:
        return
    try:
        from src.storage.pg_sync_pool import get_pg_sync_pool

        values = [
            (r["vid"], r["name"], r.get("label") or "",
             r.get("description") or "", int(r.get("mention_count") or 1))
            for r in rows
        ]
        with get_pg_sync_pool().connection() as conn, conn.cursor() as cur:
            cur.executemany(_UPSERT, values)
    except Exception as exc:
        logger.warning("entity mirror upsert failed (search only): {e}", e=exc)


__all__ = ["mirror_entities"]
