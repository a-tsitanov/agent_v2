"""Arc 2 — Alert store + watchlist Cypher helpers (called off-loop from activities).

Helpers here are synchronous and must never raise — they log a WARNING on failure
so the monitoring sweep stays alive even when the graph is transiently unavailable.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

# ── Key construction ──────────────────────────────────────────────────────────

_UPSERT_ALERT = """
MERGE (a:Alert {key: $key})
ON CREATE SET
  a.kind       = $kind,
  a.entity     = $entity,
  a.detail     = $detail,
  a.created_at = $created_at
"""

# Scored variant: the volatile value (risk/burst score) is NOT in the key, so
# one :Alert per (kind, entity, detail) is kept and its score is refreshed in
# place on re-detection (ON MATCH) instead of minting a new node each drift.
_UPSERT_ALERT_SCORED = """
MERGE (a:Alert {key: $key})
ON CREATE SET
  a.kind       = $kind,
  a.entity     = $entity,
  a.detail     = $detail,
  a.created_at = $created_at,
  a.score      = $score,
  a.updated_at = $created_at
ON MATCH SET
  a.score      = $score,
  a.updated_at = $created_at
"""

_MARK_WATCHED = """
UNWIND $names AS n
MATCH (e:__Entity__ {name: n})
SET e.watched = $watched
"""

# Canonical :Alert read query — the single source of truth reused by the
# `alerts` catalog primitive (Task 11). Optional filters are NULL-guarded so
# the caller passes them as params ($kind/$entity/$since/$top_n), newest-first.
read_alerts_cypher = (
    "MATCH (a:Alert) "
    "WHERE ($kind IS NULL OR a.kind = $kind) "
    "AND ($entity IS NULL OR a.entity = $entity) "
    "AND ($since IS NULL OR a.created_at >= $since) "
    "RETURN a.key AS key, a.kind AS kind, a.entity AS entity, "
    "a.detail AS detail, a.created_at AS created_at, "
    "a.score AS score, a.updated_at AS updated_at "
    "ORDER BY a.created_at DESC LIMIT $top_n"
)


# ── Public helpers ────────────────────────────────────────────────────────────


def alert_key(kind: str, entity: str, detail: str) -> str:
    """Compose a stable dedup key for an Alert node.

    Format: ``kind:entity:detail`` — deterministic, so re-sweeps MERGE
    onto the same node instead of creating duplicates.
    """
    return f"{kind}:{entity}:{detail}"


def upsert_alert(
    store: Any,
    *,
    kind: str,
    entity: str,
    detail: str,
    created_at: int,
    score: float | None = None,
) -> None:
    """MERGE an :Alert node keyed on (kind, entity, detail); fail-soft on error.

    When ``score`` is given, the value is stored as ``a.score`` and refreshed
    in place on re-detection (ON MATCH) — one alert per (kind, entity, detail),
    no churn as the score drifts. The score is never part of the dedup key.
    """
    key = alert_key(kind, entity, detail)
    params: dict[str, Any] = {
        "key": key,
        "kind": kind,
        "entity": entity,
        "detail": detail,
        "created_at": created_at,
    }
    cypher = _UPSERT_ALERT
    if score is not None:
        params["score"] = score
        cypher = _UPSERT_ALERT_SCORED
    try:
        store.structured_query(cypher, param_map=params)
    except Exception as exc:
        logger.warning("upsert_alert failed (non-fatal): {e}", e=exc)


def mark_watched(store: Any, names: list[str], watched: bool = True) -> None:
    """SET e.watched on __Entity__ nodes by name list; fail-soft on error."""
    if not names:
        return
    try:
        store.structured_query(
            _MARK_WATCHED,
            param_map={"names": names, "watched": watched},
        )
    except Exception as exc:
        logger.warning("mark_watched failed (non-fatal): {e}", e=exc)


__all__ = ["alert_key", "mark_watched", "read_alerts_cypher", "upsert_alert"]
