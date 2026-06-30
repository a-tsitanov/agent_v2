"""Arc 2 monitoring sweep — detect_alerts Temporal activity.

Scans watched entities for:
  (a) new first_seen edges within a rolling window, and
  (b) risk-score rises above a threshold.

Persists :Alert nodes via ``upsert_alert`` (MERGE-on-key, fail-soft).
Never raises across the Temporal boundary — all errors are returned as
``MonitorResult(error=...)`` so the sweep stays alive.
"""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger
from temporalio import activity

from src.analytics.contracts import MonitorIn, MonitorResult
from src.graph.alerts import upsert_alert
from src.graph.store import build_neo4j_graph_store
from src.retrieval.date_filters import today_epoch_days
from src.workflow.heartbeat import heartbeat_every

# Maximum rows per sweep pass — keeps a single sweep bounded even on large graphs.
_TOP_N = 200

_WATCHED_EDGES_CYPHER = (
    "MATCH (a:__Entity__)-[r]->(b:__Entity__) "
    "WHERE r.created_at >= $since AND (a.watched = true OR b.watched = true) "
    "RETURN a.name AS src, type(r) AS rel, b.name AS tgt, r.created_at AS created_at, "
    "coalesce(a.watched,false) AS a_watched, coalesce(b.watched,false) AS b_watched "
    "ORDER BY r.created_at DESC LIMIT $top_n"
)

_RISK_RISE_CYPHER = (
    "MATCH (e:__Entity__) "
    "WHERE e.watched = true AND e.risk_score IS NOT NULL "
    "AND (e.risk_score - coalesce(e.risk_score_prev, 0)) >= $delta "
    "RETURN e.name AS name, e.risk_score AS score ORDER BY e.risk_score DESC LIMIT $top_n"
)


def _get_store() -> Any:
    return build_neo4j_graph_store()


@activity.defn
async def detect_alerts(p: MonitorIn) -> MonitorResult:
    """Detect new-connection and risk-rise alerts for watched entities.

    Both Cypher reads and ``upsert_alert`` writes are synchronous Neo4j calls
    executed off the event loop via ``asyncio.to_thread``.
    """
    try:
        store = _get_store()
        since = today_epoch_days() - p.window_days
        new_conn_count = 0
        risk_rise_count = 0

        async with heartbeat_every(30.0, {"stage": "monitor"}):
            # ── (a) new-connection alerts ────────────────────────────────
            edge_rows = await asyncio.to_thread(
                store.structured_query,
                _WATCHED_EDGES_CYPHER,
                param_map={"since": since, "top_n": _TOP_N},
            )
            for row in edge_rows:
                if row.get("a_watched"):
                    await asyncio.to_thread(
                        upsert_alert,
                        store,
                        kind="new_connection",
                        entity=row["src"],
                        detail=f"{row['rel']}:{row['tgt']}",
                        created_at=row["created_at"],
                    )
                    new_conn_count += 1
                if row.get("b_watched"):
                    await asyncio.to_thread(
                        upsert_alert,
                        store,
                        kind="new_connection",
                        entity=row["tgt"],
                        detail=f"{row['rel']}:{row['src']}",
                        created_at=row["created_at"],
                    )
                    new_conn_count += 1

            # ── (b) risk-rise alerts ─────────────────────────────────────
            risk_rows = await asyncio.to_thread(
                store.structured_query,
                _RISK_RISE_CYPHER,
                param_map={"delta": p.risk_rise_delta, "top_n": _TOP_N},
            )
            for row in risk_rows:
                await asyncio.to_thread(
                    upsert_alert,
                    store,
                    kind="risk_rise",
                    entity=row["name"],
                    detail=str(row["score"]),
                    created_at=today_epoch_days(),
                )
                risk_rise_count += 1

        return MonitorResult(
            new_connection_alerts=new_conn_count,
            risk_rise_alerts=risk_rise_count,
        )
    except Exception as exc:
        logger.warning("detect_alerts failed: {e}", e=exc)
        return MonitorResult(error=str(exc))


MONITOR_ACTIVITIES = [detect_alerts]
