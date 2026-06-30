"""Arc 2 read side — query persisted :Alert nodes (monitor findings)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n
from src.analytics.store_query import run_rows
from src.retrieval.date_filters import today_epoch_days


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


_ALERTS = (
    "MATCH (a:Alert) "
    "WHERE ($kind IS NULL OR a.kind = $kind) "
    "AND ($entity IS NULL OR a.entity = $entity) "
    "AND ($since IS NULL OR a.created_at >= $since) "
    "RETURN a.key AS key, a.kind AS kind, a.entity AS entity, "
    "a.detail AS detail, a.created_at AS created_at "
    "ORDER BY a.created_at DESC LIMIT $top_n"
)


class AlertsParams(_Params):
    kind: str | None = None
    entity: str | None = None
    window_days: int | None = None
    top_n: int = 50


async def alerts(
    store: Any | None,
    *,
    kind: str | None = None,
    entity: str | None = None,
    window_days: int | None = None,
    top_n: int = 50,
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=50)
    since = (today_epoch_days() - int(window_days)) if window_days is not None else None
    params = {"kind": kind, "entity": entity, "since": since, "top_n": top_n}
    return PrimitiveResult(
        cypher=_ALERTS,
        params=params,
        rows=await run_rows(store, _ALERTS, params),
    )


register(
    Primitive(
        "alerts",
        alerts,
        AlertsParams,
        "Persisted Arc-2 alerts (:Alert), filterable by kind/entity/recency, newest first.",
    )
)
