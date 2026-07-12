"""Arc 2 read side — query persisted :Alert nodes (monitor findings)."""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n
from src.graph.alerts import read_alerts
from src.retrieval.date_filters import today_epoch_days


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


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
    cypher = "alert_store.read_alerts"
    params = {"kind": kind, "entity": entity, "since": since, "top_n": top_n}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(
        read_alerts, store, kind=kind, entity=entity, since=since, top_n=top_n
    )
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


register(
    Primitive(
        "alerts",
        alerts,
        AlertsParams,
        "Persisted Arc-2 alerts (:Alert), filterable by kind/entity/recency, newest first.",
    )
)
