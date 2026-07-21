"""E1 read side — first_seen-based "what's new" primitives."""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n, is_meaningful_entity
from src.config import settings
from src.graph.events_graph_ops import build_events_graph_ops
from src.retrieval.date_filters import today_epoch_days


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class NewEventsParams(_Params):
    window_days: int | None = None
    type: str | None = None
    exclude_identifiers: bool = True
    top_n: int = 25


async def new_events(
    store: Any | None,
    *,
    window_days: int | None = None,
    type: str | None = None,
    exclude_identifiers: bool = True,
    top_n: int = 25,
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    wd = window_days if window_days is not None else settings.events.new_window_days
    since = today_epoch_days() - int(wd)
    cypher = "events_graph_ops.new_entities ;; events_graph_ops.new_edges"
    params = {
        "since": since,
        "top_n": top_n,
        "type": type,
        "exclude_identifiers": exclude_identifiers,
    }
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    ops = build_events_graph_ops(store)
    ents = await asyncio.to_thread(ops.new_entities, since, top_n)
    edges = await asyncio.to_thread(ops.new_edges, since, top_n)
    if type:
        ents = [e for e in ents if e.get("type") == type]
    ents = [
        e
        for e in ents
        if is_meaningful_entity(e.get("name"), e.get("type"), exclude_identifiers=exclude_identifiers)
    ]
    rows = [{"kind": "entity", **e} for e in ents] + [{"kind": "edge", **e} for e in edges]
    rows.sort(key=lambda r: r.get("created_at", 0), reverse=True)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows[:top_n])


class EntityNewConnectionsParams(_Params):
    name: str
    window_days: int | None = None
    top_n: int = 25


async def entity_new_connections(
    store: Any | None,
    *,
    name: str,
    window_days: int | None = None,
    top_n: int = 25,
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    wd = window_days if window_days is not None else settings.events.new_window_days
    since = today_epoch_days() - int(wd)
    cypher = "events_graph_ops.entity_new_connections"
    params = {"name": name, "since": since, "top_n": top_n}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(
        build_events_graph_ops(store).entity_new_connections, name, since, top_n
    )
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


register(
    Primitive(
        "new_events",
        new_events,
        NewEventsParams,
        "Entities/edges that first appeared in the graph within a recent window (first_seen).",
    )
)
register(
    Primitive(
        "entity_new_connections",
        entity_new_connections,
        EntityNewConnectionsParams,
        "New connections on a named entity within a recent window.",
    )
)
