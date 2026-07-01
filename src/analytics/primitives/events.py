"""E1 read side — first_seen-based "what's new" primitives."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n
from src.analytics.store_query import run_rows
from src.config import settings
from src.retrieval.date_filters import today_epoch_days


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


_NEW_ENTITIES = (
    "MATCH (e:__Entity__) WHERE e.created_at >= $since "
    "RETURN e.name AS name, [l IN labels(e) WHERE l<>'__Entity__'][0] AS type, "
    "e.created_at AS created_at, e.first_doc_id AS first_doc_id "
    "ORDER BY e.created_at DESC LIMIT $top_n"
)
_NEW_EDGES = (
    "MATCH (a:__Entity__)-[r]->(b:__Entity__) WHERE r.created_at >= $since "
    "RETURN a.name AS src, type(r) AS rel, b.name AS tgt, r.created_at AS created_at, "
    "r.first_doc_id AS first_doc_id ORDER BY r.created_at DESC LIMIT $top_n"
)


class NewEventsParams(_Params):
    window_days: int | None = None
    type: str | None = None
    top_n: int = 25


async def new_events(
    store: Any | None,
    *,
    window_days: int | None = None,
    type: str | None = None,
    top_n: int = 25,
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    wd = window_days if window_days is not None else settings.events.new_window_days
    since = today_epoch_days() - int(wd)
    cypher_params = {"since": since, "top_n": top_n}
    ents = await run_rows(store, _NEW_ENTITIES, cypher_params)
    edges = await run_rows(store, _NEW_EDGES, cypher_params)
    if type:
        ents = [e for e in ents if e.get("type") == type]
    rows = [{"kind": "entity", **e} for e in ents] + [{"kind": "edge", **e} for e in edges]
    rows.sort(key=lambda r: r.get("created_at", 0), reverse=True)
    cypher = _NEW_ENTITIES + " ;; " + _NEW_EDGES
    params = {"since": since, "top_n": top_n, "type": type}
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
    cypher = (
        "MATCH (e:__Entity__ {name:$name})-[r]-(n:__Entity__) WHERE "
        "r.created_at >= $since "
        "RETURN type(r) AS rel, n.name AS other, r.created_at AS created_at, "
        "r.first_doc_id AS first_doc_id "
        "ORDER BY r.created_at DESC LIMIT $top_n"
    )
    params = {"name": name, "since": since, "top_n": top_n}
    return PrimitiveResult(
        cypher=cypher,
        params=params,
        rows=await run_rows(store, cypher, params),
    )


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
