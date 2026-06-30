"""E2 event read primitives — event_dossier + event_timeline."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n
from src.analytics.store_query import run_rows


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


_EVENT_CORE = (
    "MATCH (e:__Entity__:EventOrAction {name:$name}) "
    "RETURN e.name AS name, e.event_type AS event_type, e.event_ts AS event_ts, "
    "e.polarity AS polarity"
)
_EVENT_ACTORS = (
    "MATCH (e:__Entity__:EventOrAction {name:$name})-[r]-(n) "
    "RETURN n.name AS actor_name, type(r) AS rel "
    "LIMIT $top_n"
)


class EventDossierParams(_Params):
    name: str
    top_n: int = 25


async def event_dossier(store: Any | None, *, name: str, top_n: int = 25) -> PrimitiveResult:
    """Event dossier: core event info + actors."""
    top_n = clamp_top_n(top_n, default=25)
    params = {"name": name, "top_n": top_n}
    if store is None:
        return PrimitiveResult(cypher=_EVENT_CORE, params=params, rows=[])
    core = await run_rows(store, _EVENT_CORE, params)
    if not core:
        return PrimitiveResult(cypher=_EVENT_CORE, params=params, rows=[])
    actors = await run_rows(store, _EVENT_ACTORS, params)
    row = {
        "core": core[0],
        "actors": actors,
    }
    cypher = " ;; ".join([_EVENT_CORE, _EVENT_ACTORS])
    return PrimitiveResult(cypher=cypher, params=params, rows=[row])


_EVENT_TIMELINE = (
    "MATCH (p:__Entity__ {name:$entity})-[]-(e:__Entity__:EventOrAction) "
    "RETURN e.name AS name, e.event_type AS event_type, e.event_ts AS event_ts "
    "ORDER BY e.event_ts DESC LIMIT $top_n"
)


class EventTimelineParams(_Params):
    entity: str
    window_days: int | None = None
    top_n: int = 50


async def event_timeline(
    store: Any | None,
    *,
    entity: str,
    window_days: int | None = None,
    top_n: int = 50,
) -> PrimitiveResult:
    """Events a named entity participated in, ordered by event_ts."""
    top_n = clamp_top_n(top_n, default=50)
    params = {"entity": entity, "top_n": top_n}
    if window_days is not None:
        params["window_days"] = window_days
    return PrimitiveResult(
        cypher=_EVENT_TIMELINE, params=params, rows=await run_rows(store, _EVENT_TIMELINE, params)
    )


register(
    Primitive(
        "event_dossier",
        event_dossier,
        EventDossierParams,
        "Full portrait of one event: core info + participants/actors.",
    )
)
register(
    Primitive(
        "event_timeline",
        event_timeline,
        EventTimelineParams,
        "Events a named entity participated in, ordered by event_ts.",
    )
)
