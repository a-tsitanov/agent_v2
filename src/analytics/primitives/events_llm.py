"""E2 event read primitives — event_dossier + event_timeline."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.events_burst import build_burst_cypher
from src.analytics.ids import clamp_top_n
from src.analytics.store_query import run_rows
from src.retrieval.date_filters import today_epoch_days


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
    params: dict[str, Any] = {"entity": entity, "top_n": top_n}
    where = ""
    if window_days is not None:
        params["since"] = today_epoch_days() - int(window_days)
        where = "WHERE e.created_at >= $since "
    cypher = (
        "MATCH (p:__Entity__ {name:$entity})-[]-(e:__Entity__:EventOrAction) "
        f"{where}"
        "RETURN e.name AS name, e.event_type AS event_type, e.event_ts AS event_ts "
        "ORDER BY e.event_ts DESC LIMIT $top_n"
    )
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


_TRENDING = build_burst_cypher(watched_only=False)


class TrendingEventsParams(_Params):
    window_days: int = 7
    baseline_windows: int = 4
    min_count: int = 2
    top_n: int = 20


async def trending_events(
    store: Any | None,
    *,
    window_days: int = 7,
    baseline_windows: int = 4,
    min_count: int = 2,
    top_n: int = 20,
) -> PrimitiveResult:
    """(entity, event_type) pairs whose event ingest-rate surged recently."""
    top_n = clamp_top_n(top_n, default=20)
    bw = max(int(baseline_windows), 1)
    today = today_epoch_days()
    params = {
        "since_recent": today - int(window_days),
        "since_baseline": today - int(window_days) * (bw + 1),
        "baseline_windows": bw,
        "min_count": int(min_count),
        "ratio": 1.0,
        "top_n": top_n,
    }
    return PrimitiveResult(
        cypher=_TRENDING, params=params, rows=await run_rows(store, _TRENDING, params)
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
register(
    Primitive(
        "trending_events",
        trending_events,
        TrendingEventsParams,
        "Surging (entity × event_type) pairs by recent event ingest-rate vs baseline (E3).",
    )
)
