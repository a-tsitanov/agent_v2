"""E2 event read primitives — event_dossier + event_timeline."""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.events_burst import build_burst_cypher
from src.analytics.ids import clamp_top_n
from src.graph.events_llm_graph_ops import build_events_llm_graph_ops
from src.retrieval.date_filters import today_epoch_days


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class EventDossierParams(_Params):
    name: str
    top_n: int = 25


async def event_dossier(store: Any | None, *, name: str, top_n: int = 25) -> PrimitiveResult:
    """Event dossier: core event info + actors."""
    top_n = clamp_top_n(top_n, default=25)
    cypher = "events_llm_graph_ops.event_core ;; events_llm_graph_ops.event_actors"
    params = {"name": name, "top_n": top_n}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    ops = build_events_llm_graph_ops(store)
    core = await asyncio.to_thread(ops.event_core, name)
    if not core:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    actors = await asyncio.to_thread(ops.event_actors, name, top_n)
    row = {
        "core": core[0],
        "actors": actors,
    }
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
    """Events a named entity participated in, ordered by resolved start time (untimed last).

    Ordered by event_start_epoch, untimed last; window filters on event_start_epoch
    with created_at fallback.
    """
    top_n = clamp_top_n(top_n, default=50)
    since_secs = None
    if window_days is not None:
        since_secs = (today_epoch_days() - int(window_days)) * 86400
    cypher = "events_llm_graph_ops.event_timeline"
    params: dict[str, Any] = {"entity": entity, "top_n": top_n, "since_secs": since_secs}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(
        build_events_llm_graph_ops(store).event_timeline, entity, since_secs, top_n
    )
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


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
    since_recent = today - int(window_days)
    since_baseline = today - int(window_days) * (bw + 1)
    params = {
        "since_recent": since_recent,
        "since_baseline": since_baseline,
        "baseline_windows": bw,
        "min_count": int(min_count),
        "ratio": 1.0,
        "top_n": top_n,
    }
    if store is None:
        return PrimitiveResult(cypher=_TRENDING, params=params, rows=[])
    rows = await asyncio.to_thread(
        build_events_llm_graph_ops(store).trending_events,
        since_recent, since_baseline, bw, int(min_count), 1.0, top_n,
    )
    return PrimitiveResult(cypher=_TRENDING, params=params, rows=rows)


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
        "Events a named entity participated in, ordered by resolved start time (untimed last).",
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
