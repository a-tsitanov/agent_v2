"""Backend-dispatched analytics "events_llm" graph ops (E2 event reads,
read-only, fail-soft): event_core / event_actors / event_timeline.

trending_events (build_burst_cypher — a multi-window burst-rate detection) is NOT
ported: it stays on run_rows and degrades to [] under nebula. Its nGQL port is a
follow-up.

``Neo4jEventsLlmGraphOps`` runs the existing Cypher verbatim (moved from
``analytics/primitives/events_llm.py``). ``NebulaEventsLlmGraphOps`` reads the E2
event columns now carried on EventOrAction entities (see nebula_schema): FETCH by
VID for the core, GO for actors, GO OVER PARTICIPATED_IN + Python window/sort for
the timeline.

Divergence: neo4j event_core RETURNs ``e.polarity``; entities carry no polarity
column (it is an edge property), so nebula returns ``polarity=None`` — identical
to neo4j, where a non-event-bearing entity has no such property.
"""
from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, Protocol

from loguru import logger

from src.config import settings

_EVENT_CORE = (
    "MATCH (e:__Entity__:EventOrAction {name:$name}) "
    "RETURN e.name AS name, e.event_type AS event_type, e.event_ts_raw AS event_ts_raw, "
    "e.event_start_epoch AS event_start_epoch, e.event_end_epoch AS event_end_epoch, "
    "e.event_ts_precision AS event_ts_precision, e.polarity AS polarity"
)
_EVENT_ACTORS = (
    "MATCH (e:__Entity__:EventOrAction {name:$name})-[r]-(n) "
    "RETURN n.name AS actor_name, type(r) AS rel "
    "LIMIT $top_n"
)
_EVENT_TIMELINE = (
    "MATCH (p:__Entity__ {name:$entity})-[:PARTICIPATED_IN]-(e:__Entity__:EventOrAction) "
    "{where}"
    "RETURN e.name AS name, e.event_type AS event_type, e.event_ts_raw AS event_ts_raw, "
    "e.event_start_epoch AS event_start_epoch, e.event_end_epoch AS event_end_epoch, "
    "e.event_ts_precision AS event_ts_precision "
    "ORDER BY e.event_start_epoch IS NULL, e.event_start_epoch DESC LIMIT $top_n"
)

_EVENT_FIELDS = (
    "event_type", "event_ts_raw", "event_start_epoch", "event_end_epoch", "event_ts_precision",
)


class EventsLlmGraphOps(Protocol):
    def event_core(self, name: str) -> list[dict]: ...

    def event_actors(self, name: str, top_n: int) -> list[dict]: ...

    def event_timeline(self, entity: str, since_secs: int | None, top_n: int) -> list[dict]: ...


class Neo4jEventsLlmGraphOps:
    def __init__(self, store: Any):
        self._store = store

    def _rows(self, cypher: str, params: dict) -> list[dict]:
        try:
            rows = self._store.structured_query(cypher, param_map=params)
            return list(rows or [])
        except Exception as exc:  # fail-soft, like store_query.run_rows
            logger.warning("analytics query failed: {e}", e=exc)
            return []

    def event_core(self, name: str) -> list[dict]:
        return self._rows(_EVENT_CORE, {"name": name})

    def event_actors(self, name: str, top_n: int) -> list[dict]:
        return self._rows(_EVENT_ACTORS, {"name": name, "top_n": top_n})

    def event_timeline(self, entity: str, since_secs: int | None, top_n: int) -> list[dict]:
        params: dict[str, Any] = {"entity": entity, "top_n": top_n}
        where = ""
        if since_secs is not None:
            params["since_secs"] = since_secs
            where = "WHERE coalesce(e.event_start_epoch, e.created_at * 86400) >= $since_secs "
        return self._rows(_EVENT_TIMELINE.replace("{where}", where), params)


def _nebula_fail_soft(method: Callable[..., list[dict]]) -> Callable[..., list[dict]]:
    @functools.wraps(method)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> list[dict]:
        try:
            return method(self, *args, **kwargs)
        except Exception as exc:  # fail-soft, like store_query.run_rows
            logger.warning("analytics query failed: {e}", e=exc)
            return []

    return wrapper


class NebulaEventsLlmGraphOps:
    def __init__(self, store: Any):
        self._store = store

    def _exec(self, stmt: str) -> list[dict]:
        return list(self._store.structured_query(stmt) or [])

    def _fetch_events(self, vids: list[str]) -> dict[str, dict]:
        """FETCH the event columns for a set of VIDs, keyed by vid; only rows
        whose label is EventOrAction are kept."""
        if not vids:
            return {}
        from src.graph.nebula_store import _q

        vid_list = ", ".join(_q(v) for v in vids)
        rows = self._exec(
            f"FETCH PROP ON `Entity` {vid_list} YIELD id(vertex) AS vid, "
            "`Entity`.name AS name, `Entity`.label AS label, "
            "`Entity`.event_type AS event_type, `Entity`.event_ts_raw AS event_ts_raw, "
            "`Entity`.event_start_epoch AS event_start_epoch, "
            "`Entity`.event_end_epoch AS event_end_epoch, "
            "`Entity`.event_ts_precision AS event_ts_precision;"
        )
        return {r["vid"]: r for r in rows if r.get("label") == "EventOrAction"}

    @_nebula_fail_soft
    def event_core(self, name: str) -> list[dict]:
        from src.graph.nebula_store import entity_vid

        vid = entity_vid(name)
        events = self._fetch_events([vid])
        row = events.get(vid)
        if not row:
            return []
        return [{
            "name": row.get("name"),
            "event_type": row.get("event_type"),
            "event_ts_raw": row.get("event_ts_raw"),
            "event_start_epoch": row.get("event_start_epoch"),
            "event_end_epoch": row.get("event_end_epoch"),
            "event_ts_precision": row.get("event_ts_precision"),
            "polarity": None,  # entities carry no polarity column (edge property)
        }]

    @_nebula_fail_soft
    def event_actors(self, name: str, top_n: int) -> list[dict]:
        from src.graph.nebula_store import _q, entity_vid

        # confirm the anchor is an event, then walk its neighbours.
        vid = entity_vid(name)
        if not self._fetch_events([vid]):
            return []
        edges = self._exec(
            f"GO FROM {_q(vid)} OVER `RELATED` BIDIRECT YIELD "
            "dst(edge) AS nbr, src(edge) AS s, `RELATED`.rel_type AS rel;"
        )
        actors = []
        nbr_vids = []
        pending = []
        for e in edges:
            nbr = e.get("nbr") if e.get("nbr") != vid else e.get("s")
            if not nbr or nbr == vid:
                continue
            nbr_vids.append(nbr)
            pending.append((nbr, e.get("rel")))
        if not nbr_vids:
            return []
        names = self._fetch_names(nbr_vids)
        for nbr, rel in pending:
            if nbr in names:
                actors.append({"actor_name": names[nbr], "rel": rel})
        return actors[:top_n]

    def _fetch_names(self, vids: list[str]) -> dict[str, str]:
        from src.graph.nebula_store import _q

        uniq = list(dict.fromkeys(vids))
        if not uniq:
            return {}
        vid_list = ", ".join(_q(v) for v in uniq)
        rows = self._exec(
            f"FETCH PROP ON `Entity` {vid_list} YIELD id(vertex) AS vid, `Entity`.name AS name;"
        )
        return {r["vid"]: r.get("name") for r in rows}

    @_nebula_fail_soft
    def event_timeline(self, entity: str, since_secs: int | None, top_n: int) -> list[dict]:
        from src.graph.nebula_store import _q, entity_vid

        vid = entity_vid(entity)
        edges = self._exec(
            f"GO FROM {_q(vid)} OVER `RELATED` BIDIRECT "
            "WHERE `RELATED`.rel_type == 'PARTICIPATED_IN' YIELD "
            "dst(edge) AS d, src(edge) AS s;"
        )
        ev_vids = []
        for e in edges:
            other = e.get("d") if e.get("d") != vid else e.get("s")
            if other and other != vid:
                ev_vids.append(other)
        events = self._fetch_events(ev_vids)
        out = []
        for row in events.values():
            start = int(row.get("event_start_epoch") or 0)
            # coalesce(event_start_epoch, created_at*86400): a 0/untimed event is
            # kept (neo4j's null passes the >= filter, sorted untimed-last too);
            # a timed event before the window is dropped.
            if since_secs is not None and start and start < since_secs:
                continue
            out.append({
                "name": row.get("name"),
                "event_type": row.get("event_type"),
                "event_ts_raw": row.get("event_ts_raw"),
                "event_start_epoch": row.get("event_start_epoch"),
                "event_end_epoch": row.get("event_end_epoch"),
                "event_ts_precision": row.get("event_ts_precision"),
            })
        # untimed (start 0) last, then event_start_epoch desc — mirrors the neo4j
        # `ORDER BY event_start_epoch IS NULL, event_start_epoch DESC`.
        out.sort(key=lambda r: (int(r["event_start_epoch"] or 0) == 0,
                                -int(r["event_start_epoch"] or 0)))
        return out[:top_n]


def build_events_llm_graph_ops(store: Any) -> EventsLlmGraphOps:
    if settings.graph.backend == "nebula":
        return NebulaEventsLlmGraphOps(store)
    return Neo4jEventsLlmGraphOps(store)
