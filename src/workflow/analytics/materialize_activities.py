"""Temporal activities for the offline analytics materialization (kb-graph-build).

Task 5: centrality + link-prediction write-back.
Task 6 adds materialize_risk and finalizes MATERIALIZE_ACTIVITIES.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from loguru import logger
from temporalio import activity

from src.analytics import materialize as mz
from src.analytics.contracts import CentralityIn, LinkPredictionIn, RiskIn, StageResult
from src.analytics.ids import ID_TYPES
from src.analytics.risk import compute_risk
from src.config import settings
from src.graph.analysis import _with_projection
from src.graph.store import build_graph_store
from src.retrieval.date_filters import today_epoch_days
from src.workflow.heartbeat import heartbeat_every


def _get_store() -> Any:
    return build_graph_store()


@activity.defn
async def materialize_centrality(p: CentralityIn) -> StageResult:
    """Open one GDS projection, write centrality scores for each metric, drop projection."""
    try:
        store = _get_store()

        async def _do(graph_name: str) -> int:
            total = 0
            for metric in p.metrics:
                total += await mz.write_centrality(store, graph_name, metric)
            return total

        async with heartbeat_every(30.0, {"stage": "centrality"}):
            if settings.graph.backend == "nebula":
                # No GDS projection under nebula; write_centrality computes
                # in-worker (igraph over the edge-export seam) per metric.
                written: int | None = await _do("")
            else:
                written = await _with_projection(store, _do)
        if written is None:
            return StageResult(error="projection/GDS failed — see worker logs")
        return StageResult(written=written)
    except Exception as exc:
        logger.warning("materialize_centrality failed: {e}", e=exc)
        return StageResult(error=str(exc))


@activity.defn
async def materialize_link_prediction(p: LinkPredictionIn) -> StageResult:
    """Open one GDS projection, write LIKELY_LINK edges via nodeSimilarity, drop projection."""
    try:
        store = _get_store()
        s = settings.signals

        async def _do(graph_name: str) -> int:
            return await mz.write_link_prediction(
                store,
                graph_name,
                top_k=s.link_prediction_top_k,
                min_score=s.link_prediction_min_score,
            )

        async with heartbeat_every(30.0, {"stage": "link_prediction"}):
            written = await _with_projection(store, _do)
        if written is None:
            return StageResult(error="projection/GDS failed — see worker logs")
        return StageResult(written=written)
    except Exception as exc:
        logger.warning("materialize_link_prediction failed: {e}", e=exc)
        return StageResult(error=str(exc))


# ---------------------------------------------------------------------------
# Task 6: risk score materialization
# ---------------------------------------------------------------------------

_RISK_GATHER = (
    "MATCH (e:__Entity__) WHERE NONE(l IN labels(e) WHERE l IN $id_types) "
    "OPTIONAL MATCH (e)-[]-(idn:__Entity__) WHERE any(l IN labels(idn) WHERE l IN $id_types) "
    "WITH e, count(DISTINCT idn) AS id_links "
    "OPTIONAL MATCH (e)-[r]-(:__Entity__) "
    "WITH e, id_links, count(r) AS deg, "
    "sum(CASE WHEN r.polarity IN ['negated','uncertain'] THEN 1 ELSE 0 END) AS contested, "
    "sum(CASE WHEN r.created_at >= $since THEN 1 ELSE 0 END) AS recent "
    "RETURN e.name AS name, coalesce(e.betweenness,0.0) AS betweenness, "
    "id_links, deg, contested, recent"
)
_RISK_WRITE = (
    "UNWIND $rows AS r MATCH (e:__Entity__ {name:r.name}) "
    "SET e.risk_score_prev = e.risk_score "
    "SET e.risk_score=r.score, e.risk_band=r.band, e.risk_components=r.components"
)


def _risk_row(raw: dict, weights: dict, bands: dict, max_bet: float) -> dict:
    deg = max(int(raw.get("deg", 0)), 1)
    id_links = int(raw.get("id_links", 0))
    betweenness = float(raw.get("betweenness", 0.0))
    components = {
        "affiliation": 1.0 if id_links >= 2 else 0.0,
        "brokerage": (betweenness / max_bet) if max_bet > 0 else 0.0,
        "controversy": int(raw.get("contested", 0)) / deg,
        "volatility": min(int(raw.get("recent", 0)) / deg, 1.0),
        "opacity": 1.0 if id_links > 0 and int(raw.get("deg", 0)) <= id_links else 0.0,
    }
    r = compute_risk(components, weights=weights, bands=bands)
    return {
        "name": raw["name"],
        "score": r.score,
        "band": r.band,
        "components": json.dumps(r.fired, ensure_ascii=False),
    }


def _gather_risk_nebula(store: Any, since: int) -> list[dict]:
    """nebula risk gather: scan non-id entities (name+betweenness) + their
    RELATED edges (neighbour label / polarity / created_at), aggregate per
    entity in Python into the same {name, betweenness, id_links, deg, contested,
    recent} shape the neo4j _RISK_GATHER returns. O(N+E) full scans — an offline
    materialize concern. Fail-soft (each scan → [] on error)."""
    id_set = set(ID_TYPES)
    id_list = "[" + ", ".join(f'"{t}"' for t in ID_TYPES) + "]"
    ents = mz._run_query(
        store,
        f"MATCH (e:`Entity`) WHERE e.`Entity`.label NOT IN {id_list} "
        "RETURN e.`Entity`.name AS name, e.`Entity`.betweenness AS betweenness;",
        {},
    ) or []
    agg = {
        r["name"]: {"name": r["name"], "betweenness": float(r.get("betweenness") or 0.0),
                    "id_links": 0, "deg": 0, "contested": 0, "recent": 0}
        for r in ents if r.get("name") is not None
    }
    edges = mz._run_query(
        store,
        f"MATCH (e:`Entity`)-[r:`RELATED`]-(n:`Entity`) WHERE e.`Entity`.label NOT IN {id_list} "
        "RETURN e.`Entity`.name AS name, n.`Entity`.label AS nlabel, "
        "r.polarity AS polarity, r.created_at AS created_at;",
        {},
    ) or []
    for e in edges:
        a = agg.get(e.get("name"))
        if a is None:
            continue
        a["deg"] += 1
        if e.get("nlabel") in id_set:
            a["id_links"] += 1
        if e.get("polarity") in ("negated", "uncertain"):
            a["contested"] += 1
        if int(e.get("created_at") or 0) >= since:
            a["recent"] += 1
    return list(agg.values())


def _write_risk_nebula(store: Any, rows: list[dict]) -> int:
    from src.graph.nebula_store import _q, entity_vid

    written = 0
    for r in rows:
        # risk_score_prev = the OLD risk_score (nebula evaluates SET RHS against
        # pre-update values), mirroring the neo4j `SET e.risk_score_prev =
        # e.risk_score` before overwriting — backs the risk-rise monitor delta.
        stmt = (
            f'UPDATE VERTEX ON `Entity` "{entity_vid(r["name"])}" '
            f"SET risk_score_prev = risk_score, risk_score = {float(r['score'])}, "
            f"risk_band = {_q(r['band'])}, risk_components = {_q(r['components'])};"
        )
        try:
            store.structured_query(stmt)
            written += 1
        except Exception as exc:  # one missing vertex must not stop the rest
            logger.debug("risk write skipped for {n}: {e}", n=r["name"], e=exc)
    return written


@activity.defn
async def materialize_risk(p: RiskIn) -> StageResult:
    """Gather per-entity risk components, compute risk score, write back."""
    try:
        store = _get_store()
        since = today_epoch_days() - settings.events.new_window_days
        nebula = settings.graph.backend == "nebula"
        async with heartbeat_every(30.0, {"stage": "risk"}):
            if nebula:
                raws = await asyncio.to_thread(_gather_risk_nebula, store, since)
            else:
                raws = await mz._run(store, _RISK_GATHER, {"id_types": ID_TYPES, "since": since})
            if not raws:
                return StageResult(written=0)
            max_bet = max((float(x.get("betweenness", 0.0)) for x in raws), default=0.0)
            s = settings.signals
            rows = [_risk_row(x, s.risk_weights, s.risk_bands, max_bet) for x in raws]
            if nebula:
                await asyncio.to_thread(_write_risk_nebula, store, rows)
            else:
                await mz._run(store, _RISK_WRITE, {"rows": rows})
        return StageResult(written=len(rows))
    except Exception as exc:
        logger.warning("materialize_risk failed: {e}", e=exc)
        return StageResult(error=str(exc))


MATERIALIZE_ACTIVITIES = [materialize_centrality, materialize_link_prediction, materialize_risk]
