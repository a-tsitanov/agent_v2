"""Temporal activities for the offline analytics materialization (kb-graph-build).

Task 5: centrality + link-prediction write-back.
Task 6 adds materialize_risk and finalizes MATERIALIZE_ACTIVITIES.
"""

from __future__ import annotations

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


@activity.defn
async def materialize_risk(p: RiskIn) -> StageResult:
    """Gather per-entity risk components from Neo4j, compute risk score, write back."""
    try:
        store = _get_store()
        since = today_epoch_days() - settings.events.new_window_days
        async with heartbeat_every(30.0, {"stage": "risk"}):
            raws = await mz._run(store, _RISK_GATHER, {"id_types": ID_TYPES, "since": since})
            if not raws:
                return StageResult(written=0)
            max_bet = max((float(x.get("betweenness", 0.0)) for x in raws), default=0.0)
            s = settings.signals
            rows = [_risk_row(x, s.risk_weights, s.risk_bands, max_bet) for x in raws]
            await mz._run(store, _RISK_WRITE, {"rows": rows})
        return StageResult(written=len(rows))
    except Exception as exc:
        logger.warning("materialize_risk failed: {e}", e=exc)
        return StageResult(error=str(exc))


MATERIALIZE_ACTIVITIES = [materialize_centrality, materialize_link_prediction, materialize_risk]
