"""Family 3 heavy tier (offline-materialized reads): centrality + link prediction."""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n
from src.graph.centrality_graph_ops import build_centrality_graph_ops

_METRICS = {"pagerank", "betweenness", "eigenvector"}


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class TopCentralParams(_Params):
    metric: str = "pagerank"
    type: str | None = None
    top_n: int = 20


async def top_central_entities(
    store: Any | None,
    *,
    metric: str = "pagerank",
    type: str | None = None,
    top_n: int = 20,
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    if metric not in _METRICS:
        return PrimitiveResult(cypher="", params={"metric": metric}, rows=[])
    cypher = "centrality_graph_ops.top_central"
    params = {"type": type, "top_n": top_n, "metric": metric}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(
        build_centrality_graph_ops(store).top_central, metric, type, top_n
    )
    return PrimitiveResult(cypher=cypher, params=params, rows=rows, truncated=True)


class LinkPredictionParams(_Params):
    name: str
    top_n: int = 20


async def link_prediction(store: Any | None, *, name: str, top_n: int = 20) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    cypher = "centrality_graph_ops.link_prediction"
    params = {"name": name, "top_n": top_n}
    if store is None:
        return PrimitiveResult(cypher=cypher, params=params, rows=[])
    rows = await asyncio.to_thread(build_centrality_graph_ops(store).link_prediction, name, top_n)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


register(
    Primitive(
        "top_central_entities",
        top_central_entities,
        TopCentralParams,
        "Top entities by structural centrality (pagerank/betweenness/eigenvector) — reads "
        "offline-materialized scores.",
        tier="offline-mat",
    )
)
register(
    Primitive(
        "link_prediction",
        link_prediction,
        LinkPredictionParams,
        "Probable not-yet-recorded links for an entity (a hypothesis) — reads materialized "
        ":LIKELY_LINK edges.",
        tier="offline-mat",
    )
)
