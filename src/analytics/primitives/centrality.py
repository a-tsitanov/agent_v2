"""Family 3 heavy tier (offline-materialized reads): centrality + link prediction."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n
from src.analytics.store_query import run_rows

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
    cypher = (
        f"MATCH (e:__Entity__) WHERE e.{metric} IS NOT NULL "
        "AND ($type IS NULL OR $type IN labels(e)) "
        f"RETURN e.name AS name, e.{metric} AS score ORDER BY e.{metric} DESC LIMIT $top_n"
    )
    params = {"type": type, "top_n": top_n, "metric": metric}
    return PrimitiveResult(
        cypher=cypher,
        params=params,
        rows=await run_rows(store, cypher, params),
        truncated=True,
    )


class LinkPredictionParams(_Params):
    name: str
    top_n: int = 20


async def link_prediction(store: Any | None, *, name: str, top_n: int = 20) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    cypher = (
        "MATCH (e:__Entity__ {name:$name})-[l:LIKELY_LINK]->(m:__Entity__) "
        "RETURN m.name AS name, l.score AS score ORDER BY l.score DESC LIMIT $top_n"
    )
    params = {"name": name, "top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


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
