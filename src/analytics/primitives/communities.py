"""Family 3 (online subset) — communities reads + seed-biased pagerank."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n
from src.analytics.store_query import run_rows
from src.graph.analysis import personalized_pagerank as _analysis_ppr


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class CommunityOverviewParams(_Params):
    level: int = 0
    top_n: int = 20


async def community_overview(
    store: Any | None, *, level: int = 0, top_n: int = 20
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    cypher = (
        "MATCH (c:Community {level:$level}) "
        "RETURN c.title AS title, c.summary AS summary, c.member_count AS member_count "
        "ORDER BY c.member_count DESC LIMIT $top_n"
    )
    params = {"level": int(level), "top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class EntityCommunitiesParams(_Params):
    name: str


async def entity_communities(store: Any | None, *, name: str) -> PrimitiveResult:
    cypher = (
        "MATCH (e:__Entity__ {name:$name})-[:IN_COMMUNITY]->(c:Community) "
        "RETURN c.level AS level, c.title AS title, c.summary AS summary"
    )
    params = {"name": name}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class PersonalizedPagerankParams(_Params):
    seeds: list[str]
    top_n: int = 20


async def personalized_pagerank(
    store: Any | None, *, seeds: list[str], top_n: int = 20
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    params = {"seeds": list(seeds or []), "top_n": top_n}
    if store is None or not seeds:
        return PrimitiveResult(
            cypher="gds.pageRank.stream(sourceNodes=$seeds)", params=params, rows=[]
        )
    rows = await _analysis_ppr(store, list(seeds), top_n=top_n)
    return PrimitiveResult(
        cypher="gds.pageRank.stream(sourceNodes=$seeds)", params=params, rows=rows
    )


register(
    Primitive(
        "community_overview",
        community_overview,
        CommunityOverviewParams,
        "The large thematic clusters at a level (title/summary/size).",
    )
)
register(
    Primitive(
        "entity_communities",
        entity_communities,
        EntityCommunitiesParams,
        "Which thematic clusters a named entity belongs to.",
    )
)
register(
    Primitive(
        "personalized_pagerank",
        personalized_pagerank,
        PersonalizedPagerankParams,
        "Entities most central relative to given seed entities (seed-biased PageRank).",
    )
)
