"""Family 4 — temporal dynamics (online, read-only)."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n, epoch_days_to_period
from src.analytics.store_query import run_rows


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class RelationshipTimelineParams(_Params):
    name: str
    rel_type: str | None = None


async def relationship_timeline(
    store: Any | None, *, name: str, rel_type: str | None = None
) -> PrimitiveResult:
    cypher = (
        "MATCH (e:__Entity__ {name:$name})-[r]-(n:__Entity__) "
        "WHERE r.valid_from IS NOT NULL AND ($rel_type IS NULL OR type(r)=$rel_type) "
        "RETURN substring(r.valid_from,0,7) AS period, type(r) AS rel, n.name AS name, "
        "r.polarity AS polarity "
        "ORDER BY period"
    )
    params = {"name": name, "rel_type": rel_type}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class WhatsChangedParams(_Params):
    date_from: str
    date_to: str
    entity: str | None = None
    top_n: int = 50


async def whats_changed(
    store: Any | None,
    *,
    date_from: str,
    date_to: str,
    entity: str | None = None,
    top_n: int = 50,
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=50)
    cypher = (
        "MATCH (e:__Entity__)-[r]-(n:__Entity__) "
        "WHERE ($entity IS NULL OR e.name=$entity) AND "
        "((r.valid_from >= $from AND r.valid_from <= $to) OR "
        "(r.valid_to >= $from AND r.valid_to <= $to)) "
        "RETURN e.name AS name, type(r) AS rel, n.name AS other, r.polarity AS polarity, "
        "r.valid_from AS valid_from, r.valid_to AS valid_to, "
        "CASE WHEN r.valid_from>=$from THEN 'appeared' ELSE 'ended' END AS change "
        "ORDER BY coalesce(r.valid_from,r.valid_to) LIMIT $top_n"
    )
    params = {"from": date_from, "to": date_to, "entity": entity, "top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class TopicTrendParams(_Params):
    topic: str
    granularity: str = "month"


async def topic_trend(
    store: Any | None, *, topic: str, granularity: str = "month"
) -> PrimitiveResult:
    cypher = (
        "MATCH (t:__Entity__ {name:$topic})<-[:MENTIONS]-(c:Chunk) "
        "WHERE c.doc_date_epoch IS NOT NULL "
        "RETURN c.doc_date_epoch AS epoch, count(DISTINCT c) AS n"
    )
    params = {"topic": topic}
    raw = await run_rows(store, cypher, params)
    buckets: dict[str, int] = defaultdict(int)
    for r in raw:
        buckets[epoch_days_to_period(r["epoch"], granularity)] += int(r["n"])
    rows = [{"period": p, "mentions": buckets[p]} for p in sorted(buckets)]
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class PolarityEvolutionParams(_Params):
    name: str | None = None
    rel_type: str | None = None


async def polarity_evolution(
    store: Any | None, *, name: str | None = None, rel_type: str | None = None
) -> PrimitiveResult:
    cypher = (
        "MATCH (e:__Entity__)-[r]-(:__Entity__) "
        "WHERE r.valid_from IS NOT NULL AND ($name IS NULL OR e.name=$name) "
        "AND ($rel_type IS NULL OR type(r)=$rel_type) "
        "RETURN substring(r.valid_from,0,7) AS period, r.polarity AS polarity, count(*) AS n "
        "ORDER BY period"
    )
    params = {"name": name, "rel_type": rel_type}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class EntityActivityParams(_Params):
    name: str
    granularity: str = "month"


async def entity_activity(
    store: Any | None, *, name: str, granularity: str = "month"
) -> PrimitiveResult:
    res = await topic_trend(store, topic=name, granularity=granularity)
    return PrimitiveResult(cypher=res.cypher, params=res.params, rows=res.rows)


register(
    Primitive(
        "relationship_timeline",
        relationship_timeline,
        RelationshipTimelineParams,
        "How an entity's relations changed over time (by edge valid_from).",
    )
)
register(
    Primitive(
        "whats_changed",
        whats_changed,
        WhatsChangedParams,
        "Relations that appeared or ended in a date window.",
    )
)
register(
    Primitive(
        "topic_trend",
        topic_trend,
        TopicTrendParams,
        "Mention frequency of a topic/entity over time (by chunk date).",
    )
)
register(
    Primitive(
        "polarity_evolution",
        polarity_evolution,
        PolarityEvolutionParams,
        "How affirmed/negated/uncertain shares shifted over time.",
    )
)
register(
    Primitive(
        "entity_activity",
        entity_activity,
        EntityActivityParams,
        "When an entity was active/discussed over time (mention bursts).",
    )
)
