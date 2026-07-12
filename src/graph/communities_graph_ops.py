"""Backend-dispatched analytics "communities" graph ops (read-only, fail-soft):
community_overview + entity_communities. ``personalized_pagerank`` stays in the
primitive (a GDS compute that already degrades to ``[]`` under nebula via
``graph.analysis``'s fail-soft projection).

``Neo4jCommunitiesGraphOps`` runs the existing Cypher verbatim (moved from
``analytics/primitives/communities.py``). ``NebulaCommunitiesGraphOps``:
- ``community_overview``: MATCH the Community tag by level (community_level_idx),
  aliased ORDER BY member_count.
- ``entity_communities``: GO OVER IN_COMMUNITY from the entity VID, then FETCH the
  Community tag for level/title/summary.
Every method is fail-soft.
"""
from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, Protocol

from loguru import logger

from src.config import settings

_COMMUNITY_OVERVIEW = (
    "MATCH (c:Community {level:$level}) "
    "RETURN c.title AS title, c.summary AS summary, c.member_count AS member_count "
    "ORDER BY c.member_count DESC LIMIT $top_n"
)
_ENTITY_COMMUNITIES = (
    "MATCH (e:__Entity__ {name:$name})-[:IN_COMMUNITY]->(c:Community) "
    "RETURN c.level AS level, c.title AS title, c.summary AS summary"
)


class CommunitiesGraphOps(Protocol):
    def community_overview(self, level: int, top_n: int) -> list[dict]: ...

    def entity_communities(self, name: str) -> list[dict]: ...


class Neo4jCommunitiesGraphOps:
    def __init__(self, store: Any):
        self._store = store

    def _rows(self, cypher: str, params: dict) -> list[dict]:
        try:
            rows = self._store.structured_query(cypher, param_map=params)
            return list(rows or [])
        except Exception as exc:  # fail-soft, like store_query.run_rows
            logger.warning("analytics query failed: {e}", e=exc)
            return []

    def community_overview(self, level: int, top_n: int) -> list[dict]:
        return self._rows(_COMMUNITY_OVERVIEW, {"level": level, "top_n": top_n})

    def entity_communities(self, name: str) -> list[dict]:
        return self._rows(_ENTITY_COMMUNITIES, {"name": name})


def _nebula_fail_soft(method: Callable[..., list[dict]]) -> Callable[..., list[dict]]:
    @functools.wraps(method)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> list[dict]:
        try:
            return method(self, *args, **kwargs)
        except Exception as exc:  # fail-soft, like store_query.run_rows
            logger.warning("analytics query failed: {e}", e=exc)
            return []

    return wrapper


class NebulaCommunitiesGraphOps:
    def __init__(self, store: Any):
        self._store = store

    def _exec(self, stmt: str) -> list[dict]:
        return list(self._store.structured_query(stmt) or [])

    @_nebula_fail_soft
    def community_overview(self, level: int, top_n: int) -> list[dict]:
        stmt = (
            f"MATCH (c:`Community`) WHERE c.`Community`.level == {int(level)} "
            "RETURN c.`Community`.title AS title, c.`Community`.summary AS summary, "
            "c.`Community`.member_count AS member_count "
            f"ORDER BY member_count DESC LIMIT {int(top_n)};"
        )
        return self._exec(stmt)

    @_nebula_fail_soft
    def entity_communities(self, name: str) -> list[dict]:
        from src.graph.nebula_store import _q, entity_vid

        vid = entity_vid(name)
        edge_rows = self._exec(
            f"GO FROM {_q(vid)} OVER `IN_COMMUNITY` YIELD dst(edge) AS cvid;"
        )
        cvids = [row.get("cvid") for row in edge_rows if row.get("cvid")]
        if not cvids:
            return []
        vid_list = ", ".join(_q(c) for c in cvids)
        prop_rows = self._exec(
            f"FETCH PROP ON `Community` {vid_list} YIELD "
            "`Community`.level AS level, `Community`.title AS title, "
            "`Community`.summary AS summary;"
        )
        return [
            {"level": row.get("level"), "title": row.get("title"), "summary": row.get("summary")}
            for row in prop_rows
        ]


def build_communities_graph_ops(store: Any) -> CommunitiesGraphOps:
    if settings.graph.backend == "nebula":
        return NebulaCommunitiesGraphOps(store)
    return Neo4jCommunitiesGraphOps(store)
