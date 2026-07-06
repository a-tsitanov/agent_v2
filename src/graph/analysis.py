"""Read-only GDS-backed graph analysis (Track 7b).

OFFLINE / admin only — never the query hot path.  Mirrors
``communities.py``: every function is **fail-soft** (a ``None`` store or
ANY GDS / Cypher error is logged and yields a safe empty result, never
raised through the caller).  Re-uses the weighted ``__Entity__``
projection helpers so PageRank / WCC see the same graph Leiden does.

The GDS calls follow the GDS 2.x API but are UNVERIFIED against a live
install in this sandbox (no Neo4j/GDS) — same caveat as ``communities.py``.
Exposed via ``/admin/graph/*`` admin endpoints.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import Any

from loguru import logger

from src.graph.communities import (
    _drop_cypher,
    _new_graph_name,
    _project_cypher,
    _run_query,
)

# ── Cypher builders ──────────────────────────────────────────────────


def _pagerank_cypher(graph_name: str, top_n: int) -> str:
    """Weighted PageRank over the projected graph; top-N by score."""
    return f"""
CALL gds.pageRank.stream('{graph_name}', {{ relationshipWeightProperty: 'weight' }})
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).name AS name, score
ORDER BY score DESC
LIMIT {int(top_n)}
"""


def _personalized_pagerank_cypher(graph_name: str, top_n: int) -> str:
    """Personalized (seed-biased) weighted PageRank: rank entities by
    relevance to a set of seed entities (`$seeds` names → `sourceNodes`).

    The random-walk restart is biased toward the seeds, so high scorers
    are the entities most central *relative to the seeds* (e.g. "what's
    most connected to these two companies?") rather than globally."""
    return f"""
MATCH (s:__Entity__) WHERE s.name IN $seeds
WITH collect(id(s)) AS sources
CALL gds.pageRank.stream('{graph_name}', {{
    relationshipWeightProperty: 'weight', sourceNodes: sources
}})
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).name AS name, score
ORDER BY score DESC
LIMIT {int(top_n)}
"""


def _wcc_cypher(graph_name: str) -> str:
    """Weakly-connected-components stats (count + size distribution)."""
    return f"""
CALL gds.wcc.stats('{graph_name}')
YIELD componentCount, componentDistribution
RETURN componentCount, componentDistribution
"""


def _shortest_path_cypher(max_hops: int) -> str:
    """Native undirected shortest path between two named entities.

    ``max_hops`` is inlined (variable-length bounds can't be a param) and
    clamped to a sane ceiling so a pathological query can't walk the
    whole graph."""
    h = max(1, min(int(max_hops), 12))
    return f"""
MATCH (a:__Entity__ {{name: $source}}), (b:__Entity__ {{name: $target}})
MATCH p = shortestPath((a)-[*..{h}]-(b))
RETURN [n IN nodes(p) | n.name] AS path, length(p) AS hops
"""


# Pure Cypher stats — no GDS projection needed (cheap, run synchronously).
_STATS_CYPHER: dict[str, str] = {
    "entities": "MATCH (e:__Entity__) RETURN count(e) AS n",
    "relationships": (
        "MATCH (:__Entity__)-[r]->(:__Entity__) RETURN count(r) AS n"
    ),
    "degree": """
MATCH (e:__Entity__)
OPTIONAL MATCH (e)-[r]-(:__Entity__)
WITH e, count(r) AS deg
RETURN avg(deg) AS avg, percentileCont(deg, 0.5) AS p50,
       percentileCont(deg, 0.99) AS p99, max(deg) AS max
""",
    "dup": """
MATCH (e:__Entity__)
WITH toLower(e.name) AS lname, count(*) AS c
WHERE c > 1
RETURN count(*) AS dup_groups, sum(c) AS dup_entities
""",
    "communities": "MATCH (c:Community) RETURN count(c) AS n",
}


# ── projection lifecycle ─────────────────────────────────────────────


async def _with_projection(
    store: Any | None, fn: Callable[[str], Awaitable[Any]],
) -> Any | None:
    """Project the weighted ``__Entity__`` graph, run ``fn(graph_name)``,
    always drop the projection.  Returns ``None`` on a missing store or
    any error (logged) — callers map that to their empty result."""
    if store is None:
        return None
    graph_name = _new_graph_name()
    try:
        await asyncio.to_thread(_run_query, store, _drop_cypher(graph_name))
        await asyncio.to_thread(_run_query, store, _project_cypher(graph_name))
        return await fn(graph_name)
    except Exception as exc:
        logger.warning("graph analysis projection/algo failed: {e}", e=exc)
        return None
    finally:
        with contextlib.suppress(Exception):
            await asyncio.to_thread(_run_query, store, _drop_cypher(graph_name))


# ── public analysis functions ────────────────────────────────────────


async def pagerank(store: Any | None, *, top_n: int = 20) -> list[dict]:
    """Top-N entities by weighted PageRank (importance/centrality)."""
    async def _run(graph_name: str) -> list[dict]:
        rows = await asyncio.to_thread(
            _run_query, store, _pagerank_cypher(graph_name, top_n),
        )
        return [
            {"name": r.get("name"), "score": float(r.get("score") or 0.0)}
            for r in rows
            if isinstance(r, dict) and r.get("name")
        ]

    return (await _with_projection(store, _run)) or []


async def personalized_pagerank(
    store: Any | None, seeds: list[str], *, top_n: int = 20,
) -> list[dict]:
    """Top-N entities by PageRank biased toward `seeds` (seed entity names).

    Empty/blank seeds → `[]` (no projection run — there's nothing to bias
    toward).  Fail-soft like the rest: store/GDS error → `[]`."""
    cleaned = [s for s in (seeds or []) if s and str(s).strip()]
    if store is None or not cleaned:
        return []

    async def _run(graph_name: str) -> list[dict]:
        rows = await asyncio.to_thread(
            _run_query, store,
            _personalized_pagerank_cypher(graph_name, top_n),
            {"seeds": cleaned},
        )
        return [
            {"name": r.get("name"), "score": float(r.get("score") or 0.0)}
            for r in rows
            if isinstance(r, dict) and r.get("name")
        ]

    return (await _with_projection(store, _run)) or []


async def components(store: Any | None) -> dict:
    """Weakly-connected-component count + size distribution (connectivity
    health; large singleton fraction explains empty Leiden communities)."""
    async def _run(graph_name: str) -> dict:
        rows = await asyncio.to_thread(_run_query, store, _wcc_cypher(graph_name))
        if not rows:
            return {"component_count": 0, "distribution": {}}
        r = rows[0]
        return {
            "component_count": int(r.get("componentCount") or 0),
            "distribution": r.get("componentDistribution") or {},
        }

    return (await _with_projection(store, _run)) or {
        "component_count": 0, "distribution": {},
    }


async def shortest_path(
    store: Any | None, source: str, target: str, *, max_hops: int = 6,
) -> dict:
    """Shortest undirected path between two entities by name.

    ``{"path": [], "hops": -1}`` when there's no path / on any error."""
    empty = {"path": [], "hops": -1}
    if store is None:
        return empty
    try:
        rows = await asyncio.to_thread(
            _run_query, store, _shortest_path_cypher(max_hops),
            {"source": source, "target": target},
        )
    except Exception as exc:
        logger.warning("graph analysis shortest_path failed: {e}", e=exc)
        return empty
    if not rows or not isinstance(rows[0], dict):
        return empty
    r = rows[0]
    hops = r.get("hops")
    return {"path": r.get("path") or [], "hops": int(hops) if hops is not None else -1}


async def graph_stats(store: Any | None) -> dict:
    """Operational snapshot: entity/relationship counts, degree
    distribution (p50/p99/max), duplicate-name groups, community count.

    Feeds the live diagnostics from the 250k-scale assessment.  Every
    sub-query is independent + fail-soft so one failure doesn't void the
    rest."""
    out: dict[str, Any] = {
        "entities": 0,
        "relationships": 0,
        "degree": {"avg": 0.0, "p50": 0, "p99": 0, "max": 0},
        "duplicate_name_groups": 0,
        "duplicate_entities": 0,
        "communities": 0,
    }
    if store is None:
        return out

    async def _one(cypher: str) -> dict:
        try:
            rows = await asyncio.to_thread(_run_query, store, cypher)
        except Exception as exc:
            logger.warning("graph_stats sub-query failed: {e}", e=exc)
            return {}
        return rows[0] if rows and isinstance(rows[0], dict) else {}

    ent = await _one(_STATS_CYPHER["entities"])
    out["entities"] = int(ent.get("n") or 0)
    rel = await _one(_STATS_CYPHER["relationships"])
    out["relationships"] = int(rel.get("n") or 0)
    deg = await _one(_STATS_CYPHER["degree"])
    if deg:
        out["degree"] = {
            "avg": float(deg.get("avg") or 0.0),
            "p50": int(deg.get("p50") or 0),
            "p99": int(deg.get("p99") or 0),
            "max": int(deg.get("max") or 0),
        }
    dup = await _one(_STATS_CYPHER["dup"])
    out["duplicate_name_groups"] = int(dup.get("dup_groups") or 0)
    out["duplicate_entities"] = int(dup.get("dup_entities") or 0)
    comm = await _one(_STATS_CYPHER["communities"])
    out["communities"] = int(comm.get("n") or 0)
    return out


__all__ = [
    "components",
    "graph_stats",
    "pagerank",
    "personalized_pagerank",
    "shortest_path",
]
