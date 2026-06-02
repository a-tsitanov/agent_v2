"""Offline graph-community detection (Search R6, decision C1).

DECOUPLED / OFFLINE only — runs from the ``CommunityBuildWorkflow`` on
the dedicated ``kb-graph-build`` queue (admin endpoint / schedule), NEVER
on the query hot path.

``detect_communities`` runs Neo4j GDS Leiden over the ``__Entity__``
sub-graph and materialises ``:Community`` nodes:

  1. Project the ``__Entity__`` nodes + their relationships into an
     in-memory GDS graph (Cypher projection — works for the arbitrary
     relationship types the KG extractor emits).
  2. ``gds.leiden.stream`` → one ``communityId`` per entity.
  3. Group members by ``communityId``, drop communities below
     ``min_size``.
  4. Idempotently MERGE one ``:Community {id, level}`` node per group and
     link its members via ``(:__Entity__)-[:IN_COMMUNITY]->(:Community)``.
  5. Drop the in-memory projection.

EVERYTHING is fail-safe: a ``None`` store or any GDS / Cypher error is
logged and yields ``[]`` so the calling activity never raises through the
Temporal boundary (mirrors the defensive ``structured_query`` helpers in
``entity_resolution.py`` / ``storage/wikibase.py``).

All Cypher/GDS lives in this module's constants so it's a single place to
fix against the live GDS schema/version.  The exact GDS calls here are
written per the Neo4j GDS 2.x API but are UNVERIFIED against a live GDS
install in this sandbox (no Neo4j/GDS available) — see the R6 report.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import Any

from loguru import logger

from src.workflow.contracts import CommunityRef

# Prefix for the transient in-memory GDS projection.  The actual name is
# generated PER CALL (``_new_graph_name``) with a random suffix so two
# concurrent rebuilds (graph_build worker concurrency >= 2) never collide —
# one run's ``gds.graph.drop`` must not kill another's live projection.
_GDS_GRAPH_PREFIX = "kb-communities"


def _new_graph_name() -> str:
    """Unique GDS projection name for a single ``detect_communities`` call.

    ``detect_communities`` runs inside a Temporal ACTIVITY (not a workflow
    body), so non-determinism is fine here.  A per-call name isolates
    concurrent rebuilds from each other's project/drop."""
    return f"{_GDS_GRAPH_PREFIX}-{uuid.uuid4().hex[:8]}"


# 1. Cypher projection of the __Entity__ sub-graph.  Cypher projection
#    (``gds.graph.project`` aggregation form) handles the arbitrary
#    relationship types the KG extractor emits without enumerating each
#    type up front.  Self-/dangling nodes are included via OPTIONAL MATCH
#    so isolated entities still appear (they land in singleton communities
#    that the min_size floor drops).
#
#    ``undirectedRelationshipTypes: ['*']`` (5th / configuration arg) makes
#    EVERY projected type undirected — REQUIRED by Leiden, which rejects a
#    directed graph ("works only with undirected graphs").  Edge direction
#    is meaningless for community detection on a KG anyway.
def _project_cypher(graph_name: str) -> str:
    return f"""
MATCH (s:__Entity__)
OPTIONAL MATCH (s)-[r]->(t:__Entity__)
RETURN gds.graph.project(
    '{graph_name}',
    s,
    t,
    {{ sourceNodeLabels: labels(s), targetNodeLabels: labels(t),
       relationshipType: type(r) }},
    {{ undirectedRelationshipTypes: ['*'] }}
)
"""


# 2. Leiden stream — community per node, read back the entity name.
def _leiden_stream_cypher(graph_name: str) -> str:
    return f"""
CALL gds.leiden.stream('{graph_name}', {{ randomSeed: 19 }})
YIELD nodeId, communityId
RETURN gds.util.asNode(nodeId).name AS name, communityId AS communityId
"""


# 5. Drop the transient projection (idempotent: failIfMissing=false).
def _drop_cypher(graph_name: str) -> str:
    return f"CALL gds.graph.drop('{graph_name}', false) YIELD graphName"


# Before re-writing a level's communities, DETACH DELETE the prior ones for
# THAT level only: a re-run of Leiden can produce fewer/renumbered ids, so
# stale ``:Community {level:$level}`` nodes (with orphaned summaries) would
# otherwise linger.  Scoped to ``$level`` so other levels are untouched.
_PRUNE_LEVEL_CYPHER = """
MATCH (c:Community {level: $level})
DETACH DELETE c
"""

# Constraint backing the :Community MERGE (idempotent, prevents dupes).
_COMMUNITY_CONSTRAINT = (
    "CREATE CONSTRAINT community_key IF NOT EXISTS "
    "FOR (c:Community) REQUIRE (c.id, c.level) IS UNIQUE"
)

# 4. Idempotent MERGE of the :Community node + member links.  Keyed on
#    (id, level) so a re-run UPDATES the same node rather than duplicating.
#    Member links are MERGEd too; stale links from a previous detection are
#    cleared first so a re-shuffled community doesn't keep ghost members.
_MERGE_COMMUNITY_CYPHER = """
MERGE (c:Community {id: $community_id, level: $level})
SET c.member_count = $member_count, c.updated = timestamp()
WITH c
OPTIONAL MATCH (c)<-[old:IN_COMMUNITY]-(:__Entity__)
DELETE old
WITH c
UNWIND $members AS member_name
MATCH (e:__Entity__ {name: member_name})
MERGE (e)-[:IN_COMMUNITY]->(c)
"""


def _run_query(store: Any, cypher: str, params: dict | None = None) -> list[dict]:
    """Thin wrapper around the store's ``structured_query``.

    Kept as a sync helper so callers can dispatch it via
    ``asyncio.to_thread`` (the Neo4j driver is blocking) exactly like the
    ER helpers do.  Returns the rows (or ``[]``).
    """
    rows = store.structured_query(cypher, param_map=params or {})
    return list(rows or [])


def _group_by_community(
    rows: list[dict], *, min_size: int, level: int,
) -> list[CommunityRef]:
    """Group ``{name, communityId}`` stream rows into ``CommunityRef``s,
    dropping communities below ``min_size``.  Pure / unit-testable."""
    buckets: dict[int, list[str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = row.get("name")
        cid = row.get("communityId")
        if not name or cid is None:
            continue
        buckets.setdefault(int(cid), []).append(str(name))
    out: list[CommunityRef] = []
    for cid, members in buckets.items():
        if len(members) < min_size:
            continue
        out.append(CommunityRef(
            community_id=cid, level=level, members=sorted(members),
        ))
    # Deterministic ordering — largest first, then by id.
    out.sort(key=lambda c: (-c.member_count, c.community_id))
    return out


async def detect_communities(
    store: Any | None,
    *,
    min_size: int = 3,
    level: int = 0,
) -> list[CommunityRef]:
    """Run GDS Leiden over ``__Entity__`` and materialise ``:Community``.

    Returns the detected communities (``member_count >= min_size``).
    Fail-safe: ``store is None`` or ANY GDS / Cypher error → ``[]`` (logged,
    never raised) so the calling activity stays green.
    """
    if store is None:
        logger.info("communities: no graph store — skipping detection")
        return []

    # Unique per-call projection name so concurrent rebuilds don't collide.
    graph_name = _new_graph_name()

    try:
        # Re-project from scratch: drop any stale projection first, then
        # build a fresh one (a leftover projection from a crashed run would
        # otherwise make gds.graph.project fail with "already exists").
        await asyncio.to_thread(_run_query, store, _drop_cypher(graph_name))
        await asyncio.to_thread(_run_query, store, _project_cypher(graph_name))
        rows = await asyncio.to_thread(_run_query, store, _leiden_stream_cypher(graph_name))
    except Exception as exc:  # noqa: BLE001
        logger.warning("communities: GDS Leiden detection failed: {e}", e=exc)
        # Best-effort cleanup so we don't leak the projection.
        with contextlib.suppress(Exception):
            await asyncio.to_thread(_run_query, store, _drop_cypher(graph_name))
        return []

    communities = _group_by_community(rows, min_size=min_size, level=level)
    logger.info(
        "communities: detected {n} communities (>= {m} members) from {r} rows",
        n=len(communities), m=min_size, r=len(rows),
    )

    # Persist :Community nodes + member links (idempotent MERGE).
    try:
        await asyncio.to_thread(_run_query, store, _COMMUNITY_CONSTRAINT)
        # Prune the prior run's communities for THIS level FIRST so a
        # rebuild starts clean (Leiden may renumber/shrink ids, leaving
        # ghost :Community nodes + orphaned summaries).  Level-scoped.
        await asyncio.to_thread(
            _run_query, store, _PRUNE_LEVEL_CYPHER, {"level": level},
        )
        for comm in communities:
            await asyncio.to_thread(
                _run_query, store, _MERGE_COMMUNITY_CYPHER,
                {
                    "community_id": comm.community_id,
                    "level": comm.level,
                    "member_count": comm.member_count,
                    "members": comm.members,
                },
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("communities: :Community write failed: {e}", e=exc)
        # Detection still succeeded; surface what we grouped so the
        # workflow can at least attempt summaries.
    finally:
        with contextlib.suppress(Exception):
            await asyncio.to_thread(_run_query, store, _drop_cypher(graph_name))

    return communities


__all__ = ["detect_communities"]
