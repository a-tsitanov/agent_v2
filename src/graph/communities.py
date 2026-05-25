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
from typing import Any

from loguru import logger

from src.workflow.contracts import CommunityRef

# Name of the transient in-memory GDS projection.  Dropped at the end of
# every run (and defensively re-created with IF-exists handling) so reruns
# never collide.
_GDS_GRAPH_NAME = "kb-communities"

# 1. Cypher projection of the __Entity__ sub-graph.  Cypher projection
#    (``gds.graph.project`` aggregation form) handles the arbitrary
#    relationship types the KG extractor emits without enumerating each
#    type up front.  Self-/dangling nodes are included via OPTIONAL MATCH
#    so isolated entities still appear (they land in singleton communities
#    that the min_size floor drops).
_PROJECT_CYPHER = f"""
MATCH (s:__Entity__)
OPTIONAL MATCH (s)-[r]->(t:__Entity__)
RETURN gds.graph.project(
    '{_GDS_GRAPH_NAME}',
    s,
    t,
    {{ sourceNodeLabels: labels(s), targetNodeLabels: labels(t),
       relationshipType: type(r) }}
)
"""

# 2. Leiden stream — community per node, read back the entity name.
_LEIDEN_STREAM_CYPHER = f"""
CALL gds.leiden.stream('{_GDS_GRAPH_NAME}', {{ randomSeed: 19 }})
YIELD nodeId, communityId
RETURN gds.util.asNode(nodeId).name AS name, communityId AS communityId
"""

# 5. Drop the transient projection (idempotent: failIfMissing=false).
_DROP_CYPHER = f"CALL gds.graph.drop('{_GDS_GRAPH_NAME}', false) YIELD graphName"

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

    try:
        # Re-project from scratch: drop any stale projection first, then
        # build a fresh one (a leftover projection from a crashed run would
        # otherwise make gds.graph.project fail with "already exists").
        await asyncio.to_thread(_run_query, store, _DROP_CYPHER)
        await asyncio.to_thread(_run_query, store, _PROJECT_CYPHER)
        rows = await asyncio.to_thread(_run_query, store, _LEIDEN_STREAM_CYPHER)
    except Exception as exc:  # noqa: BLE001
        logger.warning("communities: GDS Leiden detection failed: {e}", e=exc)
        # Best-effort cleanup so we don't leak the projection.
        with contextlib.suppress(Exception):
            await asyncio.to_thread(_run_query, store, _DROP_CYPHER)
        return []

    communities = _group_by_community(rows, min_size=min_size, level=level)
    logger.info(
        "communities: detected {n} communities (>= {m} members) from {r} rows",
        n=len(communities), m=min_size, r=len(rows),
    )

    # Persist :Community nodes + member links (idempotent MERGE).
    try:
        await asyncio.to_thread(_run_query, store, _COMMUNITY_CONSTRAINT)
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
            await asyncio.to_thread(_run_query, store, _DROP_CYPHER)

    return communities


__all__ = ["detect_communities"]
