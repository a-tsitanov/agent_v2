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
import hashlib
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
#    ``relationshipProperties: {{ weight: coalesce(r.weight, 1.0) }}``
#    projects the merge-layer edge weight (distinct co-occurrence count)
#    so Leiden runs WEIGHTED — dense, repeatedly-attested ties dominate
#    the partition.  ``coalesce`` defends against legacy edges written
#    before weights were meaningful.  The result is aliased ``AS g`` so
#    ``_projection_stats`` can read nodeCount/relationshipCount back.
def _project_cypher(graph_name: str) -> str:
    return f"""
MATCH (s:__Entity__)
OPTIONAL MATCH (s)-[r]->(t:__Entity__)
RETURN gds.graph.project(
    '{graph_name}',
    s,
    t,
    {{ sourceNodeLabels: labels(s), targetNodeLabels: labels(t),
       relationshipType: type(r),
       relationshipProperties: {{ weight: coalesce(r.weight, 1.0) }} }},
    {{ undirectedRelationshipTypes: ['*'] }}
) AS g
"""


# 2. Leiden stream — full dendrogram per node.  ``includeIntermediate
#    Communities: true`` yields ``intermediateCommunityIds`` (a list per
#    node, finest→coarsest; its LAST element == the final ``communityId``).
#    We read that list back as ``ids`` so ``_group_by_levels`` can build the
#    HIERARCHY.  The legacy single-level path is just ``max_levels=1`` over
#    the same stream (coarsest column = ids[-1] = today's ``communityId``).
def _leiden_stream_cypher(graph_name: str) -> str:
    return f"""
CALL gds.leiden.stream(
    '{graph_name}',
    {{ randomSeed: 19, includeIntermediateCommunities: true,
       relationshipWeightProperty: 'weight' }}
)
YIELD nodeId, communityId, intermediateCommunityIds
RETURN gds.util.asNode(nodeId).name AS name,
       communityId AS communityId,
       intermediateCommunityIds AS ids
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

# Hierarchy rebuild prunes EVERY level at once: a re-run of Leiden can
# produce a different number of dendrogram levels (and renumbered ids), so
# any prior ``:Community`` (at any level, with its PARENT_OF / IN_COMMUNITY
# / summary edges) would otherwise leak.  Used by ``detect_hierarchy``.
_PRUNE_ALL_CYPHER = """
MATCH (c:Community)
DETACH DELETE c
"""

# Read the PRIOR build's reports BEFORE the prune-all wipe so a community
# whose (level, members_hash) is unchanged can carry its report over instead
# of being re-summarised.  Only nodes that actually have a report are read.
_READ_OLD_REPORTS_CYPHER = """
MATCH (c:Community)
WHERE c.members_hash IS NOT NULL AND c.report IS NOT NULL
RETURN c.level AS level, c.members_hash AS h, c.report AS report,
       c.title AS title, c.summary AS summary, c.report_vec AS report_vec,
       c.summarized_at AS summarized_at
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
SET c.member_count = $member_count, c.members_hash = $members_hash,
    c.updated = timestamp()
FOREACH (_ IN CASE WHEN $carry_report IS NULL THEN [] ELSE [1] END |
    SET c.report = $carry_report, c.title = $carry_title,
        c.summary = $carry_summary, c.report_vec = $carry_report_vec,
        c.summarized_at = coalesce($carry_summarized_at, timestamp()))
WITH c
OPTIONAL MATCH (c)<-[old:IN_COMMUNITY]-(:__Entity__)
DELETE old
WITH c
UNWIND $members AS member_name
MATCH (e:__Entity__ {name: member_name})
MERGE (e)-[:IN_COMMUNITY]->(c)
"""

# Sub-community MERGE for level k > 0 (the finer dendrogram columns).  No
# entity ``IN_COMMUNITY`` links at level > 0 — only level 0 carries those
# (back-compat).  Instead we wire ``(parent:Community {level:$level-1})-
# [:PARENT_OF]->(this)`` coarser→finer so the dendrogram is navigable.  The
# parent is MATCHed (NOT merged): the caller writes communities
# coarsest-first (``_group_by_levels`` sorts by level ascending), so the
# level-$level-1 parent already exists by the time a level-$level child is
# written — ordering-DEPENDENT by design.
_MERGE_SUBCOMMUNITY_CYPHER = """
MERGE (c:Community {id: $community_id, level: $level})
SET c.member_count = $member_count, c.members_hash = $members_hash,
    c.updated = timestamp()
FOREACH (_ IN CASE WHEN $carry_report IS NULL THEN [] ELSE [1] END |
    SET c.report = $carry_report, c.title = $carry_title,
        c.summary = $carry_summary, c.report_vec = $carry_report_vec,
        c.summarized_at = coalesce($carry_summarized_at, timestamp()))
WITH c
MATCH (p:Community {id: $parent_id, level: $level - 1})
MERGE (p)-[:PARENT_OF]->(c)
"""


def _projection_stats(project_rows: list[dict]) -> dict[str, int]:
    """Extract ``{nodes, rels}`` from the ``gds.graph.project`` result row
    (aliased ``AS g``).  Returns zeros when the projection produced no
    row — lets callers distinguish an EMPTY/disconnected entity graph
    (0 nodes → Leiden finds nothing) from a GDS ERROR (raised, caught
    separately) from an all-singletons graph (nodes > 0 but 0 communities
    survive ``min_size``)."""
    if not project_rows:
        return {"nodes": 0, "rels": 0}
    g = project_rows[0].get("g") if isinstance(project_rows[0], dict) else None
    if not isinstance(g, dict):
        return {"nodes": 0, "rels": 0}
    return {
        "nodes": int(g.get("nodeCount") or 0),
        "rels": int(g.get("relationshipCount") or 0),
    }


def _run_query(store: Any, cypher: str, params: dict | None = None) -> list[dict]:
    """Thin wrapper around the store's ``structured_query``.

    Kept as a sync helper so callers can dispatch it via
    ``asyncio.to_thread`` (the Neo4j driver is blocking) exactly like the
    ER helpers do.  Returns the rows (or ``[]``).
    """
    rows = store.structured_query(cypher, param_map=params or {})
    return list(rows or [])


async def _read_old_reports(store: Any | None) -> dict[tuple[int, str], dict]:
    """{(level, members_hash) -> {report,title,summary,report_vec}} from the
    PRIOR build, read BEFORE prune so unchanged communities keep their
    report.  Best-effort: [] / {} on None store or any error (logged)."""
    if store is None:
        return {}
    try:
        rows = await asyncio.to_thread(_run_query, store, _READ_OLD_REPORTS_CYPHER)
    except Exception as exc:  # noqa: BLE001
        logger.warning("communities: read old reports failed: {e}", e=exc)
        return {}
    out: dict[tuple[int, str], dict] = {}
    for r in rows or []:
        h = r.get("h")
        if h is None:
            continue
        out[(int(r.get("level") or 0), h)] = {
            "report": r.get("report"), "title": r.get("title"),
            "summary": r.get("summary"), "report_vec": r.get("report_vec"),
            "summarized_at": r.get("summarized_at"),
        }
    return out


def members_hash(members: list[str]) -> str:
    """Order-insensitive content hash of a community's members.

    Lets a re-run skip re-summarising a community whose membership is
    unchanged (``members_hash`` persisted on the ``:Community`` node).  The
    ``\\x1f`` (unit separator) join is collision-safe vs. member names that
    might themselves contain commas/spaces.
    """
    return hashlib.sha256("\x1f".join(sorted(members)).encode("utf-8")).hexdigest()


def _group_by_levels(
    rows: list[dict], *, min_size: int, max_levels: int,
) -> list[CommunityRef]:
    """Map a Leiden dendrogram stream into ``CommunityRef``s across levels.

    ``rows``: ``[{name, ids:[finest..coarsest]}]`` (the
    ``intermediateCommunityIds`` list per node).  level 0 == coarsest
    (``ids[-1]`` == today's ``communityId``); level k == ``ids[-(k+1)]``
    (finer as k grows).  ``parent_id`` of a level-k community == the id of
    the level-(k-1) (coarser) community its members map to.  Pure /
    unit-testable; communities below ``min_size`` are dropped.
    """
    if not rows:
        return []
    # node name → [level0_cid, level1_cid, ...] (coarsest..finer).  Rows may
    # carry ragged ``ids`` (a degenerate single-level run yields a bare int
    # column); guard the first valid row for ``depth``.
    node_level_cid: dict[str, list[str]] = {}
    depth = 0
    for r in rows:
        if not isinstance(r, dict):
            continue
        name = r.get("name")
        ids = r.get("ids")
        if not name or not isinstance(ids, (list, tuple)) or not ids:
            continue
        d = min(len(ids), max_levels)
        depth = max(depth, d)
        node_level_cid[str(name)] = [str(ids[-(k + 1)]) for k in range(d)]
    if not node_level_cid:
        return []

    out: list[CommunityRef] = []
    for k in range(depth):
        members_by_cid: dict[str, list[str]] = {}
        parent_by_cid: dict[str, str] = {}
        for name, cids in node_level_cid.items():
            if k >= len(cids):
                continue
            cid = cids[k]
            members_by_cid.setdefault(cid, []).append(name)
            if k > 0:
                parent_by_cid[cid] = cids[k - 1]  # coarser level is the parent
        # INVARIANT (Leiden nesting): a level-(k-1) community is the union of
        # its level-k children, so member_count(parent) >= member_count(child).
        # Hence `min_size` can never drop a parent while keeping a child →
        # the level>0 `MATCH (parent)` in _MERGE_SUBCOMMUNITY_CYPHER never
        # orphans. Holds only while intermediateCommunityIds stays strictly
        # nested (GDS Leiden guarantees this).
        for cid, members in members_by_cid.items():
            if len(members) < min_size:
                continue
            members = sorted(members)
            out.append(CommunityRef(
                community_id=cid, level=k, members=members,
                members_hash=members_hash(members),
                parent_id=parent_by_cid.get(cid, ""),
                needs_report=True,
            ))
    # Deterministic ordering — coarsest level first, then largest, then id.
    out.sort(key=lambda c: (c.level, -c.member_count, c.community_id))
    return out


def _coarsest_from_rows(
    rows: list[dict], *, min_size: int, level: int,
) -> list[CommunityRef]:
    """Group the COARSEST dendrogram column (today's ``communityId``) into
    ``CommunityRef``s, dropping communities below ``min_size``.

    Back-compat shim for the single-level ``detect_communities`` path: it
    reads ``communityId`` (the last/coarsest column == ``ids[-1]``) so the
    grouping is identical to the legacy ``_group_by_community`` while ALSO
    stamping ``members_hash`` (the new ``:Community`` field).  ``level``
    tags the output exactly as the caller asked (legacy ``level`` param).
    """
    buckets: dict[str, list[str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = row.get("name")
        cid = row.get("communityId")
        if cid is None:
            ids = row.get("ids")
            if isinstance(ids, (list, tuple)) and ids:
                cid = ids[-1]
        if not name or cid is None:
            continue
        buckets.setdefault(str(cid), []).append(str(name))
    out: list[CommunityRef] = []
    for cid, members in buckets.items():
        if len(members) < min_size:
            continue
        members = sorted(members)
        out.append(CommunityRef(
            community_id=cid, level=level, members=members,
            members_hash=members_hash(members), parent_id="",
            needs_report=True,
        ))
    out.sort(key=lambda c: (-c.member_count, c.community_id))
    return out


async def detect_communities(
    store: Any | None,
    *,
    min_size: int = 3,
    level: int = 0,
) -> list[CommunityRef]:
    """Run GDS Leiden over ``__Entity__`` and materialise ``:Community``.

    SINGLE-LEVEL (back-compat) path: detects only the coarsest dendrogram
    column (today's ``communityId``) and writes it at the requested
    ``level`` with ``(:__Entity__)-[:IN_COMMUNITY]->(:Community)`` links and
    a level-scoped prune — IDENTICAL to the pre-hierarchy behaviour, now
    additionally stamping ``members_hash``.  For the full dendrogram use
    ``detect_hierarchy``.

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
        proj_rows = await asyncio.to_thread(_run_query, store, _project_cypher(graph_name))
        stats = _projection_stats(proj_rows)
        rows = await asyncio.to_thread(_run_query, store, _leiden_stream_cypher(graph_name))
    except Exception as exc:  # noqa: BLE001
        # A genuine GDS/Cypher ERROR (vs an empty graph) — surfaced loudly
        # so "0 communities" is never silently mistaken for an infra fault.
        logger.error("communities: GDS Leiden detection FAILED: {e}", e=exc)
        # Best-effort cleanup so we don't leak the projection.
        with contextlib.suppress(Exception):
            await asyncio.to_thread(_run_query, store, _drop_cypher(graph_name))
        return []

    if not rows:
        logger.warning(
            "communities: Leiden returned 0 rows — projected {n} entities / "
            "{r} relationships (empty or disconnected __Entity__ graph?)",
            n=stats["nodes"], r=stats["rels"],
        )
    communities = _coarsest_from_rows(rows, min_size=min_size, level=level)
    logger.info(
        "communities: detected {n} communities (>= {m} members) from {r} "
        "rows — projected {pn} entities / {pr} relationships",
        n=len(communities), m=min_size, r=len(rows),
        pn=stats["nodes"], pr=stats["rels"],
    )

    # Persist :Community nodes + member links (idempotent MERGE).
    try:
        await asyncio.to_thread(_run_query, store, _COMMUNITY_CONSTRAINT)
        from src.graph.index import ensure_community_indexes
        await asyncio.to_thread(ensure_community_indexes, store)
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
                    "members_hash": comm.members_hash,
                    "members": comm.members,
                    # No carry-over on the single-level path; the MERGE's
                    # FOREACH no-ops when $carry_report is NULL.  Params still
                    # required so the parameterised query doesn't error.
                    "carry_report": None, "carry_title": None,
                    "carry_summary": None, "carry_report_vec": None,
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


async def detect_hierarchy(
    store: Any | None,
    *,
    max_levels: int,
    min_size: int = 3,
) -> list[CommunityRef]:
    """Run GDS Leiden over ``__Entity__`` and materialise the community
    HIERARCHY (up to ``max_levels`` dendrogram levels).

    level 0 == coarsest (today's ``communityId``) and keeps
    ``(:__Entity__)-[:IN_COMMUNITY]->(:Community {level:0})`` exactly as
    ``detect_communities`` does.  level k>0 (finer) carries NO entity links;
    instead ``(:Community {level:k-1})-[:PARENT_OF]->(:Community {level:k})``
    wires the dendrogram coarser→finer.  Every level stamps
    ``member_count`` + ``members_hash``.

    A rebuild prunes ALL prior ``:Community`` (every level) up front since
    the dendrogram depth/ids can change between runs.

    Returns the detected communities across all levels (coarsest first).
    Fail-safe: ``store is None`` or ANY GDS / Cypher error → ``[]`` (logged,
    never raised).
    """
    if store is None:
        logger.info("communities: no graph store — skipping hierarchy detection")
        return []

    graph_name = _new_graph_name()

    try:
        await asyncio.to_thread(_run_query, store, _drop_cypher(graph_name))
        proj_rows = await asyncio.to_thread(_run_query, store, _project_cypher(graph_name))
        stats = _projection_stats(proj_rows)
        rows = await asyncio.to_thread(_run_query, store, _leiden_stream_cypher(graph_name))
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "communities: GDS Leiden hierarchy detection FAILED: {e}", e=exc,
        )
        with contextlib.suppress(Exception):
            await asyncio.to_thread(_run_query, store, _drop_cypher(graph_name))
        return []

    if not rows:
        logger.warning(
            "communities: Leiden returned 0 rows — projected {n} entities / "
            "{r} relationships (empty or disconnected __Entity__ graph?)",
            n=stats["nodes"], r=stats["rels"],
        )
    communities = _group_by_levels(rows, min_size=min_size, max_levels=max_levels)
    n_levels = len({c.level for c in communities})
    logger.info(
        "communities: detected {n} communities across {L} level(s) "
        "(>= {m} members) from {r} rows — projected {pn} entities / "
        "{pr} relationships",
        n=len(communities), L=n_levels, m=min_size, r=len(rows),
        pn=stats["nodes"], pr=stats["rels"],
    )

    # Read the PRIOR build's reports BEFORE we prune so a community whose
    # (level, members_hash) is unchanged carries its report over (no
    # re-summarisation).  Best-effort: {} on any error.
    old_reports = await _read_old_reports(store)

    # Carry forward unchanged reports: a community whose (level, members_hash)
    # matches a prior report keeps it (needs_report=False); the (possibly
    # model_copied) refs are returned so the build-workflow sees the right
    # needs_report flags.  Pre-compute the per-community carry param block.
    carried_refs: list[CommunityRef] = []
    carry_params: list[dict] = []
    n_carried = 0
    for comm in communities:
        carried = old_reports.get((comm.level, comm.members_hash))
        if carried and carried.get("report"):
            comm = comm.model_copy(update={"needs_report": False})
            params = {
                "carry_report": carried.get("report"),
                "carry_title": carried.get("title"),
                "carry_summary": carried.get("summary"),
                "carry_report_vec": carried.get("report_vec"),
                # Preserve the ORIGINAL summarisation time — a carried report
                # was NOT re-summarised, so summarized_at must reflect content
                # freshness, not the rebuild time (keeps staleness logic sane).
                "carry_summarized_at": carried.get("summarized_at"),
            }
            n_carried += 1
        else:
            params = {
                "carry_report": None, "carry_title": None,
                "carry_summary": None, "carry_report_vec": None,
                "carry_summarized_at": None,
            }
        carried_refs.append(comm)
        carry_params.append(params)
    communities = carried_refs
    if n_carried:
        logger.info(
            "communities: carried over {n} unchanged report(s) (skipping re-summarise)",
            n=n_carried,
        )

    # Persist the hierarchy.  Communities are sorted coarsest-first, so a
    # level-k node's level-(k-1) parent is always written before it.
    try:
        await asyncio.to_thread(_run_query, store, _COMMUNITY_CONSTRAINT)
        from src.graph.index import ensure_community_indexes
        await asyncio.to_thread(ensure_community_indexes, store)
        # Prune EVERY prior :Community (depth/ids can change between runs).
        await asyncio.to_thread(_run_query, store, _PRUNE_ALL_CYPHER)
        for comm, carry in zip(communities, carry_params):
            if comm.level == 0:
                # Coarsest: entity IN_COMMUNITY links (identical to today).
                await asyncio.to_thread(
                    _run_query, store, _MERGE_COMMUNITY_CYPHER,
                    {
                        "community_id": comm.community_id,
                        "level": comm.level,
                        "member_count": comm.member_count,
                        "members_hash": comm.members_hash,
                        "members": comm.members,
                        **carry,
                    },
                )
            else:
                # Finer: PARENT_OF edge, no entity links.
                await asyncio.to_thread(
                    _run_query, store, _MERGE_SUBCOMMUNITY_CYPHER,
                    {
                        "community_id": comm.community_id,
                        "level": comm.level,
                        "member_count": comm.member_count,
                        "members_hash": comm.members_hash,
                        "parent_id": comm.parent_id,
                        **carry,
                    },
                )
    except Exception as exc:  # noqa: BLE001
        logger.warning("communities: :Community hierarchy write failed: {e}", e=exc)
    finally:
        with contextlib.suppress(Exception):
            await asyncio.to_thread(_run_query, store, _drop_cypher(graph_name))

    return communities


__all__ = ["detect_communities", "detect_hierarchy", "members_hash"]
