"""Backend-dispatched community BUILD write-back (the `:Community` +
`IN_COMMUNITY`/`PARENT_OF` materialisation of community detection).

`Neo4jCommunityWriteback` wraps the existing Cypher constants in
`communities.py` verbatim (default path, byte-for-byte unchanged).
`NebulaCommunityWriteback` translates the same operations to nGQL.
Only the BUILD stage lives here; SUMMARIZE (report write) and READ
(map/lexical/descent) remain neo4j-only for now.
"""
from __future__ import annotations

import hashlib
from typing import Any, Protocol

from src.config import settings
from src.graph.communities import (
    _COMMUNITY_CONSTRAINT,
    _MERGE_COMMUNITY_CYPHER,
    _MERGE_SUBCOMMUNITY_CYPHER,
    _PRUNE_ALL_CYPHER,
    _PRUNE_LEVEL_CYPHER,
    _READ_OLD_REPORTS_CYPHER,
)

# Max chars in ONE nGQL statement we will send.  Nebula's graphd rejects
# anything over `max_allowed_query_size` (default 4 MiB / 4194304) with
# `SyntaxError: Query is too large`.  A level-0 root community can hold every
# entity in the graph (60117 members ⇒ a 4568933-char INSERT EDGE), so member
# edges are emitted in <=budget batches instead of one statement.  Held well
# under the server cap so a bumped VID width or longer tag list can't creep
# over it.  Module-level so tests can monkeypatch it small.
_MAX_STMT_CHARS = 1_000_000


def community_vid(community_id: str, level: int) -> str:
    """Stable 128-bit VID (32-hex-char) for a community, scoped by level.

    Mirrors `nebula_store.entity_vid` (same digest_size=16 blake2b) so the
    whole graph shares one VID scheme under FIXED_STRING(32)."""
    key = f"{community_id}:{int(level)}"
    return hashlib.blake2b(key.encode("utf-8"), digest_size=16).hexdigest()


def _carry_params(carry: dict | None) -> dict:
    """Map the clean-keyed carry dict to the `carry_*` params the neo4j
    MERGE Cypher expects (missing/None -> None), preserving today's shape."""
    c = carry or {}
    return {
        "carry_report": c.get("report"),
        "carry_title": c.get("title"),
        "carry_summary": c.get("summary"),
        "carry_report_vec": c.get("report_vec"),
        "carry_summarized_at": c.get("summarized_at"),
    }


class CommunityWriteback(Protocol):
    def ensure_schema(self) -> None: ...
    def read_old_reports(self) -> list[dict]: ...
    def prune_level(self, level: int) -> None: ...
    def prune_all(self) -> None: ...
    def merge_community(self, *, community_id: str, level: int, member_count: int,
                        members_hash: str, members: list[str], carry: dict | None) -> None: ...
    def merge_subcommunity(self, *, community_id: str, level: int, parent_id: str,
                           member_count: int, members_hash: str, members: list[str],
                           carry: dict | None) -> None: ...


class Neo4jCommunityWriteback:
    """Runs the historical Cypher constants verbatim — zero behaviour change."""

    def __init__(self, store: Any):
        self._store = store

    def _run(self, cypher: str, params: dict | None = None) -> list[dict]:
        rows = self._store.structured_query(cypher, param_map=params or {})
        return list(rows or [])

    def ensure_schema(self) -> None:
        self._run(_COMMUNITY_CONSTRAINT)
        from src.graph.index import ensure_community_indexes
        ensure_community_indexes(self._store)

    def read_old_reports(self) -> list[dict]:
        return self._run(_READ_OLD_REPORTS_CYPHER)

    def prune_level(self, level: int) -> None:
        self._run(_PRUNE_LEVEL_CYPHER, {"level": level})

    def prune_all(self) -> None:
        self._run(_PRUNE_ALL_CYPHER)

    def merge_community(self, *, community_id, level, member_count,
                        members_hash, members, carry) -> None:
        self._run(_MERGE_COMMUNITY_CYPHER, {
            "community_id": community_id, "level": level,
            "member_count": member_count, "members_hash": members_hash,
            "members": members, **_carry_params(carry),
        })

    def merge_subcommunity(self, *, community_id, level, parent_id,
                           member_count, members_hash, members, carry) -> None:
        self._run(_MERGE_SUBCOMMUNITY_CYPHER, {
            "community_id": community_id, "level": level, "parent_id": parent_id,
            "member_count": member_count, "members_hash": members_hash,
            "members": members, **_carry_params(carry),
        })


class NebulaCommunityWriteback:
    """nGQL community BUILD write-back. INSERT is upsert-by-VID; both call
    sites prune before merge, so INSERT-overwrite == neo4j MERGE+FOREACH on a
    fresh node (no divergence). `report_vec` is never written to the vertex
    (it lives in Milvus). Values are inline-quoted (nebula binds no params)."""

    def __init__(self, store: Any):
        self._store = store

    def _exec(self, stmt: str) -> list[dict]:
        return list(self._store.structured_query(stmt) or [])

    def ensure_schema(self) -> None:
        # Community TAG + index are created by nebula_schema.ensure_schema.
        return None

    def _insert_community_vertex(self, *, cvid, community_id, level,
                                 member_count, members_hash, carry) -> None:
        import time

        from src.graph.nebula_store import _q
        c = carry or {}
        updated = int(time.time() * 1000)
        self._exec(
            "INSERT VERTEX `Community` "
            "(id, level, member_count, members_hash, updated, "
            "report, title, summary, summarized_at) VALUES "
            f"{_q(cvid)}:({_q(community_id)}, {int(level)}, {int(member_count)}, "
            f"{_q(members_hash)}, {updated}, "
            f"{_q(c.get('report') or '')}, {_q(c.get('title') or '')}, "
            f"{_q(c.get('summary') or '')}, {int(c.get('summarized_at') or 0)});"
        )

    def _insert_member_edges(self, *, cvid, level, members) -> None:
        from src.graph.nebula_store import _q, entity_vid
        # No stale-clear needed: both call sites prune (prune_level/prune_all
        # via DELETE VERTEX ... WITH EDGE) BEFORE merge, so the community vertex
        # is fresh with no incoming IN_COMMUNITY edges — the design's central
        # prune-before-merge invariant. (neo4j's MERGE clears stale inline;
        # prune-first makes that redundant on nebula, and avoids a fragile
        # GO|DELETE pipe on the main write path.)
        if not members:
            return
        # Batch by RENDERED SIZE, not member count: one statement per
        # `_MAX_STMT_CHARS` worth of VALUES, so a root community with every
        # entity in it can't exceed nebula's max query size (see the constant).
        head = "INSERT EDGE `IN_COMMUNITY` (level) VALUES "
        budget = _MAX_STMT_CHARS - len(head) - 1        # -1 for the ';'
        batch: list[str] = []
        used = 0
        for m in members:
            val = f"{_q(entity_vid(m))}->{_q(cvid)}:({int(level)})"
            add = len(val) + (2 if batch else 0)        # ", " separator
            if batch and used + add > budget:
                self._exec(head + ", ".join(batch) + ";")
                batch, used, add = [], 0, len(val)
            batch.append(val)
            used += add
        if batch:
            self._exec(head + ", ".join(batch) + ";")

    def merge_community(self, *, community_id, level, member_count,
                        members_hash, members, carry) -> None:
        cvid = community_vid(community_id, level)
        self._insert_community_vertex(
            cvid=cvid, community_id=community_id, level=level,
            member_count=member_count, members_hash=members_hash, carry=carry)
        self._insert_member_edges(cvid=cvid, level=level, members=members)

    def merge_subcommunity(self, *, community_id, level, parent_id,
                           member_count, members_hash, members, carry) -> None:
        from src.graph.nebula_store import _q
        cvid = community_vid(community_id, level)
        self._insert_community_vertex(
            cvid=cvid, community_id=community_id, level=level,
            member_count=member_count, members_hash=members_hash, carry=carry)
        parent_vid = community_vid(parent_id, level - 1)
        self._exec(
            f"INSERT EDGE `PARENT_OF` () VALUES {_q(parent_vid)}->{_q(cvid)}:();"
        )
        self._insert_member_edges(cvid=cvid, level=level, members=members)

    def _lookup_vids(self, where: str | None) -> list[str]:
        clause = f" WHERE {where}" if where else ""
        rows = self._exec(f"LOOKUP ON `Community`{clause} YIELD id(vertex) AS vid;")
        return [r["vid"] for r in rows if r.get("vid")]

    def _delete_vids(self, vids: list[str]) -> None:
        from src.graph.nebula_store import _q
        if not vids:
            return
        listed = ", ".join(_q(v) for v in vids)
        self._exec(f"DELETE VERTEX {listed} WITH EDGE;")

    def prune_level(self, level: int) -> None:
        self._delete_vids(self._lookup_vids(f"`Community`.level == {int(level)}"))

    def prune_all(self) -> None:
        self._delete_vids(self._lookup_vids(None))

    def read_old_reports(self) -> list[dict]:
        from src.graph.nebula_store import _q
        vids = self._lookup_vids(None)
        if not vids:
            return []
        listed = ", ".join(_q(v) for v in vids)
        rows = self._exec(
            f"FETCH PROP ON `Community` {listed} YIELD "
            "`Community`.level AS level, `Community`.members_hash AS h, "
            "`Community`.report AS report, `Community`.title AS title, "
            "`Community`.summary AS summary, `Community`.summarized_at AS summarized_at;"
        )
        return [r for r in rows if (r.get("report") or "").strip()]


def build_community_writeback(store: Any) -> CommunityWriteback:
    if settings.graph.backend == "nebula":
        return NebulaCommunityWriteback(store)
    return Neo4jCommunityWriteback(store)
