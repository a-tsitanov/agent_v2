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

from loguru import logger  # noqa: F401  # used by NebulaCommunityWriteback in Task 3

from src.config import settings
from src.graph.communities import (
    _COMMUNITY_CONSTRAINT,
    _MERGE_COMMUNITY_CYPHER,
    _MERGE_SUBCOMMUNITY_CYPHER,
    _PRUNE_ALL_CYPHER,
    _PRUNE_LEVEL_CYPHER,
    _READ_OLD_REPORTS_CYPHER,
)


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
    """nGQL community BUILD write-back. Implemented in Task 3."""

    def __init__(self, store: Any):
        self._store = store

    def ensure_schema(self) -> None:
        # Nebula Community TAG + index are created by nebula_schema.ensure_schema.
        return None

    def read_old_reports(self) -> list[dict]:
        raise NotImplementedError("NebulaCommunityWriteback.read_old_reports (Task 3)")

    def prune_level(self, level: int) -> None:
        raise NotImplementedError("NebulaCommunityWriteback.prune_level (Task 3)")

    def prune_all(self) -> None:
        raise NotImplementedError("NebulaCommunityWriteback.prune_all (Task 3)")

    def merge_community(self, *, community_id, level, member_count,
                        members_hash, members, carry) -> None:
        raise NotImplementedError("NebulaCommunityWriteback.merge_community (Task 3)")

    def merge_subcommunity(self, *, community_id, level, parent_id,
                           member_count, members_hash, members, carry) -> None:
        raise NotImplementedError("NebulaCommunityWriteback.merge_subcommunity (Task 3)")


def build_community_writeback(store: Any) -> CommunityWriteback:
    if settings.graph.backend == "nebula":
        return NebulaCommunityWriteback(store)
    return Neo4jCommunityWriteback(store)
