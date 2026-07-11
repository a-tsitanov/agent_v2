"""Backend-dispatched community READ (map-phase summary fetch).

`Neo4jCommunityRead` wraps the existing ``_READ_SUMMARIES_CYPHER`` constant
verbatim (default path, byte-for-byte unchanged; the constant was MOVED
here from ``global_search.py``). `NebulaCommunityRead` translates the same
read to nGQL. Only the lexical map-phase read lives here; semantic
selection (report_vec) and hierarchy descent are deferred (report_vec is
not on the nebula vertex — Milvus owns it), as is doc<->community linkage
(chunks are not nebula graph nodes).
"""
from __future__ import annotations

from typing import Any, Protocol

from src.config import settings

# Read the stored community summaries for a level, largest first so the
# most informative communities lead when ``limit`` truncates.  Empty/
# unsummarised communities are skipped (summary IS NOT NULL / non-blank).
_READ_SUMMARIES_CYPHER = """
MATCH (c:Community {level: $level})
WHERE c.summary IS NOT NULL AND trim(c.summary) <> ''
RETURN c.id AS community_id, c.level AS level, c.summary AS summary,
       coalesce(c.member_count, 0) AS member_count
ORDER BY member_count DESC, community_id ASC
"""


class CommunityRead(Protocol):
    def read_summaries(self, *, level: int) -> list[dict]: ...


class Neo4jCommunityRead:
    """Runs the historical Cypher constant verbatim — zero behaviour change."""

    def __init__(self, store: Any):
        self._store = store

    def read_summaries(self, *, level: int) -> list[dict]:
        rows = self._store.structured_query(
            _READ_SUMMARIES_CYPHER, param_map={"level": level},
        )
        return list(rows or [])


class NebulaCommunityRead:
    """nGQL community READ (map-phase). Reads the ``id`` PROPERTY (written
    by BUILD) as ``community_id`` — NOT ``id(vertex)``/VID. Values are
    inline-quoted (nebula binds no params)."""

    def __init__(self, store: Any):
        self._store = store

    def _exec(self, stmt: str) -> list[dict]:
        return list(self._store.structured_query(stmt) or [])

    def read_summaries(self, *, level: int) -> list[dict]:
        from src.graph.nebula_store import _q

        vid_rows = self._exec(
            f"LOOKUP ON `Community` WHERE `Community`.level == {int(level)} "
            "YIELD id(vertex) AS vid;"
        )
        vids = [r["vid"] for r in vid_rows if r.get("vid")]
        if not vids:
            return []
        listed = ", ".join(_q(v) for v in vids)
        rows = self._exec(
            f"FETCH PROP ON `Community` {listed} YIELD "
            "`Community`.id AS community_id, `Community`.level AS level, "
            "`Community`.summary AS summary, `Community`.member_count AS member_count;"
        )
        kept = [r for r in rows if (r.get("summary") or "").strip()]
        kept.sort(key=lambda r: (-(r.get("member_count") or 0), r.get("community_id") or ""))
        return [
            {
                "community_id": r.get("community_id"),
                "level": r.get("level"),
                "summary": r.get("summary"),
                "member_count": r.get("member_count") or 0,
            }
            for r in kept
        ]


def build_community_read(store: Any) -> CommunityRead:
    if settings.graph.backend == "nebula":
        return NebulaCommunityRead(store)
    return Neo4jCommunityRead(store)
