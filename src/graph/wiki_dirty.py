"""Dirty-flag bookkeeping for the wiki editor (Neo4j __Entity__ props)."""
from __future__ import annotations

_MARK = """
UNWIND $names AS n
MATCH (e:__Entity__ {name: n})
SET e.wiki_dirty = true, e.wiki_dirty_at = datetime()
"""

_SELECT = """
MATCH (e:__Entity__) WHERE e.wiki_dirty = true
RETURN e.name AS name ORDER BY e.wiki_dirty_at LIMIT $limit
"""

_CLEAR = """
MATCH (e:__Entity__ {name: $name})
SET e.wiki_dirty = false, e.wiki_hash = $hash, e.wiki_synced_at = datetime()
"""


def mark_dirty(store, names: list[str]) -> None:
    if not names:
        return
    store.structured_query(_MARK, param_map={"names": names})


def select_dirty(store, limit: int) -> list[str]:
    rows = store.structured_query(_SELECT, param_map={"limit": limit})
    return [r["name"] for r in rows]


def clear_dirty(store, name: str, digest: str) -> None:
    store.structured_query(_CLEAR, param_map={"name": name, "hash": digest})
