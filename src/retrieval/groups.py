"""Canonical channel-group enum, imported by ingest/search/rerank/tg_ingest.

A document's group is a single lowercase string from GROUPS (or "" =
ungrouped). GROUP_PRIORITY is the tie-break order when a channel is found
in more than one group-folder (earlier wins).
"""
from __future__ import annotations

GROUPS: tuple[str, ...] = ("news", "analytics", "digest", "opinion", "official", "data")
GROUP_SET: frozenset[str] = frozenset(GROUPS)
GROUP_PRIORITY: tuple[str, ...] = GROUPS


def pick_priority(a: str, b: str) -> str:
    """Return whichever group name ranks earlier in GROUP_PRIORITY.
    Unknown names sort last (index = len)."""
    def rank(g: str) -> int:
        return GROUP_PRIORITY.index(g) if g in GROUP_SET else len(GROUP_PRIORITY)
    return a if rank(a) <= rank(b) else b
