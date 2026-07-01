"""Bitemporal graph analytics — point-in-time snapshot + diff (v1).

Answers "what did the graph look like on date X" and "what changed
between t1 and t2".  Bitemporal with a **transaction-time fallback**: a
relation is *alive at X* when

  * it is not logically negated (``polarity != 'negated'``), AND
  * its time predicate holds:
      - **dated**    — has ``valid_from`` / ``valid_to``: the window
                       contains X (open on either NULL bound);
      - **fallback** — no window but an ``observed_at`` (ingest
                       wall-clock): the fact was known by X
                       (``observed_at <= X``);
      - **untimed**  — neither: governed by ``include_untimed`` (default
                       ``True`` — keep, so sparse temporal data doesn't
                       silently drop edges).

ISO strings are compared by prefix (``left($as_of, size(field))``) so a
year-only ``valid_to`` of a past year still expires correctly — same
convention as ``graph/retriever.py``'s ``_relation_is_live`` (#8).

Every edge is tagged with its ``timing`` (dated/fallback/untimed) so the
returned ``coverage`` shows how much of the snapshot rests on real dates
vs the fallback — essential when temporal data is sparse.

Pure Cypher (no GDS projection); offline / analytics, not the hot path.
Fail-soft throughout.  The Cypher is UNVERIFIED against a live Neo4j in
this sandbox — same caveat as ``communities.py`` / ``analysis.py``.
"""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

from src.graph.communities import _run_query

_TIMINGS = ("dated", "fallback", "untimed")


# ── Cypher builder ───────────────────────────────────────────────────


def _snapshot_cypher(*, include_untimed: bool) -> str:
    """Edges alive at ``$as_of`` with a ``timing`` tag per edge.

    ``include_untimed`` inlines the untimed branch as ``true``/``false``
    so the flag materially changes the WHERE clause."""
    untimed_branch = "true" if include_untimed else "false"
    return f"""
MATCH (s:__Entity__)-[r]->(t:__Entity__)
WITH s, t, r,
  CASE
    WHEN r.valid_from IS NOT NULL OR r.valid_to IS NOT NULL THEN 'dated'
    WHEN r.observed_at IS NOT NULL THEN 'fallback'
    ELSE 'untimed'
  END AS timing
WHERE (r.polarity IS NULL OR r.polarity <> 'negated')
  AND (
    (timing = 'dated'
      AND (r.valid_from IS NULL OR r.valid_from <= left($as_of, size(r.valid_from)))
      AND (r.valid_to IS NULL OR r.valid_to >= left($as_of, size(r.valid_to))))
    OR (timing = 'fallback'
      AND r.observed_at <= left($as_of, size(r.observed_at)))
    OR (timing = 'untimed' AND {untimed_branch})
  )
RETURN s.name AS source, t.name AS target, type(r) AS label, timing,
       r.valid_from AS valid_from, r.valid_to AS valid_to,
       r.observed_at AS observed_at
"""


# ── shaping helpers ──────────────────────────────────────────────────


def _empty_coverage() -> dict:
    return {"dated": 0, "fallback": 0, "untimed": 0, "total": 0}


def _coverage(edges: list[dict]) -> dict:
    cov = _empty_coverage()
    for e in edges:
        t = e.get("timing")
        if t in _TIMINGS:
            cov[t] += 1
    cov["total"] = len(edges)
    return cov


def _shape_edge(r: dict) -> dict:
    return {
        "source": r.get("source"),
        "target": r.get("target"),
        "label": r.get("label"),
        "timing": r.get("timing"),
        "valid_from": r.get("valid_from"),
        "valid_to": r.get("valid_to"),
        "observed_at": r.get("observed_at"),
    }


def _edge_key(e: dict) -> tuple:
    """Stable identity of an edge across snapshots."""
    return (e.get("source"), e.get("target"), e.get("label"))


# ── public analytics functions ───────────────────────────────────────


async def snapshot(
    store: Any | None, as_of: str, *, include_untimed: bool = True,
) -> dict:
    """Graph state alive at ``as_of`` (ISO date/datetime string).

    Returns ``{as_of, edges[], nodes[], coverage}``.  ``None`` store or
    any error → empty snapshot (``coverage.total == 0``)."""
    empty = {
        "as_of": as_of, "edges": [], "nodes": [], "coverage": _empty_coverage(),
    }
    if store is None:
        return empty
    try:
        rows = await asyncio.to_thread(
            _run_query, store,
            _snapshot_cypher(include_untimed=include_untimed),
            {"as_of": as_of},
        )
    except Exception as exc:  # noqa: BLE001 — fail-soft
        logger.warning("temporal snapshot failed: {e}", e=exc)
        return empty
    edges = [
        _shape_edge(r) for r in rows
        if isinstance(r, dict) and r.get("source") and r.get("target")
    ]
    nodes = sorted({n for e in edges for n in (e["source"], e["target"]) if n})
    return {
        "as_of": as_of, "edges": edges, "nodes": nodes,
        "coverage": _coverage(edges),
    }


async def diff(
    store: Any | None, t1: str, t2: str, *, include_untimed: bool = True,
) -> dict:
    """Change between two moments: ``snapshot(t2) ⊖ snapshot(t1)`` keyed
    by ``(source, target, label)``.

    Returns ``{t1, t2, added[], removed[], persisted[], t1_coverage,
    t2_coverage}``.  ``added`` = alive at t2 not t1; ``removed`` = alive
    at t1 not t2; ``persisted`` = both.  ``None`` store → empty diff."""
    empty = {
        "t1": t1, "t2": t2, "added": [], "removed": [], "persisted": [],
        "t1_coverage": _empty_coverage(), "t2_coverage": _empty_coverage(),
    }
    if store is None:
        return empty
    snap1 = await snapshot(store, t1, include_untimed=include_untimed)
    snap2 = await snapshot(store, t2, include_untimed=include_untimed)
    e1 = {_edge_key(e): e for e in snap1["edges"]}
    e2 = {_edge_key(e): e for e in snap2["edges"]}
    return {
        "t1": t1, "t2": t2,
        "added": [e for k, e in e2.items() if k not in e1],
        "removed": [e for k, e in e1.items() if k not in e2],
        "persisted": [e for k, e in e2.items() if k in e1],
        "t1_coverage": snap1["coverage"],
        "t2_coverage": snap2["coverage"],
    }


__all__ = ["snapshot", "diff"]
