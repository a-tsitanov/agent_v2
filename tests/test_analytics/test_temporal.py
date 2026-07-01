"""Unit tests for bitemporal graph analytics (analytics layer v1).

No live Neo4j: a fake store returns canned rows, optionally keyed by the
``as_of`` param so ``diff`` exercises real set logic across two snapshots.
Asserts the snapshot Cypher predicate, result shaping + ``coverage``, the
diff set algebra, and fail-soft behaviour — mirrors ``test_analysis.py``.
"""

from __future__ import annotations

import pytest

from src.analytics.temporal import (
    _snapshot_cypher,
    diff,
    snapshot,
)


class _FakeStore:
    """Returns canned rows.  ``rows`` may be a flat list (same for every
    call) or a dict ``{as_of_value: rows}`` so ``diff`` gets different
    edges per snapshot.  ``raise_on`` makes matching Cypher blow up."""

    def __init__(self, rows=None, *, raise_on=None):
        self._rows = rows if rows is not None else []
        self._raise_on = raise_on
        self.calls: list[tuple[str, dict]] = []

    def structured_query(self, cypher, param_map=None):
        params = param_map or {}
        self.calls.append((cypher, params))
        if self._raise_on and self._raise_on in cypher:
            raise RuntimeError("boom")
        if isinstance(self._rows, dict):
            return self._rows.get(params.get("as_of"), [])
        return self._rows


def _edge(source, target, label, *, timing="dated", **props):
    """Shape one canned snapshot row as the Cypher would return it."""
    row = {"source": source, "target": target, "label": label, "timing": timing}
    row.update(props)
    return row


# ── snapshot Cypher predicate ────────────────────────────────────────


def test_snapshot_cypher_excludes_negated_and_binds_as_of():
    cy = _snapshot_cypher(include_untimed=True)
    assert "$as_of" in cy
    assert "negated" in cy            # negated polarity excluded
    assert "timing" in cy            # classifies dated/fallback/untimed


def test_snapshot_cypher_include_untimed_toggles_predicate():
    incl = _snapshot_cypher(include_untimed=True)
    excl = _snapshot_cypher(include_untimed=False)
    assert incl != excl              # the flag must change the WHERE clause


# ── snapshot shaping + coverage ──────────────────────────────────────


@pytest.mark.asyncio
async def test_snapshot_shapes_edges_nodes_and_coverage():
    store = _FakeStore([
        _edge("Иванов", "СтройИнвест", "WORKS_AT", timing="dated"),
        _edge("СтройИнвест", "Контракт-7", "PARTY_OF", timing="fallback"),
        _edge("Иванов", "Москва", "RELATED_TO", timing="untimed"),
    ])
    out = await snapshot(store, "2024-01-01")
    assert out["as_of"] == "2024-01-01"
    assert len(out["edges"]) == 3
    # nodes are the de-duplicated endpoints
    assert set(out["nodes"]) == {"Иванов", "СтройИнвест", "Контракт-7", "Москва"}
    assert out["coverage"] == {
        "dated": 1, "fallback": 1, "untimed": 1, "total": 3,
    }


@pytest.mark.asyncio
async def test_snapshot_passes_as_of_param_to_store():
    store = _FakeStore([])
    await snapshot(store, "2025-06-01")
    assert store.calls[0][1]["as_of"] == "2025-06-01"


# ── diff set algebra ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_diff_classifies_added_removed_persisted():
    store = _FakeStore({
        "2023-01-01": [
            _edge("A", "B", "WORKS_AT"),       # persists
            _edge("A", "OldCo", "WORKS_AT"),   # removed by t2
        ],
        "2025-01-01": [
            _edge("A", "B", "WORKS_AT"),       # persists
            _edge("A", "NewCo", "WORKS_AT"),   # added by t2
        ],
    })
    out = await diff(store, "2023-01-01", "2025-01-01")
    added = {(e["source"], e["target"], e["label"]) for e in out["added"]}
    removed = {(e["source"], e["target"], e["label"]) for e in out["removed"]}
    persisted = {(e["source"], e["target"], e["label"]) for e in out["persisted"]}
    assert added == {("A", "NewCo", "WORKS_AT")}
    assert removed == {("A", "OldCo", "WORKS_AT")}
    assert persisted == {("A", "B", "WORKS_AT")}
    assert out["t1"] == "2023-01-01"
    assert out["t2"] == "2025-01-01"


@pytest.mark.asyncio
async def test_diff_carries_both_coverages():
    store = _FakeStore({
        "2023-01-01": [_edge("A", "B", "X", timing="dated")],
        "2025-01-01": [_edge("A", "B", "X", timing="fallback")],
    })
    out = await diff(store, "2023-01-01", "2025-01-01")
    assert out["t1_coverage"]["dated"] == 1
    assert out["t2_coverage"]["fallback"] == 1


# ── fail-soft ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_snapshot_none_store_is_failsafe():
    out = await snapshot(None, "2024-01-01")
    assert out["edges"] == []
    assert out["coverage"]["total"] == 0


@pytest.mark.asyncio
async def test_snapshot_store_error_is_failsafe():
    store = _FakeStore(raise_on="MATCH")
    out = await snapshot(store, "2024-01-01")
    assert out["edges"] == []
    assert out["coverage"]["total"] == 0


@pytest.mark.asyncio
async def test_diff_none_store_is_failsafe():
    out = await diff(None, "2023-01-01", "2025-01-01")
    assert out["added"] == []
    assert out["removed"] == []
    assert out["persisted"] == []
