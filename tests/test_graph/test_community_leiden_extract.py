from __future__ import annotations

from typing import ClassVar

from src.graph.community_leiden import extract_entity_edges


class _FakeStore:
    """Returns one page of node rows, then one page of edge rows, then empties."""

    def __init__(self):
        self.calls = 0

    def structured_query(self, cypher, param_map=None):
        param_map = param_map or {}
        if "RETURN e.name AS name" in cypher:           # node page
            if param_map.get("after") in (None, ""):
                return [{"name": "A"}, {"name": "B"}, {"name": "C"}]
            return []
        # edge page
        if param_map.get("after") in (None, ""):
            return [
                {"src": "A", "tgt": "B", "weight": 2.0},
                {"src": "B", "tgt": "C", "weight": 1.0},
            ]
        return []


def test_extract_returns_edges_and_all_node_names():
    edges, nodes = extract_entity_edges(_FakeStore(), batch_size=10)
    assert ("A", "B", 2.0) in edges
    assert ("B", "C", 1.0) in edges
    assert set(nodes) == {"A", "B", "C"}      # includes isolated entities


class _FakeStoreNullCursor:
    """Edge page whose last row carries a null cursor — would loop forever under old code.

    The store is rigged to return the SAME non-empty page every time ``after``
    is unchanged (i.e. still ``""``), so the old code would spin indefinitely.
    The fixed code must detect that ``last_cursor`` is None (not > after) and break.
    """

    def structured_query(self, cypher, param_map=None):
        param_map = param_map or {}
        if "RETURN e.name AS name" in cypher:           # node page — single batch
            if param_map.get("after") in (None, ""):
                return [{"name": "X"}]
            return []
        # edge page: always return the same page with a null cursor on the last row
        # Under the old code this would never advance `after`, looping forever.
        return [
            {"src": "X", "tgt": "X", "weight": 1.0, "cursor": "X"},
            {"src": "X", "tgt": "X", "weight": 1.0, "cursor": None},
        ]


def test_edge_loop_terminates_when_last_cursor_is_null():
    """Regression: extract_entity_edges must not hang when last cursor is falsy."""
    edges, nodes = extract_entity_edges(_FakeStoreNullCursor(), batch_size=10)
    # Just assert it returned — if it hung, pytest would time out.
    assert isinstance(edges, list)
    assert isinstance(nodes, list)


class _FakeStoreHighDegreeSource:
    """Source node 'A' has 4 outgoing edges; batch_size=2 forces 2 pages.

    Under the old s.name cursor, the second page would use ``WHERE s.name > 'A'``
    and skip all remaining 'A' edges — silently dropping half the graph.
    Under the fixed elementId(r) cursor each page advances to the last
    relationship id, so all edges are collected.
    """

    # All edges from A with unique relationship element ids.
    _ALL_EDGES: ClassVar[list[dict]] = [
        {"src": "A", "tgt": "B", "weight": 1.0, "cursor": "r1"},
        {"src": "A", "tgt": "C", "weight": 1.0, "cursor": "r2"},
        {"src": "A", "tgt": "D", "weight": 1.0, "cursor": "r3"},
        {"src": "A", "tgt": "E", "weight": 1.0, "cursor": "r4"},
    ]

    def structured_query(self, cypher, param_map=None):
        param_map = param_map or {}
        if "RETURN e.name AS name" in cypher:
            after = param_map.get("after", "")
            if after in (None, ""):
                return [{"name": "A"}, {"name": "B"}, {"name": "C"},
                        {"name": "D"}, {"name": "E"}]
            return []
        # Edge page: return edges whose cursor > after, sliced to batch_size.
        after = param_map.get("after", "") or ""
        limit = param_map.get("limit", 10)
        remaining = [e for e in self._ALL_EDGES if e["cursor"] > after]
        return remaining[:limit]


def test_high_degree_source_no_edges_dropped():
    """Regression: all edges from a high-degree source must survive multi-page reads.

    With batch_size=2 and 4 edges from 'A', the old s.name cursor would drop
    A->C, A->D, A->E on the second page (WHERE s.name > 'A' skips them).
    The fixed elementId(r) cursor collects all 4 edges across 2 pages.
    """
    edges, _nodes = extract_entity_edges(_FakeStoreHighDegreeSource(), batch_size=2)
    targets = {tgt for src, tgt, _ in edges if src == "A"}
    assert targets == {"B", "C", "D", "E"}, (
        f"Expected all 4 targets of 'A', got: {targets}"
    )
