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
