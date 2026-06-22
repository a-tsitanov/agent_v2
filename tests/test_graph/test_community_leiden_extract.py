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
