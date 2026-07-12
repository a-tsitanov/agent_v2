from __future__ import annotations

from src.graph import centrality_graph_ops as cgo


class _RecStore:
    def __init__(self, rows=None):
        self.calls: list[tuple[str, dict | None]] = []
        self._rows = list(rows or [])

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map))
        return self._rows.pop(0) if self._rows else []


class _RaisingStore:
    def structured_query(self, cypher, param_map=None):
        raise RuntimeError("boom")


class _NebulaRecStore:
    def __init__(self, canned=None):
        self.calls: list[tuple[str, dict | None]] = []
        self._canned = list(canned or [])

    def structured_query(self, stmt, param_map=None):
        self.calls.append((stmt, param_map))
        assert param_map is None
        for substr, rows in self._canned:
            if substr in stmt:
                return rows
        return []


class _NebulaRaisingStore:
    def structured_query(self, stmt, param_map=None):
        raise RuntimeError("boom")


def test_neo4j_top_central_inlines_allowlisted_metric():
    store = _RecStore(rows=[[{"name": "A", "score": 0.9}]])
    result = cgo.Neo4jCentralityGraphOps(store).top_central("betweenness", None, 5)
    assert result == [{"name": "A", "score": 0.9}]
    cypher = store.calls[0][0]
    assert "e.betweenness IS NOT NULL" in cypher and "e.betweenness AS score" in cypher


def test_neo4j_link_prediction_issues_edge_cypher():
    store = _RecStore(rows=[[{"name": "B", "score": 0.8}]])
    result = cgo.Neo4jCentralityGraphOps(store).link_prediction("A", 5)
    assert result[0]["name"] == "B"
    assert ":LIKELY_LINK" in store.calls[0][0]


def test_neo4j_fail_soft():
    assert cgo.Neo4jCentralityGraphOps(_RaisingStore()).top_central("pagerank", None, 5) == []


def test_nebula_top_central_reads_column():
    store = _NebulaRecStore(canned=[("e.`Entity`.pagerank > 0", [{"name": "Hub", "score": 0.34}])])
    result = cgo.NebulaCentralityGraphOps(store).top_central("pagerank", "Org", 10)
    assert result == [{"name": "Hub", "score": 0.34}]
    stmt = store.calls[0][0]
    assert "e.`Entity`.pagerank > 0" in stmt
    assert "e.`Entity`.label == \"Org\"" in stmt
    assert "ORDER BY score DESC LIMIT 10" in stmt


def test_nebula_top_central_rejects_unknown_metric():
    store = _NebulaRecStore()
    assert cgo.NebulaCentralityGraphOps(store).top_central("bogus", None, 5) == []
    assert store.calls == []  # never inlines an unknown column


def test_nebula_link_prediction_empty():
    store = _NebulaRecStore()
    assert cgo.NebulaCentralityGraphOps(store).link_prediction("A", 5) == []
    assert store.calls == []  # no LIKELY_LINK edge under nebula


def test_nebula_fail_soft():
    assert cgo.NebulaCentralityGraphOps(_NebulaRaisingStore()).top_central("pagerank", None, 5) == []


def test_dispatch(monkeypatch):
    monkeypatch.setattr(cgo.settings.graph, "backend", "neo4j")
    assert isinstance(cgo.build_centrality_graph_ops(_RecStore()), cgo.Neo4jCentralityGraphOps)
    monkeypatch.setattr(cgo.settings.graph, "backend", "nebula")
    assert isinstance(cgo.build_centrality_graph_ops(_NebulaRecStore()), cgo.NebulaCentralityGraphOps)
