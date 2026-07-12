from __future__ import annotations

from src.graph import rollups_graph_ops as rgo


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
    def __init__(self, canned: list[tuple[str, list[dict]]] | None = None):
        self.calls: list[tuple[str, dict | None]] = []
        self._canned = list(canned or [])

    def structured_query(self, stmt, param_map=None):
        self.calls.append((stmt, param_map))
        assert param_map is None, "nebula ops must not use param_map"
        for substr, rows in self._canned:
            if substr in stmt:
                return rows
        return []


def test_neo4j_amount_edges_issues_moved_cypher():
    store = _RecStore(rows=[[{"counterparty": "A", "amount": "1 000"}]])
    result = rgo.Neo4jRollupsGraphOps(store).amount_edges("A")
    assert result == [{"counterparty": "A", "amount": "1 000"}]
    assert store.calls == [(rgo._AMOUNT_EDGES, {"cp": "A"})]
    assert ":Amount" in rgo._AMOUNT_EDGES


def test_neo4j_fail_soft():
    assert rgo.Neo4jRollupsGraphOps(_RaisingStore()).amount_edges(None) == []


def test_nebula_amount_edges_matches_amount_label():
    store = _NebulaRecStore(canned=[("label == 'Amount'", [{"counterparty": "A", "amount": "500"}])])
    result = rgo.NebulaRollupsGraphOps(store).amount_edges(None)
    assert result == [{"counterparty": "A", "amount": "500"}]
    stmt = store.calls[0][0]
    assert "a.`Entity`.label == 'Amount'" in stmt
    assert "e.`Entity`.name ==" not in stmt  # no cp filter


def test_nebula_amount_edges_adds_cp_filter():
    store = _NebulaRecStore()
    rgo.NebulaRollupsGraphOps(store).amount_edges("Acme")
    assert "e.`Entity`.name == \"Acme\"" in store.calls[0][0]


def test_nebula_fail_soft():
    class _NebulaRaisingStore:
        def structured_query(self, stmt, param_map=None):
            raise RuntimeError("boom")

    assert rgo.NebulaRollupsGraphOps(_NebulaRaisingStore()).amount_edges(None) == []


def test_dispatch_neo4j_default(monkeypatch):
    monkeypatch.setattr(rgo.settings.graph, "backend", "neo4j")
    assert isinstance(rgo.build_rollups_graph_ops(_RecStore()), rgo.Neo4jRollupsGraphOps)


def test_dispatch_nebula(monkeypatch):
    monkeypatch.setattr(rgo.settings.graph, "backend", "nebula")
    assert isinstance(rgo.build_rollups_graph_ops(_NebulaRecStore()), rgo.NebulaRollupsGraphOps)
