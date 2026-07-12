from __future__ import annotations

from src.analytics.ids import ID_TYPES
from src.graph import signals_graph_ops as sgo


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


class _NebulaRaisingStore:
    def structured_query(self, stmt, param_map=None):
        raise RuntimeError("boom")


# --- Neo4j: byte-for-byte -----------------------------------------------


def test_neo4j_risk_score_issues_moved_cypher():
    store = _RecStore(rows=[[{"name": "S", "score": 0.8, "band": "high", "components": "{}"}]])
    result = sgo.Neo4jSignalsGraphOps(store).risk_score("S", "high", 20)
    assert result[0]["name"] == "S"
    assert store.calls == [(sgo._RISK_SCORE, {"name": "S", "band": "high", "top_n": 20})]
    assert "e.risk_score IS NOT NULL" in sgo._RISK_SCORE


def test_neo4j_recommended_merges_issues_moved_cypher():
    store = _RecStore(rows=[[{"key": "x", "names": ["X", "x"], "count": 2}]])
    result = sgo.Neo4jSignalsGraphOps(store).recommended_merges(50)
    assert result[0]["count"] == 2
    assert store.calls == [(sgo._RECOMMENDED_MERGES, {"top_n": 50, "id_types": ID_TYPES})]
    assert "toLower(trim(e.name))" in sgo._RECOMMENDED_MERGES


def test_neo4j_review_queue_issues_moved_cypher():
    store = _RecStore(rows=[[{"name": "Org", "degree": 2, "flag": "shell_signal"}]])
    result = sgo.Neo4jSignalsGraphOps(store).review_queue(50)
    assert result[0]["flag"] == "shell_signal"
    assert store.calls == [(sgo._REVIEW_QUEUE, {"top_n": 50, "id_types": ID_TYPES})]


def test_neo4j_circular_ownership_issues_moved_cypher():
    store = _RecStore(rows=[[{"cycle": ["A", "B", "A"]}]])
    result = sgo.Neo4jSignalsGraphOps(store).circular_ownership(20)
    assert result[0]["cycle"] == ["A", "B", "A"]
    assert store.calls == [(sgo._CIRCULAR_OWNERSHIP, {"top_n": 20})]
    assert "OWNS*2..6" in sgo._CIRCULAR_OWNERSHIP


def test_neo4j_fail_soft():
    assert sgo.Neo4jSignalsGraphOps(_RaisingStore()).circular_ownership(20) == []


# --- Nebula: blocked -> [] ----------------------------------------------


def test_nebula_risk_score_empty():
    store = _NebulaRecStore()
    assert sgo.NebulaSignalsGraphOps(store).risk_score(None, None, 20) == []
    assert store.calls == []  # no query — columns absent


def test_nebula_investigate_next_empty():
    store = _NebulaRecStore()
    assert sgo.NebulaSignalsGraphOps(store).investigate_next(20) == []
    assert store.calls == []


# --- Nebula: recommended_merges (Python grouping) -----------------------


def test_nebula_recommended_merges_groups():
    rows = [{"name": "Ромашка"}, {"name": " РОМАШКА "}, {"name": "Solo"}]
    store = _NebulaRecStore(canned=[("RETURN e.`Entity`.name AS name", rows)])
    result = sgo.NebulaSignalsGraphOps(store).recommended_merges(50)
    assert result == [{"key": "ромашка", "names": ["Ромашка", " РОМАШКА "], "count": 2}]


# --- Nebula: review_queue (shell classification in Python) --------------


def test_nebula_review_queue_flags_shell_orgs():
    rows = [
        {"name": "Shell", "neighbor_labels": ["INN", "PhoneNumber"]},  # all identifiers -> shell
        {"name": "Real", "neighbor_labels": ["Person"]},              # not shell
        {"name": "Empty", "neighbor_labels": []},                     # deg 0 -> not shell
    ]
    store = _NebulaRecStore(canned=[("collect(n.`Entity`.label)", rows)])
    result = sgo.NebulaSignalsGraphOps(store).review_queue(50)
    assert result == [{"name": "Shell", "degree": 2, "flag": "shell_signal"}]


# --- Nebula: circular_ownership (var-len + Python sort) -----------------


def test_nebula_circular_ownership_sorts_by_length():
    rows = [{"cycle": ["A", "B", "A"]}, {"cycle": ["X", "Y", "Z", "X"]}]
    store = _NebulaRecStore(canned=[("RELATED`*2..6", rows)])
    result = sgo.NebulaSignalsGraphOps(store).circular_ownership(20)
    # longer cycle first
    assert result == [{"cycle": ["X", "Y", "Z", "X"]}, {"cycle": ["A", "B", "A"]}]
    stmt = store.calls[0][0]
    assert "all(rel IN e WHERE rel.rel_type == 'OWNS')" in stmt


def test_nebula_fail_soft():
    assert sgo.NebulaSignalsGraphOps(_NebulaRaisingStore()).review_queue(20) == []


# --- Dispatch ------------------------------------------------------------


def test_dispatch_neo4j_default(monkeypatch):
    monkeypatch.setattr(sgo.settings.graph, "backend", "neo4j")
    assert isinstance(sgo.build_signals_graph_ops(_RecStore()), sgo.Neo4jSignalsGraphOps)


def test_dispatch_nebula(monkeypatch):
    monkeypatch.setattr(sgo.settings.graph, "backend", "nebula")
    assert isinstance(sgo.build_signals_graph_ops(_NebulaRecStore()), sgo.NebulaSignalsGraphOps)
