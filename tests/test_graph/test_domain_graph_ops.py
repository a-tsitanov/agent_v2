from __future__ import annotations

from src.graph import domain_graph_ops as dgo


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
    """Returns canned rows keyed by first matching substring of the statement."""

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


def test_neo4j_issue_stats_issues_moved_cypher():
    store = _RecStore(rows=[[{"total": 10, "unresolved": 4}]])
    result = dgo.Neo4jDomainGraphOps(store).issue_resolution_stats()
    assert result == [{"total": 10, "unresolved": 4}]
    assert store.calls == [(dgo._ISSUE_STATS, {})]
    assert ":Issue" in dgo._ISSUE_STATS and ":Resolution" in dgo._ISSUE_STATS
    assert "rr.polarity IS NULL OR rr.polarity <> 'negated'" in dgo._ISSUE_STATS


def test_neo4j_comms_issues_moved_cypher():
    store = _RecStore(rows=[[{"a": "A", "b": "B", "rel": "CONTACT", "interactions": 2}]])
    result = dgo.Neo4jDomainGraphOps(store).communication_stats("A", 20)
    assert result == [{"a": "A", "b": "B", "rel": "CONTACT", "interactions": 2}]
    assert store.calls == [(dgo._COMMS, {"name": "A", "top_n": 20})]
    assert "CONTACT|RESPONDED_TO" in dgo._COMMS


def test_neo4j_fail_soft():
    assert dgo.Neo4jDomainGraphOps(_RaisingStore()).issue_resolution_stats() == []


# --- Nebula: issue_resolution_stats (two queries + Python) --------------


def test_nebula_issue_stats_computes_total_and_unresolved():
    store = _NebulaRecStore(canned=[
        ("RETURN count(*) AS total", [{"total": 5}]),
        ("RESOLVED_BY", [{"name": "Iss1"}, {"name": "Iss2"}, {"name": "Iss1"}]),
    ])
    result = dgo.NebulaDomainGraphOps(store).issue_resolution_stats()
    # 5 issues total, 2 distinct resolved -> unresolved = 3
    assert result == [{"total": 5, "unresolved": 3}]
    assert "label == 'Issue'" in store.calls[0][0]
    assert "rr.rel_type == 'RESOLVED_BY'" in store.calls[1][0]
    assert "r.`Entity`.label == 'Resolution'" in store.calls[1][0]


def test_nebula_issue_stats_clamps_unresolved_nonnegative():
    store = _NebulaRecStore(canned=[
        ("RETURN count(*) AS total", [{"total": 1}]),
        ("RESOLVED_BY", [{"name": "A"}, {"name": "B"}]),  # more resolved than total
    ])
    assert dgo.NebulaDomainGraphOps(store).issue_resolution_stats() == [{"total": 1, "unresolved": 0}]


# --- Nebula: communication_stats (undirected dedup MATCH) ---------------


def test_nebula_comms_dedup_and_rel_filter():
    store = _NebulaRecStore(canned=[
        ("interactions", [{"a": "A", "b": "B", "rel": "CONTACT", "interactions": 3}])
    ])
    result = dgo.NebulaDomainGraphOps(store).communication_stats(None, 20)
    assert result == [{"a": "A", "b": "B", "rel": "CONTACT", "interactions": 3}]
    stmt = store.calls[0][0]
    assert "r.rel_type IN ['CONTACT', 'RESPONDED_TO']" in stmt
    assert "a.`Entity`.name < b.`Entity`.name" in stmt
    assert "ORDER BY interactions DESC LIMIT 20" in stmt
    assert "name ==" not in stmt  # no name filter when None


def test_nebula_comms_adds_name_filter():
    store = _NebulaRecStore()
    dgo.NebulaDomainGraphOps(store).communication_stats("Alice", 5)
    stmt = store.calls[0][0]
    assert "a.`Entity`.name == \"Alice\" OR b.`Entity`.name == \"Alice\"" in stmt
    assert "LIMIT 5" in stmt


def test_nebula_fail_soft():
    assert dgo.NebulaDomainGraphOps(_NebulaRaisingStore()).communication_stats(None, 20) == []


# --- Dispatch ------------------------------------------------------------


def test_dispatch_neo4j_default(monkeypatch):
    monkeypatch.setattr(dgo.settings.graph, "backend", "neo4j")
    assert isinstance(dgo.build_domain_graph_ops(_RecStore()), dgo.Neo4jDomainGraphOps)


def test_dispatch_nebula(monkeypatch):
    monkeypatch.setattr(dgo.settings.graph, "backend", "nebula")
    assert isinstance(dgo.build_domain_graph_ops(_NebulaRecStore()), dgo.NebulaDomainGraphOps)
