from __future__ import annotations

from src.analytics.ids import ID_TYPES
from src.graph import quality_graph_ops as qgo


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


# --- Neo4j: byte-for-byte guard -----------------------------------------


def test_neo4j_contradictions_issues_moved_cypher():
    store = _RecStore(rows=[[{"a": "A", "rel": "OWNS", "b": "B"}]])
    result = qgo.Neo4jQualityGraphOps(store).contradictions(10)
    assert result == [{"a": "A", "rel": "OWNS", "b": "B"}]
    assert store.calls == [(qgo._CONTRADICTIONS, {"top_n": 10})]
    assert "r1.polarity='affirmed'" in qgo._CONTRADICTIONS
    assert "source_chunks" in qgo._CONTRADICTIONS


def test_neo4j_orphans_issues_moved_cypher():
    store = _RecStore(rows=[[{"name": "L", "degree": 0, "type": "Person"}]])
    result = qgo.Neo4jQualityGraphOps(store).orphans(1, 25)
    assert result == [{"name": "L", "degree": 0, "type": "Person"}]
    assert store.calls == [(qgo._ORPHANS, {"min_degree": 1, "top_n": 25, "id_types": ID_TYPES})]


def test_neo4j_incomplete_issues_moved_cypher():
    store = _RecStore(rows=[[{"name": "Org", "missing": ["INN"], "have": []}]])
    result = qgo.Neo4jQualityGraphOps(store).incomplete_entities("Organization", ["INN"], 25)
    assert result == [{"name": "Org", "missing": ["INN"], "have": []}]
    assert store.calls == [
        (qgo._INCOMPLETE, {"type": "Organization", "expected": ["INN"], "top_n": 25}),
    ]


def test_neo4j_merge_candidates_issues_moved_cypher():
    store = _RecStore(rows=[[{"key": "x", "names": ["X", "x"], "count": 2}]])
    result = qgo.Neo4jQualityGraphOps(store).merge_candidates(25)
    assert result == [{"key": "x", "names": ["X", "x"], "count": 2}]
    assert store.calls == [(qgo._MERGE_CANDIDATES, {"top_n": 25, "id_types": ID_TYPES})]
    assert "toLower(trim(e.name))" in qgo._MERGE_CANDIDATES


def test_neo4j_fail_soft_returns_empty_on_raise():
    assert qgo.Neo4jQualityGraphOps(_RaisingStore()).contradictions(10) == []


# --- Nebula: contradictions (two-MATCH, empty chunks) --------------------


def test_nebula_contradictions_two_match_empty_chunks():
    store = _NebulaRecStore(canned=[("r1.polarity == 'affirmed'", [{"a": "A", "rel": "OWNS", "b": "B"}])])
    result = qgo.NebulaQualityGraphOps(store).contradictions(10)
    assert result == [
        {"a": "A", "rel": "OWNS", "b": "B", "affirmed_chunks": [], "negated_chunks": []},
    ]
    stmt = store.calls[0][0]
    assert "MATCH (a:`Entity`)-[r1:`RELATED`]->(b:`Entity`)" in stmt
    assert "MATCH (a)-[r2:`RELATED`]->(b)" in stmt
    assert "r1.rel_type == r2.rel_type" in stmt
    assert "source_chunks" not in stmt  # absent column never referenced
    assert "LIMIT 10" in stmt


# --- Nebula: orphans (near-verbatim MATCH) -------------------------------


def test_nebula_orphans_optional_match_having_orders_asc():
    store = _NebulaRecStore(canned=[("degree ASC", [{"name": "L", "degree": 0, "type": "Person"}])])
    result = qgo.NebulaQualityGraphOps(store).orphans(2, 25)
    assert result == [{"name": "L", "degree": 0, "type": "Person"}]
    stmt = store.calls[0][0]
    assert "OPTIONAL MATCH (e)-[r:`RELATED`]-(:`Entity`)" in stmt
    assert "WITH e, count(r) AS degree WHERE degree < 2" in stmt
    assert "e.`Entity`.label NOT IN [" in stmt
    assert "ORDER BY degree ASC LIMIT 25" in stmt


# --- Nebula: incomplete_entities (query + Python have/missing) -----------


def test_nebula_incomplete_computes_missing_in_python():
    # Org1 has INN neighbour; Org2 has none. expected = [INN, OGRN].
    rows = [
        {"name": "Org1", "have": ["INN", "Person"]},
        {"name": "Org2", "have": []},
    ]
    store = _NebulaRecStore(canned=[("collect(idn.`Entity`.label)", rows)])
    result = qgo.NebulaQualityGraphOps(store).incomplete_entities("Organization", ["INN", "OGRN"], 25)
    # Org2 (2 missing) ranks before Org1 (1 missing)
    assert result == [
        {"name": "Org2", "missing": ["INN", "OGRN"], "have": []},
        {"name": "Org1", "missing": ["OGRN"], "have": ["INN"]},
    ]
    assert "OPTIONAL MATCH (e)-[:`RELATED`]-(idn:`Entity`)" in store.calls[0][0]


def test_nebula_incomplete_empty_when_no_expected():
    store = _NebulaRecStore()
    assert qgo.NebulaQualityGraphOps(store).incomplete_entities("Organization", [], 25) == []
    assert store.calls == []  # short-circuits before querying


# --- Nebula: merge_candidates (Python grouping) --------------------------


def test_nebula_merge_candidates_groups_case_space_insensitive():
    rows = [{"name": "Ромашка"}, {"name": " РОМАШКА "}, {"name": "Solo"}]
    store = _NebulaRecStore(canned=[("RETURN e.`Entity`.name AS name", rows)])
    result = qgo.NebulaQualityGraphOps(store).merge_candidates(25)
    assert result == [{"key": "ромашка", "names": ["Ромашка", " РОМАШКА "], "count": 2}]
    assert "NOT IN [" in store.calls[0][0]


def test_nebula_fail_soft_returns_empty_on_raise():
    assert qgo.NebulaQualityGraphOps(_NebulaRaisingStore()).orphans(1, 25) == []


# --- Dispatch ------------------------------------------------------------


def test_dispatch_neo4j_default(monkeypatch):
    monkeypatch.setattr(qgo.settings.graph, "backend", "neo4j")
    assert isinstance(qgo.build_quality_graph_ops(_RecStore()), qgo.Neo4jQualityGraphOps)


def test_dispatch_nebula(monkeypatch):
    monkeypatch.setattr(qgo.settings.graph, "backend", "nebula")
    assert isinstance(qgo.build_quality_graph_ops(_NebulaRecStore()), qgo.NebulaQualityGraphOps)
