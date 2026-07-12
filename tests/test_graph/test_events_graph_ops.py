from __future__ import annotations

from src.graph import events_graph_ops as ego


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


# --- Neo4j: byte-for-byte -----------------------------------------------


def test_neo4j_new_entities_issues_moved_cypher():
    store = _RecStore(rows=[[{"name": "NewCo", "type": "Organization", "created_at": 19799}]])
    result = ego.Neo4jEventsGraphOps(store).new_entities(19786, 25)
    assert result == [{"name": "NewCo", "type": "Organization", "created_at": 19799}]
    assert store.calls == [(ego._NEW_ENTITIES, {"since": 19786, "top_n": 25})]


def test_neo4j_new_edges_issues_moved_cypher():
    store = _RecStore(rows=[[{"src": "A", "rel": "OWNS", "tgt": "B", "created_at": 19799}]])
    result = ego.Neo4jEventsGraphOps(store).new_edges(19786, 25)
    assert result[0]["rel"] == "OWNS"
    assert store.calls == [(ego._NEW_EDGES, {"since": 19786, "top_n": 25})]


def test_neo4j_entity_new_connections_issues_moved_cypher():
    store = _RecStore(rows=[[{"rel": "OWNS", "other": "B", "created_at": 19799}]])
    result = ego.Neo4jEventsGraphOps(store).entity_new_connections("A", 19786, 25)
    assert result[0]["other"] == "B"
    assert store.calls == [(ego._ENTITY_NEW_CONNECTIONS, {"name": "A", "since": 19786, "top_n": 25})]


def test_neo4j_fail_soft():
    assert ego.Neo4jEventsGraphOps(_RaisingStore()).new_entities(0, 25) == []


# --- Nebula: new_entities MATCH; edges/connections -> [] ----------------


def test_nebula_new_entities_matches_created_at():
    store = _NebulaRecStore(canned=[
        ("created_at >=", [{"name": "NewCo", "type": "Organization", "created_at": 19799,
                            "first_doc_id": "d1"}])
    ])
    result = ego.NebulaEventsGraphOps(store).new_entities(19786, 25)
    assert result == [
        {"name": "NewCo", "type": "Organization", "created_at": 19799, "first_doc_id": "d1"}
    ]
    stmt = store.calls[0][0]
    assert "e.`Entity`.created_at >= 19786" in stmt
    assert "ORDER BY created_at DESC LIMIT 25" in stmt  # aliased ORDER BY


def test_nebula_new_edges_returns_empty_rel_first_seen_absent():
    store = _NebulaRecStore()
    assert ego.NebulaEventsGraphOps(store).new_edges(19786, 25) == []
    assert store.calls == []  # no query issued — column absent


def test_nebula_entity_new_connections_returns_empty():
    store = _NebulaRecStore()
    assert ego.NebulaEventsGraphOps(store).entity_new_connections("A", 19786, 25) == []
    assert store.calls == []


# --- Dispatch ------------------------------------------------------------


def test_dispatch_neo4j_default(monkeypatch):
    monkeypatch.setattr(ego.settings.graph, "backend", "neo4j")
    assert isinstance(ego.build_events_graph_ops(_RecStore()), ego.Neo4jEventsGraphOps)


def test_dispatch_nebula(monkeypatch):
    monkeypatch.setattr(ego.settings.graph, "backend", "nebula")
    assert isinstance(ego.build_events_graph_ops(_NebulaRecStore()), ego.NebulaEventsGraphOps)
