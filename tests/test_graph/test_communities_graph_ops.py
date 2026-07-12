from __future__ import annotations

from src.graph import communities_graph_ops as cgo
from src.graph.nebula_store import entity_vid


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


def test_neo4j_community_overview_issues_moved_cypher():
    store = _RecStore(rows=[[{"title": "Alpha", "summary": "s", "member_count": 9}]])
    result = cgo.Neo4jCommunitiesGraphOps(store).community_overview(0, 20)
    assert result[0]["member_count"] == 9
    assert store.calls == [(cgo._COMMUNITY_OVERVIEW, {"level": 0, "top_n": 20})]
    assert "c:Community" in cgo._COMMUNITY_OVERVIEW


def test_neo4j_entity_communities_issues_moved_cypher():
    store = _RecStore(rows=[[{"level": 0, "title": "Alpha", "summary": "s"}]])
    result = cgo.Neo4jCommunitiesGraphOps(store).entity_communities("Ромашка")
    assert result[0]["title"] == "Alpha"
    assert store.calls == [(cgo._ENTITY_COMMUNITIES, {"name": "Ромашка"})]
    assert ":IN_COMMUNITY" in cgo._ENTITY_COMMUNITIES


def test_neo4j_fail_soft():
    assert cgo.Neo4jCommunitiesGraphOps(_RaisingStore()).community_overview(0, 20) == []


# --- Nebula: community_overview (MATCH by level) ------------------------


def test_nebula_community_overview_matches_by_level():
    store = _NebulaRecStore(canned=[
        ("Community`.level ==", [{"title": "Beta", "summary": "s", "member_count": 9}])
    ])
    result = cgo.NebulaCommunitiesGraphOps(store).community_overview(0, 20)
    assert result == [{"title": "Beta", "summary": "s", "member_count": 9}]
    stmt = store.calls[0][0]
    assert "c.`Community`.level == 0" in stmt
    assert "ORDER BY member_count DESC LIMIT 20" in stmt


# --- Nebula: entity_communities (GO + FETCH) ----------------------------


def test_nebula_entity_communities_go_then_fetch():
    vid = entity_vid("Ромашка")
    store = _NebulaRecStore(canned=[
        ("OVER `IN_COMMUNITY`", [{"cvid": "abc"}, {"cvid": "def"}]),
        ("FETCH PROP ON `Community`", [
            {"level": 0, "title": "Alpha", "summary": "sa"},
            {"level": 1, "title": "Beta", "summary": "sb"},
        ]),
    ])
    result = cgo.NebulaCommunitiesGraphOps(store).entity_communities("Ромашка")
    assert result == [
        {"level": 0, "title": "Alpha", "summary": "sa"},
        {"level": 1, "title": "Beta", "summary": "sb"},
    ]
    assert vid in store.calls[0][0]  # GO uses the entity VID
    assert '"abc"' in store.calls[1][0] and '"def"' in store.calls[1][0]  # FETCH the cvids


def test_nebula_entity_communities_empty_when_no_edges():
    store = _NebulaRecStore(canned=[("OVER `IN_COMMUNITY`", [])])
    assert cgo.NebulaCommunitiesGraphOps(store).entity_communities("Ghost") == []
    assert len(store.calls) == 1  # no FETCH when no edges


def test_nebula_fail_soft():
    assert cgo.NebulaCommunitiesGraphOps(_NebulaRaisingStore()).entity_communities("X") == []


# --- Dispatch ------------------------------------------------------------


def test_dispatch_neo4j_default(monkeypatch):
    monkeypatch.setattr(cgo.settings.graph, "backend", "neo4j")
    assert isinstance(cgo.build_communities_graph_ops(_RecStore()), cgo.Neo4jCommunitiesGraphOps)


def test_dispatch_nebula(monkeypatch):
    monkeypatch.setattr(cgo.settings.graph, "backend", "nebula")
    assert isinstance(
        cgo.build_communities_graph_ops(_NebulaRecStore()), cgo.NebulaCommunitiesGraphOps
    )
