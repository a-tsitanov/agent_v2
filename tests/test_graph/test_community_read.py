import asyncio

import src.workflow.search.activities.global_search as gs
from src.graph import community_read as cr
from src.workflow.contracts import MapCommunitiesParams


class _RecStore:
    """Records structured_query(cypher, param_map) calls; returns canned rows."""
    def __init__(self, ret=None):
        self.calls = []
        self._ret = ret if ret is not None else []
    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map))
        return self._ret


class _RecNebula:
    """Fake nebula store: records nGQL; returns canned rows per substring."""
    def __init__(self, read_map=None):
        self.stmts = []
        self._read_map = read_map or {}
    def structured_query(self, query, param_map=None):
        assert not param_map, "nebula read must inline values"
        self.stmts.append(query)
        for needle, rows in self._read_map.items():
            if needle in query:
                return rows
        return []


def test_neo4j_read_summaries_issues_exact_cypher_and_returns_rows_verbatim():
    canned = [{"community_id": "1", "level": 0, "summary": "s", "member_count": 3}]
    store = _RecStore(ret=canned)
    reader = cr.Neo4jCommunityRead(store)
    rows = reader.read_summaries(level=0)
    assert store.calls == [(cr._READ_SUMMARIES_CYPHER, {"level": 0})]
    assert rows == canned  # verbatim rows, not mutated


def test_dispatch_returns_neo4j_when_backend_not_nebula(monkeypatch):
    monkeypatch.setattr(cr.settings.graph, "backend", "neo4j")
    assert isinstance(cr.build_community_read(_RecStore()), cr.Neo4jCommunityRead)


def test_dispatch_returns_nebula_when_backend_nebula(monkeypatch):
    monkeypatch.setattr(cr.settings.graph, "backend", "nebula")
    assert isinstance(cr.build_community_read(_RecNebula()), cr.NebulaCommunityRead)


def test_nebula_read_summaries_filters_level_drops_blank_and_sorts():
    vid_a, vid_b, vid_c = "vid-a", "vid-b", "vid-c"
    s = _RecNebula(read_map={
        "LOOKUP ON `Community` WHERE `Community`.level == 0": [
            {"vid": vid_a}, {"vid": vid_b}, {"vid": vid_c},
        ],
        "FETCH PROP ON `Community`": [
            {"community_id": "a", "level": 0, "summary": "sum-a", "member_count": 5},
            {"community_id": "b", "level": 0, "summary": "   ", "member_count": 9},  # blank -> dropped
            {"community_id": "c", "level": 0, "summary": "sum-c", "member_count": 9},
        ],
    })
    reader = cr.NebulaCommunityRead(s)
    rows = reader.read_summaries(level=0)

    # LOOKUP filters by level ==
    assert any("LOOKUP ON `Community` WHERE `Community`.level == 0" in q for q in s.stmts)
    # FETCH reads the `id` PROPERTY as community_id, not id(vertex)
    assert any(
        "FETCH PROP ON `Community`" in q and "`Community`.id AS community_id" in q
        for q in s.stmts
    )
    # blank-summary row dropped; sorted by member_count desc then community_id asc
    assert rows == [
        {"community_id": "c", "level": 0, "summary": "sum-c", "member_count": 9},
        {"community_id": "a", "level": 0, "summary": "sum-a", "member_count": 5},
    ]


def test_nebula_read_summaries_no_vertices_returns_empty_and_skips_fetch():
    s = _RecNebula(read_map={
        "LOOKUP ON `Community` WHERE `Community`.level == 3": [],
    })
    reader = cr.NebulaCommunityRead(s)
    rows = reader.read_summaries(level=3)
    assert rows == []
    assert not any("FETCH PROP ON `Community`" in q for q in s.stmts)


def test_nebula_read_summaries_defaults_missing_member_count_to_zero():
    s = _RecNebula(read_map={
        "LOOKUP ON `Community` WHERE `Community`.level == 0": [{"vid": "v1"}],
        "FETCH PROP ON `Community`": [
            {"community_id": "x", "level": 0, "summary": "sx", "member_count": None},
        ],
    })
    reader = cr.NebulaCommunityRead(s)
    rows = reader.read_summaries(level=0)
    assert rows == [{"community_id": "x", "level": 0, "summary": "sx", "member_count": 0}]


def test_map_communities_lexical_routes_through_reader(monkeypatch):
    captured = {}

    class _FakeReader:
        def read_summaries(self, *, level):
            captured["level"] = level
            return [{"community_id": "1", "level": level, "summary": "hello world", "member_count": 2}]

    monkeypatch.setattr(gs, "build_community_read", lambda store: _FakeReader())
    params = MapCommunitiesParams(query="hello", level=0, limit=20)
    result = asyncio.run(gs._map_communities_lexical(object(), params))
    assert captured["level"] == 0
    assert isinstance(result, gs.MapCommunitiesResult)
    assert len(result.communities) == 1
    assert result.communities[0].community_id == "1"


def test_read_summaries_cypher_shape():
    cy = cr._READ_SUMMARIES_CYPHER
    assert "MATCH (c:Community {level: $level})" in cy
    assert "trim(c.summary)" in cy
    assert "ORDER BY member_count DESC, community_id ASC" in cy
