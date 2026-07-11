import src.graph.communities as comm
from src.graph import community_writeback as cw


class _RecStore:
    """Records structured_query(cypher, param_map) calls; returns [] (or a
    canned value for reads)."""
    def __init__(self, ret=None):
        self.calls = []
        self._ret = ret or []
    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map))
        return self._ret


def test_community_vid_is_stable_32hex_and_level_scoped():
    v = cw.community_vid("42", 0)
    assert isinstance(v, str) and len(v) == 32
    assert v == cw.community_vid("42", 0)           # stable
    assert v != cw.community_vid("42", 1)           # level-scoped
    assert v != cw.community_vid("43", 0)           # id-scoped


def test_neo4j_merge_community_issues_exact_cypher_and_params():
    store = _RecStore()
    wb = cw.Neo4jCommunityWriteback(store)
    wb.merge_community(community_id="7", level=0, member_count=3,
                       members_hash="h", members=["a", "b"], carry=None)
    assert len(store.calls) == 1
    cypher, params = store.calls[0]
    assert cypher is comm._MERGE_COMMUNITY_CYPHER          # SAME constant object
    assert params == {
        "community_id": "7", "level": 0, "member_count": 3,
        "members_hash": "h", "members": ["a", "b"],
        "carry_report": None, "carry_title": None, "carry_summary": None,
        "carry_report_vec": None, "carry_summarized_at": None,
    }


def test_neo4j_merge_subcommunity_maps_carry_and_parent():
    store = _RecStore()
    wb = cw.Neo4jCommunityWriteback(store)
    carry = {"report": "R", "title": "T", "summary": "S",
             "report_vec": [0.1], "summarized_at": 111}
    wb.merge_subcommunity(community_id="9", level=1, parent_id="7",
                          member_count=2, members_hash="h2",
                          members=["c"], carry=carry)
    cypher, params = store.calls[0]
    assert cypher is comm._MERGE_SUBCOMMUNITY_CYPHER
    assert params["parent_id"] == "7"
    assert params["carry_report"] == "R" and params["carry_summarized_at"] == 111
    assert params["carry_report_vec"] == [0.1]


def test_neo4j_prune_and_read_and_ensure_use_the_constants():
    store = _RecStore(ret=[{"level": 0, "h": "x", "report": "r"}])
    wb = cw.Neo4jCommunityWriteback(store)
    wb.prune_level(2)
    assert store.calls[-1] == (comm._PRUNE_LEVEL_CYPHER, {"level": 2})
    wb.prune_all()
    assert store.calls[-1] == (comm._PRUNE_ALL_CYPHER, {})
    rows = wb.read_old_reports()
    assert store.calls[-1] == (comm._READ_OLD_REPORTS_CYPHER, {})
    assert rows == [{"level": 0, "h": "x", "report": "r"}]


def test_dispatch_returns_neo4j_when_backend_not_nebula(monkeypatch):
    monkeypatch.setattr(cw.settings.graph, "backend", "neo4j")
    assert isinstance(cw.build_community_writeback(_RecStore()), cw.Neo4jCommunityWriteback)
