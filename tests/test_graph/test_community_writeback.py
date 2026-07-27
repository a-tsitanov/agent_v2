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


from src.graph.nebula_store import entity_vid


class _RecSession:
    """Fake NebulaGraphStore: records structured_query(q) statements; returns
    a canned row list per matched substring for reads."""
    def __init__(self, read_map=None):
        self.stmts = []
        self._read_map = read_map or {}
    def structured_query(self, query, param_map=None):
        assert not param_map, "nebula writeback must inline values (no param_map)"
        self.stmts.append(query)
        for needle, rows in self._read_map.items():
            if needle in query:
                return rows
        return []


def test_nebula_merge_community_inserts_vertex_and_member_edges():
    s = _RecSession()
    wb = cw.NebulaCommunityWriteback(s)
    wb.merge_community(community_id="7", level=0, member_count=2,
                       members_hash="h", members=["Alice", "Bob"], carry=None)
    joined = "\n".join(s.stmts)
    cvid = cw.community_vid("7", 0)
    assert 'INSERT VERTEX `Community`' in joined
    assert f'"{cvid}":(' in joined
    # report_vec never written to the vertex
    assert "report_vec" not in joined
    # IN_COMMUNITY edges from each member's entity_vid to the community vid, with level
    assert 'INSERT EDGE `IN_COMMUNITY`' in joined
    assert f'"{entity_vid("Alice")}"->"{cvid}"' in joined
    assert f'"{entity_vid("Bob")}"->"{cvid}"' in joined


def _edge_stmts(stmts):
    return [q for q in stmts if 'INSERT EDGE `IN_COMMUNITY`' in q]


def test_nebula_member_edges_are_batched_under_the_statement_budget(monkeypatch):
    """One giant INSERT EDGE ... VALUES blows nebula's max query size (4 MiB)
    and takes the WHOLE hierarchy write down with it.  Members must be split
    into <=budget statements — every member edge still written exactly once."""
    monkeypatch.setattr(cw, "_MAX_STMT_CHARS", 300)
    s = _RecSession()
    wb = cw.NebulaCommunityWriteback(s)
    members = [f"Entity{i:04d}" for i in range(200)]
    wb.merge_community(community_id="7", level=0, member_count=len(members),
                       members_hash="h", members=members, carry=None)

    edge_stmts = _edge_stmts(s.stmts)
    assert len(edge_stmts) > 1, "expected the member edges to be split"
    assert all(len(q) <= 300 for q in edge_stmts)
    cvid = cw.community_vid("7", 0)
    joined = "\n".join(edge_stmts)
    for m in members:
        assert joined.count(f'"{entity_vid(m)}"->"{cvid}"') == 1


def test_nebula_member_edges_respect_nebula_4mib_limit_for_a_root_community():
    """Regression: the level-0 root community held 60117 members and produced a
    4568933-char statement — `SyntaxError: Query is too large (> 4194304)`."""
    s = _RecSession()
    wb = cw.NebulaCommunityWriteback(s)
    members = [f"Entity{i}" for i in range(60117)]
    wb.merge_community(community_id="0", level=0, member_count=len(members),
                       members_hash="h", members=members, carry=None)
    edge_stmts = _edge_stmts(s.stmts)
    assert edge_stmts
    assert max(len(q) for q in edge_stmts) < 4_194_304


def test_nebula_member_edges_single_batch_when_small():
    """Fast path unchanged: a small community still emits ONE statement."""
    s = _RecSession()
    wb = cw.NebulaCommunityWriteback(s)
    wb.merge_community(community_id="7", level=0, member_count=2,
                       members_hash="h", members=["Alice", "Bob"], carry=None)
    assert len(_edge_stmts(s.stmts)) == 1


def test_nebula_merge_subcommunity_adds_parent_of_edge():
    s = _RecSession()
    wb = cw.NebulaCommunityWriteback(s)
    wb.merge_subcommunity(community_id="9", level=1, parent_id="7",
                          member_count=1, members_hash="h2",
                          members=["Carol"], carry={"report": "R", "title": "T",
                                                    "summary": "S", "report_vec": [0.1],
                                                    "summarized_at": 5})
    joined = "\n".join(s.stmts)
    child = cw.community_vid("9", 1)
    parent = cw.community_vid("7", 0)
    assert 'INSERT EDGE `PARENT_OF`' in joined
    assert f'"{parent}"->"{child}"' in joined
    # carry report text lands on the vertex; report_vec does not
    assert '"R"' in joined and '"T"' in joined and '"S"' in joined
    assert "0.1" not in joined


def test_nebula_prune_level_lookups_then_deletes_with_edge():
    cvid = cw.community_vid("7", 0)
    s = _RecSession(read_map={"LOOKUP ON `Community` WHERE": [{"vid": cvid}]})
    wb = cw.NebulaCommunityWriteback(s)
    wb.prune_level(0)
    joined = "\n".join(s.stmts)
    assert "LOOKUP ON `Community` WHERE `Community`.level == 0" in joined
    assert f'DELETE VERTEX "{cvid}" WITH EDGE' in joined


def test_nebula_prune_all_no_vertices_is_noop():
    s = _RecSession(read_map={"LOOKUP ON `Community` YIELD": []})
    wb = cw.NebulaCommunityWriteback(s)
    wb.prune_all()
    # LOOKUP ran, but no DELETE VERTEX (nothing to delete)
    assert any("LOOKUP ON `Community` YIELD" in q for q in s.stmts)
    assert not any("DELETE VERTEX" in q for q in s.stmts)


def test_nebula_read_old_reports_returns_rows_with_nonblank_report():
    cvid = cw.community_vid("7", 0)
    s = _RecSession(read_map={
        "LOOKUP ON `Community` YIELD": [{"vid": cvid}],
        "FETCH PROP ON `Community`": [
            {"level": 0, "h": "h", "report": "r", "title": "t",
             "summary": "s", "summarized_at": 9},
            {"level": 0, "h": "h2", "report": "", "title": "", "summary": "", "summarized_at": 0},
        ],
    })
    wb = cw.NebulaCommunityWriteback(s)
    rows = wb.read_old_reports()
    assert len(rows) == 1                       # blank-report row dropped
    assert rows[0]["h"] == "h" and rows[0]["report"] == "r"
    assert rows[0].get("report_vec") is None    # not stored on the vertex


def test_dispatch_returns_nebula_when_backend_nebula(monkeypatch):
    monkeypatch.setattr(cw.settings.graph, "backend", "nebula")
    assert isinstance(cw.build_community_writeback(_RecSession()), cw.NebulaCommunityWriteback)
