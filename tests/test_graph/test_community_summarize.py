from src.graph import community_summarize as cs


class _RecStore:
    def __init__(self, ret=None):
        self.calls = []
        self._ret = ret if ret is not None else []
    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map))
        return self._ret


def test_neo4j_read_member_context_issues_exact_cypher():
    store = _RecStore(ret=[{"name": "a", "description": "d", "rel_types": ["R"]}])
    wb = cs.Neo4jCommunitySummarize(store)
    rows = wb.read_member_context(community_id="7", level=0)
    assert store.calls == [(cs._MEMBER_CONTEXT_CYPHER, {"community_id": "7", "level": 0})]
    assert rows == [{"name": "a", "description": "d", "rel_types": ["R"]}]


def test_neo4j_read_child_reports_issues_exact_cypher():
    store = _RecStore(ret=[{"title": "t", "summary": "s"}])
    wb = cs.Neo4jCommunitySummarize(store)
    rows = wb.read_child_reports(community_id="7", level=1)
    assert store.calls == [(cs._CHILD_REPORTS_CYPHER, {"community_id": "7", "level": 1})]
    assert rows == [{"title": "t", "summary": "s"}]


def test_neo4j_write_report_issues_exact_cypher_and_params():
    store = _RecStore()
    wb = cs.Neo4jCommunitySummarize(store)
    wb.write_report(community_id="7", level=0, report="R", title="T",
                    summary="S", report_vec=[0.1])
    assert store.calls == [(cs._WRITE_REPORT_CYPHER, {
        "community_id": "7", "level": 0, "report": "R",
        "title": "T", "summary": "S", "report_vec": [0.1],
    })]


def test_dispatch_returns_neo4j_when_backend_not_nebula(monkeypatch):
    monkeypatch.setattr(cs.settings.graph, "backend", "neo4j")
    assert isinstance(cs.build_community_summarize(_RecStore()), cs.Neo4jCommunitySummarize)
