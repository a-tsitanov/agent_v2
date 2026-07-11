from __future__ import annotations

import pytest

from src.graph import wiki_graph_ops as wgo


class _RecStore:
    """Records (cypher, param_map) calls; returns canned rows per call,
    popped in call order."""

    def __init__(self, rows=None):
        self.calls: list[tuple[str, dict | None]] = []
        self._rows = list(rows or [])

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map))
        return self._rows.pop(0) if self._rows else []


# --- Neo4j: dirty-flag bookkeeping (byte-for-byte guard) --------------


def test_neo4j_mark_dirty_issues_unwind_set_cypher():
    store = _RecStore()
    ops = wgo.Neo4jWikiGraphOps(store)

    ops.mark_dirty(["A", "B"])

    assert store.calls == [(wgo._MARK, {"names": ["A", "B"]})]
    assert wgo._MARK == (
        "\nUNWIND $names AS n\n"
        "MATCH (e:__Entity__ {name: n})\n"
        "SET e.wiki_dirty = true, e.wiki_dirty_at = datetime()\n"
    )


def test_neo4j_mark_dirty_noop_on_empty():
    store = _RecStore()
    wgo.Neo4jWikiGraphOps(store).mark_dirty([])
    assert store.calls == []


def test_neo4j_select_dirty_issues_select_cypher_and_maps_names():
    store = _RecStore(rows=[[{"name": "A"}, {"name": "B"}]])
    ops = wgo.Neo4jWikiGraphOps(store)

    result = ops.select_dirty(10)

    assert result == ["A", "B"]
    assert store.calls == [(wgo._SELECT, {"limit": 10})]
    assert wgo._SELECT == (
        "\nMATCH (e:__Entity__) WHERE e.wiki_dirty = true\n"
        "RETURN e.name AS name ORDER BY e.wiki_dirty_at LIMIT $limit\n"
    )


def test_neo4j_clear_dirty_issues_clear_cypher():
    store = _RecStore()
    ops = wgo.Neo4jWikiGraphOps(store)

    ops.clear_dirty("A", "deadbeef")

    assert store.calls == [(wgo._CLEAR, {"name": "A", "hash": "deadbeef"})]
    assert wgo._CLEAR == (
        "\nMATCH (e:__Entity__ {name: $name})\n"
        "SET e.wiki_dirty = false, e.wiki_hash = $hash, "
        "e.wiki_synced_at = datetime()\n"
    )


def test_neo4j_mark_all_dirty_issues_admin_mark_all_cypher_no_params():
    store = _RecStore()
    ops = wgo.Neo4jWikiGraphOps(store)

    ops.mark_all_dirty()

    assert store.calls == [(wgo._MARK_ALL_CYPHER, None)]
    assert wgo._MARK_ALL_CYPHER == (
        "MATCH (e:__Entity__) SET e.wiki_dirty = true, e.wiki_dirty_at = datetime()"
    )


# --- Neo4j: article-context reads (byte-for-byte guard) ----------------


def test_neo4j_read_subgraph_issues_subgraph_cypher_and_returns_raw_rows():
    canned_rows = [{
        "name": "ООО Альфа", "label": "Organization",
        "description": "A supplier.", "qid": "Q5", "page_title": "ООО Альфа",
        "relations": [
            {"rl": "заключила договор", "dir": "out",
             "nn": "Договор № 17-К", "nl": "Document", "rd": "signed"},
        ],
    }]
    store = _RecStore(rows=[canned_rows])
    ops = wgo.Neo4jWikiGraphOps(store)

    result = ops.read_subgraph("ООО Альфа", max_relations=30)

    assert result == canned_rows
    assert store.calls == [
        (wgo._SUBGRAPH_CYPHER, {"name": "ООО Альфа", "max_rel": 30}),
    ]
    assert wgo._SUBGRAPH_CYPHER == (
        "\nMATCH (e:__Entity__ {name: $name})\n"
        "OPTIONAL MATCH (e)-[r]-(m:__Entity__)\n"
        "WITH e, r, m, coalesce(m.mention_count, 0) AS mc\n"
        "ORDER BY mc DESC, m.name\n"
        "WITH e, collect(CASE WHEN m IS NULL THEN NULL ELSE {\n"
        "    rl: type(r),\n"
        "    dir: CASE WHEN startNode(r) = e THEN 'out' ELSE 'in' END,\n"
        "    nn: m.name,\n"
        "    nl: head([l IN labels(m) WHERE l <> '__Entity__' AND "
        "l <> '__Node__']),\n"
        "    rd: coalesce(r.description, '')\n"
        "  } END) AS rels\n"
        "RETURN e.name AS name,\n"
        "  head([l IN labels(e) WHERE l <> '__Entity__' AND "
        "l <> '__Node__']) AS label,\n"
        "  coalesce(e.description, '') AS description,\n"
        "  coalesce(e.wikibase_qid, '') AS qid,\n"
        "  coalesce(e.wiki_page_title, '') AS page_title,\n"
        "  [x IN rels WHERE x IS NOT NULL][0..$max_rel] AS relations\n"
    )


def test_neo4j_read_subgraph_returns_empty_list_when_no_rows():
    store = _RecStore(rows=[[]])
    assert wgo.Neo4jWikiGraphOps(store).read_subgraph("X", 30) == []


def test_neo4j_read_citations_issues_citations_cypher_and_returns_raw_rows():
    canned_rows = [
        {"text": "ООО Альфа заключила…", "doc_id": "d1"},
        {"text": "…контакт +7495…", "doc_id": "d2"},
    ]
    store = _RecStore(rows=[canned_rows])
    ops = wgo.Neo4jWikiGraphOps(store)

    result = ops.read_citations("ООО Альфа", k=5)

    assert result == canned_rows
    assert store.calls == [
        (wgo._CITATIONS_CYPHER, {"name": "ООО Альфа", "k": 5}),
    ]
    assert wgo._CITATIONS_CYPHER == (
        "\nMATCH (c:Chunk)-[:MENTIONS]->(e:__Entity__ {name: $name})\n"
        "WITH c.doc_id AS doc_id, c ORDER BY c.text\n"
        "WITH doc_id, collect(c)[0] AS c\n"
        "RETURN coalesce(c.text, '') AS text, doc_id\n"
        "ORDER BY doc_id LIMIT $k\n"
    )


def test_neo4j_read_source_docs_filters_and_extracts_doc_id():
    store = _RecStore(rows=[[
        {"doc_id": "d1"}, {"doc_id": "d2"}, {"doc_id": None}, {},
    ]])
    ops = wgo.Neo4jWikiGraphOps(store)

    result = ops.read_source_docs("X")

    assert result == ["d1", "d2"]
    assert store.calls == [(wgo._SOURCE_DOCS_CYPHER, {"name": "X"})]
    assert wgo._SOURCE_DOCS_CYPHER == (
        "\nMATCH (c:Chunk)-[:MENTIONS]->(e:__Entity__ {name: $name})\n"
        "RETURN DISTINCT c.doc_id AS doc_id ORDER BY doc_id\n"
    )


# --- Neo4j: sweep inline reads/writes (byte-for-byte guard) ------------


def test_neo4j_read_wiki_hash_extracts_h_from_first_row():
    store = _RecStore(rows=[[{"h": "deadbeef"}]])
    ops = wgo.Neo4jWikiGraphOps(store)

    result = ops.read_wiki_hash("X")

    assert result == "deadbeef"
    assert store.calls == [(wgo._READ_HASH_CYPHER, {"n": "X"})]
    assert wgo._READ_HASH_CYPHER == (
        "MATCH (e:__Entity__ {name:$n}) RETURN coalesce(e.wiki_hash,'') AS h"
    )


def test_neo4j_read_wiki_hash_returns_empty_string_when_no_rows():
    store = _RecStore(rows=[[]])
    assert wgo.Neo4jWikiGraphOps(store).read_wiki_hash("X") == ""


def test_neo4j_write_page_title_issues_set_cypher():
    store = _RecStore()
    ops = wgo.Neo4jWikiGraphOps(store)

    ops.write_page_title("X", "Page Title")

    assert store.calls == [
        (wgo._WRITE_TITLE_CYPHER, {"n": "X", "t": "Page Title"}),
    ]
    assert wgo._WRITE_TITLE_CYPHER == (
        "MATCH (e:__Entity__ {name:$n}) SET e.wiki_page_title=$t"
    )


# --- Dispatch ------------------------------------------------------------


def test_dispatch_returns_neo4j_when_backend_not_nebula(monkeypatch):
    monkeypatch.setattr(wgo.settings.graph, "backend", "neo4j")
    assert isinstance(wgo.build_wiki_graph_ops(_RecStore()), wgo.Neo4jWikiGraphOps)


def test_dispatch_returns_nebula_when_backend_nebula(monkeypatch):
    monkeypatch.setattr(wgo.settings.graph, "backend", "nebula")
    assert isinstance(wgo.build_wiki_graph_ops(_RecStore()), wgo.NebulaWikiGraphOps)


# --- Nebula: stub raises NotImplementedError (Task 3) -------------------


def test_nebula_stub_methods_all_raise_not_implemented():
    ops = wgo.NebulaWikiGraphOps(_RecStore())

    with pytest.raises(NotImplementedError):
        ops.mark_dirty(["A"])
    with pytest.raises(NotImplementedError):
        ops.select_dirty(10)
    with pytest.raises(NotImplementedError):
        ops.clear_dirty("A", "h")
    with pytest.raises(NotImplementedError):
        ops.mark_all_dirty()
    with pytest.raises(NotImplementedError):
        ops.read_subgraph("A", 30)
    with pytest.raises(NotImplementedError):
        ops.read_citations("A", 5)
    with pytest.raises(NotImplementedError):
        ops.read_source_docs("A")
    with pytest.raises(NotImplementedError):
        ops.read_wiki_hash("A")
    with pytest.raises(NotImplementedError):
        ops.write_page_title("A", "T")
