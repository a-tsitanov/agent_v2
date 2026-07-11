from __future__ import annotations

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


# --- Nebula: fake store -------------------------------------------------


class _NebulaRecStore:
    """Records nGQL statements (positional; nebula never binds param_map);
    returns canned rows keyed by the first matching substring of the
    statement (not popped — a substring may back several calls)."""

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


# --- Nebula: dirty-flag bookkeeping --------------------------------------


def test_nebula_mark_dirty_issues_update_vertex_per_name():
    from src.graph.nebula_store import entity_vid

    store = _NebulaRecStore()
    wgo.NebulaWikiGraphOps(store).mark_dirty(["A", "B"])

    assert len(store.calls) == 2
    stmt_a, pm_a = store.calls[0]
    stmt_b, pm_b = store.calls[1]
    assert pm_a is None and pm_b is None
    assert "UPDATE VERTEX ON `Entity`" in stmt_a
    assert entity_vid("A") in stmt_a
    assert "SET wiki_dirty = true, wiki_dirty_at =" in stmt_a
    assert "UPDATE VERTEX ON `Entity`" in stmt_b
    assert entity_vid("B") in stmt_b
    assert "SET wiki_dirty = true, wiki_dirty_at =" in stmt_b


def test_nebula_select_dirty_issues_lookup_ordered_limited_and_maps_names():
    store = _NebulaRecStore(canned=[
        ("LOOKUP ON `Entity`", [{"name": "A", "at": 100}, {"name": "B", "at": 200}]),
    ])

    result = wgo.NebulaWikiGraphOps(store).select_dirty(3)

    assert result == ["A", "B"]
    assert len(store.calls) == 1
    stmt, pm = store.calls[0]
    assert pm is None
    assert "wiki_dirty == true" in stmt
    assert "| ORDER BY $-.at ASC | LIMIT 3" in stmt


def test_nebula_clear_dirty_issues_update_vertex():
    from src.graph.nebula_store import entity_vid

    store = _NebulaRecStore()
    wgo.NebulaWikiGraphOps(store).clear_dirty("A", "h9")

    assert len(store.calls) == 1
    stmt, pm = store.calls[0]
    assert pm is None
    assert "UPDATE VERTEX ON `Entity`" in stmt
    assert entity_vid("A") in stmt
    assert 'SET wiki_dirty = false, wiki_hash = "h9", wiki_synced_at =' in stmt


def test_nebula_mark_all_dirty_issues_lookup_then_per_vid_update():
    store = _NebulaRecStore(canned=[
        ("LOOKUP ON `Entity`", [{"vid": "vidA"}, {"vid": "vidB"}]),
    ])

    wgo.NebulaWikiGraphOps(store).mark_all_dirty()

    assert len(store.calls) == 3  # 1 LOOKUP + 2 per-vid UPDATE
    lookup_stmt, lookup_pm = store.calls[0]
    assert lookup_pm is None
    assert "wiki_dirty != true" in lookup_stmt
    for stmt, pm in store.calls[1:]:
        assert pm is None
        assert "UPDATE VERTEX ON `Entity`" in stmt
        assert "SET wiki_dirty = true, wiki_dirty_at =" in stmt
    assert "vidA" in store.calls[1][0]
    assert "vidB" in store.calls[2][0]


# --- Nebula: article-context subgraph read -------------------------------


def test_nebula_read_subgraph_assembles_one_row_sorted_by_mention_count():
    from src.graph.nebula_store import entity_vid

    vid_a, vid_b, vid_c = entity_vid("A"), entity_vid("B"), entity_vid("C")
    store = _NebulaRecStore(canned=[
        ("AS page_title", [{
            "name": "A", "label": "Organization", "description": "Desc A",
            "qid": "Q1", "page_title": "Page A",
        }]),
        ("OVER `RELATED` BIDIRECT", [
            {"s": vid_a, "d": vid_b, "rl": "RELATED_TO"},   # out: A -> B
            {"s": vid_c, "d": vid_a, "rl": "MENTIONS"},      # in: C -> A
        ]),
        ("AS mc", [
            {"vid": vid_b, "nn": "B", "nl": "Person", "mc": 5},
            {"vid": vid_c, "nn": "C", "nl": "Document", "mc": 9},
        ]),
    ])

    result = wgo.NebulaWikiGraphOps(store).read_subgraph("A", max_relations=2)

    assert len(result) == 1
    row = result[0]
    assert row["name"] == "A"
    assert row["label"] == "Organization"
    assert row["description"] == "Desc A"
    assert row["qid"] == "Q1"
    assert row["page_title"] == "Page A"
    rels = row["relations"]
    assert len(rels) == 2
    # sorted by neighbour mention_count DESC -> C (mc=9) before B (mc=5)
    assert rels[0] == {"rl": "MENTIONS", "dir": "in", "nn": "C", "nl": "Document", "rd": ""}
    assert rels[1] == {"rl": "RELATED_TO", "dir": "out", "nn": "B", "nl": "Person", "rd": ""}
    assert all(pm is None for _, pm in store.calls)


def test_nebula_read_subgraph_caps_at_max_relations():
    from src.graph.nebula_store import entity_vid

    vid_a, vid_b, vid_c = entity_vid("A"), entity_vid("B"), entity_vid("C")
    store = _NebulaRecStore(canned=[
        ("AS page_title", [{
            "name": "A", "label": "Organization", "description": "",
            "qid": "", "page_title": "",
        }]),
        ("OVER `RELATED` BIDIRECT", [
            {"s": vid_a, "d": vid_b, "rl": "RELATED_TO"},
            {"s": vid_c, "d": vid_a, "rl": "MENTIONS"},
        ]),
        ("AS mc", [
            {"vid": vid_b, "nn": "B", "nl": "Person", "mc": 5},
            {"vid": vid_c, "nn": "C", "nl": "Document", "mc": 9},
        ]),
    ])

    result = wgo.NebulaWikiGraphOps(store).read_subgraph("A", max_relations=1)

    assert len(result[0]["relations"]) == 1
    assert result[0]["relations"][0]["nn"] == "C"  # higher mention_count kept


def test_nebula_read_subgraph_returns_empty_when_entity_not_found():
    store = _NebulaRecStore(canned=[("AS page_title", [])])

    result = wgo.NebulaWikiGraphOps(store).read_subgraph("Ghost", max_relations=10)

    assert result == []
    # missing entity short-circuits before GO/neighbour FETCH.
    assert len(store.calls) == 1


# --- Nebula: chunk-dependent reads (deferred -> []) ----------------------


def test_nebula_read_citations_returns_empty():
    assert wgo.NebulaWikiGraphOps(_NebulaRecStore()).read_citations("A", 5) == []


def test_nebula_read_source_docs_returns_empty():
    assert wgo.NebulaWikiGraphOps(_NebulaRecStore()).read_source_docs("A") == []


# --- Nebula: sweep inline reads/writes -----------------------------------


def test_nebula_read_wiki_hash_extracts_h():
    store = _NebulaRecStore(canned=[("AS h", [{"h": "deadbeef"}])])
    assert wgo.NebulaWikiGraphOps(store).read_wiki_hash("A") == "deadbeef"


def test_nebula_read_wiki_hash_returns_empty_string_when_no_rows():
    store = _NebulaRecStore(canned=[("AS h", [])])
    assert wgo.NebulaWikiGraphOps(store).read_wiki_hash("A") == ""


def test_nebula_write_page_title_issues_update_vertex():
    from src.graph.nebula_store import entity_vid

    store = _NebulaRecStore()
    wgo.NebulaWikiGraphOps(store).write_page_title("A", "T")

    assert len(store.calls) == 1
    stmt, pm = store.calls[0]
    assert pm is None
    assert "UPDATE VERTEX ON `Entity`" in stmt
    assert entity_vid("A") in stmt
    assert 'SET wiki_page_title = "T"' in stmt
