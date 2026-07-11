from __future__ import annotations

from src.graph import graph_edge_export as gee
from src.graph.nebula_store import entity_vid


class _RecStore:
    """Records (cypher, param_map) calls; returns canned pages per call,
    popped in call order, keyed by query kind (nodes vs edges)."""

    def __init__(self, node_pages=None, edge_pages=None):
        self.calls: list[tuple[str, dict | None]] = []
        self._node_pages = list(node_pages or [])
        self._edge_pages = list(edge_pages or [])

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map))
        if "RETURN e.name AS name" in cypher:
            return self._node_pages.pop(0) if self._node_pages else []
        return self._edge_pages.pop(0) if self._edge_pages else []


class _RecNebula:
    """Fake nebula store: records nGQL (asserts inline, no param_map);
    returns canned pages per call, popped in call order, keyed by
    statement kind (LOOKUP vs GO)."""

    def __init__(self, lookup_pages=None, go_pages=None):
        self.stmts: list[str] = []
        self._lookup_pages = list(lookup_pages or [])
        self._go_pages = list(go_pages or [])

    def structured_query(self, query, param_map=None):
        assert not param_map, "nebula read must inline values"
        self.stmts.append(query)
        if query.startswith("LOOKUP ON `Entity`"):
            return self._lookup_pages.pop(0) if self._lookup_pages else []
        if query.startswith("GO FROM"):
            return self._go_pages.pop(0) if self._go_pages else []
        return []


# --- Neo4j: byte-for-byte guard ---------------------------------------


def test_neo4j_stream_names_issues_nodes_cypher_across_pages_and_concatenates():
    pages = [
        [{"name": "A"}, {"name": "B"}],   # full page (== batch_size) -> continue
        [{"name": "C"}],                  # short page -> stop
    ]
    store = _RecStore(node_pages=pages)
    exp = gee.Neo4jGraphEdgeExport(store)

    names = exp.stream_names(batch_size=2)

    assert names == ["A", "B", "C"]
    assert store.calls == [
        (gee._NODES_CYPHER, {"after": "", "limit": 2}),
        (gee._NODES_CYPHER, {"after": "B", "limit": 2}),
    ]


def test_neo4j_stream_edges_issues_edges_cypher_across_pages_floats_weight():
    pages = [
        [{"src": "A", "tgt": "B", "weight": 2, "cursor": "r1"}],   # full page -> continue
        [{"src": "B", "tgt": "C", "weight": None, "cursor": "r2"}],  # full page -> continue
        [],                                                          # empty -> stop
    ]
    store = _RecStore(edge_pages=pages)
    exp = gee.Neo4jGraphEdgeExport(store)

    edges = exp.stream_edges(batch_size=1)

    assert edges == [("A", "B", 2.0), ("B", "C", 1.0)]
    assert store.calls == [
        (gee._EDGES_CYPHER, {"after": "", "limit": 1}),
        (gee._EDGES_CYPHER, {"after": "r1", "limit": 1}),
        (gee._EDGES_CYPHER, {"after": "r2", "limit": 1}),
    ]


def test_neo4j_stream_names_stops_on_empty_page():
    store = _RecStore(node_pages=[[]])
    exp = gee.Neo4jGraphEdgeExport(store)
    assert exp.stream_names(batch_size=10) == []


def test_neo4j_stream_edges_stops_when_cursor_does_not_advance():
    # last row's cursor equal to `after` ("") -> must break, not loop forever.
    pages = [[{"src": "X", "tgt": "X", "weight": 1.0, "cursor": None}]]
    store = _RecStore(edge_pages=pages)
    exp = gee.Neo4jGraphEdgeExport(store)
    edges = exp.stream_edges(batch_size=10)
    assert edges == [("X", "X", 1.0)]


def test_neo4j_stream_edges_ignores_names_still_issues_edges_cypher():
    # neo4j runs its own self-contained _EDGES_CYPHER query; a `names` list
    # passed in must be ignored — behaviour identical to names=None.
    pages = [[{"src": "A", "tgt": "B", "weight": 2.0, "cursor": "r1"}]]
    store = _RecStore(edge_pages=pages)
    exp = gee.Neo4jGraphEdgeExport(store)

    edges = exp.stream_edges(batch_size=10, names=["A", "B", "unused"])

    assert edges == [("A", "B", 2.0)]
    assert store.calls == [(gee._EDGES_CYPHER, {"after": "", "limit": 10})]


# --- Nebula: nGQL keyset LOOKUP + batched GO ----------------------------


def test_nebula_stream_names_keyset_lookup_advances_after_and_stops():
    pages = [
        [{"name": "A"}, {"name": "B"}],   # full page (== batch_size) -> continue
        [{"name": "C"}],                  # short page -> stop
    ]
    store = _RecNebula(lookup_pages=pages)
    exp = gee.NebulaGraphEdgeExport(store)

    names = exp.stream_names(batch_size=2)

    assert names == ["A", "B", "C"]
    assert store.stmts == [
        'LOOKUP ON `Entity` WHERE `Entity`.name > "" YIELD `Entity`.name AS name '
        "| ORDER BY $-.name ASC LIMIT 2;",
        'LOOKUP ON `Entity` WHERE `Entity`.name > "B" YIELD `Entity`.name AS name '
        "| ORDER BY $-.name ASC LIMIT 2;",
    ]


def test_nebula_stream_names_no_param_map():
    store = _RecNebula(lookup_pages=[[]])
    gee.NebulaGraphEdgeExport(store).stream_names(batch_size=5)
    # _RecNebula.structured_query already asserts param_map is falsy; if we
    # got here without AssertionError, the seam is inline-only as required.


def test_nebula_stream_edges_with_names_skips_internal_name_scan():
    # Single-scan path: caller already streamed `names` (the real usage via
    # extract_entity_edges) -> stream_edges must NOT re-issue the LOOKUP
    # name-scan, only the GO edge queries.
    vid_a, vid_b = entity_vid("A"), entity_vid("B")
    dangling_vid = entity_vid("ghost")  # not a member of the exported name set

    go_pages = [[
        {"s": vid_a, "d": vid_b, "w": 3.0},        # known edge, explicit weight
        {"s": vid_a, "d": dangling_vid, "w": 2.0},  # dangling endpoint -> dropped
        {"s": vid_b, "d": vid_a, "w": None},        # known edge, weight defaults to 1.0
    ]]
    store = _RecNebula(go_pages=go_pages)  # no lookup_pages: LOOKUP must never fire
    exp = gee.NebulaGraphEdgeExport(store)

    edges = exp.stream_edges(batch_size=10, names=["A", "B"])

    assert edges == [("A", "B", 3.0), ("B", "A", 1.0)]
    lookup_stmts = [s for s in store.stmts if s.startswith("LOOKUP ON")]
    go_stmts = [s for s in store.stmts if s.startswith("GO FROM")]
    assert lookup_stmts == [], "names was supplied — must not re-scan via LOOKUP"
    assert len(go_stmts) == 1
    assert "OVER `RELATED` YIELD" in go_stmts[0]
    assert "src(edge) AS s, dst(edge) AS d, `RELATED`.weight AS w" in go_stmts[0]


def test_nebula_stream_edges_names_none_falls_back_to_internal_stream_names():
    # Fallback path: names=None -> stream_edges internally calls
    # stream_names(...) (today's behaviour), issuing the LOOKUP scan.
    vid_a, vid_b = entity_vid("A"), entity_vid("B")
    dangling_vid = entity_vid("ghost")

    lookup_pages = [[{"name": "A"}, {"name": "B"}]]  # single page, short -> stop
    go_pages = [[
        {"s": vid_a, "d": vid_b, "w": 3.0},
        {"s": vid_a, "d": dangling_vid, "w": 2.0},  # dangling endpoint -> dropped
        {"s": vid_b, "d": vid_a, "w": None},
    ]]
    store = _RecNebula(lookup_pages=lookup_pages, go_pages=go_pages)
    exp = gee.NebulaGraphEdgeExport(store)

    edges = exp.stream_edges(batch_size=10)

    assert edges == [("A", "B", 3.0), ("B", "A", 1.0)]
    lookup_stmts = [s for s in store.stmts if s.startswith("LOOKUP ON")]
    go_stmts = [s for s in store.stmts if s.startswith("GO FROM")]
    assert len(lookup_stmts) == 1, "names=None must fall back to the internal LOOKUP scan"
    assert len(go_stmts) == 1


def test_nebula_stream_edges_chunks_go_calls_by_batch_size():
    vid_a, vid_b, vid_c = entity_vid("A"), entity_vid("B"), entity_vid("C")
    go_pages = [
        [{"s": vid_a, "d": vid_b, "w": 1.0}],  # first chunk of 2 vids
        [{"s": vid_c, "d": vid_a, "w": 5.0}],  # second chunk of 1 vid
    ]
    store = _RecNebula(go_pages=go_pages)
    exp = gee.NebulaGraphEdgeExport(store)

    edges = exp.stream_edges(batch_size=2, names=["A", "B", "C"])

    assert edges == [("A", "B", 1.0), ("C", "A", 5.0)]
    go_stmts = [s for s in store.stmts if s.startswith("GO FROM")]
    assert len(go_stmts) == 2


# --- Dispatch ------------------------------------------------------------


def test_dispatch_returns_neo4j_when_backend_not_nebula(monkeypatch):
    monkeypatch.setattr(gee.settings.graph, "backend", "neo4j")
    assert isinstance(gee.build_graph_edge_export(_RecStore()), gee.Neo4jGraphEdgeExport)


def test_dispatch_returns_nebula_when_backend_nebula(monkeypatch):
    monkeypatch.setattr(gee.settings.graph, "backend", "nebula")
    assert isinstance(gee.build_graph_edge_export(_RecNebula()), gee.NebulaGraphEdgeExport)
