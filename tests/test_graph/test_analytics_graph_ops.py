from __future__ import annotations

from src.analytics.ids import ID_TYPES
from src.graph import analytics_graph_ops as ago


class _RecStore:
    """Records (cypher, param_map) calls; returns canned rows per call,
    popped in call order."""

    def __init__(self, rows=None):
        self.calls: list[tuple[str, dict | None]] = []
        self._rows = list(rows or [])

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map))
        return self._rows.pop(0) if self._rows else []


class _RaisingStore:
    """Always raises on structured_query — used to assert the seam's
    fail-soft try/except swallows the error and returns []."""

    def __init__(self):
        self.calls: list[tuple[str, dict | None]] = []

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map))
        raise RuntimeError("boom")


# --- Neo4j: byte-for-byte guard (cypher + params) ----------------------


def test_neo4j_entity_core_issues_core_cypher():
    store = _RecStore(rows=[[{"name": "A"}]])
    ops = ago.Neo4jAnalyticsGraphOps(store)

    result = ops.entity_core("A")

    assert result == [{"name": "A"}]
    assert store.calls == [(ago._CORE, {"name": "A"})]
    assert ago._CORE == (
        "MATCH (e:__Entity__ {name:$name}) "
        "RETURN e.name AS name, e.description AS description, labels(e) AS labels, "
        "e.mention_count AS mention_count"
    )


def test_neo4j_entity_neighbors_issues_neighbors_cypher():
    store = _RecStore(rows=[[{"rel": "OWNS", "name": "B"}]])
    ops = ago.Neo4jAnalyticsGraphOps(store)

    result = ops.entity_neighbors("A", 10)

    assert result == [{"rel": "OWNS", "name": "B"}]
    assert store.calls == [
        (ago._NEIGHBORS, {"name": "A", "top_n": 10, "id_types": ID_TYPES}),
    ]
    assert ago._NEIGHBORS == (
        "MATCH (e:__Entity__ {name:$name})-[r]-(n:__Entity__) "
        "WHERE (r.polarity IS NULL OR r.polarity <> 'negated') AND NONE(l IN labels(n) WHERE l IN $id_types) "
        "RETURN type(r) AS rel, n.name AS name, "
        "[l IN labels(n) WHERE l <> '__Entity__' AND l <> '__Node__'][0] AS ntype, r.weight AS w "
        "ORDER BY r.weight DESC LIMIT $top_n"
    )


def test_neo4j_entity_identifiers_issues_identifiers_cypher():
    store = _RecStore(rows=[[{"id_type": "INN", "value": "123"}]])
    ops = ago.Neo4jAnalyticsGraphOps(store)

    result = ops.entity_identifiers("A", ID_TYPES, 7)

    assert result == [{"id_type": "INN", "value": "123"}]
    # top_n is caller-supplied (7 here), NOT a hardcoded default — proves the
    # primitive's user-adjustable top_n reaches the query.
    assert store.calls == [
        (ago._IDENTIFIERS, {"name": "A", "id_types": ID_TYPES, "top_n": 7}),
    ]
    assert ago._IDENTIFIERS == (
        "MATCH (e:__Entity__ {name:$name})-[]-(id:__Entity__) "
        "WHERE any(l IN labels(id) WHERE l IN $id_types) "
        "RETURN [l IN labels(id) WHERE l IN $id_types][0] AS id_type, id.name AS value "
        "LIMIT $top_n"
    )


def test_neo4j_entity_communities_issues_communities_cypher():
    store = _RecStore(rows=[[{"level": 0, "title": "T"}]])
    ops = ago.Neo4jAnalyticsGraphOps(store)

    result = ops.entity_communities("A")

    assert result == [{"level": 0, "title": "T"}]
    assert store.calls == [(ago._COMMUNITIES, {"name": "A"})]
    assert ago._COMMUNITIES == (
        "MATCH (e:__Entity__ {name:$name})-[:IN_COMMUNITY]->(c:Community) "
        "RETURN c.level AS level, c.title AS title"
    )


def test_neo4j_neighbors_by_relation_issues_relation_cypher():
    store = _RecStore(rows=[[{"name": "B", "w": 1.0}]])
    ops = ago.Neo4jAnalyticsGraphOps(store)

    result = ops.neighbors_by_relation("A", "OWNS", "negated", 5)

    assert result == [{"name": "B", "w": 1.0}]
    assert store.calls == [
        (
            (
                "MATCH (e:__Entity__ {name:$name})-[r]-(n:__Entity__) "
                "WHERE type(r)=$rel_type AND ($polarity IS NULL OR r.polarity=$polarity) "
                "RETURN n.name AS name, r.weight AS w, r.valid_from AS valid_from, "
                "r.valid_to AS valid_to "
                "ORDER BY r.weight DESC LIMIT $top_n"
            ),
            {"name": "A", "rel_type": "OWNS", "polarity": "negated", "top_n": 5},
        ),
    ]


def test_neo4j_common_connections_issues_bridge_cypher():
    store = _RecStore(rows=[[{"name": "M", "type": "Organization", "via": ["OWNS"]}]])
    ops = ago.Neo4jAnalyticsGraphOps(store)

    result = ops.common_connections("A", "B", 5)

    assert result == [{"name": "M", "type": "Organization", "via": ["OWNS"]}]
    assert store.calls == [
        (
            (
                "MATCH (x:__Entity__ {name:$a})-[r1]-(m:__Entity__)-[r2]-"
                "(y:__Entity__ {name:$b}) "
                "WHERE (r1.polarity IS NULL OR r1.polarity<>'negated') AND (r2.polarity IS NULL OR r2.polarity<>'negated') "
                "RETURN m.name AS name, [l IN labels(m) WHERE l<>'__Entity__' AND l<>'__Node__'][0] AS type, "
                "collect(DISTINCT type(r1))+collect(DISTINCT type(r2)) AS via "
                "ORDER BY size(via) DESC LIMIT $top_n"
            ),
            {"a": "A", "b": "B", "top_n": 5},
        ),
    ]


def test_neo4j_identifier_lookup_issues_lookup_cypher():
    store = _RecStore(rows=[[{"name": "A", "labels": ["Organization"], "rel": "HAS_ID"}]])
    ops = ago.Neo4jAnalyticsGraphOps(store)

    result = ops.identifier_lookup("7701234567")

    assert result == [{"name": "A", "labels": ["Organization"], "rel": "HAS_ID"}]
    assert store.calls == [
        (
            (
                "MATCH (id:__Entity__ {name:$value})-[r]-(e:__Entity__) "
                "WHERE any(l IN labels(id) WHERE l IN $id_types) "
                "AND NONE(l IN labels(e) WHERE l IN $id_types) "
                "RETURN e.name AS name, labels(e) AS labels, type(r) AS rel"
            ),
            {"value": "7701234567", "id_types": ID_TYPES},
        ),
    ]


def test_neo4j_shared_identifier_entities_issues_grouping_cypher():
    store = _RecStore(rows=[[{"value": "123", "id_type": "INN", "owners": ["A", "B"]}]])
    ops = ago.Neo4jAnalyticsGraphOps(store)

    result = ops.shared_identifier_entities("INN", 5)

    assert result == [{"value": "123", "id_type": "INN", "owners": ["A", "B"]}]
    assert store.calls == [
        (
            (
                "MATCH (id:__Entity__) WHERE any(l IN labels(id) WHERE l IN $id_types) "
                "AND ($id_type IS NULL OR $id_type IN labels(id)) "
                "MATCH (id)-[]-(owner:__Entity__) "
                "WHERE NONE(l IN labels(owner) WHERE l IN $id_types) "
                "WITH id, [l IN labels(id) WHERE l IN $id_types][0] AS id_type, "
                "collect(DISTINCT owner.name) AS owners "
                "WHERE size(owners) >= $min_owners "
                "RETURN id.name AS value, id_type, owners ORDER BY size(owners) DESC "
                "LIMIT $top_n"
            ),
            {
                "id_type": "INN",
                "min_owners": ago._DEFAULT_MIN_OWNERS,
                "top_n": 5,
                "id_types": ID_TYPES,
            },
        ),
    ]


def test_neo4j_connection_path_inlines_hops_and_issues_shortest_path_cypher():
    store = _RecStore(rows=[[{"path": ["A", "B"], "rels": ["OWNS"], "hops": 1}]])
    ops = ago.Neo4jAnalyticsGraphOps(store)

    result = ops.connection_path("A", "B", 6)

    assert result == [{"path": ["A", "B"], "rels": ["OWNS"], "hops": 1}]
    assert store.calls == [
        (
            (
                "MATCH (a:__Entity__ {name:$source}),(b:__Entity__ {name:$target}) "
                "MATCH p = shortestPath((a)-[*..6]-(b)) "
                "RETURN [n IN nodes(p)|n.name] AS path, [r IN relationships(p)|type(r)] AS "
                "rels, length(p) AS hops"
            ),
            {"source": "A", "target": "B", "max_hops": 6},
        ),
    ]


def test_neo4j_connection_path_inlines_a_different_hops_value():
    store = _RecStore()
    ops = ago.Neo4jAnalyticsGraphOps(store)

    ops.connection_path("A", "B", 12)

    cypher, params = store.calls[0]
    assert "*..12" in cypher
    assert params == {"source": "A", "target": "B", "max_hops": 12}


def test_neo4j_cooccurrence_issues_chunk_cypher():
    store = _RecStore(rows=[[{"name": "B", "shared": 3}]])
    ops = ago.Neo4jAnalyticsGraphOps(store)

    result = ops.cooccurrence("A", 5)

    assert result == [{"name": "B", "shared": 3}]
    assert store.calls == [
        (
            (
                "MATCH (e:__Entity__ {name:$name})<-[:MENTIONS]-(c:Chunk)-[:MENTIONS]->"
                "(other:__Entity__) "
                "WHERE other <> e "
                "RETURN other.name AS name, count(DISTINCT c) AS shared ORDER BY shared DESC "
                "LIMIT $top_n"
            ),
            {"name": "A", "top_n": 5},
        ),
    ]


# --- Neo4j: fail-soft (mirrors store_query.run_rows) --------------------


def test_neo4j_entity_core_fail_soft_returns_empty_on_raise():
    store = _RaisingStore()
    ops = ago.Neo4jAnalyticsGraphOps(store)

    assert ops.entity_core("A") == []
    assert len(store.calls) == 1  # the query was attempted


def test_neo4j_entity_neighbors_fail_soft_returns_empty_on_raise():
    assert ago.Neo4jAnalyticsGraphOps(_RaisingStore()).entity_neighbors("A", 5) == []


def test_neo4j_entity_identifiers_fail_soft_returns_empty_on_raise():
    assert ago.Neo4jAnalyticsGraphOps(_RaisingStore()).entity_identifiers("A", ID_TYPES, 7) == []


def test_neo4j_entity_communities_fail_soft_returns_empty_on_raise():
    assert ago.Neo4jAnalyticsGraphOps(_RaisingStore()).entity_communities("A") == []


def test_neo4j_neighbors_by_relation_fail_soft_returns_empty_on_raise():
    assert ago.Neo4jAnalyticsGraphOps(_RaisingStore()).neighbors_by_relation("A", "OWNS", None, 5) == []


def test_neo4j_common_connections_fail_soft_returns_empty_on_raise():
    assert ago.Neo4jAnalyticsGraphOps(_RaisingStore()).common_connections("A", "B", 5) == []


def test_neo4j_identifier_lookup_fail_soft_returns_empty_on_raise():
    assert ago.Neo4jAnalyticsGraphOps(_RaisingStore()).identifier_lookup("123") == []


def test_neo4j_shared_identifier_entities_fail_soft_returns_empty_on_raise():
    assert (
        ago.Neo4jAnalyticsGraphOps(_RaisingStore()).shared_identifier_entities("INN", 5) == []
    )


def test_neo4j_connection_path_fail_soft_returns_empty_on_raise():
    assert ago.Neo4jAnalyticsGraphOps(_RaisingStore()).connection_path("A", "B", 6) == []


def test_neo4j_cooccurrence_fail_soft_returns_empty_on_raise():
    assert ago.Neo4jAnalyticsGraphOps(_RaisingStore()).cooccurrence("A", 5) == []


def test_neo4j_fail_soft_logs_warning(monkeypatch):
    warnings: list[str] = []
    monkeypatch.setattr(
        ago.logger, "warning", lambda msg, **kw: warnings.append(msg.format(**kw))
    )
    ago.Neo4jAnalyticsGraphOps(_RaisingStore()).entity_core("A")
    assert len(warnings) == 1
    assert "analytics query failed" in warnings[0]


# --- Dispatch ------------------------------------------------------------


def test_dispatch_returns_neo4j_when_backend_not_nebula(monkeypatch):
    monkeypatch.setattr(ago.settings.graph, "backend", "neo4j")
    assert isinstance(ago.build_analytics_graph_ops(_RecStore()), ago.Neo4jAnalyticsGraphOps)


def test_dispatch_returns_nebula_when_backend_nebula(monkeypatch):
    monkeypatch.setattr(ago.settings.graph, "backend", "nebula")
    assert isinstance(ago.build_analytics_graph_ops(_RecStore()), ago.NebulaAnalyticsGraphOps)


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


class _NebulaRaisingStore:
    def __init__(self):
        self.calls: list[tuple[str, dict | None]] = []

    def structured_query(self, stmt, param_map=None):
        self.calls.append((stmt, param_map))
        raise RuntimeError("boom")


# Fakes for the nebula3 Path data API (PathWrapper.nodes()/.relationships()
# — see NebulaAnalyticsGraphOps._path_names_and_rels).


class _FakeVal:
    def __init__(self, v):
        self._v = v

    def cast(self):
        return self._v


class _FakeNode:
    def __init__(self, vid):
        self._vid = vid

    def get_id(self):
        return _FakeVal(self._vid)


class _FakeRel:
    def __init__(self, rel_type=None, edge_name="RELATED"):
        self._rel_type = rel_type
        self._edge_name = edge_name

    def properties(self):
        if self._rel_type is None:
            return {}
        return {"rel_type": _FakeVal(self._rel_type)}

    def edge_name(self):
        return self._edge_name


class _FakePath:
    def __init__(self, node_vids, rels):
        self._nodes = [_FakeNode(v) for v in node_vids]
        self._rels = list(rels)

    def nodes(self):
        return self._nodes

    def relationships(self):
        return self._rels


# --- Nebula: entity_core -------------------------------------------------


def test_nebula_entity_core_fetches_and_maps_row():
    from src.graph.nebula_store import entity_vid

    vid = entity_vid("A")
    store = _NebulaRecStore(canned=[
        ("FETCH PROP ON `Entity`", [
            {"name": "A", "description": "d", "label": "Organization", "mention_count": 4},
        ]),
    ])
    ops = ago.NebulaAnalyticsGraphOps(store)

    result = ops.entity_core("A")

    assert result == [
        {"name": "A", "description": "d", "labels": ["Organization"], "mention_count": 4},
    ]
    stmt, pm = store.calls[0]
    assert pm is None
    assert vid in stmt
    assert stmt.startswith("FETCH PROP ON `Entity`")


def test_nebula_entity_core_returns_empty_when_fetch_empty():
    store = _NebulaRecStore(canned=[("FETCH PROP ON `Entity`", [])])
    assert ago.NebulaAnalyticsGraphOps(store).entity_core("Ghost") == []


def test_nebula_entity_core_fail_soft_returns_empty_on_raise():
    store = _NebulaRaisingStore()
    assert ago.NebulaAnalyticsGraphOps(store).entity_core("A") == []
    assert len(store.calls) == 1


# --- Nebula: entity_neighbors --------------------------------------------


def test_nebula_entity_neighbors_filters_id_types_and_negated_orders_by_weight():
    from src.graph.nebula_store import entity_vid

    va = entity_vid("A")
    vb, vc, vd = entity_vid("B"), entity_vid("C"), entity_vid("D")
    store = _NebulaRecStore(canned=[
        ("OVER `RELATED` BIDIRECT", [
            {"s": va, "d": vb, "rl": "OWNS", "w": 1.0, "pol": None},
            {"s": vc, "d": va, "rl": "MENTIONS", "w": 5.0, "pol": None},
            {"s": va, "d": vd, "rl": "HAS_EMAIL", "w": 9.0, "pol": None},
            {"s": va, "d": vb, "rl": "DISPUTES", "w": 8.0, "pol": "negated"},
        ]),
        ("FETCH PROP ON `Entity`", [
            {"vid": vb, "name": "B", "label": "Organization"},
            {"vid": vc, "name": "C", "label": "Person"},
            {"vid": vd, "name": "d@x.com", "label": "Email"},
        ]),
    ])
    ops = ago.NebulaAnalyticsGraphOps(store)

    result = ops.entity_neighbors("A", 10)

    # HAS_EMAIL -> vd excluded (Email is an ID_TYPE label); DISPUTES (negated)
    # excluded regardless of weight; remaining two ordered by weight desc.
    assert result == [
        {"rel": "MENTIONS", "name": "C", "ntype": "Person", "w": 5.0},
        {"rel": "OWNS", "name": "B", "ntype": "Organization", "w": 1.0},
    ]


def test_nebula_entity_neighbors_caps_at_top_n():
    from src.graph.nebula_store import entity_vid

    va = entity_vid("A")
    vb, vc = entity_vid("B"), entity_vid("C")
    store = _NebulaRecStore(canned=[
        ("OVER `RELATED` BIDIRECT", [
            {"s": va, "d": vb, "rl": "OWNS", "w": 1.0, "pol": None},
            {"s": va, "d": vc, "rl": "OWNS", "w": 9.0, "pol": None},
        ]),
        ("FETCH PROP ON `Entity`", [
            {"vid": vb, "name": "B", "label": "Organization"},
            {"vid": vc, "name": "C", "label": "Organization"},
        ]),
    ])
    result = ago.NebulaAnalyticsGraphOps(store).entity_neighbors("A", 1)
    assert result == [{"rel": "OWNS", "name": "C", "ntype": "Organization", "w": 9.0}]


def test_nebula_entity_neighbors_returns_empty_when_no_edges():
    store = _NebulaRecStore(canned=[("OVER `RELATED` BIDIRECT", [])])
    assert ago.NebulaAnalyticsGraphOps(store).entity_neighbors("A", 5) == []


def test_nebula_entity_neighbors_fail_soft_returns_empty_on_raise():
    assert ago.NebulaAnalyticsGraphOps(_NebulaRaisingStore()).entity_neighbors("A", 5) == []


# --- Nebula: entity_identifiers ------------------------------------------


def test_nebula_entity_identifiers_keeps_only_id_type_neighbours():
    from src.graph.nebula_store import entity_vid

    va = entity_vid("A")
    vb, vc = entity_vid("B"), entity_vid("C")
    store = _NebulaRecStore(canned=[
        ("OVER `RELATED` BIDIRECT", [
            {"s": va, "d": vb, "rl": "HAS_INN"},
            {"s": vc, "d": va, "rl": "OWNS"},
        ]),
        ("FETCH PROP ON `Entity`", [
            {"vid": vb, "name": "7701234567", "label": "INN"},
            {"vid": vc, "name": "C", "label": "Organization"},
        ]),
    ])
    result = ago.NebulaAnalyticsGraphOps(store).entity_identifiers("A", ID_TYPES, 5)
    assert result == [{"id_type": "INN", "value": "7701234567"}]


def test_nebula_entity_identifiers_returns_empty_when_no_edges():
    store = _NebulaRecStore(canned=[("OVER `RELATED` BIDIRECT", [])])
    assert ago.NebulaAnalyticsGraphOps(store).entity_identifiers("A", ID_TYPES, 5) == []


def test_nebula_entity_identifiers_fail_soft_returns_empty_on_raise():
    assert (
        ago.NebulaAnalyticsGraphOps(_NebulaRaisingStore()).entity_identifiers("A", ID_TYPES, 5)
        == []
    )


# --- Nebula: entity_communities ------------------------------------------


def test_nebula_entity_communities_go_then_fetch():
    from src.graph.nebula_store import entity_vid

    va = entity_vid("A")
    store = _NebulaRecStore(canned=[
        ("OVER `IN_COMMUNITY`", [{"c": "commA"}, {"c": "commB"}]),
        ("FETCH PROP ON `Community`", [
            {"level": 0, "title": "T1"},
            {"level": 1, "title": "T2"},
        ]),
    ])
    result = ago.NebulaAnalyticsGraphOps(store).entity_communities("A")
    assert result == [{"level": 0, "title": "T1"}, {"level": 1, "title": "T2"}]
    go_stmt = store.calls[0][0]
    assert va in go_stmt


def test_nebula_entity_communities_returns_empty_when_no_edges():
    store = _NebulaRecStore(canned=[("OVER `IN_COMMUNITY`", [])])
    assert ago.NebulaAnalyticsGraphOps(store).entity_communities("A") == []


def test_nebula_entity_communities_fail_soft_returns_empty_on_raise():
    assert ago.NebulaAnalyticsGraphOps(_NebulaRaisingStore()).entity_communities("A") == []


# --- Nebula: neighbors_by_relation ---------------------------------------


def test_nebula_neighbors_by_relation_no_polarity_filter_when_none_orders_by_weight():
    from src.graph.nebula_store import entity_vid

    va = entity_vid("A")
    vb, vc = entity_vid("B"), entity_vid("C")
    store = _NebulaRecStore(canned=[
        ("OVER `RELATED` BIDIRECT", [
            {"s": va, "d": vb, "rl": "OWNS", "w": 3.0, "pol": None, "vf": 1, "vt": 2},
            {"s": vc, "d": va, "rl": "OWNS", "w": 9.0, "pol": "negated", "vf": 3, "vt": 4},
            {"s": va, "d": vc, "rl": "MENTIONS", "w": 7.0, "pol": None, "vf": 5, "vt": 6},
        ]),
        ("FETCH PROP ON `Entity`", [
            {"vid": vb, "name": "B"},
            {"vid": vc, "name": "C"},
        ]),
    ])
    # polarity=None -> no polarity filtering (matches neo4j's
    # "$polarity IS NULL OR r.polarity=$polarity"); only rel_type filters.
    result = ago.NebulaAnalyticsGraphOps(store).neighbors_by_relation("A", "OWNS", None, 5)
    assert result == [
        {"name": "C", "w": 9.0, "valid_from": 3, "valid_to": 4},
        {"name": "B", "w": 3.0, "valid_from": 1, "valid_to": 2},
    ]


def test_nebula_neighbors_by_relation_polarity_filter_when_given():
    from src.graph.nebula_store import entity_vid

    va = entity_vid("A")
    vb, vc = entity_vid("B"), entity_vid("C")
    store = _NebulaRecStore(canned=[
        ("OVER `RELATED` BIDIRECT", [
            {"s": va, "d": vb, "rl": "OWNS", "w": 3.0, "pol": None, "vf": 1, "vt": 2},
            {"s": vc, "d": va, "rl": "OWNS", "w": 9.0, "pol": "negated", "vf": 3, "vt": 4},
        ]),
        ("FETCH PROP ON `Entity`", [
            {"vid": vb, "name": "B"},
            {"vid": vc, "name": "C"},
        ]),
    ])
    result = ago.NebulaAnalyticsGraphOps(store).neighbors_by_relation(
        "A", "OWNS", "negated", 5
    )
    assert result == [{"name": "C", "w": 9.0, "valid_from": 3, "valid_to": 4}]


def test_nebula_neighbors_by_relation_fail_soft_returns_empty_on_raise():
    assert (
        ago.NebulaAnalyticsGraphOps(_NebulaRaisingStore()).neighbors_by_relation(
            "A", "OWNS", None, 5
        )
        == []
    )


# --- Nebula: common_connections ------------------------------------------


def test_nebula_common_connections_intersects_neighbours_and_collects_via():
    from src.graph.nebula_store import entity_vid

    va, vb = entity_vid("A"), entity_vid("B")
    vm, vx = entity_vid("M"), entity_vid("X")
    store = _NebulaRecStore(canned=[
        (va, [
            {"s": va, "d": vm, "rl": "OWNS", "pol": None},
            {"s": va, "d": vx, "rl": "MENTIONS", "pol": None},
        ]),
        (vb, [
            {"s": vb, "d": vm, "rl": "SUPPLIES", "pol": None},
        ]),
        ("FETCH PROP ON `Entity`", [
            {"vid": vm, "name": "M", "label": "Organization"},
        ]),
    ])
    ops = ago.NebulaAnalyticsGraphOps(store)

    result = ops.common_connections("A", "B", 5)

    assert result == [{"name": "M", "type": "Organization", "via": ["OWNS", "SUPPLIES"]}]


def test_nebula_common_connections_excludes_negated_edges():
    from src.graph.nebula_store import entity_vid

    va, vb = entity_vid("A"), entity_vid("B")
    vm = entity_vid("M")
    store = _NebulaRecStore(canned=[
        (va, [{"s": va, "d": vm, "rl": "OWNS", "pol": "negated"}]),
        (vb, [{"s": vb, "d": vm, "rl": "SUPPLIES", "pol": None}]),
    ])
    assert ago.NebulaAnalyticsGraphOps(store).common_connections("A", "B", 5) == []


def test_nebula_common_connections_returns_empty_when_no_overlap():
    from src.graph.nebula_store import entity_vid

    va, vb = entity_vid("A"), entity_vid("B")
    store = _NebulaRecStore(canned=[(va, []), (vb, [])])
    assert ago.NebulaAnalyticsGraphOps(store).common_connections("A", "B", 5) == []


def test_nebula_common_connections_fail_soft_returns_empty_on_raise():
    assert (
        ago.NebulaAnalyticsGraphOps(_NebulaRaisingStore()).common_connections("A", "B", 5) == []
    )


# --- Nebula: identifier_lookup -------------------------------------------


def test_nebula_identifier_lookup_returns_non_id_neighbours_of_an_id_entity():
    from src.graph.nebula_store import entity_vid

    vid = entity_vid("7701234567")
    ve = entity_vid("E")
    store = _NebulaRecStore(canned=[
        ("YIELD `Entity`.label AS label;", [{"label": "INN"}]),
        ("OVER `RELATED` BIDIRECT", [{"s": vid, "d": ve, "rl": "HAS_INN"}]),
        ("id(vertex) AS vid", [{"vid": ve, "name": "E", "label": "Organization"}]),
    ])
    result = ago.NebulaAnalyticsGraphOps(store).identifier_lookup("7701234567")
    assert result == [{"name": "E", "labels": ["Organization"], "rel": "HAS_INN"}]


def test_nebula_identifier_lookup_returns_empty_when_value_not_an_id_type():
    store = _NebulaRecStore(canned=[
        ("YIELD `Entity`.label AS label;", [{"label": "Organization"}]),
    ])
    result = ago.NebulaAnalyticsGraphOps(store).identifier_lookup("Not An Id")
    assert result == []
    # short-circuits before the GO/neighbour FETCH
    assert len(store.calls) == 1


def test_nebula_identifier_lookup_returns_empty_when_value_missing():
    store = _NebulaRecStore(canned=[("YIELD `Entity`.label AS label;", [])])
    assert ago.NebulaAnalyticsGraphOps(store).identifier_lookup("Ghost") == []


def test_nebula_identifier_lookup_fail_soft_returns_empty_on_raise():
    assert ago.NebulaAnalyticsGraphOps(_NebulaRaisingStore()).identifier_lookup("123") == []


# --- Nebula: shared_identifier_entities ----------------------------------


def test_nebula_shared_identifier_entities_single_type_groups_owners():
    from src.graph.nebula_store import entity_vid

    vid_inn = entity_vid("INN#123")
    vowner_a, vowner_b, vid_type_owner = (
        entity_vid("A"), entity_vid("B"), entity_vid("SomeOtherId"),
    )
    store = _NebulaRecStore(canned=[
        ('== "INN"', [{"vid": vid_inn, "name": "123"}]),
        ("OVER `RELATED` BIDIRECT", [
            {"s": vid_inn, "d": vowner_a},
            {"s": vowner_b, "d": vid_inn},
            {"s": vid_inn, "d": vid_type_owner},
        ]),
        ("id(vertex) AS vid", [
            {"vid": vowner_a, "name": "A", "label": "Organization"},
            {"vid": vowner_b, "name": "B", "label": "Person"},
            {"vid": vid_type_owner, "name": "other-id", "label": "Email"},
        ]),
    ])
    result = ago.NebulaAnalyticsGraphOps(store).shared_identifier_entities("INN", 5)
    assert result == [{"value": "123", "id_type": "INN", "owners": ["A", "B"]}]


def test_nebula_shared_identifier_entities_below_min_owners_excluded():
    from src.graph.nebula_store import entity_vid

    vid_inn = entity_vid("INN#123")
    vowner_a = entity_vid("A")
    store = _NebulaRecStore(canned=[
        ('== "INN"', [{"vid": vid_inn, "name": "123"}]),
        ("OVER `RELATED` BIDIRECT", [{"s": vid_inn, "d": vowner_a}]),
        ("id(vertex) AS vid", [{"vid": vowner_a, "name": "A", "label": "Organization"}]),
    ])
    assert ago.NebulaAnalyticsGraphOps(store).shared_identifier_entities("INN", 5) == []


def test_nebula_shared_identifier_entities_none_scans_all_id_types():
    store = _NebulaRecStore(canned=[("LOOKUP ON `Entity`", [])])
    result = ago.NebulaAnalyticsGraphOps(store).shared_identifier_entities(None, 5)
    assert result == []
    lookup_calls = [c for c in store.calls if c[0].startswith("LOOKUP ON `Entity`")]
    assert len(lookup_calls) == len(ID_TYPES)


def test_nebula_shared_identifier_entities_fail_soft_returns_empty_on_raise():
    assert (
        ago.NebulaAnalyticsGraphOps(_NebulaRaisingStore()).shared_identifier_entities("INN", 5)
        == []
    )


# --- Nebula: connection_path (FIND SHORTEST PATH) ------------------------


def test_nebula_connection_path_issues_find_shortest_path_and_maps_path():
    from src.graph.nebula_store import entity_vid

    va, vb = entity_vid("A"), entity_vid("B")
    fake_path = _FakePath([va, vb], [_FakeRel(rel_type="OWNS")])
    store = _NebulaRecStore(canned=[
        ("FIND SHORTEST PATH", [{"p": fake_path}]),
        ("FETCH PROP ON `Entity`", [
            {"vid": va, "name": "A"},
            {"vid": vb, "name": "B"},
        ]),
    ])
    ops = ago.NebulaAnalyticsGraphOps(store)

    result = ops.connection_path("A", "B", 6)

    assert result == [{"path": ["A", "B"], "rels": ["OWNS"], "hops": 1}]
    stmt, pm = store.calls[0]
    assert pm is None
    assert stmt.startswith("FIND SHORTEST PATH FROM")
    assert va in stmt and vb in stmt
    assert "OVER * BIDIRECT" in stmt
    assert "UPTO 6 STEPS" in stmt


def test_nebula_connection_path_falls_back_to_edge_name_when_no_rel_type_prop():
    from src.graph.nebula_store import entity_vid

    va, vb = entity_vid("A"), entity_vid("B")
    fake_path = _FakePath([va, vb], [_FakeRel(rel_type=None, edge_name="RELATED")])
    store = _NebulaRecStore(canned=[
        ("FIND SHORTEST PATH", [{"p": fake_path}]),
        ("FETCH PROP ON `Entity`", [
            {"vid": va, "name": "A"},
            {"vid": vb, "name": "B"},
        ]),
    ])
    result = ago.NebulaAnalyticsGraphOps(store).connection_path("A", "B", 6)
    assert result == [{"path": ["A", "B"], "rels": ["RELATED"], "hops": 1}]


def test_nebula_connection_path_returns_empty_when_no_path_found():
    store = _NebulaRecStore(canned=[("FIND SHORTEST PATH", [])])
    assert ago.NebulaAnalyticsGraphOps(store).connection_path("A", "B", 6) == []


def test_nebula_connection_path_fail_soft_returns_empty_on_raise():
    assert ago.NebulaAnalyticsGraphOps(_NebulaRaisingStore()).connection_path("A", "B", 6) == []


# --- Nebula: cooccurrence (Chunk-dependent -> deferred) -------------------


def test_nebula_cooccurrence_returns_empty_and_issues_no_query():
    store = _NebulaRecStore()
    assert ago.NebulaAnalyticsGraphOps(store).cooccurrence("A", 5) == []
    assert store.calls == []


def test_nebula_cooccurrence_logs_debug_once(monkeypatch):
    ago.NebulaAnalyticsGraphOps._cooccurrence_deferred_logged = False
    debug_calls: list[str] = []
    monkeypatch.setattr(
        ago.logger, "debug", lambda msg, **kw: debug_calls.append(msg.format(**kw))
    )
    ago.NebulaAnalyticsGraphOps(_NebulaRecStore()).cooccurrence("A", 5)
    ago.NebulaAnalyticsGraphOps(_NebulaRecStore()).cooccurrence("B", 5)
    assert len(debug_calls) == 1
    assert "cooccurrence" in debug_calls[0]
