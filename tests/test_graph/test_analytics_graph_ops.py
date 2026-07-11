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

    result = ops.entity_identifiers("A", ID_TYPES)

    assert result == [{"id_type": "INN", "value": "123"}]
    assert store.calls == [
        (ago._IDENTIFIERS, {"name": "A", "id_types": ID_TYPES, "top_n": ago._DEFAULT_TOP_N}),
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

    result = ops.neighbors_by_relation("A", "OWNS", 5)

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
            {"name": "A", "rel_type": "OWNS", "polarity": None, "top_n": 5},
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
    assert ago.Neo4jAnalyticsGraphOps(_RaisingStore()).entity_identifiers("A", ID_TYPES) == []


def test_neo4j_entity_communities_fail_soft_returns_empty_on_raise():
    assert ago.Neo4jAnalyticsGraphOps(_RaisingStore()).entity_communities("A") == []


def test_neo4j_neighbors_by_relation_fail_soft_returns_empty_on_raise():
    assert ago.Neo4jAnalyticsGraphOps(_RaisingStore()).neighbors_by_relation("A", "OWNS", 5) == []


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


# --- Nebula: stub (Task 2) -------------------------------------------


def test_nebula_entity_core_raises_not_implemented():
    ops = ago.NebulaAnalyticsGraphOps(_RecStore())
    try:
        ops.entity_core("A")
        raise AssertionError("expected NotImplementedError")
    except NotImplementedError:
        pass


def test_nebula_entity_neighbors_raises_not_implemented():
    ops = ago.NebulaAnalyticsGraphOps(_RecStore())
    try:
        ops.entity_neighbors("A", 5)
        raise AssertionError("expected NotImplementedError")
    except NotImplementedError:
        pass


def test_nebula_entity_identifiers_raises_not_implemented():
    ops = ago.NebulaAnalyticsGraphOps(_RecStore())
    try:
        ops.entity_identifiers("A", ID_TYPES)
        raise AssertionError("expected NotImplementedError")
    except NotImplementedError:
        pass


def test_nebula_entity_communities_raises_not_implemented():
    ops = ago.NebulaAnalyticsGraphOps(_RecStore())
    try:
        ops.entity_communities("A")
        raise AssertionError("expected NotImplementedError")
    except NotImplementedError:
        pass


def test_nebula_neighbors_by_relation_raises_not_implemented():
    ops = ago.NebulaAnalyticsGraphOps(_RecStore())
    try:
        ops.neighbors_by_relation("A", "OWNS", 5)
        raise AssertionError("expected NotImplementedError")
    except NotImplementedError:
        pass


def test_nebula_common_connections_raises_not_implemented():
    ops = ago.NebulaAnalyticsGraphOps(_RecStore())
    try:
        ops.common_connections("A", "B", 5)
        raise AssertionError("expected NotImplementedError")
    except NotImplementedError:
        pass


def test_nebula_identifier_lookup_raises_not_implemented():
    ops = ago.NebulaAnalyticsGraphOps(_RecStore())
    try:
        ops.identifier_lookup("123")
        raise AssertionError("expected NotImplementedError")
    except NotImplementedError:
        pass


def test_nebula_shared_identifier_entities_raises_not_implemented():
    ops = ago.NebulaAnalyticsGraphOps(_RecStore())
    try:
        ops.shared_identifier_entities("INN", 5)
        raise AssertionError("expected NotImplementedError")
    except NotImplementedError:
        pass


def test_nebula_connection_path_raises_not_implemented():
    ops = ago.NebulaAnalyticsGraphOps(_RecStore())
    try:
        ops.connection_path("A", "B", 6)
        raise AssertionError("expected NotImplementedError")
    except NotImplementedError:
        pass


def test_nebula_cooccurrence_raises_not_implemented():
    ops = ago.NebulaAnalyticsGraphOps(_RecStore())
    try:
        ops.cooccurrence("A", 5)
        raise AssertionError("expected NotImplementedError")
    except NotImplementedError:
        pass
