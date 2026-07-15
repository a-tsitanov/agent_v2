from __future__ import annotations

from src.analytics.ids import ID_TYPES
from src.graph import aggregations_graph_ops as ago


class _RecStore:
    """Records (cypher, param_map); returns canned rows popped in call order."""

    def __init__(self, rows=None):
        self.calls: list[tuple[str, dict | None]] = []
        self._rows = list(rows or [])

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map))
        return self._rows.pop(0) if self._rows else []


class _RaisingStore:
    def __init__(self):
        self.calls: list[tuple[str, dict | None]] = []

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map))
        raise RuntimeError("boom")


class _NebulaRecStore:
    """Records nGQL statements (nebula never binds param_map); returns canned
    rows keyed by the first matching substring of the statement."""

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


# --- Neo4j: byte-for-byte guard (moved Cypher constants + params) --------


def test_neo4j_count_entities_issues_moved_cypher():
    store = _RecStore(rows=[[{"n": 7}]])
    result = ago.Neo4jAggregationsGraphOps(store).count_entities("Organization", True)
    assert result == [{"n": 7}]
    assert store.calls == [
        (ago._COUNT_ENTITIES, {"type": "Organization", "exclude_ids": True, "id_types": ID_TYPES}),
    ]
    assert ago._COUNT_ENTITIES == (
        "MATCH (e:__Entity__) "
        "WHERE ($type IS NULL OR $type IN labels(e)) "
        "AND ($exclude_ids = false OR NONE(l IN labels(e) WHERE l IN $id_types)) "
        "RETURN count(e) AS n"
    )


def test_neo4j_count_relationships_issues_moved_cypher():
    store = _RecStore(rows=[[{"n": 3}]])
    result = ago.Neo4jAggregationsGraphOps(store).count_relationships("OWNS", "negated")
    assert result == [{"n": 3}]
    assert store.calls == [(ago._COUNT_RELATIONSHIPS, {"rel_type": "OWNS", "polarity": "negated"})]


def test_neo4j_distribution_by_type_issues_moved_cypher():
    store = _RecStore(rows=[[{"type": "Person", "n": 5}]])
    result = ago.Neo4jAggregationsGraphOps(store).distribution_by_type(True)
    assert result == [{"type": "Person", "n": 5}]
    assert store.calls == [(ago._DISTRIBUTION_BY_TYPE, {"exclude_ids": True, "id_types": ID_TYPES})]


def test_neo4j_distribution_by_relation_type_issues_moved_cypher():
    store = _RecStore(rows=[[{"rel": "OWNS", "n": 2}]])
    result = ago.Neo4jAggregationsGraphOps(store).distribution_by_relation_type()
    assert result == [{"rel": "OWNS", "n": 2}]
    assert store.calls == [(ago._DISTRIBUTION_BY_RELATION_TYPE, {})]
    assert "type(r) AS rel" in ago._DISTRIBUTION_BY_RELATION_TYPE


def test_neo4j_distribution_by_polarity_issues_moved_cypher():
    store = _RecStore(rows=[[{"polarity": "affirmed", "n": 5}]])
    result = ago.Neo4jAggregationsGraphOps(store).distribution_by_polarity("OWNS")
    assert result == [{"polarity": "affirmed", "n": 5}]
    assert store.calls == [(ago._DISTRIBUTION_BY_POLARITY, {"rel_type": "OWNS"})]


def test_neo4j_top_entities_by_mentions_issues_moved_cypher():
    store = _RecStore(rows=[[{"name": "X", "mentions": 9}]])
    result = ago.Neo4jAggregationsGraphOps(store).top_entities_by_mentions("Organization", 10, True)
    assert result == [{"name": "X", "mentions": 9}]
    assert store.calls == [
        (
            ago._TOP_ENTITIES_BY_MENTIONS,
            {"type": "Organization", "exclude_ids": True, "id_types": ID_TYPES, "top_n": 10},
        ),
    ]


def test_neo4j_top_entities_by_degree_issues_moved_cypher():
    store = _RecStore(rows=[[{"name": "X", "degree": 4}]])
    result = ago.Neo4jAggregationsGraphOps(store).top_entities_by_degree("Organization", 10)
    assert result == [{"name": "X", "degree": 4}]
    assert store.calls == [(ago._TOP_ENTITIES_BY_DEGREE, {"type": "Organization", "top_n": 10})]
    assert "r.polarity <> 'negated'" in ago._TOP_ENTITIES_BY_DEGREE


def test_neo4j_fail_soft_returns_empty_on_raise():
    assert ago.Neo4jAggregationsGraphOps(_RaisingStore()).count_entities(None, True) == []


# --- Nebula: nGQL shape per the rules doc --------------------------------


def test_nebula_count_entities_builds_label_filters():
    store = _NebulaRecStore(canned=[("count(e)", [{"n": 7}])])
    result = ago.NebulaAggregationsGraphOps(store).count_entities("Organization", True)
    assert result == [{"n": 7}]
    stmt = store.calls[0][0]
    assert "MATCH (e:`Entity`)" in stmt
    assert "e.`Entity`.label == \"Organization\"" in stmt
    assert "e.`Entity`.label NOT IN [" in stmt  # exclude_identifiers
    assert "RETURN count(e) AS n" in stmt


def test_nebula_count_entities_no_filters_when_none():
    store = _NebulaRecStore()
    ago.NebulaAggregationsGraphOps(store).count_entities(None, False)
    stmt = store.calls[0][0]
    assert "WHERE" not in stmt
    assert "MATCH (e:`Entity`) RETURN count(e) AS n" in stmt


def test_nebula_count_relationships_unfiltered_direct_count():
    store = _NebulaRecStore(canned=[("count(r)", [{"n": 4}])])
    result = ago.NebulaAggregationsGraphOps(store).count_relationships(None, None)
    assert result == [{"n": 4}]
    stmt = store.calls[0][0]
    assert "-[r:`RELATED`]->" in stmt
    assert "RETURN count(r) AS n" in stmt
    assert "WHERE" not in stmt  # no edge-property WHERE (IndexNotFound)


def test_nebula_count_relationships_filtered_counts_client_side():
    # nebula can't push an edge-property WHERE on a full edge scan, so the seam
    # scans rel_type/polarity and counts matches in Python.
    rows = [
        {"rel_type": "OWNS", "polarity": "positive"},
        {"rel_type": "OWNS", "polarity": "negated"},
        {"rel_type": "CONTACT", "polarity": "positive"},
    ]
    store = _NebulaRecStore(canned=[("RETURN r.rel_type AS rel_type", rows)])
    ops = ago.NebulaAggregationsGraphOps(store)
    assert ops.count_relationships("OWNS", None) == [{"n": 2}]
    assert ops.count_relationships("OWNS", "negated") == [{"n": 1}]
    assert ops.count_relationships(None, "positive") == [{"n": 2}]
    # the scan statement carries no WHERE (would IndexNotFound)
    assert "WHERE" not in store.calls[0][0]


def test_nebula_distribution_by_type_groups_on_label():
    store = _NebulaRecStore()
    ago.NebulaAggregationsGraphOps(store).distribution_by_type(True)
    stmt = store.calls[0][0]
    assert "RETURN e.`Entity`.label AS type, count(*) AS n ORDER BY n DESC" in stmt
    assert "e.`Entity`.label NOT IN [" in stmt


def test_nebula_distribution_by_relation_type_shape():
    store = _NebulaRecStore()
    ago.NebulaAggregationsGraphOps(store).distribution_by_relation_type()
    stmt = store.calls[0][0]
    assert "RETURN r.rel_type AS rel, count(*) AS n ORDER BY n DESC" in stmt


def test_nebula_distribution_by_polarity_unfiltered_group_by():
    store = _NebulaRecStore()
    ago.NebulaAggregationsGraphOps(store).distribution_by_polarity(None)
    stmt = store.calls[0][0]
    assert "WHERE" not in stmt
    assert "RETURN r.polarity AS polarity, count(*) AS n ORDER BY n DESC" in stmt


def test_nebula_distribution_by_polarity_filtered_counts_client_side():
    # filtered edge-property WHERE would IndexNotFound, so scan + group in Python
    rows = [
        {"rel_type": "OWNS", "polarity": "affirmed"},
        {"rel_type": "OWNS", "polarity": "affirmed"},
        {"rel_type": "OWNS", "polarity": "negated"},
        {"rel_type": "CONTACT", "polarity": "affirmed"},
    ]
    store = _NebulaRecStore(canned=[("RETURN r.rel_type AS rel_type", rows)])
    result = ago.NebulaAggregationsGraphOps(store).distribution_by_polarity("OWNS")
    assert result == [{"polarity": "affirmed", "n": 2}, {"polarity": "negated", "n": 1}]
    assert "WHERE" not in store.calls[0][0]  # no edge-property WHERE


def test_nebula_top_entities_by_mentions_orders_by_alias():
    store = _NebulaRecStore()
    ago.NebulaAggregationsGraphOps(store).top_entities_by_mentions("Organization", 10, True)
    stmt = store.calls[0][0]
    # rule 5: ORDER BY the aliased column, not the raw property expression
    assert "ORDER BY mentions DESC LIMIT 10" in stmt
    assert "e.`Entity`.mention_count IS NOT NULL" in stmt
    assert "e.`Entity`.mention_count AS mentions" in stmt


def test_nebula_top_entities_by_degree_plain_match_orders_by_alias():
    store = _NebulaRecStore()
    ago.NebulaAggregationsGraphOps(store).top_entities_by_degree(None, 5)
    stmt = store.calls[0][0]
    # plain MATCH (nebula rejects WHERE inside OPTIONAL MATCH)
    assert "MATCH (e:`Entity`)-[r:`RELATED`]-(:`Entity`)" in stmt
    assert "OPTIONAL MATCH" not in stmt
    assert "r.polarity != 'negated'" in stmt
    assert "count(r) AS degree" in stmt
    assert "ORDER BY degree DESC LIMIT 5" in stmt


def test_nebula_top_entities_by_degree_adds_type_filter():
    store = _NebulaRecStore()
    ago.NebulaAggregationsGraphOps(store).top_entities_by_degree("Organization", 5)
    stmt = store.calls[0][0]
    assert "e.`Entity`.label == \"Organization\"" in stmt


def test_canonical_label_maps_case_insensitively():
    # kb_analyze's planner emits lowercase types ('organization'); nebula labels
    # are canonical-case ('Organization'). CamelCase types must survive too.
    assert ago._canonical_label("organization") == "Organization"
    assert ago._canonical_label("eventoraction") == "EventOrAction"
    assert ago._canonical_label("PERSON") == "Person"
    assert ago._canonical_label("Location") == "Location"
    assert ago._canonical_label("unknowntype") == "unknowntype"  # fallback unchanged


def test_nebula_type_filter_canonicalizes_lowercase_type():
    # An exact-case nGQL filter on the planner's lowercase 'organization' returns
    # [] against 'Organization' labels — the 0b/kb_analyze-empty bug. All three
    # type-filtered nebula methods must canonicalize the incoming type.
    for meth, extra in [
        ("count_entities", (True,)),
        ("top_entities_by_degree", (10,)),
        ("top_entities_by_mentions", (10, True)),
    ]:
        store = _NebulaRecStore()
        getattr(ago.NebulaAggregationsGraphOps(store), meth)("organization", *extra)
        stmt = store.calls[0][0]
        assert 'e.`Entity`.label == "Organization"' in stmt, f"{meth}: {stmt}"
        assert '"organization"' not in stmt, f"{meth} kept lowercase: {stmt}"


def test_nebula_fail_soft_returns_empty_on_raise():
    assert ago.NebulaAggregationsGraphOps(_NebulaRaisingStore()).count_entities(None, True) == []


# --- Dispatch ------------------------------------------------------------


def test_dispatch_returns_neo4j_when_backend_not_nebula(monkeypatch):
    monkeypatch.setattr(ago.settings.graph, "backend", "neo4j")
    assert isinstance(ago.build_aggregations_graph_ops(_RecStore()), ago.Neo4jAggregationsGraphOps)


def test_dispatch_returns_nebula_when_backend_nebula(monkeypatch):
    monkeypatch.setattr(ago.settings.graph, "backend", "nebula")
    assert isinstance(
        ago.build_aggregations_graph_ops(_NebulaRecStore()), ago.NebulaAggregationsGraphOps
    )
