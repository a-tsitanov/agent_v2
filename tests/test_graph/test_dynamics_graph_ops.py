from __future__ import annotations

from src.graph import dynamics_graph_ops as dgo


class _RecStore:
    def __init__(self, rows=None):
        self.calls: list[tuple[str, dict | None]] = []
        self._rows = list(rows or [])

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map))
        return self._rows.pop(0) if self._rows else []


class _RaisingStore:
    def structured_query(self, cypher, param_map=None):
        raise RuntimeError("boom")


class _NebulaRecStore:
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
    def structured_query(self, stmt, param_map=None):
        raise RuntimeError("boom")


# --- Neo4j: byte-for-byte -----------------------------------------------


def test_neo4j_relationship_timeline_issues_moved_cypher():
    store = _RecStore(rows=[[{"period": "2024-03", "rel": "OWNS", "name": "X", "polarity": "a"}]])
    result = dgo.Neo4jDynamicsGraphOps(store).relationship_timeline("Ромашка", None)
    assert result[0]["period"] == "2024-03"
    assert store.calls == [(dgo._RELATIONSHIP_TIMELINE, {"name": "Ромашка", "rel_type": None})]
    assert "substring(r.valid_from,0,7)" in dgo._RELATIONSHIP_TIMELINE


def test_neo4j_polarity_evolution_issues_moved_cypher():
    store = _RecStore(rows=[[{"period": "2024-03", "polarity": "affirmed", "n": 2}]])
    result = dgo.Neo4jDynamicsGraphOps(store).polarity_evolution(None, "OWNS")
    assert result[0]["n"] == 2
    assert store.calls == [(dgo._POLARITY_EVOLUTION, {"name": None, "rel_type": "OWNS"})]


def test_neo4j_whats_changed_issues_moved_cypher():
    store = _RecStore(rows=[[{"name": "A", "rel": "OWNS", "other": "B", "change": "appeared"}]])
    result = dgo.Neo4jDynamicsGraphOps(store).whats_changed(
        "2024-01-01", "2024-12-31", None, 100, 200, 50
    )
    assert result[0]["change"] == "appeared"
    assert store.calls == [(
        dgo._WHATS_CHANGED,
        {"from": "2024-01-01", "to": "2024-12-31", "from_epoch": 100, "to_epoch": 200,
         "entity": None, "top_n": 50},
    )]
    assert "created_at" in dgo._WHATS_CHANGED and "first_seen" in dgo._WHATS_CHANGED
    assert "LIKELY_LINK" in dgo._WHATS_CHANGED


def test_neo4j_fail_soft():
    assert dgo.Neo4jDynamicsGraphOps(_RaisingStore()).relationship_timeline("X", None) == []


# --- Nebula: relationship_timeline (substr, name-anchored) --------------


def test_nebula_relationship_timeline_substr_and_rel_filter():
    store = _NebulaRecStore(canned=[
        ("substr(r.valid_from,0,7)", [{"period": "2026-01", "rel": "OWNS", "name": "B", "polarity": "affirmed"}])
    ])
    result = dgo.NebulaDynamicsGraphOps(store).relationship_timeline("A", "OWNS")
    assert result == [{"period": "2026-01", "rel": "OWNS", "name": "B", "polarity": "affirmed"}]
    stmt = store.calls[0][0]
    assert "e.`Entity`.name == \"A\"" in stmt
    assert "r.valid_from != ''" in stmt
    assert "r.rel_type == \"OWNS\"" in stmt
    assert "substr(r.valid_from,0,7) AS period" in stmt
    assert "ORDER BY period" in stmt


def test_nebula_polarity_evolution_optional_filters():
    store = _NebulaRecStore()
    dgo.NebulaDynamicsGraphOps(store).polarity_evolution(None, None)
    stmt = store.calls[0][0]
    assert "r.valid_from != ''" in stmt
    assert "e.`Entity`.name ==" not in stmt  # no name filter
    assert "count(*) AS n ORDER BY period" in stmt


# --- Nebula: whats_changed (window match + Python classify) -------------


def test_nebula_whats_changed_classifies_appeared_and_ended():
    rows = [
        {"name": "A", "rel": "OWNS", "other": "B", "polarity": "aff",
         "valid_from": "2026-03-01", "valid_to": ""},                     # appeared
        {"name": "C", "rel": "OWNS", "other": "D", "polarity": "aff",
         "valid_from": "2025-01-01", "valid_to": "2026-05-01"},           # ended (vf out, vt in)
    ]
    store = _NebulaRecStore(canned=[("r.rel_type != 'LIKELY_LINK'", rows)])
    result = dgo.NebulaDynamicsGraphOps(store).whats_changed(
        "2026-01", "2026-12", None, 20000, 20300, 50
    )
    assert result == [
        {"name": "C", "rel": "OWNS", "other": "D", "polarity": "aff",
         "valid_from": "2025-01-01", "valid_to": "2026-05-01", "created_at": None, "change": "ended"},
        {"name": "A", "rel": "OWNS", "other": "B", "polarity": "aff",
         "valid_from": "2026-03-01", "valid_to": "", "created_at": None, "change": "appeared"},
    ]
    stmt = store.calls[0][0]
    assert "r.rel_type != 'LIKELY_LINK'" in stmt
    assert "r.valid_from >= \"2026-01\"" in stmt


def test_nebula_whats_changed_entity_filter_and_limit():
    store = _NebulaRecStore()
    dgo.NebulaDynamicsGraphOps(store).whats_changed("2026-01", "2026-12", "Acme", None, None, 5)
    assert "e.`Entity`.name == \"Acme\"" in store.calls[0][0]


def test_nebula_fail_soft():
    assert dgo.NebulaDynamicsGraphOps(_NebulaRaisingStore()).whats_changed(
        "2026-01", "2026-12", None, None, None, 50
    ) == []


# --- Dispatch ------------------------------------------------------------


def test_dispatch_neo4j_default(monkeypatch):
    monkeypatch.setattr(dgo.settings.graph, "backend", "neo4j")
    assert isinstance(dgo.build_dynamics_graph_ops(_RecStore()), dgo.Neo4jDynamicsGraphOps)


def test_dispatch_nebula(monkeypatch):
    monkeypatch.setattr(dgo.settings.graph, "backend", "nebula")
    assert isinstance(dgo.build_dynamics_graph_ops(_NebulaRecStore()), dgo.NebulaDynamicsGraphOps)
