import pytest

from src.analytics.primitives import connections as conn
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_entity_dossier_assembles_sections():
    # Default backend (neo4j) -> build_analytics_graph_ops(store) returns
    # Neo4jAnalyticsGraphOps, which issues the SAME 4 structured_query
    # calls (core, neighbors, identifiers, communities) in the SAME order
    # as the pre-seam implementation -- _FakeStore's by_call mechanism
    # (canned rows per sequential call) still applies unmodified.
    store = _FakeStore(
        by_call=[
            [
                {
                    "name": "Ромашка",
                    "description": "d",
                    "labels": ["__Entity__", "Organization"],
                    "mention_count": 4,
                }
            ],  # core
            [{"rel": "OWNS", "name": "ООО Лютик", "ntype": "Organization", "w": 2.0}],  # neighbors
            [{"id_type": "INN", "value": "7701234567"}],  # identifiers
            [{"level": 0, "title": "Поставки"}],  # communities
        ]
    )
    res = await conn.entity_dossier(store, name="Ромашка")
    row = res.rows[0]
    assert row["core"]["name"] == "Ромашка"
    assert row["connections"][0]["rel"] == "OWNS"
    assert row["identifiers"][0]["id_type"] == "INN"
    assert row["communities"][0]["title"] == "Поставки"
    assert res.params["name"] == "Ромашка"


@pytest.mark.asyncio
async def test_shared_identifier_entities_min_owners():
    store = _FakeStore(rows=[{"value": "7701234567", "id_type": "INN", "owners": ["A", "B"]}])
    res = await conn.shared_identifier_entities(store, min_owners=2)
    assert res.params["min_owners"] == 2
    # The underlying Neo4j Cypher issued through the seam is unchanged
    # (byte-for-byte) -- verify it on the store's actually-executed query,
    # not on the primitive's now backend-agnostic `res.cypher` descriptor.
    assert "size(owners) >= $min_owners" in store.last_cypher


@pytest.mark.asyncio
async def test_connection_path_clamps_hops_inline():
    store = _FakeStore(rows=[{"path": ["A", "B"], "rels": ["OWNS"], "hops": 1}])
    res = await conn.connection_path(store, source="A", target="B", max_hops=99)
    # hops clamped (max 12) -- reflected in params, and inlined into the
    # real Cypher literal (shortestPath bound) issued via the seam.
    assert res.params["max_hops"] == 12
    assert "*..12" in store.last_cypher


@pytest.mark.asyncio
async def test_cooccurrence_via_shared_chunks():
    store = _FakeStore(rows=[{"name": "B", "shared": 3}])
    res = await conn.cooccurrence(store, name="A")
    assert res.rows == [{"name": "B", "shared": 3}]
    assert ":MENTIONS" in store.last_cypher


@pytest.mark.asyncio
async def test_failsoft():
    assert (await conn.entity_dossier(None, name="X")).rows == []
    assert (await conn.identifier_lookup(None, value="x")).rows == []


# --- Routed through the seam (fake build_analytics_graph_ops) ------------


class _FakeOps:
    """Records (method, args) calls; returns canned rows per method name."""

    def __init__(self, **returns):
        self.calls: dict[str, tuple] = {}
        self._returns = returns

    def _record(self, method: str, *args):
        self.calls[method] = args
        return self._returns.get(method, [])

    def entity_core(self, name):
        return self._record("entity_core", name)

    def entity_neighbors(self, name, top_n):
        return self._record("entity_neighbors", name, top_n)

    def entity_identifiers(self, name, id_types, top_n):
        return self._record("entity_identifiers", name, id_types, top_n)

    def entity_communities(self, name):
        return self._record("entity_communities", name)

    def neighbors_by_relation(self, name, rel, polarity, top_n):
        return self._record("neighbors_by_relation", name, rel, polarity, top_n)

    def common_connections(self, a, b, top_n):
        return self._record("common_connections", a, b, top_n)

    def identifier_lookup(self, value):
        return self._record("identifier_lookup", value)

    def shared_identifier_entities(self, id_types, top_n):
        return self._record("shared_identifier_entities", id_types, top_n)

    def connection_path(self, source, target, hops):
        return self._record("connection_path", source, target, hops)

    def cooccurrence(self, name, top_n):
        return self._record("cooccurrence", name, top_n)


def _patch_ops(monkeypatch, ops):
    monkeypatch.setattr(conn, "build_analytics_graph_ops", lambda store: ops)


@pytest.mark.asyncio
async def test_entity_dossier_routes_through_seam(monkeypatch):
    ops = _FakeOps(
        entity_core=[{"name": "A"}],
        entity_neighbors=[{"rel": "OWNS", "name": "B", "ntype": "Organization", "w": 1.0}],
        entity_identifiers=[{"id_type": "INN", "value": "1"}],
        entity_communities=[{"level": 0, "title": "T"}],
    )
    _patch_ops(monkeypatch, ops)

    res = await conn.entity_dossier(object(), name="A", top_n=10)

    assert ops.calls["entity_core"] == ("A",)
    assert ops.calls["entity_neighbors"] == ("A", 10)
    assert ops.calls["entity_identifiers"][0] == "A"
    assert ops.calls["entity_communities"] == ("A",)
    row = res.rows[0]
    assert row["core"] == {"name": "A"}
    assert row["connections"][0]["name"] == "B"
    assert row["identifiers"][0]["id_type"] == "INN"
    assert row["communities"][0]["title"] == "T"


@pytest.mark.asyncio
async def test_entity_dossier_empty_core_short_circuits(monkeypatch):
    ops = _FakeOps(entity_core=[])
    _patch_ops(monkeypatch, ops)

    res = await conn.entity_dossier(object(), name="Ghost")

    assert res.rows == []
    # neighbours/identifiers/communities never called once core is empty
    assert "entity_neighbors" not in ops.calls


@pytest.mark.asyncio
async def test_neighbors_by_relation_routes_through_seam(monkeypatch):
    ops = _FakeOps(neighbors_by_relation=[{"name": "B", "w": 1.0}])
    _patch_ops(monkeypatch, ops)

    res = await conn.neighbors_by_relation(
        object(), name="A", rel_type="OWNS", polarity="negated", top_n=7
    )

    assert ops.calls["neighbors_by_relation"] == ("A", "OWNS", "negated", 7)
    assert res.rows == [{"name": "B", "w": 1.0}]


@pytest.mark.asyncio
async def test_common_connections_routes_through_seam(monkeypatch):
    ops = _FakeOps(common_connections=[{"name": "M", "type": "Organization", "via": ["OWNS"]}])
    _patch_ops(monkeypatch, ops)

    res = await conn.common_connections(object(), a="A", b="B", top_n=5)

    assert ops.calls["common_connections"] == ("A", "B", 5)
    assert res.rows[0]["name"] == "M"


@pytest.mark.asyncio
async def test_connection_path_routes_through_seam_with_clamped_hops(monkeypatch):
    ops = _FakeOps(connection_path=[{"path": ["A", "B"], "rels": ["OWNS"], "hops": 1}])
    _patch_ops(monkeypatch, ops)

    res = await conn.connection_path(object(), source="A", target="B", max_hops=99)

    assert ops.calls["connection_path"] == ("A", "B", 12)  # clamped
    assert res.rows[0]["path"] == ["A", "B"]


@pytest.mark.asyncio
async def test_shared_identifier_entities_routes_through_seam(monkeypatch):
    ops = _FakeOps(
        shared_identifier_entities=[{"value": "1", "id_type": "INN", "owners": ["A", "B"]}]
    )
    _patch_ops(monkeypatch, ops)

    res = await conn.shared_identifier_entities(object(), id_type="INN", top_n=5)

    assert ops.calls["shared_identifier_entities"] == ("INN", 5)
    assert res.rows[0]["owners"] == ["A", "B"]


@pytest.mark.asyncio
async def test_identifier_lookup_routes_through_seam(monkeypatch):
    ops = _FakeOps(identifier_lookup=[{"name": "A", "labels": ["Organization"], "rel": "HAS_ID"}])
    _patch_ops(monkeypatch, ops)

    res = await conn.identifier_lookup(object(), value="7701234567")

    assert ops.calls["identifier_lookup"] == ("7701234567",)
    assert res.rows[0]["name"] == "A"


@pytest.mark.asyncio
async def test_cooccurrence_routes_through_seam(monkeypatch):
    ops = _FakeOps(cooccurrence=[])
    _patch_ops(monkeypatch, ops)

    res = await conn.cooccurrence(object(), name="A", top_n=5)

    assert ops.calls["cooccurrence"] == ("A", 5)
    assert res.rows == []
