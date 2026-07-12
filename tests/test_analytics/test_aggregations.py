import pytest

from src.analytics.primitives import aggregations as agg
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_count_entities_excludes_identifiers_by_default():
    store = _FakeStore(rows=[{"n": 7}])
    res = await agg.count_entities(store, type="Organization")
    assert res.rows == [{"n": 7}]
    assert res.params["type"] == "Organization"
    # identifier exclusion present
    assert res.params["id_types"]  # passed in
    # The underlying Neo4j Cypher issued through the seam is unchanged
    # (byte-for-byte) -- verify it on the store's actually-executed query,
    # not on the primitive's now backend-agnostic `res.cypher` descriptor.
    assert "count(e)" in store.last_cypher
    assert "$id_types" in store.last_cypher


@pytest.mark.asyncio
async def test_count_entities_failsoft():
    res = await agg.count_entities(None)
    assert res.rows == []


@pytest.mark.asyncio
async def test_count_relationships_filters_rel_type_and_polarity():
    store = _FakeStore(rows=[{"n": 3}])
    res = await agg.count_relationships(store, rel_type="OWNS", polarity="negated")
    assert res.params["rel_type"] == "OWNS" and res.params["polarity"] == "negated"


@pytest.mark.asyncio
async def test_top_entities_by_mentions_clamps_and_orders():
    store = _FakeStore(rows=[{"name": "X", "mentions": 9}])
    res = await agg.top_entities_by_mentions(store, top_n=99999)
    assert res.params["top_n"] == 200  # clamp
    assert "mention_count" in store.last_cypher and "ORDER BY" in store.last_cypher


@pytest.mark.asyncio
async def test_distribution_by_type_shape():
    store = _FakeStore(rows=[{"type": "Person", "n": 5}])
    res = await agg.distribution_by_type(store)
    assert res.rows[0]["type"] == "Person"


@pytest.mark.asyncio
async def test_top_entities_by_degree_excludes_negated():
    store = _FakeStore(rows=[{"name": "X", "degree": 4}])
    res = await agg.top_entities_by_degree(store)
    assert "negated" in store.last_cypher


# --- Routed through the seam (fake build_aggregations_graph_ops) ---------


class _FakeOps:
    """Records (method, args) calls; returns canned rows per method name."""

    def __init__(self, **returns):
        self.calls: dict[str, tuple] = {}
        self._returns = returns

    def _record(self, method: str, *args):
        self.calls[method] = args
        return self._returns.get(method, [])

    def count_entities(self, type, exclude_identifiers):
        return self._record("count_entities", type, exclude_identifiers)

    def count_relationships(self, rel_type, polarity):
        return self._record("count_relationships", rel_type, polarity)

    def distribution_by_type(self, exclude_identifiers):
        return self._record("distribution_by_type", exclude_identifiers)

    def distribution_by_relation_type(self):
        return self._record("distribution_by_relation_type")

    def distribution_by_polarity(self, rel_type):
        return self._record("distribution_by_polarity", rel_type)

    def top_entities_by_mentions(self, type, top_n, exclude_identifiers):
        return self._record("top_entities_by_mentions", type, top_n, exclude_identifiers)

    def top_entities_by_degree(self, type, top_n):
        return self._record("top_entities_by_degree", type, top_n)


def _patch_ops(monkeypatch, ops):
    monkeypatch.setattr(agg, "build_aggregations_graph_ops", lambda store: ops)


@pytest.mark.asyncio
async def test_count_entities_routes_through_seam(monkeypatch):
    ops = _FakeOps(count_entities=[{"n": 7}])
    _patch_ops(monkeypatch, ops)

    res = await agg.count_entities(object(), type="Organization", exclude_identifiers=False)

    assert ops.calls["count_entities"] == ("Organization", False)
    assert res.rows == [{"n": 7}]


@pytest.mark.asyncio
async def test_count_relationships_routes_through_seam(monkeypatch):
    ops = _FakeOps(count_relationships=[{"n": 3}])
    _patch_ops(monkeypatch, ops)

    res = await agg.count_relationships(object(), rel_type="OWNS", polarity="negated")

    assert ops.calls["count_relationships"] == ("OWNS", "negated")
    assert res.rows == [{"n": 3}]


@pytest.mark.asyncio
async def test_distribution_by_type_routes_through_seam(monkeypatch):
    ops = _FakeOps(distribution_by_type=[{"type": "Person", "n": 5}])
    _patch_ops(monkeypatch, ops)

    res = await agg.distribution_by_type(object(), exclude_identifiers=True)

    assert ops.calls["distribution_by_type"] == (True,)
    assert res.rows[0]["type"] == "Person"


@pytest.mark.asyncio
async def test_distribution_by_relation_type_routes_through_seam(monkeypatch):
    ops = _FakeOps(distribution_by_relation_type=[{"rel": "OWNS", "n": 2}])
    _patch_ops(monkeypatch, ops)

    res = await agg.distribution_by_relation_type(object())

    assert ops.calls["distribution_by_relation_type"] == ()
    assert res.rows[0]["rel"] == "OWNS"


@pytest.mark.asyncio
async def test_distribution_by_polarity_routes_through_seam(monkeypatch):
    ops = _FakeOps(distribution_by_polarity=[{"polarity": "affirmed", "n": 5}])
    _patch_ops(monkeypatch, ops)

    res = await agg.distribution_by_polarity(object(), rel_type="OWNS")

    assert ops.calls["distribution_by_polarity"] == ("OWNS",)
    assert res.rows[0]["polarity"] == "affirmed"


@pytest.mark.asyncio
async def test_top_entities_by_mentions_routes_through_seam(monkeypatch):
    ops = _FakeOps(top_entities_by_mentions=[{"name": "X", "mentions": 9}])
    _patch_ops(monkeypatch, ops)

    res = await agg.top_entities_by_mentions(
        object(), type="Organization", top_n=10, exclude_identifiers=True
    )

    assert ops.calls["top_entities_by_mentions"] == ("Organization", 10, True)
    assert res.rows[0]["name"] == "X"


@pytest.mark.asyncio
async def test_top_entities_by_degree_routes_through_seam(monkeypatch):
    ops = _FakeOps(top_entities_by_degree=[{"name": "X", "degree": 4}])
    _patch_ops(monkeypatch, ops)

    res = await agg.top_entities_by_degree(object(), type="Organization", top_n=10)

    assert ops.calls["top_entities_by_degree"] == ("Organization", 10)
    assert res.rows[0]["name"] == "X"


@pytest.mark.asyncio
async def test_failsoft_all_primitives_return_empty_without_store():
    assert (await agg.count_entities(None)).rows == []
    assert (await agg.count_relationships(None)).rows == []
    assert (await agg.distribution_by_type(None)).rows == []
    assert (await agg.distribution_by_relation_type(None)).rows == []
    assert (await agg.distribution_by_polarity(None)).rows == []
    assert (await agg.top_entities_by_mentions(None)).rows == []
    assert (await agg.top_entities_by_degree(None)).rows == []
