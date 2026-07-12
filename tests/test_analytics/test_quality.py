# tests/test_analytics/test_quality.py
import pytest

from src.analytics.primitives import quality as q


class _FakeOps:
    """Records (method, args); returns canned rows per method name."""

    def __init__(self, **returns):
        self.calls: dict[str, tuple] = {}
        self._returns = returns

    def _record(self, method, *args):
        self.calls[method] = args
        return self._returns.get(method, [])

    def contradictions(self, top_n):
        return self._record("contradictions", top_n)

    def orphans(self, min_degree, top_n):
        return self._record("orphans", min_degree, top_n)

    def incomplete_entities(self, type, expected, top_n):
        return self._record("incomplete_entities", type, expected, top_n)

    def merge_candidates(self, top_n):
        return self._record("merge_candidates", top_n)


def _patch(monkeypatch, ops):
    monkeypatch.setattr(q, "build_quality_graph_ops", lambda store: ops)


@pytest.mark.asyncio
async def test_contradictions_routes_through_seam(monkeypatch):
    ops = _FakeOps(contradictions=[{"a": "A", "rel": "OWNS", "b": "B"}])
    _patch(monkeypatch, ops)
    res = await q.contradictions(object(), top_n=10)
    assert ops.calls["contradictions"] == (10,)
    assert res.rows[0]["a"] == "A"


@pytest.mark.asyncio
async def test_orphans_threads_min_degree(monkeypatch):
    ops = _FakeOps(orphans=[{"name": "Lonely", "degree": 0, "type": "Person"}])
    _patch(monkeypatch, ops)
    res = await q.orphans(object(), min_degree=1, top_n=25)
    assert res.params["min_degree"] == 1
    assert ops.calls["orphans"] == (1, 25)


@pytest.mark.asyncio
async def test_incomplete_entities_resolves_and_threads_expected(monkeypatch):
    ops = _FakeOps(incomplete_entities=[{"name": "Орг1", "missing": ["INN"], "have": ["OGRN"]}])
    _patch(monkeypatch, ops)
    res = await q.incomplete_entities(object(), type="Organization", top_n=25)
    assert res.params["type"] == "Organization"
    # expected pulled from settings.signals.expected_attrs and threaded to the seam
    assert "INN" in res.params["expected"]
    method, expected, top_n = ops.calls["incomplete_entities"]
    assert method == "Organization" and "INN" in expected and top_n == 25


@pytest.mark.asyncio
async def test_merge_candidates_routes_through_seam(monkeypatch):
    ops = _FakeOps(
        merge_candidates=[{"key": "ромашка", "names": ["Ромашка", "РОМАШКА"], "count": 2}]
    )
    _patch(monkeypatch, ops)
    res = await q.merge_candidates(object(), top_n=25)
    assert ops.calls["merge_candidates"] == (25,)
    assert res.rows[0]["count"] == 2


@pytest.mark.asyncio
async def test_failsoft_all_primitives_return_empty_without_store():
    assert (await q.contradictions(None)).rows == []
    assert (await q.orphans(None)).rows == []
    assert (await q.incomplete_entities(None)).rows == []
    assert (await q.merge_candidates(None)).rows == []
