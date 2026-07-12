import pytest

from src.analytics.primitives import events as ev
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_new_events_filters_by_created_at(monkeypatch):
    monkeypatch.setattr(ev, "today_epoch_days", lambda: 19800)
    # _FakeStore drives the default Neo4jEventsGraphOps (new_entities then new_edges).
    store = _FakeStore(
        by_call=[
            [{"name": "NewCo", "type": "Organization", "created_at": 19799}],  # new entities
            [{"src": "A", "rel": "OWNS", "tgt": "NewCo", "created_at": 19799}],  # new edges
        ]
    )
    res = await ev.new_events(store, window_days=14)
    assert res.params["since"] == 19800 - 14
    kinds = {r["kind"] for r in res.rows}
    assert kinds == {"entity", "edge"}


@pytest.mark.asyncio
async def test_new_events_type_filter_excludes_nonmatching(monkeypatch):
    monkeypatch.setattr(ev, "today_epoch_days", lambda: 19800)
    store = _FakeStore(by_call=[
        [{"name": "NewCo", "type": "Organization", "created_at": 19799},
         {"name": "Bob", "type": "Person", "created_at": 19799}],   # new entities
        [],                                                          # new edges
    ])
    res = await ev.new_events(store, window_days=14, type="Organization")
    names = {r["name"] for r in res.rows}
    assert names == {"NewCo"}            # Person excluded by type filter


@pytest.mark.asyncio
async def test_new_events_fail_soft_none_store():
    assert (await ev.new_events(None)).rows == []


@pytest.mark.asyncio
async def test_entity_new_connections(monkeypatch):
    monkeypatch.setattr(ev, "today_epoch_days", lambda: 19800)
    store = _FakeStore(rows=[{"rel": "OWNS", "other": "NewCo", "created_at": 19799}])
    res = await ev.entity_new_connections(store, name="A", window_days=30)
    assert res.params["name"] == "A" and res.params["since"] == 19770


@pytest.mark.asyncio
async def test_entity_new_connections_fail_soft_none_store():
    assert (await ev.entity_new_connections(None, name="A")).rows == []
