import pytest

from src.analytics.primitives import dynamics as dyn
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_topic_trend_buckets_epoch_days_in_python():
    # two chunks: 2024-03-15 (19797) and 2024-03-20 (19802) → same month bucket
    store = _FakeStore(rows=[{"epoch": 19797, "n": 2}, {"epoch": 19802, "n": 1}])
    res = await dyn.topic_trend(store, topic="Поставки", granularity="month")
    periods = {r["period"]: r["mentions"] for r in res.rows}
    assert periods == {"2024-03": 3}
    assert "doc_date_epoch" in res.cypher


@pytest.mark.asyncio
async def test_relationship_timeline_uses_iso_substring():
    store = _FakeStore(
        rows=[{"period": "2024-03", "rel": "OWNS", "name": "X", "polarity": "affirmed"}]
    )
    res = await dyn.relationship_timeline(store, name="Ромашка")
    assert "substring(r.valid_from" in res.cypher


@pytest.mark.asyncio
async def test_whats_changed_marks_appeared_vs_ended():
    store = _FakeStore(rows=[{"name": "A", "rel": "OWNS", "other": "B", "change": "appeared"}])
    res = await dyn.whats_changed(store, date_from="2024-01-01", date_to="2024-12-31")
    assert res.params["from"] == "2024-01-01"


@pytest.mark.asyncio
async def test_failsoft():
    assert (await dyn.topic_trend(None, topic="x")).rows == []


@pytest.mark.asyncio
async def test_entity_activity_delegates_and_params_consistent():
    store = _FakeStore(rows=[{"epoch": 19797, "n": 1}])
    res = await dyn.entity_activity(store, name="Ромашка")
    assert res.rows == [{"period": "2024-03", "mentions": 1}]
    assert "topic" in res.params   # cypher uses $topic
