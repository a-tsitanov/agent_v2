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


# ── whats_changed: fallback-ось created_at (first_seen) ──────────────
# valid_from/valid_to покрывают ~1/3 рёбер и до фикса экстракции в
# основном сфабрикованы; created_at (эпоха-день ингеста, E1) — надёжная
# ось «связь появилась в базе». Ребро без validity-дат попадает в окно
# по created_at с пометкой change='first_seen'.


@pytest.mark.asyncio
async def test_whats_changed_adds_created_at_fallback_axis():
    from datetime import date

    store = _FakeStore(rows=[])
    res = await dyn.whats_changed(store, date_from="2026-06-28", date_to="2026-07-05")
    epoch = date(1970, 1, 1)
    assert res.params["from_epoch"] == (date(2026, 6, 28) - epoch).days
    assert res.params["to_epoch"] == (date(2026, 7, 5) - epoch).days
    assert "created_at" in res.cypher
    assert "first_seen" in res.cypher
    # предсказанные рёбра — не изменения мира (карантин LIKELY_LINK)
    assert "LIKELY_LINK" in res.cypher


@pytest.mark.asyncio
async def test_whats_changed_epoch_bounds_for_partial_dates():
    from datetime import date

    store = _FakeStore(rows=[])
    res = await dyn.whats_changed(store, date_from="2024", date_to="2024-06")
    epoch = date(1970, 1, 1)
    assert res.params["from_epoch"] == (date(2024, 1, 1) - epoch).days   # год → 1 января
    assert res.params["to_epoch"] == (date(2024, 6, 30) - epoch).days    # месяц → конец месяца


@pytest.mark.asyncio
async def test_whats_changed_unparseable_dates_disable_epoch_axis():
    store = _FakeStore(rows=[])
    res = await dyn.whats_changed(store, date_from="недавно", date_to="сейчас")
    assert res.params["from_epoch"] is None
    assert res.params["to_epoch"] is None
    assert "$from_epoch IS NOT NULL" in res.cypher   # ветка сама себя выключает
