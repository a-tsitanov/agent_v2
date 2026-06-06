"""Unit tests for the per-process LLMPool (hierarchical tier+lane limits)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.retrieval import llm_pool as pool_mod
from src.retrieval.llm_pool import Lane, LLMPool, get_llm_pool, reset_for_tests


@pytest.fixture(autouse=True)
def _reset():
    reset_for_tests()
    yield
    reset_for_tests()


def _fake_llm():
    m = MagicMock()
    m.achat = AsyncMock(return_value="ok")
    return m


@pytest.mark.asyncio
async def test_lane_counts_in_use_and_available():
    lane = Lane("extraction", "small", cap=2)
    assert lane.available == 2
    async with lane:
        assert lane.in_use == 1
        assert lane.available == 1
    assert lane.in_use == 0


@pytest.mark.asyncio
async def test_get_is_singleton_per_role(monkeypatch):
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())
    pool = get_llm_pool()
    a = pool.get("extraction")
    b = pool.get("extraction")
    assert a is b  # same wrapped instance -> same semaphores


@pytest.mark.asyncio
async def test_tier_global_bounds_across_lanes(monkeypatch):
    """Two small lanes over-subscribe (caps 10 each) but the small-tier
    global (3) bounds total in-flight to 3."""
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())

    in_flight = 0
    max_observed = 0
    lock = asyncio.Lock()

    async def fake(*a, **kw):
        nonlocal in_flight, max_observed
        async with lock:
            in_flight += 1
            max_observed = max(max_observed, in_flight)
        await asyncio.sleep(0.05)
        async with lock:
            in_flight -= 1
        return "ok"

    settings = MagicMock()
    settings.llm_pool.tier_small_total = 3
    settings.llm_pool.tier_large_total = 8
    settings.llm_pool.lane_caps = {"extraction": 10, "judge": 10}
    settings.litellm.tier_for = lambda role: "small"

    pool = LLMPool(settings)
    ext = pool.get("extraction")
    jud = pool.get("judge")
    ext._inner.achat = fake
    jud._inner.achat = fake

    calls = [ext.achat() for _ in range(6)] + [jud.achat() for _ in range(6)]
    await asyncio.gather(*calls)
    assert max_observed <= 3


@pytest.mark.asyncio
async def test_judge_floor_under_extraction_flood(monkeypatch):
    """Sizing rule: with tier=10, extraction ceiling 6, judge can always
    get >= 4 (10-6). Flood extraction; assert judge still runs concurrently."""
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())

    settings = MagicMock()
    settings.llm_pool.tier_small_total = 10
    settings.llm_pool.tier_large_total = 8
    settings.llm_pool.lane_caps = {"extraction": 6, "judge": 14}
    settings.litellm.tier_for = lambda role: "small"

    pool = LLMPool(settings)
    ext = pool.get("extraction")
    jud = pool.get("judge")

    ext_block = asyncio.Event()

    async def ext_call(*a, **kw):
        await ext_block.wait()
        return "ok"

    async def jud_call(*a, **kw):
        await asyncio.sleep(0.02)
        return "ok"

    ext._inner.achat = ext_call
    jud._inner.achat = jud_call

    # Saturate extraction (6 calls parked on ext_block).
    ext_tasks = [asyncio.create_task(ext.achat()) for _ in range(6)]
    await asyncio.sleep(0.05)
    # Judge must still complete (floor = 10 - 6 = 4 >= 1).
    await asyncio.wait_for(jud.achat(), timeout=1.0)
    ext_block.set()
    await asyncio.gather(*ext_tasks)


@pytest.mark.asyncio
async def test_stats_reports_lanes(monkeypatch):
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())
    pool = get_llm_pool()
    pool.get("extraction")
    st = pool.stats()
    assert "extraction" in st["lanes"]
    assert st["lanes"]["extraction"]["cap"] >= 1
    assert "small" in st["tiers"]


def test_lane_rejects_zero_cap():
    with pytest.raises(ValueError, match="cap must be >= 1"):
        Lane("x", "small", cap=0)


@pytest.mark.asyncio
async def test_reset_rebuilds_singleton(monkeypatch):
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())
    p1 = get_llm_pool()
    reset_for_tests()
    p2 = get_llm_pool()
    assert p1 is not p2


@pytest.mark.asyncio
async def test_lane_warns_on_saturation():
    """When a lane is full and a caller must wait, emit a WARNING so the
    backlog that moved from Temporal schedule_to_start into pool-wait
    stays visible."""
    from loguru import logger

    messages: list[str] = []
    sink_id = logger.add(messages.append, level="WARNING")
    try:
        lane = Lane("extraction", "small", cap=1)
        block = asyncio.Event()

        async def hold():
            async with lane:
                await block.wait()

        holder = asyncio.create_task(hold())
        await asyncio.sleep(0.01)  # ensure holder owns the only permit

        async def enter_and_release():
            async with lane:
                pass

        waiter = asyncio.create_task(enter_and_release())
        await asyncio.sleep(0.01)  # waiter is now blocked -> should have warned
        block.set()
        await holder
        await waiter
    finally:
        logger.remove(sink_id)

    assert any("saturated" in m for m in messages), messages


@pytest.mark.asyncio
async def test_lane_no_warning_when_free():
    """No saturation warning when the lane has a free permit."""
    from loguru import logger

    messages: list[str] = []
    sink_id = logger.add(messages.append, level="WARNING")
    try:
        lane = Lane("extraction", "small", cap=2)
        async with lane:
            pass
    finally:
        logger.remove(sink_id)

    assert not any("saturated" in m for m in messages), messages
