"""Unit tests for the per-process LLMPool (K+N single-semaphore model)."""

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


def _settings(n: int):
    s = MagicMock()
    s.llm_pool.n = n
    return s


@pytest.mark.asyncio
async def test_lane_counts_in_use_and_available():
    lane = Lane("pool", cap=2)
    assert lane.available == 2
    async with lane:
        assert lane.in_use == 1
        assert lane.available == 1
    assert lane.in_use == 0


def test_lane_rejects_zero_cap():
    with pytest.raises(ValueError, match="cap must be >= 1"):
        Lane("pool", cap=0)


@pytest.mark.asyncio
async def test_get_is_singleton_per_role(monkeypatch):
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())
    pool = LLMPool(_settings(8))
    assert pool.get("extraction") is pool.get("extraction")


@pytest.mark.asyncio
async def test_one_semaphore_bounds_all_roles(monkeypatch):
    """ONE semaphore of size N gates EVERY role - total in-flight <= N."""
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

    pool = LLMPool(_settings(2))
    ext, jud, syn = pool.get("extraction"), pool.get("judge"), pool.get("synthesis")
    for w in (ext, jud, syn):
        w._inner.achat = fake

    calls = (
        [ext.achat() for _ in range(5)]
        + [jud.achat() for _ in range(5)]
        + [syn.achat() for _ in range(5)]
    )
    await asyncio.gather(*calls)
    assert max_observed <= 2


@pytest.mark.asyncio
async def test_stats_reports_kn(monkeypatch):
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())
    pool = LLMPool(_settings(5))
    pool.get("extraction")
    st = pool.stats()
    assert st["mode"] == "kn"
    assert st["n"] == 5
    assert st["available"] == 5
    assert st["in_use"] == 0


@pytest.mark.asyncio
async def test_reset_rebuilds_singleton(monkeypatch):
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())
    p1 = get_llm_pool()
    reset_for_tests()
    assert get_llm_pool() is not p1


@pytest.mark.asyncio
async def test_lane_warns_on_saturation():
    from loguru import logger

    messages: list[str] = []
    sink_id = logger.add(messages.append, level="WARNING")
    try:
        lane = Lane("pool", cap=1)
        block = asyncio.Event()

        async def hold():
            async with lane:
                await block.wait()

        holder = asyncio.create_task(hold())
        await asyncio.sleep(0.01)

        async def enter_and_release():
            async with lane:
                pass

        waiter = asyncio.create_task(enter_and_release())
        await asyncio.sleep(0.01)
        block.set()
        await holder
        await waiter
    finally:
        logger.remove(sink_id)

    assert any("saturated" in m for m in messages), messages
