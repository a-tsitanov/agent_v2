from __future__ import annotations

import asyncio

import pytest
from temporalio.testing import ActivityEnvironment

from src.workflow.heartbeat import heartbeat_every


@pytest.mark.asyncio
async def test_pulses_periodically_during_long_work():
    """A long-running awaitable wrapped in heartbeat_every emits
    multiple heartbeats — closing the gap that lets heartbeat_timeout
    fire mid-work."""
    beats: list[tuple] = []

    async def slow_activity():
        async with heartbeat_every(0.05, {"stage": "working"}):
            await asyncio.sleep(0.3)
        return "done"

    env = ActivityEnvironment()
    env.on_heartbeat = lambda *args: beats.append(args)
    result = await env.run(slow_activity)

    assert result == "done"
    # 0.3s of work / 0.05s interval => several pulses (bare code emits 0)
    assert len(beats) >= 3
    assert all(b == ({"stage": "working"},) for b in beats)


@pytest.mark.asyncio
async def test_stops_pulsing_after_block_exits():
    """The background heartbeater is cancelled on exit — no pulses keep
    firing once the wrapped work is done."""
    beats: list[tuple] = []

    async def quick_activity():
        async with heartbeat_every(0.05, {"stage": "working"}):
            pass  # exits immediately
        return "done"

    env = ActivityEnvironment()
    env.on_heartbeat = lambda *args: beats.append(args)
    await env.run(quick_activity)

    before = len(beats)
    await asyncio.sleep(0.2)  # well past several intervals
    assert len(beats) == before  # no late pulses after exit


@pytest.mark.asyncio
async def test_propagates_work_exceptions():
    """An error inside the wrapped block surfaces (heartbeater must not
    swallow it) and the heartbeater is still torn down."""
    async def failing_activity():
        async with heartbeat_every(0.05, {"stage": "working"}):
            raise ValueError("boom")

    env = ActivityEnvironment()
    with pytest.raises(ValueError, match="boom"):
        await env.run(failing_activity)
