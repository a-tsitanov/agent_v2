"""Unit tests for the BoundedLLM wrapper.

We don't talk to a real LLM — verify the semaphore actually serialises
concurrent ``achat`` calls and that pass-through attribute access works.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.retrieval.llm_semaphore import BoundedLLM, wrap_if_needed


def _make_inner():
    inner = MagicMock()
    inner.achat = AsyncMock(return_value="response")
    inner.acomplete = AsyncMock(return_value="completion")
    inner.achat_with_tools = AsyncMock(return_value="tools_response")
    inner.astructured_predict = AsyncMock(return_value="structured")
    inner.metadata = {"model": "test-model"}
    inner.foo = "bar"
    return inner


@pytest.mark.asyncio
async def test_basic_passthrough():
    inner = _make_inner()
    llm = BoundedLLM(inner, max_concurrent=2)
    out = await llm.achat("hello")
    assert out == "response"
    inner.achat.assert_awaited_once_with("hello")


@pytest.mark.asyncio
async def test_concurrency_caps_at_max():
    """N=2 parallel calls must serialise into 2 batches when 4 are launched.

    We use a barrier inside achat to detect overlap: only 2 should be
    in-flight at once.
    """
    in_flight = 0
    max_observed = 0
    lock = asyncio.Lock()

    async def fake_achat(*a, **kw):
        nonlocal in_flight, max_observed
        async with lock:
            in_flight += 1
            if in_flight > max_observed:
                max_observed = in_flight
        await asyncio.sleep(0.05)
        async with lock:
            in_flight -= 1
        return "ok"

    inner = _make_inner()
    inner.achat = fake_achat
    llm = BoundedLLM(inner, max_concurrent=2)

    # Launch 6 parallel calls.
    results = await asyncio.gather(*[llm.achat() for _ in range(6)])
    assert results == ["ok"] * 6
    assert max_observed <= 2, (
        f"semaphore breached: max in-flight = {max_observed}, expected ≤ 2"
    )


@pytest.mark.asyncio
async def test_attribute_passthrough_for_metadata():
    inner = _make_inner()
    llm = BoundedLLM(inner, max_concurrent=4)
    # Attributes that aren't async methods (e.g. metadata) should
    # resolve to the inner LLM transparently.
    assert llm.metadata == {"model": "test-model"}
    assert llm.foo == "bar"


@pytest.mark.asyncio
async def test_multiple_methods_share_one_semaphore():
    """achat + acomplete + achat_with_tools all share the same gate —
    so 1 in-flight achat blocks an acomplete from starting if cap=1."""
    in_flight = 0
    max_observed = 0
    lock = asyncio.Lock()

    async def fake(*a, **kw):
        nonlocal in_flight, max_observed
        async with lock:
            in_flight += 1
            if in_flight > max_observed:
                max_observed = in_flight
        await asyncio.sleep(0.05)
        async with lock:
            in_flight -= 1
        return "x"

    inner = _make_inner()
    inner.achat = fake
    inner.acomplete = fake
    inner.achat_with_tools = fake
    llm = BoundedLLM(inner, max_concurrent=1)
    await asyncio.gather(
        llm.achat(), llm.acomplete(), llm.achat_with_tools([], []),
    )
    assert max_observed == 1


def test_invalid_max_concurrent_raises():
    inner = _make_inner()
    with pytest.raises(ValueError):
        BoundedLLM(inner, max_concurrent=0)


def test_wrap_if_needed_does_not_double_wrap():
    inner = _make_inner()
    first = BoundedLLM(inner, max_concurrent=2)
    second = wrap_if_needed(first, max_concurrent=5)
    assert second is first


def test_wrap_if_needed_wraps_raw():
    inner = _make_inner()
    wrapped = wrap_if_needed(inner, max_concurrent=4)
    assert isinstance(wrapped, BoundedLLM)
    assert wrapped.inner is inner
    assert wrapped.max_concurrent == 4


def test_repr_useful():
    inner = _make_inner()
    inner.__class__.__name__ = "OpenAILike"
    llm = BoundedLLM(inner, max_concurrent=3)
    s = repr(llm)
    assert "BoundedLLM" in s
    assert "max_concurrent=3" in s
