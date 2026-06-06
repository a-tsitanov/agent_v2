"""Regression: the lazy-singleton getters in ``_search_deps`` must not
self-deadlock on the non-reentrant module ``_lock``.

``get_synthesizer`` / ``get_synthesis_synthesizer`` resolve their LLM via
another lock-taking getter.  If they do so WHILE already holding ``_lock``,
the coroutine waits forever for a lock it already holds — which is exactly
the cold-worker ``synthesize_answer`` hang (Started → no progress →
start_to_close timeout).  These tests fail with ``TimeoutError`` before the
fix and pass after.

Canary design: the fake LLM getters below ACQUIRE AND RELEASE the module
``_lock`` themselves, simulating the old pattern where the getter took the
same lock.  With the current production code (getter called BEFORE
``async with _lock``), they complete fine.  If a future regression moved the
getter call INSIDE ``async with _lock``, the fake's re-entry on the same
non-reentrant lock would deadlock and ``asyncio.wait_for`` would raise
``TimeoutError``, catching the regression.
"""

from __future__ import annotations

import asyncio

import pytest

from src.workflow import _search_deps as deps_module


async def _fake_synth(llm):
    return ("synth", llm)


@pytest.mark.asyncio
async def test_get_synthesis_synthesizer_no_self_deadlock(monkeypatch):
    # Fresh lock bound to this test's loop; stub the heavy builders so we
    # exercise ONLY the lock interaction.
    monkeypatch.setattr(deps_module, "_lock", asyncio.Lock())
    deps_module.reset_for_tests()
    _fake_llm = object()

    async def _fake_synthesis_llm():
        # Acquires and releases the module _lock — simulates the old pattern
        # where the getter took _lock.  If get_synthesis_synthesizer() calls
        # this while already holding _lock, the non-reentrant acquire deadlocks.
        async with deps_module._lock:
            pass
        return _fake_llm

    monkeypatch.setattr(deps_module, "get_synthesis_llm", _fake_synthesis_llm)
    monkeypatch.setattr(deps_module, "_build_synthesizer_once", _fake_synth)

    res = await asyncio.wait_for(deps_module.get_synthesis_synthesizer(), timeout=3.0)
    assert res is not None
    deps_module.reset_for_tests()


@pytest.mark.asyncio
async def test_get_synthesizer_no_self_deadlock(monkeypatch):
    monkeypatch.setattr(deps_module, "_lock", asyncio.Lock())
    deps_module.reset_for_tests()
    _fake_llm = object()

    async def _fake_search_llm():
        # Acquires and releases the module _lock — simulates the old pattern
        # where the getter took _lock.  If get_synthesizer() calls this while
        # already holding _lock, the non-reentrant acquire deadlocks.
        async with deps_module._lock:
            pass
        return _fake_llm

    monkeypatch.setattr(deps_module, "get_search_llm", _fake_search_llm)
    monkeypatch.setattr(deps_module, "_build_synthesizer_once", _fake_synth)

    res = await asyncio.wait_for(deps_module.get_synthesizer(), timeout=3.0)
    assert res is not None
    deps_module.reset_for_tests()
