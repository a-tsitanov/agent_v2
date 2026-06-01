"""Regression: the lazy-singleton getters in ``_search_deps`` must not
self-deadlock on the non-reentrant module ``_lock``.

``get_synthesizer`` / ``get_synthesis_synthesizer`` resolve their LLM via
another lock-taking getter.  If they do so WHILE already holding ``_lock``,
the coroutine waits forever for a lock it already holds — which is exactly
the cold-worker ``synthesize_answer`` hang (Started → no progress →
start_to_close timeout).  These tests fail with ``TimeoutError`` before the
fix and pass after.
"""

from __future__ import annotations

import asyncio

import pytest

from src.workflow import _search_deps as sd


async def _fake_synth(llm):
    return ("synth", llm)


@pytest.mark.asyncio
async def test_get_synthesis_synthesizer_no_self_deadlock(monkeypatch):
    # Fresh lock bound to this test's loop; stub the heavy builders so we
    # exercise ONLY the lock interaction.
    monkeypatch.setattr(sd, "_lock", asyncio.Lock())
    sd.reset_for_tests()
    monkeypatch.setattr(sd, "wrap_if_needed", lambda llm, **k: llm)
    import src.retrieval.llm as llm_mod
    monkeypatch.setattr(llm_mod, "build_synthesis_llm", lambda: object())
    monkeypatch.setattr(sd, "_build_synthesizer_once", _fake_synth)

    res = await asyncio.wait_for(sd.get_synthesis_synthesizer(), timeout=3.0)
    assert res is not None
    sd.reset_for_tests()


@pytest.mark.asyncio
async def test_get_synthesizer_no_self_deadlock(monkeypatch):
    monkeypatch.setattr(sd, "_lock", asyncio.Lock())
    sd.reset_for_tests()
    monkeypatch.setattr(sd, "wrap_if_needed", lambda llm, **k: llm)
    monkeypatch.setattr(sd, "build_search_llm", lambda: object())
    monkeypatch.setattr(sd, "_build_synthesizer_once", _fake_synth)

    res = await asyncio.wait_for(sd.get_synthesizer(), timeout=3.0)
    assert res is not None
    sd.reset_for_tests()
