"""Guard: synthesize_answer must heartbeat DURING the synthesis LLM call.

The activity pulsed once at ``{"stage": "plain_synth"}`` and then awaited
``synthesizer.asynthesize`` with no further beat.  Compact-and-refine issues
one LLM request per refinement, so the real gap is minutes-to-tens-of-minutes
wide (observed in prod: last heartbeat 35 minutes stale while the activity was
still STARTED).  With no pulse there is nothing to distinguish a wedged
synthesis from a slow one, so a stuck attempt burns the full
``LLM_START_TO_CLOSE`` (1h) before Temporal reclaims it — and the whole
Auto → Drift → Global chain sits blocked behind it.

``heartbeat_every`` closes the gap (mirrors build_property_graph /
inject_canonical / detect_communities).  The deliberate 1h start-to-close
ceiling stays — a slow-but-healthy generation must NOT be killed early.
"""

from __future__ import annotations

import asyncio
import importlib
import types

import pytest
from temporalio.testing import ActivityEnvironment

from src.workflow.contracts import SerializedNode, SynthesizeParams

sa = importlib.import_module("src.workflow.activities.synthesize_answer")


@pytest.mark.asyncio
async def test_pulses_during_the_synthesis_llm_call(monkeypatch):
    # tiny interval so a short test synthesis yields many pulses.
    # raising=False: the constant only exists once the fix lands, so the RED
    # run sets it harmlessly and fails on the beat count instead of erroring.
    monkeypatch.setattr(sa, "_HEARTBEAT_INTERVAL_S", 0.02, raising=False)

    async def slow_synthesize(*, query, nodes):
        await asyncio.sleep(0.3)      # stands in for compact-and-refine
        return types.SimpleNamespace(response="итог")

    synthesizer = types.SimpleNamespace(asynthesize=slow_synthesize)

    async def _get():
        return synthesizer

    monkeypatch.setattr(sa, "get_synthesizer", _get)
    monkeypatch.setattr(sa, "get_synthesis_synthesizer", _get)
    monkeypatch.setattr(sa, "serialized_to_node", lambda sn: sn)

    beats: list[tuple] = []
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *a: beats.append(a)

    params = SynthesizeParams(
        query="кто Иванов?",
        mode="simple",
        accumulated=[SerializedNode(chunk_id="a", text="t", score=0.5)],
        max_refinements=3,
    )
    result = await env.run(sa.synthesize_answer, params)

    assert result.text == "итог"
    # 2 discrete checkpoint beats (init + plain_synth) fire regardless of the
    # fix. The 0.3s call at a 0.02s interval adds ~15 pulses. Pre-fix only the
    # 2 checkpoints fire → fails.
    assert len(beats) >= 8
