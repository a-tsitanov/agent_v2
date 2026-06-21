"""Guard: LLM-bound ingest activities are bounded by ATTEMPT COUNT, not
wall-clock.

History (two incidents, two opposite failure modes — this is the middle
ground):
  1. A wall-clock ``schedule_to_close_timeout`` once made tasks permanently
     fail under proxy saturation ("падают в fail на 48ч").  → that wall
     stays GONE on the LLM stages.
  2. Pure infinite-by-attempts then let a PERMANENTLY-failing document
     (corrupt input / a doc the model deterministically rejects) retry
     forever and hold its admission slot, starving ingest at scale.

So the retry policies are now bounded to ``_MAX_INGEST_ATTEMPTS`` (50):
a truly broken doc gives up and frees its slot, while a long transient
outage still survives far past 48h because under saturation each attempt
is long (50 long attempts span many days). Attempt cap, NOT wall-clock cap.
"""

from __future__ import annotations

import inspect

import src.workflow.document_ingest as di
import src.workflow.graph_build as gb


def _activity_block(src: str, start_anchor: str, end_anchor: str) -> str:
    i = src.index(start_anchor)
    j = src.index(end_anchor, i)
    return src[i:j]


def test_extract_kg_has_no_walltime_cap():
    block = _activity_block(
        inspect.getsource(di), "result_type=KGExtracted", "← extract_kg",
    )
    assert "schedule_to_close_timeout" not in block  # no wall-clock cap (incident #1)
    assert "_HEAVY_RETRY" in block                    # bounded-attempts profile


def test_merge_and_resolve_has_no_walltime_cap():
    block = _activity_block(
        inspect.getsource(gb), '"merge_and_resolve", kg', "← merge_and_resolve",
    )
    assert "schedule_to_close_timeout" not in block
    assert "_HEAVY_RETRY" in block


def test_retry_policies_bounded_to_max_attempts():
    """Permanently-failing docs must give up and free their slot (incident #2)."""
    assert di._MAX_INGEST_ATTEMPTS == 50
    assert gb._MAX_INGEST_ATTEMPTS == 50
    assert di._HEAVY_RETRY.maximum_attempts == 50
    assert di._FAST_RETRY.maximum_attempts == 50
    assert gb._HEAVY_RETRY.maximum_attempts == 50
    assert gb._FAST_RETRY.maximum_attempts == 50
