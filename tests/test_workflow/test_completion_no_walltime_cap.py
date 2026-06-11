"""Guard: LLM-bound ingest activities must retry until success.

Under LLM-proxy saturation an extract_kg / merge_and_resolve attempt can
take a long time; a wall-clock ``schedule_to_close_timeout`` made such a
task permanently fail (the prod symptom: "падают в fail на 48ч").  With
the heartbeat fix in place the retry storm is gone, so these activities
should retry until they succeed — no overall wall-clock cap.
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
    assert "schedule_to_close_timeout" not in block
    assert "_HEAVY_FOREVER" in block  # still infinite-by-attempts


def test_merge_and_resolve_has_no_walltime_cap():
    block = _activity_block(
        inspect.getsource(gb), '"merge_and_resolve", kg', "← merge_and_resolve",
    )
    assert "schedule_to_close_timeout" not in block
    assert "_HEAVY_FOREVER" in block
