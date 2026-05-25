"""Unit tests for the plan-execute merge/dedup helpers (R2).

These cover the CORE behaviour the SubQuery + orchestrator workflows
delegate to, without needing a live Temporal test env — the workflow
bodies are thin wrappers over these pure functions.
"""

from __future__ import annotations

from src.workflow.contracts import SerializedNode
from src.workflow.search._merge import dedup_by_chunk_id, merge_subquery_sources


def _n(cid: str, text: str = "t") -> SerializedNode:
    return SerializedNode(chunk_id=cid, text=text, score=0.5)


def test_dedup_keeps_first_occurrence():
    out = dedup_by_chunk_id([_n("a"), _n("b"), _n("a"), _n("c")])
    assert [n.chunk_id for n in out] == ["a", "b", "c"]


def test_dedup_empty():
    assert dedup_by_chunk_id([]) == []


def test_merge_flattens_and_dedups_across_subqueries():
    g1 = [_n("a"), _n("b")]
    g2 = [_n("b"), _n("c")]  # b overlaps g1
    g3 = [_n("a"), _n("d")]  # a overlaps g1
    out = merge_subquery_sources([g1, g2, g3])
    assert [n.chunk_id for n in out] == ["a", "b", "c", "d"]
