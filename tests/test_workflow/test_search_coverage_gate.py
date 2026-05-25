"""Unit tests for the orchestrator coverage-gate decision helpers (R4).

The orchestrator's coverage gate logic is extracted into PURE functions
(no Temporal imports) so its branching — gap / complete / fail-open /
bound — is unit-testable WITHOUT a live Temporal test env.  The
workflow body just calls these.

Behaviours covered:
  * gap path     — incomplete + named gap + rounds left → return the gap
  * complete     — complete=True → None (no extra round)
  * empty gap    — incomplete but no gap named → None (nothing actionable)
  * bound        — rounds_left == 0 → None even on a gap
"""

from __future__ import annotations

from src.workflow.contracts import CoverageResult, SerializedNode
from src.workflow.search._coverage import (
    build_evidence,
    should_run_coverage_round,
)


def _n(cid: str, text: str = "t") -> SerializedNode:
    return SerializedNode(chunk_id=cid, text=text, score=0.5)


# ── should_run_coverage_round ──────────────────────────────────────


def test_gap_path_returns_missing_question():
    res = CoverageResult(complete=False, missing="Иванов's employer")
    assert should_run_coverage_round(res, rounds_left=1) == "Иванов's employer"


def test_complete_returns_none():
    res = CoverageResult(complete=True, missing="")
    assert should_run_coverage_round(res, rounds_left=1) is None


def test_complete_with_stray_missing_returns_none():
    # complete=True wins even if a stray gap is present.
    res = CoverageResult(complete=True, missing="ignored")
    assert should_run_coverage_round(res, rounds_left=1) is None


def test_incomplete_but_empty_gap_returns_none():
    res = CoverageResult(complete=False, missing="")
    assert should_run_coverage_round(res, rounds_left=1) is None


def test_incomplete_whitespace_gap_returns_none():
    res = CoverageResult(complete=False, missing="   ")
    assert should_run_coverage_round(res, rounds_left=1) is None


def test_bound_no_rounds_left_returns_none():
    res = CoverageResult(complete=False, missing="something")
    assert should_run_coverage_round(res, rounds_left=0) is None


def test_bound_negative_rounds_left_returns_none():
    res = CoverageResult(complete=False, missing="something")
    assert should_run_coverage_round(res, rounds_left=-1) is None


# ── build_evidence ─────────────────────────────────────────────────


def test_build_evidence_joins_text():
    ev = build_evidence([_n("a", "alpha"), _n("b", "beta")])
    assert "alpha" in ev and "beta" in ev


def test_build_evidence_empty():
    assert build_evidence([]) == ""


def test_build_evidence_truncates_to_max_chars():
    big = [_n(str(i), "x" * 5000) for i in range(10)]
    ev = build_evidence(big, max_chars=1000)
    assert len(ev) <= 1000
