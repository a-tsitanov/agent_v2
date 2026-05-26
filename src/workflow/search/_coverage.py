"""Pure coverage-gate helpers for the plan-execute flow (R4).

Extracted as plain functions (no Temporal imports) so the orchestrator's
coverage-gate decision — "given a coverage verdict and how many extra
rounds remain, should we issue another sub-question, and for what?" — is
unit-testable WITHOUT a live Temporal test environment.  The workflow
body just calls these and dispatches the activity / child workflow.

Fail-open is enforced at the call site (any coverage_check exception →
skip the extra round); these helpers only encode the happy-path decision
over an already-obtained ``CoverageResult``.
"""

from __future__ import annotations

from collections.abc import Iterable

from src.workflow.contracts import CoverageResult, SerializedNode

# Cap on the evidence text handed to coverage_check — mirrors the legacy
# ReAct path's _COVERAGE_EVIDENCE_MAX_CHARS so the small-tier judge sees a
# comparably bounded context regardless of how many sources merged.
COVERAGE_EVIDENCE_MAX_CHARS = 12_000


def build_evidence(
    sources: Iterable[SerializedNode],
    max_chars: int = COVERAGE_EVIDENCE_MAX_CHARS,
) -> str:
    """Join merged source texts into one bounded evidence blob for the
    coverage judge.  Truncated to ``max_chars`` so a large merged pool
    can't blow the small-tier model's context."""
    return "\n\n".join(n.text for n in sources)[:max_chars]


def should_run_coverage_round(
    result: CoverageResult,
    rounds_left: int,
) -> str | None:
    """Decide whether to issue ONE more sub-question after a coverage check.

    Returns the gap phrase to retrieve when the evidence is INCOMPLETE,
    a non-empty gap is named, AND there is still round budget left;
    otherwise ``None`` (proceed straight to synthesis).

    Note ``coverage_check`` already collapses "incomplete but no gap" to
    ``complete=True`` — we re-check ``missing`` here anyway so the helper
    is correct in isolation and robust to a stray verdict.
    """
    if rounds_left <= 0:
        return None
    if result.complete:
        return None
    gap = (result.missing or "").strip()
    if not gap:
        return None
    return gap
