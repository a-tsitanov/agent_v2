"""Unit tests for numeric-faithfulness scoring in src.analytics.synthesis.

Tests the deterministic faithfulness_score function on hand-crafted (answer, rows)
pairs to ensure it correctly identifies hallucinated numbers vs. grounded numerics
before using the scorer in live evaluation.
"""

from __future__ import annotations

import json
from pathlib import Path

from src.analytics.contracts import StepResult
from src.analytics.synthesis import faithfulness_score

_CASES = json.loads(
    (Path(__file__).parent / "golden_analytics" / "cases.json").read_text(encoding="utf-8")
)


def _steps(rows: list[dict]) -> list[StepResult]:
    return [StepResult(primitive="x", rows=rows, row_count=len(rows))]


def test_faithfulness_golden_cases() -> None:
    for c in _CASES:
        score = faithfulness_score(c["answer"], _steps(c["rows"]))
        if "min_faithfulness" in c:
            assert score >= c["min_faithfulness"], (
                f"{c['id']}: score={score} < min={c['min_faithfulness']}"
            )
        if "max_faithfulness" in c:
            assert score <= c["max_faithfulness"], (
                f"{c['id']}: score={score} > max={c['max_faithfulness']}"
            )
