"""Tests for synthesis prompt builder and numeric-faithfulness checker."""

from src.analytics.contracts import StepResult
from src.analytics.synthesis import (
    build_synthesis_prompt,
    extract_numbers,
    faithfulness_score,
)


def test_extract_numbers():
    assert extract_numbers("There are 7 orgs and 12% growth") == {"7", "12"}


def test_faithfulness_all_present():
    steps = [StepResult(primitive="count_entities", rows=[{"n": 7}], row_count=1)]
    assert faithfulness_score("There are 7 organizations.", steps) == 1.0


def test_faithfulness_detects_hallucinated_number():
    steps = [StepResult(primitive="count_entities", rows=[{"n": 7}], row_count=1)]
    # answer invents "42"
    assert faithfulness_score("There are 7 orgs and 42 people.", steps) == 0.5


def test_faithfulness_no_numbers_is_one():
    steps = [StepResult(primitive="x", rows=[{"n": 7}], row_count=1)]
    assert faithfulness_score("No quantitative claim here.", steps) == 1.0


def test_build_prompt_includes_rows_and_only_rows_rule():
    steps = [StepResult(primitive="count_entities", rows=[{"n": 7}], row_count=1)]
    msgs = build_synthesis_prompt("how many?", steps)
    joined = " ".join(m.content for m in msgs)
    assert "7" in joined and "only" in joined.lower()
