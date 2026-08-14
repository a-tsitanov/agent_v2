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


def test_build_prompt_renders_failed_step_as_uncomputable_not_zero():
    """A step with .error set must not look like an empty-but-successful result."""
    steps = [
        StepResult(
            primitive="topic_trend",
            params={"topic": "x"},
            rows=[],
            row_count=0,
            error="NebulaGraphStore.structured_query does not bind nGQL params yet (Phase 2)",
        ),
    ]
    msgs = build_synthesis_prompt("what is the trend for x?", steps)
    joined = " ".join(m.content for m in msgs)
    assert "не удалось вычислить" in joined
    assert "does not bind nGQL params yet" in joined
    assert "rows: []" not in joined


def test_build_prompt_distinguishes_failed_step_from_genuine_empty_result():
    """A mixed plan: one real result, one structural failure — the failure
    must be visibly different from both the real result and a plain empty row set."""
    ok = StepResult(primitive="count_entities", rows=[{"n": 3}], row_count=1)
    empty_but_successful = StepResult(primitive="count_relationships", rows=[], row_count=0)
    failed = StepResult(
        primitive="topic_trend",
        rows=[],
        row_count=0,
        error="backend cannot run this query",
    )
    msgs = build_synthesis_prompt("q", [ok, empty_but_successful, failed])
    joined = " ".join(m.content for m in msgs)
    assert '"n": 3' in joined
    assert "backend cannot run this query" in joined
    assert "не удалось вычислить" in joined
    # the genuinely-empty-but-successful step still reads as an empty result, not a failure
    assert "count_relationships" in joined
