"""Unit tests for `tests/eval/answer_quality.py` — the scorer.

These tests run without any external service.  They exercise the
deterministic scoring logic on hand-crafted responses so we trust
the metrics before pointing the runner at a live API.

Also enforces that the 15 golden Q&A files load and have valid
shape — keeps the suite honest as cases evolve.
"""

from __future__ import annotations

import pytest

from tests.eval.answer_quality import (
    GOLDEN_DIR_DEFAULT,
    GoldenCase,
    aggregate_by,
    check_thresholds,
    load_golden_cases,
    score_case,
)


# ── golden corpus shape ─────────────────────────────────────────────


def test_15_golden_cases_present_and_balanced() -> None:
    cases = load_golden_cases()
    by_doc = {dt: 0 for dt in ("report", "email", "transcript")}
    for c in cases:
        assert c.doc_type in by_doc, f"unexpected doc_type {c.doc_type!r}"
        by_doc[c.doc_type] += 1
    # 5 per doc_type — that's the contract from the R9 plan.
    assert by_doc == {"report": 5, "email": 5, "transcript": 5}, by_doc


def test_golden_case_fields_present() -> None:
    cases = load_golden_cases()
    for c in cases:
        assert c.id
        assert c.query
        assert c.category, f"case {c.id} missing category"
        # Lists may be empty (for negative-lookup cases) but must
        # be lists, not None.
        assert isinstance(c.must_include_facts, list)
        assert isinstance(c.must_include_entities, list)
        assert isinstance(c.uncertainty_ok_for, list)


# ── scorer: fact_recall ─────────────────────────────────────────────


def test_fact_recall_partial() -> None:
    case = GoldenCase(
        id="t", doc_type="report", category="single_fact",
        query="?",
        must_include_facts=["конверсия", "Q1 2024", "12%"],
    )
    score = score_case(
        case, endpoint="search",
        answer_text="Конверсия выросла в Q1 2024.",
        sources=[],
    )
    # 2 of 3 hit
    assert abs(score.fact_recall - 2 / 3) < 1e-9


def test_fact_recall_full_when_no_facts_listed() -> None:
    case = GoldenCase(
        id="t", doc_type="report", category="x",
        query="?", must_include_facts=[],
    )
    s = score_case(case, endpoint="search", answer_text="", sources=[])
    assert s.fact_recall == 1.0


# ── scorer: entity_recall ────────────────────────────────────────────


def test_entity_recall_matches_in_answer_or_sources() -> None:
    case = GoldenCase(
        id="t", doc_type="email", category="single_fact",
        query="?",
        must_include_entities=["Иванов", "Петров", "договор"],
    )
    s = score_case(
        case, endpoint="agent",
        answer_text="Они обсуждали договор.",
        sources=[
            {"chunk_id": "c1", "content": "Иванов пишет Петрову..."},
        ],
    )
    # All 3 covered (Ivanov + Petrov from sources, договор from answer)
    assert s.entity_recall == 1.0


# ── scorer: citation_precision ──────────────────────────────────────


def test_citation_precision_all_valid() -> None:
    case = GoldenCase(id="t", doc_type="report", category="x", query="?")
    s = score_case(
        case, endpoint="selfrag",
        answer_text="X.",
        sources=[{"chunk_id": "c1", "content": ""}, {"chunk_id": "c2", "content": ""}],
        citations=[{"chunk_id": "c1"}, {"chunk_id": "c2"}],
    )
    assert s.citation_precision == 1.0


def test_citation_precision_drops_invalid() -> None:
    case = GoldenCase(id="t", doc_type="report", category="x", query="?")
    s = score_case(
        case, endpoint="selfrag",
        answer_text="X.",
        sources=[{"chunk_id": "c1", "content": ""}],
        citations=[{"chunk_id": "c1"}, {"chunk_id": "zzz"}],
    )
    assert abs(s.citation_precision - 0.5) < 1e-9


def test_citation_precision_perfect_when_no_citations_claimed() -> None:
    case = GoldenCase(id="t", doc_type="report", category="x", query="?")
    s = score_case(
        case, endpoint="search",
        answer_text="X.", sources=[], citations=None,
    )
    assert s.citation_precision == 1.0


# ── scorer: hallucination_rate ──────────────────────────────────────


def test_hallucination_zero_when_answer_is_grounded() -> None:
    case = GoldenCase(id="t", doc_type="report", category="x", query="?")
    s = score_case(
        case, endpoint="agent",
        answer_text="Конверсия выросла на двенадцать процентов.",
        sources=[{"chunk_id": "c1",
                  "content": "Конверсия выросла на двенадцать процентов в Q1."}],
    )
    assert s.hallucination_rate == 0.0


def test_hallucination_high_when_answer_is_invented() -> None:
    case = GoldenCase(id="t", doc_type="report", category="x", query="?")
    s = score_case(
        case, endpoint="agent",
        answer_text="The Andromeda galaxy spins at warp speed.",
        sources=[{"chunk_id": "c1", "content": "Quarterly revenue report."}],
    )
    assert s.hallucination_rate > 0.5


# ── scorer: uncertainty_honesty ─────────────────────────────────────


def test_uncertainty_honesty_passes_when_marked() -> None:
    case = GoldenCase(
        id="t", doc_type="report", category="negative_lookup",
        query="?",
        uncertainty_ok_for=["когорта 65+"],
    )
    s = score_case(
        case, endpoint="selfrag",
        answer_text="Про когорту 65+ нет данных [UNCERTAIN:not in corpus].",
        sources=[],
    )
    assert s.uncertainty_honesty is True


def test_uncertainty_honesty_fails_when_claimed_without_marker() -> None:
    case = GoldenCase(
        id="t", doc_type="report", category="negative_lookup",
        query="?",
        uncertainty_ok_for=["когорта 65+"],
    )
    s = score_case(
        case, endpoint="agent",
        answer_text="Когорта 65+ выросла на 30%.",
        sources=[{"chunk_id": "c1", "content": "..."}],
    )
    assert s.uncertainty_honesty is False


def test_uncertainty_honesty_passes_when_topic_not_in_answer() -> None:
    """If the answer simply doesn't mention the topic, we don't
    penalize — honesty is only at stake when the model claims."""
    case = GoldenCase(
        id="t", doc_type="report", category="negative_lookup",
        query="?",
        uncertainty_ok_for=["когорта 65+"],
    )
    s = score_case(
        case, endpoint="search",
        answer_text="Здесь информации по этому запросу нет.",
        sources=[],
    )
    assert s.uncertainty_honesty is True


# ── aggregation ─────────────────────────────────────────────────────


def test_aggregate_by_endpoint() -> None:
    from tests.eval.answer_quality import CaseScore as CS

    scores = [
        CS(case_id="a", doc_type="report", category="x", endpoint="search",
           fact_recall=1.0, entity_recall=1.0, citation_precision=1.0,
           hallucination_rate=0.0, uncertainty_honesty=True),
        CS(case_id="b", doc_type="report", category="x", endpoint="search",
           fact_recall=0.5, entity_recall=0.5, citation_precision=1.0,
           hallucination_rate=0.1, uncertainty_honesty=False),
        CS(case_id="c", doc_type="email", category="x", endpoint="agent",
           fact_recall=0.8, entity_recall=0.9, citation_precision=0.9,
           hallucination_rate=0.05, uncertainty_honesty=True),
    ]
    bucketed = aggregate_by(scores, "endpoint")
    assert bucketed["search"]["n_cases"] == 2
    assert bucketed["search"]["fact_recall"] == 0.75
    assert bucketed["agent"]["n_cases"] == 1
    assert bucketed["agent"]["fact_recall"] == 0.8


def test_check_thresholds_finds_violation() -> None:
    by_ep_dt = {
        "agent": {
            "report": {
                "fact_recall": 0.5,   # below 0.80
                "entity_recall": 0.9,
                "citation_precision": 1.0,
                "hallucination_rate": 0.01,
                "uncertainty_honesty_pct": 1.0,
                "n_cases": 5,
            }
        }
    }
    violations = check_thresholds(by_ep_dt)
    assert any("fact_recall" in v for v in violations)
    assert all("entity_recall" not in v for v in violations)
