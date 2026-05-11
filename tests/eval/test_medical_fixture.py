"""Tests for `tests/eval/medical_fixture.py`.

Validates the fixture loader + Q&A → GoldenCase conversion +
end-to-end scoring pipeline on hand-picked medical examples.
The full 2 062-item set is NOT exercised here — it would
dominate the suite.  `run_answer_eval.py --medical-sample N`
hits the live API for opt-in benchmarking.
"""

from __future__ import annotations

import pytest

from tests.eval.answer_quality import score_case
from tests.eval.medical_fixture import (
    KNOWN_QUESTION_TYPES,
    MEDICAL_CORPUS_PATH,
    MEDICAL_QUESTIONS_PATH,
    MedicalQA,
    load_medical_golden_cases,
    load_medical_qas,
    load_medical_source,
    to_golden_case,
)


# ── fixtures are present and shape stable ──────────────────────────


def test_fixture_files_exist() -> None:
    assert MEDICAL_CORPUS_PATH.is_file(), MEDICAL_CORPUS_PATH
    assert MEDICAL_QUESTIONS_PATH.is_file(), MEDICAL_QUESTIONS_PATH


def test_source_loads_and_is_substantial() -> None:
    text = load_medical_source()
    assert isinstance(text, str)
    assert "basal cell" in text.lower()
    # Roughly 4-5 KB of body text — way above the splitter's
    # min-chunk threshold (the ingest path should yield multiple
    # chunks).
    assert len(text) > 2000


def test_all_questions_load() -> None:
    qas = load_medical_qas()
    assert len(qas) == 2062
    assert all(isinstance(q, MedicalQA) for q in qas)
    types = {q.question_type for q in qas}
    # The dataset must not have drifted to unknown question_type
    # values — that would silently break categorisation.
    assert types <= KNOWN_QUESTION_TYPES, types - KNOWN_QUESTION_TYPES


def test_filter_by_question_type() -> None:
    fact = load_medical_qas(question_types={"Fact Retrieval"})
    assert all(q.question_type == "Fact Retrieval" for q in fact)
    assert len(fact) > 1000   # Fact Retrieval dominates the set


def test_deterministic_sample() -> None:
    a = load_medical_qas(limit=20, sample_seed=42)
    b = load_medical_qas(limit=20, sample_seed=42)
    c = load_medical_qas(limit=20, sample_seed=7)
    assert [x.id for x in a] == [x.id for x in b]
    assert [x.id for x in a] != [x.id for x in c]


# ── conversion to GoldenCase ────────────────────────────────────────


def test_to_golden_case_shape() -> None:
    qa = MedicalQA(
        id="m1",
        source="Medical",
        question="What is the most common type of skin cancer?",
        answer="Basal cell carcinoma (BCC) is the most common type of skin cancer.",
        question_type="Fact Retrieval",
        evidence="Basal cell carcinoma (BCC) is the most common type of skin cancer.",
        evidence_relations="BCC is the most common type of skin cancer",
    )
    gc = to_golden_case(qa)
    assert gc.id == "m1"
    assert gc.doc_type == "report"
    assert gc.category == "single_fact"  # mapped from Fact Retrieval
    # Acronyms pulled into must_include_entities
    assert "BCC" in gc.must_include_entities
    # Evidence keywords carried as facts (not the full sentence)
    facts_lc = {f.lower() for f in gc.must_include_facts}
    assert "bcc" in facts_lc
    assert "skin" in facts_lc
    assert "cancer" in facts_lc


def test_to_golden_case_handles_multi_relation() -> None:
    qa = MedicalQA(
        id="m2", source="Medical",
        question="What raises BCC risk?",
        answer="UV exposure and fair skin both increase risk.",
        question_type="Complex Reasoning",
        evidence="UV exposure raises risk; fair skin raises risk.",
        evidence_relations=(
            "UV radiation exposure is a primary risk factor for BCC; "
            "Fair skin, light hair, and light eye color increase the risk of BCC"
        ),
    )
    gc = to_golden_case(qa)
    assert gc.category == "multi_hop"
    # Facts are now per-keyword (not per-sentence) so the answer
    # text can be paraphrased without losing fact_recall.
    facts_lc = {f.lower() for f in gc.must_include_facts}
    assert "uv" in facts_lc or "radiation" in facts_lc or "exposure" in facts_lc
    assert "bcc" in facts_lc
    assert "skin" in facts_lc
    # Stopwords and medical filler should be filtered out.
    assert "the" not in facts_lc
    assert "factor" not in facts_lc


def test_load_medical_golden_cases_defaults() -> None:
    cases = load_medical_golden_cases()
    assert len(cases) == 10
    assert all(c.doc_type == "report" for c in cases)
    # `notes` is "medical/<question_type>" — useful when debugging
    # which subset a failing case came from.
    assert all(c.notes.startswith("medical/") for c in cases)


# ── end-to-end: convert + score against an answer ───────────────────


def test_score_perfect_medical_answer() -> None:
    """A correct answer that quotes the evidence verbatim should
    score perfect fact_recall + perfect entity_recall."""
    qa = MedicalQA(
        id="m3", source="Medical",
        question="What is BCC?",
        answer="BCC is the most common type of skin cancer.",
        question_type="Fact Retrieval",
        evidence="BCC is the most common type of skin cancer.",
        evidence_relations="BCC is the most common type of skin cancer",
    )
    gc = to_golden_case(qa)
    score = score_case(
        gc, endpoint="search",
        answer_text=qa.answer,
        sources=[{"chunk_id": "c1",
                  "content": "Basal cell carcinoma (BCC) is the most common type of skin cancer."}],
    )
    # Every keyword from evidence_relations is present in the
    # answer → fact_recall pegged to 1.0.
    assert score.fact_recall == 1.0
    assert score.entity_recall == 1.0


def test_paraphrased_correct_answer_now_scores() -> None:
    """Regression: a correct answer phrased differently from the
    evidence sentence used to score 0% fact_recall under the old
    sentence-level substring matcher.  Per-keyword facts fix it."""
    qa = MedicalQA(
        id="m-paraphrase", source="Medical",
        question="What are the two main treatment phases for primary CNS lymphoma?",
        answer="Treatment is typically given in phases: induction and consolidation.",
        question_type="Fact Retrieval",
        evidence="Treatment is typically given in phases: induction and consolidation",
        evidence_relations="Treatment is typically given in phases: induction and consolidation",
    )
    gc = to_golden_case(qa)
    paraphrased = (
        "The two main treatment phases for primary CNS lymphoma are "
        "induction and consolidation."
    )
    score = score_case(
        gc, endpoint="agent",
        answer_text=paraphrased,
        sources=[{"chunk_id": "c1", "content": qa.evidence}],
    )
    assert score.fact_recall == 1.0, score.fact_recall


def test_score_wrong_medical_answer() -> None:
    """A wrong answer should fall on fact_recall."""
    qa = MedicalQA(
        id="m4", source="Medical",
        question="What is BCC?",
        answer="It's a heart condition.",
        question_type="Fact Retrieval",
        evidence="BCC is the most common type of skin cancer.",
        evidence_relations="BCC is the most common type of skin cancer",
    )
    gc = to_golden_case(qa)
    score = score_case(
        gc, endpoint="search",
        answer_text="BCC stands for basal cell something. Unsure.",
        sources=[{"chunk_id": "c1", "content": ""}],
    )
    # Evidence phrase isn't present in the answer or sources →
    # fact_recall must drop.
    assert score.fact_recall < 1.0


# ── tiny end-to-end with the pipeline (offline, no live LLM) ───────


def test_medical_source_chunks_through_pipeline() -> None:
    """Smoke: feed the medical corpus through the ingestion pipeline
    (sentence splitter only — no embedding / no LLM) and confirm
    we get a reasonable number of chunks."""
    from llama_index.core.node_parser import SentenceSplitter
    from llama_index.core.schema import Document

    text = load_medical_source()
    doc = Document(text=text, metadata={"doc_id": "medical", "doc_type": "report"})
    splitter = SentenceSplitter(chunk_size=512, chunk_overlap=50)
    nodes = splitter.get_nodes_from_documents([doc])
    # ~1MB text at chunk_size=512 → ~300-700 chunks; bound generously.
    # The exact count drifts with splitter heuristics; both ends
    # of the bound here are sanity checks not exact assertions.
    assert 100 <= len(nodes) <= 1000, len(nodes)
    # Metadata carries through
    assert nodes[0].metadata["doc_id"] == "medical"
