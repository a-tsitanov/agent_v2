"""Loader for the Medical benchmark corpus.

The corpus comes from
`tests/eval/corpora/medical/{medical.json, medical_questions.json}`:

* ``medical.json`` — a single English source document about
  basal cell skin cancer (the corpus we ingest as the
  knowledge base under test).
* ``medical_questions.json`` — 2 062 Q&A pairs derived from the
  same source, split across four question types:
  Fact Retrieval, Complex Reasoning, Contextual Summarize,
  Creative Generation.  Each item ships its own
  `evidence` / `evidence_relations` strings so we can score
  recall without an LLM-judge.

This module:

1. exposes the raw paths and helper loaders;
2. converts each medical Q&A into a `GoldenCase`-compatible shape
   so it plugs into ``tests/eval/answer_quality.score_case``;
3. lets callers sample / filter the set by question_type, ID,
   or count — useful both for unit tests (10 items) and the
   live runner (200-item sample).

The full set is NOT exercised in CI — 2 062 questions × 3
endpoints × multi-second per call would dominate the suite.
Unit tests sample a few items to validate the conversion and
loader code; live evaluation is opt-in via
``run_answer_eval.py --medical-sample N``.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from pathlib import Path

from tests.eval.answer_quality import GoldenCase


CORPUS_DIR = Path(__file__).resolve().parent / "corpora" / "medical"
MEDICAL_CORPUS_PATH = CORPUS_DIR / "medical.json"
MEDICAL_QUESTIONS_PATH = CORPUS_DIR / "medical_questions.json"


# `question_type` strings present in the upstream dataset.  We
# keep the canonical set as a sanity check (an unknown type
# probably means the dataset format changed).
KNOWN_QUESTION_TYPES = frozenset({
    "Fact Retrieval",
    "Complex Reasoning",
    "Contextual Summarize",
    "Creative Generation",
})


# Map medical question_type → eval category (used by
# `answer_quality.aggregate_by("category")`).  The two vocabularies
# don't perfectly align (the medical set is broader than our R9
# golden_qa categories), so we keep the upstream label verbatim
# and slot it under the closest match.
_CATEGORY_MAP = {
    "Fact Retrieval": "single_fact",
    "Complex Reasoning": "multi_hop",
    "Contextual Summarize": "open_ended_summary",
    "Creative Generation": "creative",
}


@dataclass
class MedicalQA:
    """One raw entry from `medical_questions.json`."""

    id: str
    source: str
    question: str
    answer: str
    question_type: str
    evidence: str
    evidence_relations: str

    @classmethod
    def from_dict(cls, d: dict) -> MedicalQA:
        return cls(
            id=d["id"],
            source=d.get("source", "Medical"),
            question=d["question"],
            answer=d["answer"],
            question_type=d.get("question_type", "Fact Retrieval"),
            evidence=d.get("evidence", "") or "",
            evidence_relations=d.get("evidence_relations", "") or "",
        )


# ── loaders ─────────────────────────────────────────────────────────


def load_medical_source() -> str:
    """Return the raw `context` text from `medical.json`."""
    payload = json.loads(MEDICAL_CORPUS_PATH.read_text())
    return payload["context"]


def load_medical_qas(
    *,
    limit: int | None = None,
    question_types: set[str] | None = None,
    sample_seed: int | None = None,
) -> list[MedicalQA]:
    """Return parsed Q&A entries, optionally filtered & sampled.

    `limit=None`: return all (2062 items).
    `question_types`: filter to those types (subset of `KNOWN_QUESTION_TYPES`).
    `sample_seed`: if set, shuffle deterministically before applying limit.
    """
    raw = json.loads(MEDICAL_QUESTIONS_PATH.read_text())
    items = [MedicalQA.from_dict(d) for d in raw]
    if question_types:
        items = [x for x in items if x.question_type in question_types]
    if sample_seed is not None:
        rng = random.Random(sample_seed)
        rng.shuffle(items)
    if limit is not None:
        items = items[:limit]
    return items


# ── conversion to GoldenCase ────────────────────────────────────────


def _evidence_phrases(qa: MedicalQA) -> list[str]:
    """Extract substring-matchable medical keywords from
    `evidence_relations`.

    The upstream Medical benchmark ships evidence as full
    sentences ("Treatment is typically given in phases: induction
    and consolidation") or JSON-y lists ("Biomarker: [\"BRAF gene
    mutation\", \"RET gene fusion\"]").  Whole-sentence substring
    matching against the model's paraphrased answer fails almost
    always — the model is correct ("the two main treatment phases
    are induction and consolidation"), just rephrased.

    We therefore pull medical-significant content words /
    multi-word terms out of `evidence_relations` and use those as
    individual facts.  Each must appear in the answer for full
    fact_recall on that case.

    Filters: drop short tokens (<3 chars), drop common English
    stopwords, drop generic medical filler like "patient" /
    "type".  Keep acronyms (BCC, BRAF, CEA, NTRK), drugs, and
    domain nouns.
    """
    import re

    raw = (qa.evidence_relations or qa.evidence or "").strip()
    if not raw:
        return []

    # Tokenize on any non-alphanumeric (keeps acronyms intact).
    tokens = re.findall(r"[A-Za-z0-9]+", raw)
    seen: set[str] = set()
    out: list[str] = []
    for tok in tokens:
        low = tok.lower()
        if len(tok) < 3:
            continue
        if low in _EVIDENCE_STOPWORDS:
            continue
        if low in seen:
            continue
        seen.add(low)
        # Preserve original casing — acronyms (BCC, CEA) score
        # exact, other tokens match case-insensitively via the
        # scorer's lower-case substring.
        out.append(tok)
    return out


# English stopwords + generic medical filler that adds no signal
# when checking whether the answer "got the fact".
_EVIDENCE_STOPWORDS = frozenset({
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "at",
    "by", "for", "with", "is", "are", "was", "were", "be", "as",
    "it", "this", "that", "these", "those", "from", "into", "such",
    "but", "not", "no", "than", "then", "also", "any", "all", "may",
    "can", "if", "given", "typically", "listed", "include", "includes",
    "considered", "used", "based", "main", "common", "primary",
    "method", "methods", "factor", "factors", "type", "types",
    "diagnostic", "biomarker", "biomarkers",
})


def _entity_candidates(qa: MedicalQA) -> list[str]:
    """Pull short noun-phrase candidates from the answer/evidence —
    those become the `must_include_entities` for entity_recall.

    Heuristic: capitalised words / acronyms / multi-word capitalised
    phrases.  Cheap and good-enough for the medical corpus where
    most named entities (BCC, UV, Mohs) are capitalised; deeper
    NER would be over-engineering for a fixture loader.
    """
    import re

    text = f"{qa.answer} {qa.evidence}"
    # Acronyms (2-6 uppercase letters) — BCC, UV, NMSC, ...
    acronyms = set(re.findall(r"\b[A-Z]{2,6}\b", text))
    # Capitalised multi-word terms — "Basal Cell Carcinoma".
    titlecase = set(re.findall(r"\b(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})\b", text))
    # Drop sentence-initial filler ("The", "A", "If", ...).
    drop = {"The", "A", "An", "If", "When", "How", "What", "Why", "It",
            "This", "That", "These", "Those", "Most"}
    titlecase = {t for t in titlecase if t not in drop and len(t) > 2}
    return sorted(acronyms | titlecase)


def to_golden_case(qa: MedicalQA) -> GoldenCase:
    """Project a medical Q&A onto the project `GoldenCase` shape.

    `doc_type` is set to "report" — the upstream document is a
    medically-oriented analytical piece.  Aggregation by `doc_type`
    in `answer_quality.aggregate_by` will bucket all medical Qs
    together; per-question-type breakdown surfaces through
    `aggregate_by("category")`.
    """
    return GoldenCase(
        id=qa.id,
        doc_type="report",
        category=_CATEGORY_MAP.get(qa.question_type, "single_fact"),
        query=qa.question,
        must_include_facts=_evidence_phrases(qa),
        must_include_entities=_entity_candidates(qa),
        uncertainty_ok_for=[],
        notes=f"medical/{qa.question_type}",
    )


def load_medical_golden_cases(
    *,
    limit: int | None = 10,
    question_types: set[str] | None = None,
    sample_seed: int | None = 42,
) -> list[GoldenCase]:
    """Convenience: load N medical Q&As converted to GoldenCase."""
    return [
        to_golden_case(qa)
        for qa in load_medical_qas(
            limit=limit,
            question_types=question_types,
            sample_seed=sample_seed,
        )
    ]


__all__ = [
    "CORPUS_DIR",
    "KNOWN_QUESTION_TYPES",
    "MedicalQA",
    "MEDICAL_CORPUS_PATH",
    "MEDICAL_QUESTIONS_PATH",
    "load_medical_golden_cases",
    "load_medical_qas",
    "load_medical_source",
    "to_golden_case",
]
