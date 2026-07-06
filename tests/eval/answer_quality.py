"""Answer-quality eval primitives (R9).

Given a golden Q&A case and a `SearchResponse`-like payload from
one of the three endpoints, computes:

  * **fact_recall** — fraction of `must_include_facts` substrings
    that appear (case-insensitive) in the answer text.
  * **entity_recall** — fraction of `must_include_entities` that
    appear in either the answer text OR any source chunk content.
  * **citation_precision** — for `/agent` and `/selfrag`: fraction
    of structured citations whose `chunk_id` is also in `sources`.
    (a 100% baseline for `/search` since it doesn't claim
    per-claim grounding).
  * **hallucination_rate** — proxy: fraction of sentences with
    no overlapping content from any source chunk.  Substring-based,
    so it's a coarse signal not a guarantee — eval reports it as
    an *upper bound*.
  * **uncertainty_honesty** — when the golden case lists topics
    under `uncertainty_ok_for`, did the endpoint either avoid
    claiming about them OR mark them `[UNCERTAIN]`?  Boolean.

The scorer is **deterministic and offline** — no LLM-as-judge here.
LLM-judge can be layered on top for nuanced cases; we don't want
the eval to depend on the very thing we're trying to grade.

The CLI runner lives in `run_answer_eval.py` — this module is
import-friendly so test stubs can call it directly.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

GOLDEN_DIR_DEFAULT = Path(__file__).resolve().parent / "golden_qa"

# Hallucination heuristic: a sentence "is supported" if at least
# this many of its content words appear (lowercased) in any source.
_SUPPORT_WORD_THRESHOLD = 2
_STOPWORDS = frozenset({
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "at",
    "by", "for", "with", "is", "are", "was", "were", "be", "as",
    "it", "this", "that", "i", "you", "he", "she", "we", "they",
    "и", "в", "на", "с", "по", "для", "от", "о", "к", "из",
    "это", "что", "как", "так", "же", "не", "но", "а", "за",
    "под", "над", "при", "у", "до", "со",
})


# ── golden case shape ────────────────────────────────────────────────


@dataclass
class GoldenCase:
    """One entry from `golden_qa/*.json`."""

    id: str
    doc_type: str  # "report" | "email" | "transcript"
    category: str  # single_fact | multi_hop | open_ended_summary | ...
    query: str
    must_include_facts: list[str] = field(default_factory=list)
    must_include_entities: list[str] = field(default_factory=list)
    uncertainty_ok_for: list[str] = field(default_factory=list)
    notes: str = ""

    @classmethod
    def from_path(cls, path: Path) -> GoldenCase:
        data = json.loads(path.read_text())
        return cls(
            id=data["id"],
            doc_type=data["doc_type"],
            category=data["category"],
            query=data["query"],
            must_include_facts=list(data.get("must_include_facts") or []),
            must_include_entities=list(data.get("must_include_entities") or []),
            uncertainty_ok_for=list(data.get("uncertainty_ok_for") or []),
            notes=data.get("notes", ""),
        )


def load_golden_cases(golden_dir: Path = GOLDEN_DIR_DEFAULT) -> list[GoldenCase]:
    files = sorted(golden_dir.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"no golden Q&A under {golden_dir}")
    return [GoldenCase.from_path(p) for p in files]


# ── scorer ───────────────────────────────────────────────────────────


@dataclass
class CaseScore:
    """Per-case scoring breakdown."""

    case_id: str
    doc_type: str
    category: str
    endpoint: str  # "search" | "agent" | "selfrag" | "legacy"
    fact_recall: float = 0.0
    entity_recall: float = 0.0
    citation_precision: float = 1.0
    hallucination_rate: float = 0.0
    uncertainty_honesty: bool = True
    answer_length: int = 0


def _norm(text: str) -> str:
    return (text or "").lower()


def _contains(haystack_lc: str, needle: str) -> bool:
    return needle.lower() in haystack_lc


def _split_sentences(text: str) -> list[str]:
    """Split on sentence terminators; cheap and good-enough for eval."""
    if not text:
        return []
    parts = re.split(r"(?<=[.!?])\s+", text.strip())
    return [p for p in parts if p]


def _content_words(text: str) -> list[str]:
    return [
        w.strip(".,;:!?\"'()[]{}«»") for w in _norm(text).split()
        if w not in _STOPWORDS and len(w) > 2
    ]


def score_case(
    case: GoldenCase,
    *,
    endpoint: str,
    answer_text: str,
    sources: list[dict],
    citations: list[dict] | None = None,
) -> CaseScore:
    """Score one (case, response) pair.

    `sources`: each dict must have `chunk_id` and `content`.
    `citations`: optional — list of `{"chunk_id": ...}` from
        `ReflectiveAnswerDetail.citations`.  None → not claimed.
    """
    answer_lc = _norm(answer_text)
    source_concat_lc = _norm(" ".join(s.get("content", "") for s in sources))

    # fact_recall: substring match in answer text only
    if case.must_include_facts:
        hits = sum(1 for f in case.must_include_facts if _contains(answer_lc, f))
        fact_recall = hits / len(case.must_include_facts)
    else:
        fact_recall = 1.0

    # entity_recall: present in answer OR retrieved sources
    if case.must_include_entities:
        hits = sum(
            1 for e in case.must_include_entities
            if _contains(answer_lc, e) or _contains(source_concat_lc, e)
        )
        entity_recall = hits / len(case.must_include_entities)
    else:
        entity_recall = 1.0

    # citation_precision: only meaningful when citations provided
    if citations is None or not citations:
        citation_precision = 1.0
    else:
        valid_ids = {s.get("chunk_id") for s in sources}
        ok = sum(1 for c in citations if c.get("chunk_id") in valid_ids)
        citation_precision = ok / len(citations)

    # hallucination_rate (upper bound, see module docstring)
    sentences = _split_sentences(answer_text)
    if sentences and source_concat_lc:
        unsupported = 0
        for sent in sentences:
            words = _content_words(sent)
            if not words:
                continue
            n_in = sum(1 for w in words if w in source_concat_lc)
            if n_in < _SUPPORT_WORD_THRESHOLD:
                unsupported += 1
        hallucination_rate = unsupported / len(sentences)
    else:
        hallucination_rate = 0.0

    # uncertainty_honesty
    honesty = True
    if case.uncertainty_ok_for:
        for topic in case.uncertainty_ok_for:
            if _contains(answer_lc, topic):
                # answer mentioned the topic — it must either be marked
                # [UNCERTAIN:...] or explicitly said "no data".
                marked = "[uncertain" in answer_lc
                said_none = any(
                    p in answer_lc
                    for p in ("не нашёл", "не найдено", "no data",
                              "not found", "недостаточно данных",
                              "нет упоминаний", "нет данных")
                )
                if not (marked or said_none):
                    honesty = False
                    break

    return CaseScore(
        case_id=case.id,
        doc_type=case.doc_type,
        category=case.category,
        endpoint=endpoint,
        fact_recall=fact_recall,
        entity_recall=entity_recall,
        citation_precision=citation_precision,
        hallucination_rate=hallucination_rate,
        uncertainty_honesty=honesty,
        answer_length=len(answer_text or ""),
    )


# ── aggregation ─────────────────────────────────────────────────────


def aggregate_by(
    scores: list[CaseScore], key: str,
) -> dict[str, dict[str, float]]:
    """Group `scores` by attribute (`doc_type`, `category`, `endpoint`)
    and return averaged metrics per bucket."""
    buckets: dict[str, list[CaseScore]] = {}
    for s in scores:
        buckets.setdefault(getattr(s, key), []).append(s)
    out: dict[str, dict[str, float]] = {}
    for k, group in buckets.items():
        n = len(group)
        out[k] = {
            "n_cases": n,
            "fact_recall": round(
                sum(s.fact_recall for s in group) / n, 4,
            ),
            "entity_recall": round(
                sum(s.entity_recall for s in group) / n, 4,
            ),
            "citation_precision": round(
                sum(s.citation_precision for s in group) / n, 4,
            ),
            "hallucination_rate": round(
                sum(s.hallucination_rate for s in group) / n, 4,
            ),
            "uncertainty_honesty_pct": round(
                sum(1 for s in group if s.uncertainty_honesty) / n, 4,
            ),
        }
    return out


# ── thresholds (from plan) ──────────────────────────────────────────


THRESHOLDS = {
    "fact_recall": 0.80,
    "entity_recall": 0.85,
    "citation_precision": 0.95,
    "hallucination_rate_max": 0.02,
}


def check_thresholds(by_endpoint_and_doc: dict[str, dict[str, dict[str, float]]]) -> list[str]:
    """`by_endpoint_and_doc[endpoint][doc_type]` → metrics dict.

    Returns list of violations (empty when all pass).  Used by
    `run_answer_eval.py --strict` for CI.
    """
    violations: list[str] = []
    for endpoint, by_dt in by_endpoint_and_doc.items():
        for doc_type, m in by_dt.items():
            tag = f"{endpoint}/{doc_type}"
            if m["entity_recall"] < THRESHOLDS["entity_recall"]:
                violations.append(
                    f"{tag} entity_recall {m['entity_recall']:.2%} < "
                    f"{THRESHOLDS['entity_recall']:.0%}"
                )
            if m["fact_recall"] < THRESHOLDS["fact_recall"]:
                violations.append(
                    f"{tag} fact_recall {m['fact_recall']:.2%} < "
                    f"{THRESHOLDS['fact_recall']:.0%}"
                )
            if m["citation_precision"] < THRESHOLDS["citation_precision"]:
                violations.append(
                    f"{tag} citation_precision {m['citation_precision']:.2%} < "
                    f"{THRESHOLDS['citation_precision']:.0%}"
                )
            if m["hallucination_rate"] > THRESHOLDS["hallucination_rate_max"]:
                violations.append(
                    f"{tag} hallucination_rate {m['hallucination_rate']:.2%} > "
                    f"{THRESHOLDS['hallucination_rate_max']:.0%}"
                )
    return violations


__all__ = [
    "THRESHOLDS",
    "CaseScore",
    "GoldenCase",
    "aggregate_by",
    "check_thresholds",
    "load_golden_cases",
    "score_case",
]
