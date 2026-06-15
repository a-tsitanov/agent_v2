"""Classifier eval scaffold (Track 2).

A small labelled keep/skip set + a precision/recall reporter over the
DETERMINISTIC rule layer (no LLM, so it runs offline / in CI).  Extend
`SCENARIOS` with real samples; the LLM layer is benchmarked separately
once a model is wired.  Optimise for HIGH RECALL on "keep" — a false
skip silently loses a good document.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.ingestion.classifier import apply_rules

_MAX_MB = 25.0
_MIN_BYTES = 1
_SKIP_EXT = ["exe", "dll", "bin", "zip", "png", "jpg", "mp4"]


@dataclass(frozen=True)
class Case:
    filename: str
    size_bytes: int
    keep: bool  # ground truth: should this be ingested?


SCENARIOS: list[Case] = [
    Case("contract_2026.pdf", 48_000, keep=True),
    Case("meeting_notes.docx", 12_000, keep=True),
    Case("Makefile", 800, keep=True),
    Case("invoice.txt", 3_500, keep=True),
    Case("photo.png", 2_400_000, keep=False),
    Case("archive.zip", 5_000_000, keep=False),
    Case("empty.txt", 0, keep=False),
    Case("huge_dump.txt", 40 * 1024 * 1024, keep=False),
]


def _predict_keep(case: Case) -> bool:
    v = apply_rules(
        case.filename, case.size_bytes,
        max_size_mb=_MAX_MB, min_size_bytes=_MIN_BYTES, skip_extensions=_SKIP_EXT,
    )
    return not v.skip


def evaluate(scenarios: list[Case] = SCENARIOS) -> dict:
    """Rule-layer precision/recall on the 'keep' class."""
    tp = fp = fn = tn = 0
    for c in scenarios:
        pred = _predict_keep(c)
        if c.keep and pred:
            tp += 1
        elif c.keep and not pred:
            fn += 1  # FALSE SKIP — the costly error
        elif not c.keep and pred:
            fp += 1
        else:
            tn += 1
    precision = tp / (tp + fp) if (tp + fp) else 1.0
    recall = tp / (tp + fn) if (tp + fn) else 1.0
    return {"precision": precision, "recall": recall, "false_skips": fn}


def test_rule_layer_never_false_skips_the_eval_set() -> None:
    """Guardrail: the deterministic rules must not drop any 'keep'
    document in the eval set (recall == 1.0 on keep)."""
    report = evaluate()
    assert report["false_skips"] == 0, report
    assert report["recall"] == 1.0, report


if __name__ == "__main__":
    print(evaluate())
