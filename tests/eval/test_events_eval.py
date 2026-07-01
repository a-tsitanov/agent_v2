"""Unit tests for the events extraction-quality harness (deterministic stub —
no LLM). The live-LLM factory is integration and not exercised here."""

from __future__ import annotations

import json

from tests.eval.events_eval import (
    GOLDEN_DIR_DEFAULT,
    EventStats,
    format_report,
    run_eval,
    score_case,
)


def test_score_case_matches_on_event_key():
    expected = [
        {"event_type": "deal", "participants": ["Romashka", "Lutik"], "event_ts": "2024-03-01"}
    ]
    predicted = [
        # reordered participants, same ts → same event_key → a true positive
        {"event_type": "deal", "participants": ["Lutik", "Romashka"], "event_ts": "2024-03-01"},
        # far ts → different key → a false positive
        {"event_type": "deal", "participants": ["X"], "event_ts": "2024-09-01"},
    ]
    stats: dict[str, EventStats] = {}
    score_case(expected, predicted, stats, bucket_days=7)
    s = stats["deal"]
    assert s.tp == 1 and s.fp == 1 and s.fn == 0
    assert s.precision == 0.5 and s.recall == 1.0


def test_score_case_wrong_type_is_miss():
    expected = [{"event_type": "lawsuit", "participants": ["A"], "event_ts": "2024-01-01"}]
    predicted = [{"event_type": "deal", "participants": ["A"], "event_ts": "2024-01-01"}]
    stats: dict[str, EventStats] = {}
    score_case(expected, predicted, stats, bucket_days=7)
    assert stats["lawsuit"].fn == 1 and stats["lawsuit"].tp == 0
    assert stats["deal"].fp == 1


def test_run_eval_over_stub(tmp_path):
    (tmp_path / "c1.json").write_text(
        json.dumps(
            {
                "text": "irrelevant for the stub",
                "lang": "en",
                "expected": [
                    {"event_type": "meeting", "participants": ["A", "B"], "event_ts": "2024-01-01"}
                ],
            }
        )
    )

    def _stub(text: str) -> list[dict]:
        return [{"event_type": "meeting", "participants": ["B", "A"], "event_ts": "2024-01-01"}]

    per_type, elapsed = run_eval(_stub, tmp_path, bucket_days=7)
    assert per_type["meeting"].tp == 1 and per_type["meeting"].f1 == 1.0
    assert elapsed >= 0.0


def test_shipped_golden_events_load_and_are_valid():
    files = sorted(GOLDEN_DIR_DEFAULT.glob("*.json"))
    assert files, "no golden events cases shipped"
    for f in files:
        case = json.loads(f.read_text())
        assert case.get("text") and isinstance(case.get("expected"), list) and case["expected"]
        for ev in case["expected"]:
            assert "event_type" in ev and "participants" in ev


def test_format_report_returns_table():
    out = format_report({"deal": EventStats(tp=2, fp=1, fn=0)}, 0.12)
    assert "deal" in out and "P" in out and "F1" in out
