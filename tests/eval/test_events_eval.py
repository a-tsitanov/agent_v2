"""Unit tests for the events extraction-quality harness (deterministic stub —
no LLM). The live-LLM factory is integration and not exercised here."""

from __future__ import annotations

import json

from tests.eval.events_eval import (
    GOLDEN_DIR_DEFAULT,
    EventStats,
    _ts_epoch,
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


def test_ts_epoch_prefers_carried_event_start_epoch():
    # LIVE path: event_ts is a raw year-less phrase that would resolve to None
    # without a document anchor, but the node already carries the ingestion's
    # anchor-resolved epoch — that must win over re-resolving from event_ts.
    carried = {"event_ts": "6 июля", "event_start_epoch": 12345}
    assert _ts_epoch(carried) == 12345

    # Even when event_ts alone WOULD resolve (absolute ISO), a carried epoch
    # still takes priority — it's the source of truth for the live path.
    carried_overrides_resolvable = {"event_ts": "2024-03-01", "event_start_epoch": 999}
    assert _ts_epoch(carried_overrides_resolvable) == 999


def test_ts_epoch_falls_back_to_resolving_golden_iso_without_carried_epoch():
    # GOLDEN path: no event_start_epoch on the dict at all — resolve the
    # absolute ISO event_ts directly, no anchor needed.
    golden = {"event_type": "deal", "participants": ["A"], "event_ts": "2024-03-01"}
    epoch = _ts_epoch(golden)
    assert epoch is not None and isinstance(epoch, int)

    # A None event_start_epoch (e.g. carried from a predicted dict where the
    # node had no resolved epoch) must also fall back, not short-circuit.
    predicted_unresolved_on_node = {"event_ts": "2024-03-01", "event_start_epoch": None}
    assert _ts_epoch(predicted_unresolved_on_node) == epoch
