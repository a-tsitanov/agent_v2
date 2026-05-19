"""Unit tests for parse_activity_timings.

We synthesize WorkflowHistory protobuf events directly so the test
doesn't need a running Temporal cluster.  The parser is the only
piece of metrics-extraction logic that's worth unit-testing: the
fetch + persist halves wrap async IO already covered by their
integration suites.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from temporalio.api.enums.v1 import EventType
from temporalio.api.history.v1 import (
    ActivityTaskCompletedEventAttributes,
    ActivityTaskFailedEventAttributes,
    ActivityTaskScheduledEventAttributes,
    ActivityTaskStartedEventAttributes,
    HistoryEvent,
)
from temporalio.api.common.v1 import ActivityType
from temporalio.client import WorkflowHistory
from google.protobuf.timestamp_pb2 import Timestamp

from src.observability.ingest_metrics_extractor import parse_activity_timings


def _ts(dt: datetime) -> Timestamp:
    t = Timestamp()
    t.FromDatetime(dt)
    return t


_BASE = datetime(2026, 5, 19, 10, 0, 0, tzinfo=timezone.utc)


def _scheduled(eid: int, name: str, when: datetime) -> HistoryEvent:
    return HistoryEvent(
        event_id=eid,
        event_time=_ts(when),
        event_type=EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
        activity_task_scheduled_event_attributes=(
            ActivityTaskScheduledEventAttributes(
                activity_id=str(eid),
                activity_type=ActivityType(name=name),
            )
        ),
    )


def _started(eid: int, sched_id: int, attempt: int, when: datetime) -> HistoryEvent:
    return HistoryEvent(
        event_id=eid,
        event_time=_ts(when),
        event_type=EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED,
        activity_task_started_event_attributes=(
            ActivityTaskStartedEventAttributes(
                scheduled_event_id=sched_id, attempt=attempt,
            )
        ),
    )


def _completed(eid: int, sched_id: int, when: datetime) -> HistoryEvent:
    return HistoryEvent(
        event_id=eid,
        event_time=_ts(when),
        event_type=EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
        activity_task_completed_event_attributes=(
            ActivityTaskCompletedEventAttributes(scheduled_event_id=sched_id)
        ),
    )


def _failed(eid: int, sched_id: int, when: datetime) -> HistoryEvent:
    return HistoryEvent(
        event_id=eid,
        event_time=_ts(when),
        event_type=EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
        activity_task_failed_event_attributes=(
            ActivityTaskFailedEventAttributes(scheduled_event_id=sched_id)
        ),
    )


def _hist(*events: HistoryEvent) -> WorkflowHistory:
    return WorkflowHistory(workflow_id="ingest-x", events=list(events))


_LABELS = dict(
    doc_id="11111111-2222-3333-4444-555555555555",
    workflow_id="ingest-x",
    workflow_run_id="run-yyy",
    version_tag="t-abc",
    model="qwen3:8b",
    env="dev-local",
)


def test_empty_history_yields_no_rows():
    rows = parse_activity_timings(_hist(), **_LABELS)
    assert rows == []


def test_single_activity_one_row_with_correct_duration():
    started = _BASE + timedelta(seconds=10)
    finished = started + timedelta(milliseconds=2_500)
    h = _hist(
        _scheduled(5, "fetch_source", _BASE),
        _started(6, 5, 1, started),
        _completed(7, 5, finished),
    )
    rows = parse_activity_timings(h, **_LABELS)
    assert len(rows) == 1
    r = rows[0]
    assert r.activity_name == "fetch_source"
    assert r.attempt == 1
    assert r.duration_ms == 2500
    assert r.started_at == started
    assert r.completed_at == finished
    assert r.version_tag == "t-abc"
    assert r.model == "qwen3:8b"
    assert r.env == "dev-local"


def test_failed_terminal_is_still_recorded():
    h = _hist(
        _scheduled(5, "extract_kg", _BASE),
        _started(6, 5, 1, _BASE + timedelta(seconds=1)),
        _failed(7, 5, _BASE + timedelta(seconds=4)),
    )
    rows = parse_activity_timings(h, **_LABELS)
    assert len(rows) == 1
    assert rows[0].activity_name == "extract_kg"
    assert rows[0].duration_ms == 3000


def test_retry_produces_two_rows_with_separate_attempts():
    """Temporal emits a fresh SCHEDULED+STARTED pair per retry — both
    attempts should land as separate rows with monotone attempt nums."""
    h = _hist(
        _scheduled(5, "parse_and_chunk", _BASE),
        _started(6, 5, 1, _BASE + timedelta(seconds=1)),
        _failed(7, 5, _BASE + timedelta(seconds=2)),
        _scheduled(8, "parse_and_chunk", _BASE + timedelta(seconds=3)),
        _started(9, 8, 2, _BASE + timedelta(seconds=4)),
        _completed(10, 8, _BASE + timedelta(seconds=7)),
    )
    rows = parse_activity_timings(h, **_LABELS)
    assert len(rows) == 2
    a, b = sorted(rows, key=lambda r: r.attempt)
    assert (a.attempt, a.duration_ms) == (1, 1000)
    assert (b.attempt, b.duration_ms) == (2, 3000)
    assert all(r.activity_name == "parse_and_chunk" for r in rows)


def test_in_flight_activity_is_skipped():
    """A SCHEDULED + STARTED without a terminal event = activity still
    running.  Skip rather than fabricate a half-row."""
    h = _hist(
        _scheduled(5, "merge_and_resolve", _BASE),
        _started(6, 5, 1, _BASE + timedelta(seconds=1)),
        # no completed / failed yet
    )
    rows = parse_activity_timings(h, **_LABELS)
    assert rows == []


def test_multiple_activities_in_order():
    h = _hist(
        _scheduled(5, "fetch_source", _BASE),
        _started(6, 5, 1, _BASE + timedelta(seconds=1)),
        _completed(7, 5, _BASE + timedelta(seconds=2)),
        _scheduled(8, "parse_and_chunk", _BASE + timedelta(seconds=2)),
        _started(9, 8, 1, _BASE + timedelta(seconds=3)),
        _completed(10, 8, _BASE + timedelta(seconds=6)),
        _scheduled(11, "index_vector", _BASE + timedelta(seconds=6)),
        _started(12, 11, 1, _BASE + timedelta(seconds=7)),
        _completed(13, 11, _BASE + timedelta(seconds=12)),
    )
    rows = parse_activity_timings(h, **_LABELS)
    names = [r.activity_name for r in rows]
    assert names == ["fetch_source", "parse_and_chunk", "index_vector"]
    assert [r.duration_ms for r in rows] == [1000, 3000, 5000]
