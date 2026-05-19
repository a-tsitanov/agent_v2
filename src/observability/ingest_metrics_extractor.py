"""Pure-functional extractor that turns Temporal workflow history
into ``MetricRow`` rows suitable for the ``ingest_metrics`` table.

Strategy:
  * walk ``WorkflowHistory.events`` once
  * remember each ``ACTIVITY_TASK_SCHEDULED`` event by id, keeping
    the activity name and the attempt number that the Temporal
    server assigned (retries get fresh scheduled events with
    higher ``attempt``)
  * remember each ``ACTIVITY_TASK_STARTED`` event's time keyed by
    the ``scheduled_event_id`` it references
  * when ``ACTIVITY_TASK_COMPLETED`` / ``_FAILED`` / ``_TIMED_OUT`` /
    ``_CANCELED`` lands, look up its ``scheduled_event_id`` and
    emit a ``MetricRow`` with ``duration = completed_time -
    started_time``

The function is pure — no Temporal client or Postgres calls.  The
``finalize`` activity (Stage 5) wraps it with the actual ``fetch_history``
and ``insert_metrics`` IO.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone

from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowHistory

from src.observability.role_map import ACTIVITY_TO_ROLE
from src.storage.ingest_metrics import MetricRow


_TERMINAL_TYPES = {
    EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
    EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
    EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
    EventType.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
}


def parse_activity_timings(
    history: WorkflowHistory,
    *,
    doc_id: str,
    workflow_id: str,
    workflow_run_id: str,
    version_tag: str,
    model: str,
    env: str,
    models_per_role: dict[str, str] | None = None,
) -> list[MetricRow]:
    """Return one ``MetricRow`` per (activity, attempt) found in the
    given workflow history.

    ``models_per_role`` is a snapshot of
    ``{"extraction": "...", "judge": "...", "search": "..."}``
    captured at submit time.  For each activity, the per-row ``model``
    is resolved via ``ACTIVITY_TO_ROLE[name]`` → ``models_per_role[role]``
    with fallback to the ``model`` argument; non-LLM activities
    (``role=None``) write ``model=NULL`` (honest: nothing
    model-specific happened).

    Activities still in flight at the time of the call (no terminal
    event yet) are skipped.  Retries appear as separate rows with
    monotonically increasing ``attempt`` because Temporal emits a
    new ``ACTIVITY_TASK_SCHEDULED`` event per retry.
    """
    models_per_role = models_per_role or {}
    # event_id of the SCHEDULED event → activity name
    scheduled: dict[int, str] = {}
    # scheduled_event_id → (started_at, attempt)  — attempt only lives
    # on the STARTED event per the Temporal protobuf schema
    started: dict[int, tuple[datetime, int]] = {}
    rows: list[MetricRow] = []

    for ev in history.events:
        et = ev.event_type
        if et == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            attrs = ev.activity_task_scheduled_event_attributes
            scheduled[ev.event_id] = attrs.activity_type.name
        elif et == EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED:
            attrs = ev.activity_task_started_event_attributes
            started[attrs.scheduled_event_id] = (
                ev.event_time.ToDatetime(tzinfo=timezone.utc),
                attrs.attempt or 1,
            )
        elif et in _TERMINAL_TYPES:
            attrs = _terminal_attrs(ev, et)
            sched_id = attrs.scheduled_event_id
            if sched_id not in scheduled or sched_id not in started:
                # Event missing its pair — shouldn't happen on a
                # well-formed history; skip rather than crash.
                continue
            name = scheduled[sched_id]
            started_at, attempt = started[sched_id]
            completed_at = ev.event_time.ToDatetime(tzinfo=timezone.utc)
            duration_ms = max(
                0, int((completed_at - started_at).total_seconds() * 1000),
            )
            # Resolve the per-row model: lookup the role this
            # activity uses, then pull the snapshotted model for that
            # role; fall back to the default ``model`` argument when
            # the per-role snapshot is empty; emit NULL for non-LLM
            # activities (role=None — fetch, embed, regex, etc.).
            role = ACTIVITY_TO_ROLE.get(name)
            if role is None:
                row_model: str | None = None
            else:
                row_model = (
                    models_per_role.get(role) or model or None
                )

            rows.append(MetricRow(
                doc_id=doc_id,
                workflow_id=workflow_id,
                workflow_run_id=workflow_run_id,
                activity_name=name,
                attempt=attempt,
                duration_ms=duration_ms,
                started_at=started_at,
                completed_at=completed_at,
                version_tag=version_tag or None,
                model=row_model,
                env=env or None,
            ))

    return rows


def _terminal_attrs(event, event_type: int):
    """Resolve the right oneof attributes block for a terminal event."""
    if event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
        return event.activity_task_completed_event_attributes
    if event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED:
        return event.activity_task_failed_event_attributes
    if event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
        return event.activity_task_timed_out_event_attributes
    return event.activity_task_canceled_event_attributes
