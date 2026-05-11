"""Structured tracing for the search endpoints.

A trace is a list of `TraceEvent` per request, accumulated by a
`TraceCollector`.  Events get logged as structured loguru records
(`extra={"trace": ...}`) and, if the caller chose, retained in
memory for `/api/v1/search/trace/{request_id}` (out-of-scope here,
hook ready).

Why a collector and not just `logger.bind`:

  * a ReAct loop emits 5-15 events per request; correlating them
    in flat logs is annoying.
  * answer-quality eval wants the per-step record per query
    structured (tool count, total LLM calls) — the eval script
    consumes `TraceCollector.export()`.

This module is intentionally thin — no OTLP exporter, no Jaeger.
That's a deployment decision (next iteration).  Today we want
the *signal* visible.
"""

from __future__ import annotations

import contextlib
import time
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

from loguru import logger


@dataclass
class TraceEvent:
    """One structured event in the request trace."""

    name: str
    """Short event label — `agent_step`, `synthesize`, `retrieve`,
    `refinement_round`, ..."""

    payload: dict[str, Any] = field(default_factory=dict)
    """Free-form attributes.  By convention: numeric metrics go in
    `payload['metrics']`, free-text fields elsewhere."""

    duration_ms: float = 0.0
    """Wall time for this event; 0 if the caller didn't measure."""

    ts_offset_ms: float = 0.0
    """Time since trace start, ms.  Useful for waterfall views."""


@dataclass
class Trace:
    """Per-request bundle of events."""

    request_id: str
    endpoint: str  # "search" | "agent" | "selfrag"
    query: str
    started_at: float = field(default_factory=time.monotonic)
    events: list[TraceEvent] = field(default_factory=list)

    def export(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "endpoint": self.endpoint,
            "query": self.query,
            "events": [
                {
                    "name": e.name,
                    "payload": e.payload,
                    "duration_ms": round(e.duration_ms, 2),
                    "ts_offset_ms": round(e.ts_offset_ms, 2),
                }
                for e in self.events
            ],
            "summary": self.summary(),
        }

    def summary(self) -> dict[str, Any]:
        """Aggregate metrics — handy for answer-quality eval and
        operational dashboards alike."""
        tool_calls = [e for e in self.events if e.name == "tool_call"]
        llm_calls = [e for e in self.events if e.name == "llm_call"]
        refinements = [e for e in self.events if e.name == "refinement_round"]
        return {
            "n_tool_calls": len(tool_calls),
            "n_llm_calls": len(llm_calls),
            "n_refinements": len(refinements),
            "total_ms": round(
                sum(e.duration_ms for e in self.events), 2,
            ),
            "tool_breakdown": _count_by(tool_calls, "tool_name"),
        }


def _count_by(events: list[TraceEvent], key: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for e in events:
        v = str(e.payload.get(key, ""))
        if not v:
            continue
        out[v] = out.get(v, 0) + 1
    return out


# ── context-var so handlers don't have to plumb a trace object ──────


_current_trace: ContextVar[Trace | None] = ContextVar(
    "kb_llamaindex_trace", default=None,
)


def get_current_trace() -> Trace | None:
    return _current_trace.get()


@contextlib.contextmanager
def trace_request(endpoint: str, query: str, request_id: str | None = None):
    """Bind a fresh Trace to the current context for the duration
    of the `with` block.  Yields the Trace so callers (route handler)
    can attach summary metadata to the response.

    Threads / async tasks spawned inside inherit the trace via
    contextvars (asyncio propagates ContextVar by default).
    """
    rid = request_id or uuid.uuid4().hex[:12]
    trace = Trace(request_id=rid, endpoint=endpoint, query=query)
    token = _current_trace.set(trace)
    try:
        yield trace
    finally:
        _current_trace.reset(token)
        logger.info(
            "trace done  endpoint={ep}  rid={rid}  summary={s}",
            ep=endpoint, rid=rid, s=trace.summary(),
        )


def record_event(
    name: str,
    *,
    payload: dict[str, Any] | None = None,
    duration_ms: float = 0.0,
) -> None:
    """Append an event to the current trace.  Cheap no-op if no
    trace is active (allows the same code to run inside and outside
    `trace_request`)."""
    trace = _current_trace.get()
    if trace is None:
        return
    ts_offset_ms = (time.monotonic() - trace.started_at) * 1000.0
    trace.events.append(TraceEvent(
        name=name,
        payload=payload or {},
        duration_ms=duration_ms,
        ts_offset_ms=ts_offset_ms,
    ))


@contextlib.contextmanager
def record_timed(name: str, **payload: Any):
    """Convenience: time the wrapped block, append one event."""
    t0 = time.monotonic()
    try:
        yield
    finally:
        record_event(
            name,
            payload=payload,
            duration_ms=(time.monotonic() - t0) * 1000.0,
        )


__all__ = [
    "Trace",
    "TraceEvent",
    "get_current_trace",
    "record_event",
    "record_timed",
    "trace_request",
]
