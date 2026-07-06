"""Unit tests for `src/observability/trace.py`."""

from __future__ import annotations

import asyncio

import pytest

from src.observability.trace import (
    get_current_trace,
    record_event,
    record_timed,
    trace_request,
)


def test_record_event_outside_context_is_noop() -> None:
    """Without an active trace, record_event silently no-ops."""
    assert get_current_trace() is None
    record_event("orphan_event", payload={"foo": "bar"})
    # No exception, no trace allocated.
    assert get_current_trace() is None


def test_trace_collects_events_and_summary() -> None:
    with trace_request("agent", "Q") as trace:
        record_event("tool_call", payload={"tool_name": "vector_search"})
        record_event("tool_call", payload={"tool_name": "vector_search"})
        record_event("tool_call", payload={"tool_name": "graph_search"})
        record_event("llm_call", payload={"kind": "reasoning"})
        record_event("synthesize", payload={"n_sources": 5})

    assert trace.endpoint == "agent"
    assert trace.query == "Q"
    assert len(trace.events) == 5

    summary = trace.summary()
    assert summary["n_tool_calls"] == 3
    assert summary["n_llm_calls"] == 1
    assert summary["tool_breakdown"] == {
        "vector_search": 2, "graph_search": 1,
    }


def test_trace_context_unbinds_after_exit() -> None:
    with trace_request("search", "Q"):
        assert get_current_trace() is not None
    assert get_current_trace() is None


def test_record_timed_measures_block_duration() -> None:
    import time as _time

    with trace_request("agent", "Q") as trace, record_timed("tool_call", tool_name="x"):
        _time.sleep(0.01)  # 10ms — well above timer noise floor

    assert len(trace.events) == 1
    e = trace.events[0]
    assert e.name == "tool_call"
    assert e.duration_ms >= 9.0  # allow some slack


def test_export_serializable() -> None:
    """Trace.export() must produce a plain dict that survives json.dumps."""
    import json

    with trace_request("selfrag", "Q") as trace:
        record_event("tool_call", payload={"tool_name": "vector_search"})
        record_event("refinement_round", payload={"round": 0, "needs": 2})

    payload = trace.export()
    serialized = json.dumps(payload)
    assert "vector_search" in serialized
    assert payload["summary"]["n_tool_calls"] == 1
    assert payload["summary"]["n_refinements"] == 1


@pytest.mark.asyncio
async def test_async_context_propagates_to_inner_task() -> None:
    """ContextVar trace is inherited by nested async functions —
    so `tool_call` events fired by react_agent's inner tools land
    in the right trace."""
    async def inner() -> None:
        record_event("nested_event", payload={"ok": True})

    with trace_request("agent", "Q") as trace:
        await inner()

    assert len(trace.events) == 1
    assert trace.events[0].name == "nested_event"


@pytest.mark.asyncio
async def test_concurrent_traces_isolated() -> None:
    """Two concurrent `trace_request` blocks don't bleed into
    each other.  Tested by running two tasks in parallel."""

    async def worker(label: str, sleep_s: float) -> tuple[str, int]:
        with trace_request(label, "Q") as trace:
            record_event("a")
            await asyncio.sleep(sleep_s)
            record_event("b")
            return label, len(trace.events)

    results = await asyncio.gather(
        worker("agent", 0.01),
        worker("selfrag", 0.02),
    )
    for _, n in results:
        assert n == 2
