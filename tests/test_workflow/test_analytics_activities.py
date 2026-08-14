"""Tests for src/workflow/analytics/activities.py (Task 12)."""

from __future__ import annotations

import pytest

from src.analytics.contracts import ExecInput, PrimitiveCall, StepResult, SynthInput
from src.analytics.primitives import (
    aggregations,  # noqa: F401 — register count_entities et al.
    dynamics,  # noqa: F401 — register topic_trend
)
from src.workflow.analytics import activities as act


class _Store:
    """Minimal graph-store stub: structured_query returns one row."""

    def structured_query(self, cypher: str, param_map: dict | None = None) -> list[dict]:
        return [{"n": 5}]


# ---------------------------------------------------------------------------
# execute_step
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_step_runs_primitive(monkeypatch):
    monkeypatch.setattr(act, "build_graph_store", lambda: _Store())
    p = ExecInput(
        call=PrimitiveCall(primitive="count_entities", params={"type": "Organization"}),
        top_n=20,
        date_from_epoch=None,
        date_to_epoch=None,
    )
    sr = await act.execute_step(p)
    assert sr.primitive == "count_entities"
    assert sr.row_count == 1 and sr.rows == [{"n": 5}]


@pytest.mark.asyncio
async def test_execute_step_unknown_primitive_returns_empty(monkeypatch):
    monkeypatch.setattr(act, "build_graph_store", lambda: _Store())
    p = ExecInput(
        call=PrimitiveCall(primitive="nope", params={}),
        top_n=20,
        date_from_epoch=None,
        date_to_epoch=None,
    )
    sr = await act.execute_step(p)
    assert sr.rows == [] and sr.row_count == 0


class _MentionsStore:
    """Minimal graph-store stub shaped like the real top_entities_by_mentions
    graph-op output ({name, mentions} — no type/label key), so the row
    survives the is_meaningful_entity gate applied in the primitive."""

    def structured_query(self, cypher: str, param_map: dict | None = None) -> list[dict]:
        return [{"name": "Acme Corp", "mentions": 5}]


@pytest.mark.asyncio
async def test_execute_step_injects_top_n_when_omitted(monkeypatch):
    """When the primitive param_model has top_n and caller omits it, default is injected."""
    monkeypatch.setattr(act, "build_graph_store", lambda: _MentionsStore())
    # top_entities_by_mentions has a top_n field; we omit it from params
    p = ExecInput(
        call=PrimitiveCall(
            primitive="top_entities_by_mentions",
            params={"type": "Organization"},
        ),
        top_n=5,
        date_from_epoch=None,
        date_to_epoch=None,
    )
    sr = await act.execute_step(p)
    assert sr.primitive == "top_entities_by_mentions"
    # top_n=5 was injected — store still returns one row
    assert sr.row_count == 1


class _NebulaLikeStore:
    """Mimics NebulaGraphStore.structured_query: raises NotImplementedError on
    any non-empty param_map instead of running the query — a structural
    limitation, not a transient failure."""

    def structured_query(self, cypher: str, param_map: dict | None = None) -> list[dict]:
        if param_map:
            raise NotImplementedError(
                "NebulaGraphStore.structured_query does not bind nGQL params yet "
                f"(Phase 2); got param_map keys: {sorted(param_map)}"
            )
        return []


@pytest.mark.asyncio
async def test_execute_step_reports_structural_backend_limitation(monkeypatch):
    """topic_trend on a nebula-like backend must report the limitation, not
    a silent empty result indistinguishable from 'no trend'."""
    monkeypatch.setattr(act, "build_graph_store", lambda: _NebulaLikeStore())
    p = ExecInput(
        call=PrimitiveCall(primitive="topic_trend", params={"topic": "x"}),
        top_n=20,
        date_from_epoch=None,
        date_to_epoch=None,
    )
    sr = await act.execute_step(p)
    assert sr.rows == [] and sr.row_count == 0
    assert sr.error != ""
    assert sr.primitive == "topic_trend"
    assert sr.params == {"topic": "x"}


@pytest.mark.asyncio
async def test_execute_step_logs_error_on_structural_backend_limitation(monkeypatch):
    """The per-request `error` field on StepResult answers "did this fire on my
    request". Operators diagnosing a recurring backend limitation across a week
    of traffic need a grep-able log line — this must not be silent."""
    monkeypatch.setattr(act, "build_graph_store", lambda: _NebulaLikeStore())
    errors: list[str] = []
    monkeypatch.setattr(
        act.activity.logger,
        "error",
        lambda msg, *args, **kw: errors.append(msg % args if args else msg),
    )
    p = ExecInput(
        call=PrimitiveCall(primitive="topic_trend", params={"topic": "x"}),
        top_n=20,
        date_from_epoch=None,
        date_to_epoch=None,
    )
    await act.execute_step(p)
    assert len(errors) == 1
    assert "topic_trend" in errors[0]


@pytest.mark.asyncio
async def test_execute_step_error_is_short_and_jargon_free(monkeypatch):
    """StepResult.error feeds the synthesis prompt (and, from there, can reach
    the user-facing answer) — it must not contain internal implementation
    detail (nGQL, param_map, Phase 2). The full exception text still has to go
    somewhere for operators: error_detail."""
    monkeypatch.setattr(act, "build_graph_store", lambda: _NebulaLikeStore())
    monkeypatch.setattr(act.activity.logger, "error", lambda *a, **kw: None)
    p = ExecInput(
        call=PrimitiveCall(primitive="topic_trend", params={"topic": "x"}),
        top_n=20,
        date_from_epoch=None,
        date_to_epoch=None,
    )
    sr = await act.execute_step(p)
    assert sr.error != ""
    for jargon in ("nGQL", "param_map", "Phase 2", "NotImplementedError"):
        assert jargon not in sr.error
    assert "Phase 2" in sr.error_detail
    assert "param_map" in sr.error_detail


@pytest.mark.asyncio
async def test_execute_step_type_error_stays_fail_soft_without_error(monkeypatch):
    """A planner mistake (bad kwarg) is fail-soft with StepResult.error left
    empty — it is not a backend failure."""
    monkeypatch.setattr(act, "build_graph_store", lambda: _Store())
    p = ExecInput(
        call=PrimitiveCall(primitive="count_entities", params={"bogus_kwarg": "x"}),
        top_n=20,
        date_from_epoch=None,
        date_to_epoch=None,
    )
    sr = await act.execute_step(p)
    assert sr.rows == [] and sr.row_count == 0
    assert sr.error == ""


# ---------------------------------------------------------------------------
# synthesize_analytical
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_synthesize_analytical_all_failed_reports_uncomputable():
    """All steps failed structurally (empty rows, non-empty error) — must not
    claim a computed answer, same guard as the plain all-empty case."""
    steps = [
        StepResult(
            primitive="topic_trend",
            params={"topic": "x"},
            rows=[],
            row_count=0,
            error="backend cannot run this query",
        ),
    ]
    res = await act.synthesize_analytical(SynthInput(query="trend for x?", steps=steps))
    assert "не удалось вычислить" in res.text.lower()


# ---------------------------------------------------------------------------
# Module-level lists
# ---------------------------------------------------------------------------


def test_activity_lists_contain_expected_functions():
    assert act.analytical_plan in act.ANALYTICS_ACTIVITIES
    assert act.execute_step in act.ANALYTICS_ACTIVITIES
    assert act.synthesize_analytical in act.ANALYTICS_LARGE_ACTIVITIES
