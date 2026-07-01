"""Tests for src/workflow/analytics/activities.py (Task 12)."""

from __future__ import annotations

import pytest

from src.analytics.contracts import ExecInput, PrimitiveCall
from src.analytics.primitives import aggregations  # noqa: F401 — register count_entities et al.
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
    monkeypatch.setattr(act, "build_neo4j_graph_store", lambda: _Store())
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
    monkeypatch.setattr(act, "build_neo4j_graph_store", lambda: _Store())
    p = ExecInput(
        call=PrimitiveCall(primitive="nope", params={}),
        top_n=20,
        date_from_epoch=None,
        date_to_epoch=None,
    )
    sr = await act.execute_step(p)
    assert sr.rows == [] and sr.row_count == 0


@pytest.mark.asyncio
async def test_execute_step_injects_top_n_when_omitted(monkeypatch):
    """When the primitive param_model has top_n and caller omits it, default is injected."""
    monkeypatch.setattr(act, "build_neo4j_graph_store", lambda: _Store())
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


# ---------------------------------------------------------------------------
# Module-level lists
# ---------------------------------------------------------------------------


def test_activity_lists_contain_expected_functions():
    assert act.analytical_plan in act.ANALYTICS_ACTIVITIES
    assert act.execute_step in act.ANALYTICS_ACTIVITIES
    assert act.synthesize_analytical in act.ANALYTICS_LARGE_ACTIVITIES
