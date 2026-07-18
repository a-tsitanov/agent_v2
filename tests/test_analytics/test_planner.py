import pytest

from src.analytics import planner
from src.analytics.primitives import (
    aggregations,  # noqa: F401 — registers primitives
    connections,  # noqa: F401 — registers entity_dossier
    events,  # noqa: F401 — registers new_events (trend fallback)
)
from tests.test_analytics.conftest import _StubLLM


def test_parse_plan_valid_json():
    raw = '{"route":"catalog","steps":[{"primitive":"count_entities","params":{"type":"Organization"}}],"reason":"r"}'
    plan = planner.parse_plan(raw, max_steps=3)
    assert plan.steps[0].primitive == "count_entities"
    assert plan.steps[0].params["type"] == "Organization"


def test_parse_plan_drops_unknown_primitive():
    raw = '{"route":"catalog","steps":[{"primitive":"no_such","params":{}}],"reason":"r"}'
    plan = planner.parse_plan(raw, max_steps=3)
    assert plan.steps == []


def test_parse_plan_tolerates_prose_around_json():
    raw = 'Sure! Here:\n{"route":"catalog","steps":[{"primitive":"count_entities","params":{}}]}\nHope it helps'
    plan = planner.parse_plan(raw, max_steps=3)
    assert plan.steps[0].primitive == "count_entities"


def test_parse_plan_caps_steps():
    raw = '{"steps":[{"primitive":"count_entities"},{"primitive":"distribution_by_type"},{"primitive":"distribution_by_relation_type"},{"primitive":"count_relationships"}]}'
    plan = planner.parse_plan(raw, max_steps=2)
    assert len(plan.steps) == 2


def test_parse_plan_garbage_returns_empty():
    assert planner.parse_plan("not json at all", max_steps=3).steps == []


def test_parse_plan_drops_step_with_bad_params():
    # entity_dossier requires 'name' field — missing → step dropped
    raw = '{"steps":[{"primitive":"entity_dossier","params":{}}]}'
    plan = planner.parse_plan(raw, max_steps=3)
    assert plan.steps == []  # missing required 'name' → dropped


@pytest.mark.asyncio
async def test_plan_query_failopen_on_llm_error():
    plan = await planner.plan_query("q", _StubLLM(raises=True), max_steps=3)
    assert plan.steps == [] and "llm" in plan.reason.lower()


@pytest.mark.asyncio
async def test_plan_query_happy_path():
    llm = _StubLLM(reply='{"steps":[{"primitive":"count_entities","params":{}}],"reason":"ok"}')
    plan = await planner.plan_query("how many entities", llm, max_steps=3)
    assert plan.steps[0].primitive == "count_entities"


def test_parse_plan_malformed_braces_is_graceful():
    # The regex finds a {...} span but it is NOT valid JSON. The fallback
    # json.loads must be guarded — degrade to the not-JSON path with empty
    # steps, never crash to reason="parse error".
    raw = '{"steps": [this is not valid json}'
    plan = planner.parse_plan(raw, max_steps=3)
    assert plan.steps == []
    assert plan.reason == "planner output not JSON"


def test_trend_fallback_steps_detects_intent():
    steps = planner.trend_fallback_steps("что сегодня в трендах")
    names = [s.primitive for s in steps]
    assert "top_entities_by_mentions" in names
    assert "new_events" in names


def test_trend_fallback_steps_ignores_non_trend():
    assert planner.trend_fallback_steps("сколько организаций в базе") == []


@pytest.mark.asyncio
async def test_plan_query_trend_fallback_when_llm_plans_nothing():
    # The real trending failure mode: the LLM returns empty steps ("no matching
    # primitive"). A deterministic fallback routes to backend-working primitives
    # so "что в трендах" still yields data.
    llm = _StubLLM(reply='{"route":"catalog","steps":[],"reason":"no matching primitive"}')
    plan = await planner.plan_query("что в трендах сегодня", llm, max_steps=3)
    names = [s.primitive for s in plan.steps]
    assert "top_entities_by_mentions" in names
    assert "fallback" in plan.reason.lower()
