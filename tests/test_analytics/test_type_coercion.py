from src.analytics.ids import coerce_entity_type
from src.analytics.planner import parse_plan
from src.analytics.primitives import aggregations  # noqa: F401 — registers primitives


def test_coerce_valid_type_canonicalizes():
    assert coerce_entity_type("organization") == "Organization"
    assert coerce_entity_type("Person") == "Person"


def test_coerce_invalid_type_to_none():
    assert coerce_entity_type("entity") is None  # the LLM's bogus value
    assert coerce_entity_type("сущность") is None
    assert coerce_entity_type("") is None
    assert coerce_entity_type(None) is None


def test_parse_plan_strips_bogus_type():
    # planner emitted type="entity" — must be coerced to None so the
    # primitive filters on all types instead of matching zero.
    raw = '{"route":"catalog","steps":[{"primitive":"top_entities_by_mentions","params":{"type":"entity","top_n":10}}],"reason":"x"}'
    plan = parse_plan(raw, max_steps=3)
    assert len(plan.steps) == 1
    assert plan.steps[0].params.get("type") is None
    assert plan.steps[0].params.get("top_n") == 10


def test_parse_plan_keeps_valid_type():
    raw = '{"route":"catalog","steps":[{"primitive":"top_entities_by_mentions","params":{"type":"Organization"}}],"reason":"x"}'
    plan = parse_plan(raw, max_steps=3)
    assert plan.steps[0].params.get("type") == "Organization"
