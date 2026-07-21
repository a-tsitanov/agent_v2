from src.analytics.planner import trend_fallback_steps
from src.analytics.primitives import (
    aggregations,  # noqa: F401 — registers primitives
    connections,  # noqa: F401 — registers entity_dossier
    events,  # noqa: F401 — registers new_events (trend fallback)
)


def test_upominaemye_triggers_fallback():
    steps = trend_fallback_steps("самые упоминаемые сущности за сегодня")
    assert [s.primitive for s in steps] == ["top_entities_by_mentions", "new_events"]


def test_non_trend_no_fallback():
    assert trend_fallback_steps("кто такой Вячеслав Володин") == []
