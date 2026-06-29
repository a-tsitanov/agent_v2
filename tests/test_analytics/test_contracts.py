import pytest
from pydantic import ValidationError

from src.analytics.contracts import (
    AnalysisPlan,
    AnalyticsOutcome,
    AnalyzeParams,
    PrimitiveCall,
    Provenance,
    StepResult,
)


def test_plan_defaults_and_frozen():
    plan = AnalysisPlan(
        steps=[PrimitiveCall(primitive="count_entities", params={"type": "Organization"})]
    )
    assert plan.route == "catalog"
    assert plan.steps[0].primitive == "count_entities"
    with pytest.raises(ValidationError):
        plan.route = "cypher"  # frozen


def test_outcome_roundtrips_provenance():
    sr = StepResult(
        primitive="count_entities",
        params={},
        cypher="MATCH ...",
        rows=[{"n": 3}],
        row_count=1,
    )
    prov = Provenance(route="catalog", plan_reason="r", steps=[sr], elapsed_ms=12)
    out = AnalyticsOutcome(query="q", answer="a", provenance=prov, latency_ms=20)
    assert out.provenance.steps[0].rows == [{"n": 3}]


def test_analyze_params_defaults():
    p = AnalyzeParams(query="q")
    assert p.top_n == 20 and p.date_from_epoch is None
