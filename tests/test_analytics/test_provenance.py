from src.analytics.catalog import PrimitiveResult
from src.analytics.contracts import AnalysisPlan, PrimitiveCall
from src.analytics.provenance import assemble_provenance, step_from_primitive


def test_step_from_primitive_counts_rows():
    call = PrimitiveCall(primitive="count_entities", params={"type": "Organization"})
    pr = PrimitiveResult(
        cypher="MATCH ...",
        params={"type": "Organization"},
        rows=[{"n": 3}],
        source_chunks=["c1"],
        truncated=False,
    )
    sr = step_from_primitive(call, pr)
    assert sr.primitive == "count_entities" and sr.row_count == 1
    assert sr.cypher == "MATCH ..." and sr.source_chunks == ["c1"]


def test_assemble_provenance_carries_plan_meta():
    plan = AnalysisPlan(route="catalog", steps=[], reason="why")
    prov = assemble_provenance(plan, steps=[], elapsed_ms=42)
    assert prov.route == "catalog" and prov.plan_reason == "why" and prov.elapsed_ms == 42


def test_step_from_primitive_default_error_is_empty():
    call = PrimitiveCall(primitive="count_entities", params={})
    pr = PrimitiveResult(cypher="MATCH ...", params={}, rows=[{"n": 1}])
    sr = step_from_primitive(call, pr)
    assert sr.error == ""


def test_step_from_primitive_passes_error_through():
    call = PrimitiveCall(primitive="topic_trend", params={"topic": "x"})
    pr = PrimitiveResult(cypher="", params={"topic": "x"}, rows=[])
    sr = step_from_primitive(call, pr, error="backend cannot run this query")
    assert sr.error == "backend cannot run this query"
    assert sr.rows == [] and sr.row_count == 0
