import pytest
from pydantic import ValidationError

from src.analytics.contracts import (
    CentralityIn,
    LinkPredictionIn,
    MaterializeParams,
    MaterializeResult,
    RiskIn,
    StageResult,
)


def test_materialize_defaults_and_frozen():
    p = MaterializeParams()
    assert p.metrics == ["pagerank", "betweenness", "eigenvector"]
    assert p.link_prediction is True and p.risk is True
    with pytest.raises(ValidationError):
        p.risk = False


def test_stage_and_result_defaults():
    assert StageResult().written == 0 and StageResult().error == ""
    r = MaterializeResult(centrality_written=5)
    assert r.links_written == 0 and r.errors == []


def test_centrality_in_defaults():
    c = CentralityIn()
    assert c.metrics == ["pagerank", "betweenness", "eigenvector"]


def test_link_prediction_in_empty():
    lp = LinkPredictionIn()
    assert isinstance(lp, LinkPredictionIn)


def test_risk_in_empty():
    r = RiskIn()
    assert isinstance(r, RiskIn)


def test_materialize_result_all_fields():
    result = MaterializeResult(
        centrality_written=10, links_written=20, risk_written=5, errors=["error1"]
    )
    assert result.centrality_written == 10
    assert result.links_written == 20
    assert result.risk_written == 5
    assert result.errors == ["error1"]
