"""Tests for materialize_activities (centrality + link prediction + risk)."""

from __future__ import annotations

import json

import pytest

from src.analytics.contracts import CentralityIn, LinkPredictionIn, RiskIn
from src.workflow.analytics import materialize_activities as ma


class _Store:
    def __init__(self):
        self.calls = []

    def structured_query(self, cypher, param_map=None):
        self.calls.append(cypher)
        # GDS stream → 1 row; everything else → []
        return [{"name": "A", "score": 0.5}] if "gds." in cypher and "stream" in cypher else []


@pytest.mark.asyncio
async def test_materialize_centrality_runs_each_metric(monkeypatch):
    monkeypatch.setattr(ma, "_get_store", lambda: _Store())
    res = await ma.materialize_centrality(CentralityIn(metrics=["pagerank", "betweenness"]))
    assert res.written == 2 and res.error == ""  # 1 row each


@pytest.mark.asyncio
async def test_materialize_centrality_failsoft(monkeypatch):
    def _boom():
        raise RuntimeError("neo4j down")

    monkeypatch.setattr(ma, "_get_store", _boom)
    res = await ma.materialize_centrality(CentralityIn())
    assert res.written == 0 and "neo4j down" in res.error


@pytest.mark.asyncio
async def test_materialize_centrality_none_projection(monkeypatch):
    """If _with_projection returns None (projection failed), written should be 0."""
    monkeypatch.setattr(ma, "_get_store", lambda: None)
    res = await ma.materialize_centrality(CentralityIn(metrics=["pagerank"]))
    assert res.written == 0 and res.error == ""


@pytest.mark.asyncio
async def test_materialize_link_prediction_runs(monkeypatch):
    monkeypatch.setattr(ma, "_get_store", lambda: _Store())
    res = await ma.materialize_link_prediction(LinkPredictionIn())
    # _Store returns 1 stream row → but min_score filter may reduce to 0; just check no error
    assert res.error == ""


@pytest.mark.asyncio
async def test_materialize_link_prediction_failsoft(monkeypatch):
    def _boom():
        raise RuntimeError("link pred down")

    monkeypatch.setattr(ma, "_get_store", _boom)
    res = await ma.materialize_link_prediction(LinkPredictionIn())
    assert res.written == 0 and "link pred down" in res.error


# ---------------------------------------------------------------------------
# Task 6: materialize_risk
# ---------------------------------------------------------------------------


class _RiskStore:
    def __init__(self, rows):
        self._rows, self.writes = rows, []

    def structured_query(self, cypher, param_map=None):
        if cypher.strip().startswith("UNWIND"):  # write-back
            self.writes.append((cypher, param_map))
            return []
        return self._rows  # the component-gather read


@pytest.mark.asyncio
async def test_materialize_risk_writes_scores(monkeypatch):
    rows = [
        {
            "name": "Shell",
            "betweenness": 1.0,
            "id_links": 3,
            "deg": 3,
            "contested": 2,
            "recent": 4,
        }
    ]
    store = _RiskStore(rows)
    monkeypatch.setattr(ma, "_get_store", lambda: store)
    res = await ma.materialize_risk(RiskIn())
    assert res.written == 1 and res.error == ""
    write_cypher = store.writes[0][0]
    assert "SET e.risk_score" in write_cypher and "e.risk_band" in write_cypher
    rowarg = store.writes[0][1]["rows"][0]
    assert 0.0 <= rowarg["score"] <= 1.0 and rowarg["band"] in {"low", "medium", "high"}
    json.loads(rowarg["components"])  # components serialized as JSON


@pytest.mark.asyncio
async def test_materialize_risk_empty_returns_zero(monkeypatch):
    store = _RiskStore([])
    monkeypatch.setattr(ma, "_get_store", lambda: store)
    res = await ma.materialize_risk(RiskIn())
    assert res.written == 0 and res.error == ""


@pytest.mark.asyncio
async def test_materialize_risk_failsoft(monkeypatch):
    def _boom():
        raise RuntimeError("risk db down")

    monkeypatch.setattr(ma, "_get_store", _boom)
    res = await ma.materialize_risk(RiskIn())
    assert res.written == 0 and "risk db down" in res.error
