"""Tests that the three materialize activities appear in GRAPH_BUILD_ACTIVITIES."""

from src.workflow.analytics.materialize_activities import (
    materialize_centrality,
    materialize_link_prediction,
    materialize_risk,
)
from src.workflow.search.activities import GRAPH_BUILD_ACTIVITIES


def test_materialize_activities_registered():
    for a in (materialize_centrality, materialize_link_prediction, materialize_risk):
        assert a in GRAPH_BUILD_ACTIVITIES
