from __future__ import annotations

from src.analytics.centrality_compute import compute_centrality


def test_compute_pagerank_ranks_hub_highest():
    # star: Hub connected to 3 leaves -> Hub has the highest pagerank
    edges = [("Hub", "L1", 1.0), ("Hub", "L2", 1.0), ("Hub", "L3", 1.0)]
    names = ["Hub", "L1", "L2", "L3"]
    scores = compute_centrality(edges, names, "pagerank")
    assert set(scores) == {"Hub", "L1", "L2", "L3"}
    assert scores["Hub"] == max(scores.values())


def test_compute_betweenness_hub_is_broker():
    edges = [("Hub", "L1", 1.0), ("Hub", "L2", 1.0), ("Hub", "L3", 1.0)]
    names = ["Hub", "L1", "L2", "L3"]
    scores = compute_centrality(edges, names, "betweenness")
    assert scores["Hub"] > scores["L1"]  # only path between leaves goes through Hub


def test_compute_unknown_metric_raises():
    import pytest
    with pytest.raises(ValueError):
        compute_centrality([], [], "bogus")


def test_compute_empty_graph_returns_empty():
    assert compute_centrality([], [], "pagerank") == {}
