import pytest
from src.workflow.search.activities.retrieve import _walk_seeds


def test_walk_seeds_unions_distinct_graph_and_fulltext_seeds():
    gs = '{"entities":[{"entity_name":"Alpha"}]}'
    fn = '{"entities":[{"entity_name":"Beta"}]}'
    assert _walk_seeds(gs, fn, dual=True) == ["Alpha", "Beta"]


def test_walk_seeds_dedupes_when_same():
    gs = '{"entities":[{"entity_name":"Alpha"}]}'
    fn = '{"entities":[{"entity_name":"Alpha"}]}'
    assert _walk_seeds(gs, fn, dual=True) == ["Alpha"]


def test_walk_seeds_single_when_dual_off_matches_legacy():
    gs = '{"entities":[{"entity_name":"Alpha"}]}'
    fn = '{"entities":[{"entity_name":"Beta"}]}'
    assert _walk_seeds(gs, fn, dual=False) == ["Alpha"]      # graph_search wins
    assert _walk_seeds("", fn, dual=False) == ["Beta"]        # falls back to fulltext
    assert _walk_seeds("", "", dual=True) == []
