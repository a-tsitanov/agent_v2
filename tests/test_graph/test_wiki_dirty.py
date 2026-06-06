from unittest.mock import MagicMock
from src.graph.wiki_dirty import mark_dirty, select_dirty, clear_dirty


def test_mark_dirty_runs_cypher_with_names():
    store = MagicMock()
    mark_dirty(store, ["A", "B"])
    args, kwargs = store.structured_query.call_args
    assert kwargs["param_map"]["names"] == ["A", "B"]
    assert "wiki_dirty = true" in args[0]


def test_mark_dirty_noop_on_empty():
    store = MagicMock()
    mark_dirty(store, [])
    store.structured_query.assert_not_called()


def test_select_dirty_returns_names():
    store = MagicMock()
    store.structured_query.return_value = [{"name": "A"}, {"name": "B"}]
    assert select_dirty(store, limit=10) == ["A", "B"]
    args, kwargs = store.structured_query.call_args
    assert kwargs["param_map"]["limit"] == 10


def test_clear_dirty_sets_hash_and_flags():
    store = MagicMock()
    clear_dirty(store, "A", "deadbeef")
    args, kwargs = store.structured_query.call_args
    assert kwargs["param_map"] == {"name": "A", "hash": "deadbeef"}
    assert "wiki_dirty = false" in args[0]
