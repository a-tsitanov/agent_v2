"""Unit tests for ``ensure_first_seen_indexes`` (E1 created_at DDL helper)."""

from __future__ import annotations

from src.graph.index import ensure_first_seen_indexes


class _Rec:
    def __init__(self):
        self.queries: list[str] = []

    def structured_query(self, cypher: str, param_map=None):
        self.queries.append(cypher)
        return []


def test_ensure_first_seen_indexes_creates_entity_and_rel_index():
    store = _Rec()
    assert ensure_first_seen_indexes(store) is True
    joined = " ".join(store.queries)
    assert "created_at" in joined
    assert "IF NOT EXISTS" in joined
    assert any("FOR (e:__Entity__)" in q for q in store.queries)


def test_ensure_first_seen_indexes_returns_false_on_error():
    class _Fail:
        def structured_query(self, cypher: str, param_map=None):
            raise RuntimeError("boom")

    assert ensure_first_seen_indexes(_Fail()) is False


def test_ensure_first_seen_indexes_two_queries_issued():
    """Both entity and relationship indexes are created."""
    store = _Rec()
    ensure_first_seen_indexes(store)
    assert len(store.queries) == 2
