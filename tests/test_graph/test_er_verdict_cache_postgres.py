"""The ER verdict cache, once it lives in Postgres rather than the graph.

No live database: a stub pool stands in, and — like the stats-repository
stub — it HONOURS `row_factory`, returning dicts only when `dict_row` was
actually asked for.  A stub that always yields dicts hides a whole class
of bug: on real psycopg3 a cursor opened without `row_factory=dict_row`
gets tuples, and `r["er_key"]` raises.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field

import pytest
from psycopg.rows import dict_row

from src.graph import er_graph_ops as ops_mod
from src.graph.er_graph_ops import (
    PostgresERVerdictCache,
    _CompositeERGraphOps,
    build_er_graph_ops,
)

# ── stubs ────────────────────────────────────────────────────────────


@dataclass
class _StubCursor:
    rows: list[dict]
    row_factory: object = None
    executed: list[tuple] = field(default_factory=list)

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, params_seq):
        self.executed.append((sql, list(params_seq)))

    def _shaped(self, row):
        if self.row_factory is dict_row:
            return row
        return tuple(row.values()) if isinstance(row, dict) else row

    def fetchall(self):
        return [self._shaped(r) for r in self.rows]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@dataclass
class _StubConn:
    rows: list[dict]
    cursors: list[_StubCursor] = field(default_factory=list)

    def cursor(self, row_factory=None):
        cur = _StubCursor(rows=self.rows, row_factory=row_factory)
        self.cursors.append(cur)
        return cur

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _StubPool:
    def __init__(self, rows: list[dict] | None = None) -> None:
        self.conn = _StubConn(rows=rows or [])

    @contextmanager
    def connection(self):
        yield self.conn

    @property
    def executed(self) -> list[tuple]:
        return [e for cur in self.conn.cursors for e in cur.executed]


@pytest.fixture
def cache_with_pool():
    def _make(rows=None):
        cache = PostgresERVerdictCache()
        pool = _StubPool(rows)
        cache._pool = lambda: pool  # noqa: SLF001 - test seam
        return cache, pool

    return _make


# ── load ─────────────────────────────────────────────────────────────


def test_load_empty_keys_issues_no_query(cache_with_pool):
    cache, pool = cache_with_pool()
    assert cache.load_verdicts([]) == {}
    assert pool.executed == []


def test_load_uses_one_query_for_the_whole_batch(cache_with_pool):
    """One round trip per batch, not one per key — ER passes hundreds of
    candidate keys at a time."""
    cache, pool = cache_with_pool([{"er_key": "a", "same": True}])
    cache.load_verdicts(["a", "b", "c"])
    assert len(pool.executed) == 1
    sql, params = pool.executed[0]
    assert "er_key = ANY(%s)" in sql
    assert params == (["a", "b", "c"],)


def test_load_maps_rows_to_key_and_verdict(cache_with_pool):
    cache, _ = cache_with_pool(
        [{"er_key": "a", "same": True}, {"er_key": "b", "same": False}],
    )
    assert cache.load_verdicts(["a", "b"]) == {"a": True, "b": False}


def test_load_omits_a_miss_rather_than_calling_it_different(cache_with_pool):
    """A key the cache has never seen must be ABSENT, not `False`.
    `False` means "judged DIFFERENT", so returning it for a miss would
    silently suppress a real LLM judgement."""
    cache, _ = cache_with_pool([{"er_key": "known", "same": True}])
    out = cache.load_verdicts(["known", "never-seen"])
    assert out == {"known": True}
    assert "never-seen" not in out


def test_load_asks_for_dict_rows(cache_with_pool):
    """Without `row_factory=dict_row` live psycopg3 returns tuples and the
    mapping below raises — the stub honours it, so this is a real check."""
    cache, pool = cache_with_pool([{"er_key": "a", "same": True}])
    cache.load_verdicts(["a"])
    assert pool.conn.cursors[0].row_factory is dict_row


# ── store ────────────────────────────────────────────────────────────


def test_store_empty_is_a_noop(cache_with_pool):
    cache, pool = cache_with_pool()
    cache.store_verdicts({})
    assert pool.executed == []


def test_store_upserts_by_key_so_a_rejudged_pair_wins(cache_with_pool):
    cache, pool = cache_with_pool()
    cache.store_verdicts({"a": True, "b": False})
    sql, params = pool.executed[0]
    assert "ON CONFLICT (er_key) DO UPDATE" in sql
    assert "same = EXCLUDED.same" in sql
    assert params == [("a", True), ("b", False)]


def test_store_sends_the_batch_in_one_call(cache_with_pool):
    cache, pool = cache_with_pool()
    cache.store_verdicts({f"k{i}": i % 2 == 0 for i in range(50)})
    assert len(pool.executed) == 1


def test_ensure_schema_creates_the_table_idempotently(cache_with_pool):
    cache, pool = cache_with_pool()
    cache.ensure_verdict_schema()
    sql, _ = pool.executed[0]
    assert "CREATE TABLE IF NOT EXISTS er_verdict" in sql


# ── the seam ─────────────────────────────────────────────────────────


class _GraphOpsSpy:
    def __init__(self) -> None:
        self.merges: list[tuple[str, str]] = []
        self.cache_calls: list[str] = []

    def ensure_verdict_schema(self) -> None:
        self.cache_calls.append("ensure")

    def load_verdicts(self, keys):
        self.cache_calls.append("load")
        return {}

    def store_verdicts(self, entries) -> None:
        self.cache_calls.append("store")

    def merge_loser_into_canonical(self, *, loser, canon) -> None:
        self.merges.append((loser, canon))


def test_composite_splits_cache_from_graph_merge():
    """The cache goes to Postgres; `merge_loser_into_canonical` is a real
    graph operation and must still reach the graph backend."""
    graph = _GraphOpsSpy()
    cache = _GraphOpsSpy()
    composite = _CompositeERGraphOps(cache, graph)

    composite.load_verdicts(["a"])
    composite.store_verdicts({"a": True})
    composite.ensure_verdict_schema()
    composite.merge_loser_into_canonical(loser="l", canon="c")

    assert cache.cache_calls == ["load", "store", "ensure"]
    assert graph.cache_calls == []
    assert graph.merges == [("l", "c")]
    assert cache.merges == []


def test_factory_returns_the_composite_by_default(monkeypatch):
    monkeypatch.setattr(
        ops_mod.settings.agent, "er_verdict_cache_backend", "postgres", raising=False,
    )
    assert isinstance(build_er_graph_ops(object()), _CompositeERGraphOps)


def test_factory_graph_backend_setting_restores_the_old_path(monkeypatch):
    """`AGENT_ER_VERDICT_CACHE_BACKEND=graph` is the rollback: no Postgres
    anywhere in the returned ops."""
    monkeypatch.setattr(
        ops_mod.settings.agent, "er_verdict_cache_backend", "graph", raising=False,
    )
    ops = build_er_graph_ops(object())
    assert not isinstance(ops, _CompositeERGraphOps)
    assert isinstance(ops, ops_mod.NebulaERGraphOps | ops_mod.Neo4jERGraphOps)


# ── the fail-safe guarantee ──────────────────────────────────────────


def test_er_degrades_to_pure_llm_judging_when_the_cache_raises(monkeypatch):
    """The cache is OPTIONAL and FAIL-SAFE, and moving its storage must
    not narrow that: a load failure yields no verdicts (ER judges
    everything) and a store failure is swallowed."""
    from src.graph import entity_resolution as er

    class _Boom:
        def load_verdicts(self, keys):
            raise RuntimeError("postgres down")

        def store_verdicts(self, entries):
            raise RuntimeError("postgres down")

    monkeypatch.setattr(er, "build_er_graph_ops", lambda store: _Boom())

    assert er._load_verdict_cache(object(), ["a"]) == {}
    er._store_verdicts(object(), {"a": True})  # must not raise
