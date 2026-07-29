"""Nebula centrality write-back: compute once, write batched.

Two defects, both measured on the live graph:

1. `compute_all` computes ALL THREE metrics per call, but `materialize_centrality`
   called it once PER metric and kept 1/3 of each result. betweenness is O(V*E)
   and dominates everything else (measured 1877s at V=78829, vs 0.8s pagerank
   and 0.5s eigenvector), so it ran 3x — ~82 wasted minutes per materialize.

2. The write-back issued one `UPDATE VERTEX` round-trip per entity per metric:
   91023 entities x 3 metrics = 273069 sequential requests.

Both fixed together: one compute, one `UPDATE VERTEX` per vertex carrying every
metric, batched into multi-statement requests under the nebula query-size cap.
"""

from __future__ import annotations

import pytest

import src.analytics.centrality_compute as cc
from src.analytics import materialize as m
from src.graph.nebula_store import entity_vid

_SCORES = {
    "pagerank": {"A": 0.1, "B": 0.2},
    "betweenness": {"A": 1.0, "B": 2.0},
    "eigenvector": {"A": 0.5, "B": 0.6},
}
_ALL = ["pagerank", "betweenness", "eigenvector"]


class _RecStore:
    """Records every nGQL request; optionally fails any request mentioning a vid."""

    def __init__(self, fail_on: tuple[str, ...] = ()) -> None:
        self.reqs: list[str] = []
        self._fail_on = fail_on

    def structured_query(self, query, param_map=None):
        self.reqs.append(query)
        if any(bad in query for bad in self._fail_on):
            raise RuntimeError("Storage Error: Vertex or edge not found.")
        return []


@pytest.fixture
def _nebula(monkeypatch):
    monkeypatch.setattr(m.settings.graph, "backend", "nebula")


@pytest.mark.asyncio
async def test_compute_all_runs_once_for_every_metric(_nebula, monkeypatch):
    """The whole point: betweenness must be computed ONCE, not once per metric."""
    calls: list[int] = []

    def _fake_compute_all(store, **_kw):
        calls.append(1)
        return _SCORES

    monkeypatch.setattr(cc, "compute_all", _fake_compute_all)

    await m.write_centrality_all(_RecStore(), _ALL)

    assert len(calls) == 1, f"compute_all ran {len(calls)}x — betweenness recomputed"


@pytest.mark.asyncio
async def test_one_update_per_vertex_carries_every_metric(_nebula, monkeypatch):
    monkeypatch.setattr(cc, "compute_all", lambda store, **kw: _SCORES)
    store = _RecStore()

    await m.write_centrality_all(store, _ALL)

    joined = "\n".join(store.reqs)
    # 2 vertices → 2 UPDATE statements total, NOT 2x3
    assert joined.count("UPDATE VERTEX") == 2
    for metric in _ALL:
        assert f"{metric} = " in joined
    # each vertex is addressed by its entity_vid
    for name in ("A", "B"):
        assert f'"{entity_vid(name)}"' in joined


@pytest.mark.asyncio
async def test_writes_are_batched_into_few_requests(_nebula, monkeypatch):
    """273069 sequential round-trips is the thing being removed."""
    n = 500
    scores = {metric: {f"E{i}": float(i) for i in range(n)} for metric in _ALL}
    monkeypatch.setattr(cc, "compute_all", lambda store, **kw: scores)
    store = _RecStore()

    written = await m.write_centrality_all(store, _ALL)

    assert written == n
    assert len(store.reqs) < 20, f"{len(store.reqs)} requests for {n} vertices"


@pytest.mark.asyncio
async def test_batch_respects_the_statement_size_budget(_nebula, monkeypatch):
    """Nebula rejects a request over `max_allowed_query_size` (4 MiB) with
    `SyntaxError: Query is too large` — the same cap that broke the community
    write-back. Batches must be bounded by rendered size, not vertex count."""
    monkeypatch.setattr(m, "_MAX_STMT_CHARS", 400)
    scores = {metric: {f"E{i}": float(i) for i in range(50)} for metric in _ALL}
    monkeypatch.setattr(cc, "compute_all", lambda store, **kw: scores)
    store = _RecStore()

    await m.write_centrality_all(store, _ALL)

    assert len(store.reqs) > 1, "expected the batch to be split"
    assert all(len(r) <= 400 for r in store.reqs)


@pytest.mark.asyncio
async def test_failed_batch_falls_back_to_per_vertex(_nebula, monkeypatch):
    """Fail-soft must survive batching: an ER-merged or deleted vertex raises
    `Vertex or edge not found`, and must not drop the rest of its batch."""
    monkeypatch.setattr(cc, "compute_all", lambda store, **kw: _SCORES)
    store = _RecStore(fail_on=(entity_vid("B"),))

    written = await m.write_centrality_all(store, _ALL)

    assert written == 1, "the good vertex must still be written"
    # the batch was retried statement-by-statement
    assert len(store.reqs) > 1


@pytest.mark.asyncio
async def test_rejects_unknown_metric(_nebula):
    with pytest.raises(ValueError):
        await m.write_centrality_all(_RecStore(), ["bogus; DROP"])


@pytest.mark.asyncio
async def test_none_store_is_failsoft(_nebula):
    assert await m.write_centrality_all(None, _ALL) == 0
