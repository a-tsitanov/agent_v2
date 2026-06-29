import pytest

from src.analytics import materialize as m
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_write_centrality_pagerank_shape():
    # call 1 = GDS stream rows; the write UNWIND returns nothing
    store = _FakeStore(by_call=[[{"name": "A", "score": 0.9}, {"name": "B", "score": 0.1}], []])
    n = await m.write_centrality(store, "g1", "pagerank")
    assert n == 2
    cyphers = " ".join(c for c, _ in store.calls)
    assert "gds.pageRank.stream" in cyphers
    assert "SET e.pagerank" in cyphers  # metric inlined into write-back


@pytest.mark.asyncio
async def test_write_centrality_rejects_unknown_metric():
    store = _FakeStore(rows=[])
    with pytest.raises(ValueError):
        await m.write_centrality(store, "g1", "bogus; DROP")  # allowlist guard


@pytest.mark.asyncio
async def test_write_link_prediction_filters_and_writes():
    store = _FakeStore(
        by_call=[
            [],  # delete stale
            [
                {"a": "A", "b": "B", "score": 0.9},
                {"a": "A", "b": "C", "score": 0.2},
            ],  # nodeSimilarity
            [],  # write
        ]
    )
    n = await m.write_link_prediction(store, "g1", top_k=10, min_score=0.5)
    assert n == 1  # only the 0.9 pair kept
    joined = " ".join(c for c, _ in store.calls)
    assert "gds.nodeSimilarity.stream" in joined and ":LIKELY_LINK" in joined


@pytest.mark.asyncio
async def test_failsoft_none_store():
    assert await m.write_centrality(None, "g", "pagerank") == 0
