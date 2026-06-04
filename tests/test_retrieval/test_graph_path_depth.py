"""Unit tests for GraphRetriever per-call ``path_depth``.

Uses a fake PropertyGraphIndex whose ``as_retriever`` records the
requested ``path_depth`` and returns a stub retriever — no live store /
LLM. Asserts: the default depth is pre-built, a per-call depth builds +
caches a retriever, repeated calls reuse the cache, and out-of-range
depths are clamped to ``[1, GRAPH_PATH_DEPTH_MAX]``.
"""

from __future__ import annotations

import pytest

from src.graph.retriever import (
    GRAPH_PATH_DEPTH_MAX,
    GraphRetriever,
    RoundGraphData,
)


class _FakeRetriever:
    def __init__(self, path_depth: int) -> None:
        self.path_depth = path_depth

    async def aretrieve(self, query: str):
        return []  # empty node list → empty RoundGraphData


class _FakePGIndex:
    property_graph_store = None

    def __init__(self) -> None:
        self.depths_built: list[int] = []

    def as_retriever(self, *, similarity_top_k, path_depth, include_text):
        self.depths_built.append(path_depth)
        return _FakeRetriever(path_depth)


def _build():
    idx = _FakePGIndex()
    r = GraphRetriever(idx, similarity_top_k=10, path_depth=1)
    return idx, r


@pytest.mark.asyncio
async def test_default_depth_prebuilt_and_reused():
    idx, r = _build()
    assert idx.depths_built == [1]  # default built once in __init__
    out = await r.aretrieve("q")  # path_depth=None → default
    assert isinstance(out, RoundGraphData)
    assert idx.depths_built == [1]  # no rebuild


@pytest.mark.asyncio
async def test_per_call_depth_builds_then_caches():
    idx, r = _build()
    await r.aretrieve("q", path_depth=2)
    assert idx.depths_built == [1, 2]  # depth-2 retriever built
    await r.aretrieve("q", path_depth=2)
    assert idx.depths_built == [1, 2]  # second call reuses cache


@pytest.mark.asyncio
async def test_depth_clamped_to_max():
    idx, r = _build()
    await r.aretrieve("q", path_depth=99)
    assert idx.depths_built == [1, GRAPH_PATH_DEPTH_MAX]


@pytest.mark.asyncio
async def test_depth_clamped_to_min():
    idx, r = _build()
    await r.aretrieve("q", path_depth=0)
    # clamped to 1 → already the pre-built default, no new build
    assert idx.depths_built == [1]
