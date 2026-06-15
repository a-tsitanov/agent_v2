"""Regression: every search-side LLM accessor goes through the LLM pool, so
the global N semaphore actually counts these calls (audit finding #3)."""

from __future__ import annotations

import importlib

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.retrieval import llm_pool as pool_mod


def _fake_llm():
    m = MagicMock()
    m.achat = AsyncMock(return_value="ok")
    return m


@pytest.fixture(autouse=True)
def _pool(monkeypatch):
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())
    s = MagicMock()
    s.llm_pool.n = 8
    pool_mod.reset_for_tests()
    pool_mod._pool = pool_mod.LLMPool(s)
    yield
    pool_mod.reset_for_tests()


@pytest.mark.parametrize(
    "import_path,attr,role",
    [
        ("src.workflow.search.activities.route", "_get_route_llm", "route"),
        ("src.workflow.search.activities.contextualize", "_get_contextualize_llm", "route"),
        ("src.workflow.search.activities.global_search", "_get_map_llm", "retrieve"),
        ("src.workflow.search.activities.community", "_get_summary_llm", "retrieve"),
    ],
)
def test_accessor_returns_pooled_llm(import_path, attr, role):
    mod = importlib.import_module(import_path)
    accessor = getattr(mod, attr)
    assert accessor() is pool_mod.get_llm_pool().get(role)
