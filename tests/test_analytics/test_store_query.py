"""Tests for src/analytics/store_query.py (fail-soft except NotImplementedError).

NotImplementedError signals a structural backend limitation (e.g. NebulaGraphStore
refusing a parameterised nGQL query) — it must propagate so the caller can report
it, instead of being swallowed into an indistinguishable ``[]``.
"""

from __future__ import annotations

import pytest

from src.analytics import store_query


class _RaisingStore:
    """Store whose structured_query always raises the given exception."""

    def __init__(self, exc: Exception):
        self._exc = exc

    def structured_query(self, cypher, param_map=None):
        raise self._exc


@pytest.mark.asyncio
async def test_run_rows_returns_empty_when_store_is_none():
    assert await store_query.run_rows(None, "MATCH ...", {}) == []


@pytest.mark.asyncio
async def test_run_rows_reraises_not_implemented_error():
    """A structural backend limitation must not be swallowed into []."""
    store = _RaisingStore(NotImplementedError("nGQL params not bound yet (Phase 2)"))
    with pytest.raises(NotImplementedError):
        await store_query.run_rows(store, "MATCH ...", {"topic": "x"})


@pytest.mark.asyncio
async def test_run_rows_swallows_ordinary_exception_and_returns_empty():
    store = _RaisingStore(RuntimeError("transient boom"))
    rows = await store_query.run_rows(store, "MATCH ...", {})
    assert rows == []


@pytest.mark.asyncio
async def test_run_rows_logs_warning_on_ordinary_exception(monkeypatch):
    warnings: list[str] = []
    monkeypatch.setattr(
        store_query.logger, "warning", lambda msg, **kw: warnings.append(msg.format(**kw))
    )
    store = _RaisingStore(RuntimeError("transient boom"))
    await store_query.run_rows(store, "MATCH ...", {})
    assert len(warnings) == 1
    assert "analytics query failed" in warnings[0]


@pytest.mark.asyncio
async def test_run_rows_does_not_log_on_not_implemented_error(monkeypatch):
    """The re-raised structural failure is the caller's job to report, not this
    module's job to log — logging here would duplicate whatever the caller does."""
    warnings: list[str] = []
    monkeypatch.setattr(
        store_query.logger, "warning", lambda msg, **kw: warnings.append(msg.format(**kw))
    )
    store = _RaisingStore(NotImplementedError("nGQL params not bound yet"))
    with pytest.raises(NotImplementedError):
        await store_query.run_rows(store, "MATCH ...", {"topic": "x"})
    assert warnings == []
