"""Tests for numeric_rollup primitive."""

from __future__ import annotations

import pytest

from src.analytics.primitives import rollups as ro
from tests.test_analytics.conftest import _FakeStore


def test_parse_amount():
    assert ro.parse_amount("1 200,50") == 1200.5
    assert ro.parse_amount("$3,000.00") == 3000.0
    assert ro.parse_amount("n/a") is None


@pytest.mark.asyncio
async def test_numeric_rollup_sums_amounts_in_python():
    store = _FakeStore(
        rows=[
            {"counterparty": "A", "amount": "1 000"},
            {"counterparty": "A", "amount": "500"},
            {"counterparty": "B", "amount": "x"},
        ]
    )
    res = await ro.numeric_rollup(store)
    by = {r["counterparty"]: r for r in res.rows}
    assert by["A"]["total"] == 1500.0 and by["A"]["count"] == 2
    assert "B" not in by or by["B"]["count"] == 0  # unparseable dropped


@pytest.mark.asyncio
async def test_failsoft():
    assert (await ro.numeric_rollup(None)).rows == []
