"""ASGI tests for `POST /api/v1/statistics/load`.

`StatsRepository` is patched so the route is exercised end-to-end
against the real FastAPI app without a live Postgres — same approach as
`tests/test_api/test_ingest.py`.
"""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import settings


def _api_key_header() -> dict[str, str]:
    return {"X-API-Key": settings.api.keys_list[0]}


def _body(**overrides) -> dict:
    body = {
        "indicator": {
            "source": "fom",
            "code": "anxiety",
            "title": "Уровень тревожности",
            "question_text": "Какое настроение преобладает?",
            "unit": "%",
            "value_kind": "share",
            "granularity": "week",
        },
        "observations": [
            {
                "period_start": "2026-01-05",
                "period_end": "2026-01-11",
                "value": 57.5,
                "sample_n": 1500,
            },
        ],
    }
    body.update(overrides)
    return body


async def _post(body: dict) -> tuple[int, dict]:
    from src.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post(
            "/api/v1/statistics/load", json=body, headers=_api_key_header(),
        )
    return resp.status_code, resp.json()


async def _post_raw(raw: str) -> tuple[int, dict]:
    """Post a body verbatim.

    `NaN` / `Infinity` are not JSON, but Python's `json.loads` — which is
    what Starlette calls — accepts them, so they cannot be reproduced by
    building a dict and letting the client serialise it.
    """
    from src.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post(
            "/api/v1/statistics/load",
            content=raw.encode(),
            headers={**_api_key_header(), "Content-Type": "application/json"},
        )
    return resp.status_code, resp.json()


@pytest.mark.asyncio
async def test_load_upserts_indicator_then_observations() -> None:
    from src.storage.stats import StatsRepository

    with (
        patch.object(
            StatsRepository, "upsert_indicator", new=AsyncMock(return_value=7),
        ) as up_ind,
        patch.object(
            StatsRepository, "upsert_observations", new=AsyncMock(return_value=1),
        ) as up_obs,
    ):
        code, payload = await _post(_body())

    assert code == 200
    assert payload == {"indicator_id": 7, "observations": 1}
    # The route hand-maps nine fields onto keyword arguments.  Counting
    # the call only proved it happened; swapping `title` and `unit` — both
    # plain strings — would have passed.  Assert the whole mapping.
    assert up_ind.await_count == 1
    assert up_ind.await_args.kwargs == {
        "source": "fom",
        "code": "anxiety",
        "title": "Уровень тревожности",
        "unit": "%",
        "value_kind": "share",
        "granularity": "week",
        "question_text": "Какое настроение преобладает?",
        "dims_schema": {},
        "entity_vid": None,
    }
    assert up_ind.await_args.args == ()

    rows = up_obs.await_args.args[0]
    assert rows == [{
        "indicator_id": 7,
        "period_start": date(2026, 1, 5),
        "period_end": date(2026, 1, 11),
        "dims": {},
        "value": 57.5,
        "sample_n": 1500,
        "revision": 0,
        "source_doc_id": None,
    }]


@pytest.mark.asyncio
async def test_load_requires_an_api_key() -> None:
    from src.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post("/api/v1/statistics/load", json=_body())
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_load_rejects_unknown_value_kind() -> None:
    body = _body()
    body["indicator"]["value_kind"] = "ratio"
    code, payload = await _post(body)
    assert code == 422
    assert "value_kind" in str(payload)


@pytest.mark.asyncio
async def test_load_rejects_unknown_granularity() -> None:
    body = _body()
    body["indicator"]["granularity"] = "fortnight"
    code, payload = await _post(body)
    assert code == 422
    assert "granularity" in str(payload)


@pytest.mark.asyncio
async def test_load_rejects_period_end_before_period_start() -> None:
    body = _body()
    body["observations"][0]["period_end"] = "2026-01-01"
    code, payload = await _post(body)
    assert code == 422
    assert "period_end" in str(payload)


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
@pytest.mark.asyncio
async def test_load_rejects_non_finite_values(literal: str) -> None:
    """A stored NaN poisons everything downstream: `align` yields
    `divergence: NaN` with no warning, and `nan` is not valid JSON on the
    way out — the response cannot even be serialised.  Reject at the
    door."""
    raw = json.dumps(_body())
    raw = raw.replace('"value": 57.5', f'"value": {literal}')
    code, payload = await _post_raw(raw)
    assert code == 422
    assert payload["detail"][0]["loc"] == ["body", "observations", 0, "value"]
    assert "non-finite" in payload["detail"][0]["input"]
    # The REJECTION must itself be serialisable.  Echoing the offending
    # value back would make Starlette's renderer (allow_nan=False) raise
    # and turn a validation error into a 500.
    assert json.dumps(payload, allow_nan=False)


@pytest.mark.asyncio
async def test_load_rejects_a_negative_sample_n() -> None:
    """`sample_n` is a respondent count.  A negative one is not a small
    sample, it is a corrupt payload."""
    body = _body()
    body["observations"][0]["sample_n"] = -5
    code, payload = await _post(body)
    assert code == 422
    assert "sample_n" in str(payload)


@pytest.mark.asyncio
async def test_load_accepts_a_zero_sample_n() -> None:
    """Zero is legitimate — a published cut with no respondents in it."""
    from src.storage.stats import StatsRepository

    body = _body()
    body["observations"][0]["sample_n"] = 0
    with (
        patch.object(
            StatsRepository, "upsert_indicator", new=AsyncMock(return_value=7),
        ),
        patch.object(
            StatsRepository, "upsert_observations", new=AsyncMock(return_value=1),
        ) as up_obs,
    ):
        code, _ = await _post(body)

    assert code == 200
    assert up_obs.await_args.args[0][0]["sample_n"] == 0


@pytest.mark.asyncio
async def test_load_accepts_an_empty_observation_list() -> None:
    """Registering an indicator before any data exists is legitimate —
    it is how a source gets seeded, and `list_sources` is built to show
    such a source with NULL period bounds."""
    from src.storage.stats import StatsRepository

    with (
        patch.object(
            StatsRepository, "upsert_indicator", new=AsyncMock(return_value=7),
        ),
        patch.object(
            StatsRepository, "upsert_observations", new=AsyncMock(return_value=0),
        ),
    ):
        code, payload = await _post(_body(observations=[]))

    assert code == 200
    assert payload == {"indicator_id": 7, "observations": 0}


@pytest.mark.asyncio
async def test_load_carries_dims_and_revision_through() -> None:
    from src.storage.stats import StatsRepository

    body = _body(observations=[
        {
            "period_start": "2026-06-01",
            "period_end": "2026-06-30",
            "value": 8.1,
            "dims": {"region": "Москва"},
            "revision": 1,
        },
    ])
    with (
        patch.object(
            StatsRepository, "upsert_indicator", new=AsyncMock(return_value=2),
        ),
        patch.object(
            StatsRepository, "upsert_observations", new=AsyncMock(return_value=1),
        ) as up_obs,
    ):
        code, _ = await _post(body)

    assert code == 200
    row = up_obs.await_args.args[0][0]
    assert row["dims"] == {"region": "Москва"}
    assert row["revision"] == 1
    assert row["sample_n"] is None
