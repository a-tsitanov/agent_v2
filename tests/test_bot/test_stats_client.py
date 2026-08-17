"""The bot's HTTP adapters, against a mock transport — no server needed.

Covers both the volume routes and the sources-preserving search, because
the request log is only as good as what the client hands it.
"""

from __future__ import annotations

import json

import httpx
import pytest

from src.bot.search_client import make_search_full
from src.bot.stats_client import make_channels, make_timeline


def _client_patch(monkeypatch, handler):
    """Point httpx.AsyncClient at a mock transport."""
    real = httpx.AsyncClient

    def _factory(*a, **kw):
        kw["transport"] = httpx.MockTransport(handler)
        return real(*a, **kw)

    monkeypatch.setattr(httpx, "AsyncClient", _factory)


# ── /channels ────────────────────────────────────────────────────────


async def test_channels_calls_the_messages_route_grouped_by_channel(monkeypatch):
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["key"] = request.headers.get("X-API-Key")
        return httpx.Response(200, json={"group_by": "channel", "rows": [{"key": "tass"}]})

    _client_patch(monkeypatch, handler)
    rows = await make_channels(api_base="http://api:8000", api_key="k")()
    assert rows == [{"key": "tass"}]
    assert "/api/v1/stats/messages" in seen["url"]
    assert "group_by=channel" in seen["url"]
    assert seen["key"] == "k"


async def test_channels_returns_a_list_when_the_body_has_no_rows(monkeypatch):
    _client_patch(monkeypatch, lambda r: httpx.Response(200, json={}))
    assert await make_channels(api_base="http://api:8000", api_key="k")() == []


# ── /volume ──────────────────────────────────────────────────────────


async def test_timeline_uses_the_document_date_not_the_ingest_date(monkeypatch):
    """`created_at` would chart when we back-filled the corpus, not when
    anything was published."""
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"buckets": [{"day": "2026-08-01", "count": 5}]})

    _client_patch(monkeypatch, handler)
    out = await make_timeline(api_base="http://api:8000", api_key="k")()
    assert out == [{"day": "2026-08-01", "count": 5}]
    assert "date_field=doc_date" in seen["url"]
    assert "channel=" not in seen["url"]
    assert "since=" not in seen["url"]


async def test_timeline_passes_channel_and_since_only_when_given(monkeypatch):
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"buckets": []})

    _client_patch(monkeypatch, handler)
    fn = make_timeline(api_base="http://api:8000", api_key="k")
    await fn(channel="tass", since="2026-08-01")
    assert "channel=tass" in seen["url"]
    assert "since=2026-08-01" in seen["url"]


async def test_timeline_treats_an_empty_channel_as_no_filter(monkeypatch):
    """`channel=` is a filter that matches nothing — not an absent one."""
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"buckets": []})

    _client_patch(monkeypatch, handler)
    await make_timeline(api_base="http://api:8000", api_key="k")(channel="")
    assert "channel=" not in seen["url"]


# ── search that keeps its sources ────────────────────────────────────


async def test_search_full_returns_answer_and_sources(monkeypatch):
    """`make_search` keeps only the text; the request log needs the
    provenance so `/history` can show an old answer with its sources."""
    _client_patch(
        monkeypatch,
        lambda r: httpx.Response(
            200, json={"answer": "ответ", "sources": [{"chunk_id": "c1"}]},
        ),
    )
    fn = make_search_full(api_base="http://api:8000", api_key="k", mode="local")
    out = await fn("вопрос")
    assert out == {"answer": "ответ", "sources": [{"chunk_id": "c1"}]}


async def test_search_full_sends_synthesize_false_for_find(monkeypatch):
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content)
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"answer": "", "sources": []})

    _client_patch(monkeypatch, handler)
    fn = make_search_full(
        api_base="http://api:8000", api_key="k", mode="local", synthesize=False,
    )
    await fn("вопрос")
    assert seen["body"]["synthesize"] is False
    assert seen["url"].endswith("/api/v1/search/local")


async def test_search_full_raises_on_error_status(monkeypatch):
    """So the caller can record the failure against the request row
    instead of storing an empty answer as if it were one."""
    _client_patch(monkeypatch, lambda r: httpx.Response(500, json={"detail": "boom"}))
    fn = make_search_full(api_base="http://api:8000", api_key="k")
    with pytest.raises(httpx.HTTPStatusError):
        await fn("вопрос")


async def test_search_full_rejects_an_unknown_mode():
    with pytest.raises(ValueError, match="unknown search mode"):
        make_search_full(api_base="http://api:8000", api_key="k", mode="telepathy")


async def test_search_full_defaults_missing_fields(monkeypatch):
    """A body without `sources` must yield [], not None — the caller
    serialises it straight into a NOT NULL jsonb column."""
    _client_patch(monkeypatch, lambda r: httpx.Response(200, json={}))
    out = await make_search_full(api_base="http://api:8000", api_key="k")("q")
    assert out == {"answer": "", "sources": []}
