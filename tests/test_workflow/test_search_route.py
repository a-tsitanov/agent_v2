"""Query-routing tests (Search R7a).

``route_query`` classifies a question into local / global / drift via the
small ``route`` model.  The parse/classify logic is extracted into the
pure ``classify_route`` helper so the four behaviours are unit-testable
WITHOUT a live Temporal env or a real LLM:

  * thematic / aggregate / corpus-level question → "global",
  * specific factual question → "local",
  * complex / mixed question → "drift",
  * unparseable reply / LLM error → "local" (fail-safe default).

The activity wrapper is also covered as a plain async fn with a stubbed
LLM (same pattern as the other search-activity tests).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.workflow.contracts import RouteParams
from src.workflow.search.activities import route as route_mod
from src.workflow.search.activities.route import classify_route, route_query

# ── pure classifier helper ──────────────────────────────────────────


def test_classify_global_from_thematic_label():
    r = classify_route("GLOBAL", query="каковы основные темы корпуса?")
    assert r.route == "global"


def test_classify_local_from_factual_label():
    r = classify_route("local", query="кто такой Иванов?")
    assert r.route == "local"


def test_classify_drift_from_complex_label():
    r = classify_route("drift", query="сравни компании и их связи по всему графу")
    assert r.route == "drift"


def test_classify_failsafe_on_unparseable():
    # Garbled / unknown reply → safe default "local".
    assert classify_route("???", query="q").route == "local"
    assert classify_route("", query="q").route == "local"
    assert classify_route(None, query="q").route == "local"  # type: ignore[arg-type]


def test_classify_tolerates_wrapping_prose():
    # Small models often wrap the label in prose / punctuation.
    assert classify_route("Route: GLOBAL.", query="q").route == "global"
    assert classify_route("the answer is drift", query="q").route == "drift"


# ── activity wrapper (stubbed LLM) ──────────────────────────────────


@pytest.fixture(autouse=True)
def _stub_activity_ctx(monkeypatch):
    mock = MagicMock()
    mock.heartbeat = MagicMock()
    mock.logger = MagicMock()
    monkeypatch.setattr(route_mod, "activity", mock)


class _FakeLLM:
    def __init__(self, text):
        self._text = text

    async def achat(self, messages, **_kw):
        return MagicMock(message=MagicMock(content=self._text))


@pytest.mark.asyncio
async def test_route_query_activity_global(monkeypatch):
    monkeypatch.setattr(route_mod, "_get_route_llm", lambda: _FakeLLM("global"))
    out = await route_query(RouteParams(query="каковы основные темы?"))
    assert out.route == "global"


@pytest.mark.asyncio
async def test_route_query_activity_failsafe_on_llm_error(monkeypatch):
    class _BoomLLM:
        async def achat(self, *_a, **_k):
            raise RuntimeError("router down")

    monkeypatch.setattr(route_mod, "_get_route_llm", lambda: _BoomLLM())
    out = await route_query(RouteParams(query="q"))
    assert out.route == "local"
