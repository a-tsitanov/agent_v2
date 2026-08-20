"""The openclaw agent client — a plain OpenAI call, against a mock transport."""
from __future__ import annotations

import json

import httpx

from src.bot.agent_client import make_agent


def _patch(monkeypatch, handler):
    real = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient",
                        lambda *a, **kw: real(*a, **{**kw, "transport": httpx.MockTransport(handler)}))


async def test_posts_model_and_message(monkeypatch):
    seen = {}

    def h(req):
        seen["body"] = json.loads(req.content)
        seen["url"] = str(req.url)
        seen["auth"] = req.headers.get("Authorization")
        return httpx.Response(200, json={"choices": [{"message": {"content": "ответ"}}]})

    _patch(monkeypatch, h)
    out = await make_agent(base_url="http://openclaw:18789", token="k")("вопрос")
    assert out == "ответ"
    assert seen["url"].endswith("/v1/chat/completions")
    assert seen["body"]["model"] == "openclaw"
    assert seen["body"]["messages"] == [{"role": "user", "content": "вопрос"}]
    assert seen["auth"] == "Bearer k"


async def test_session_goes_into_the_user_field(monkeypatch):
    """openclaw maps OpenAI `user` to its sessionKey — this is what makes a
    follow-up carry. Without it the field must be absent, not empty."""
    seen = {}

    def h(req):
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    _patch(monkeypatch, h)
    fn = make_agent(base_url="http://openclaw:18789", token="k")
    await fn("q", session="tg:555")
    assert seen["body"]["user"] == "tg:555"

    await fn("q")
    assert "user" not in seen["body"]


async def test_raises_on_error_status(monkeypatch):
    _patch(monkeypatch, lambda r: httpx.Response(500, json={"error": "boom"}))
    try:
        await make_agent(base_url="http://openclaw:18789", token="k")("q")
        raise AssertionError("should have raised")
    except httpx.HTTPStatusError:
        pass


async def test_empty_choices_yield_empty_string(monkeypatch):
    _patch(monkeypatch, lambda r: httpx.Response(200, json={}))
    assert await make_agent(base_url="http://openclaw:18789", token="k")("q") == ""
