import pytest

from src.workflow.monitor import delivery


@pytest.mark.asyncio
async def test_post_alert_true_on_2xx(monkeypatch):
    sent = {}

    class _Resp:
        status_code = 200

    class _Client:
        def __init__(self, *a, **k): ...
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None):
            sent["url"] = url
            sent["json"] = json
            return _Resp()

    monkeypatch.setattr(delivery.httpx, "AsyncClient", _Client)
    ok = await delivery.post_alert("http://hook", {"key": "k1"}, timeout_s=1.0)
    assert ok is True and sent["url"] == "http://hook" and sent["json"]["key"] == "k1"


@pytest.mark.asyncio
async def test_post_alert_false_on_error(monkeypatch):
    class _Client:
        def __init__(self, *a, **k): ...
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None):
            raise RuntimeError("connection refused")

    monkeypatch.setattr(delivery.httpx, "AsyncClient", _Client)
    assert await delivery.post_alert("http://hook", {"key": "k1"}) is False
