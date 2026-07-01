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


@pytest.mark.asyncio
async def test_deliver_alerts_pushes_unpushed_and_marks(monkeypatch):
    from src.analytics.contracts import DeliverIn
    from src.workflow.monitor import activities as act

    class _Store:
        def __init__(self):
            self.calls = []

        def structured_query(self, cypher, param_map=None):
            self.calls.append((cypher, param_map or {}))
            if "a.pushed_at IS NULL" in cypher:
                return [
                    {"key": "k1", "kind": "burst", "entity": "A", "detail": "d", "created_at": 1},
                    {"key": "k2", "kind": "burst", "entity": "B", "detail": "d", "created_at": 1},
                ]
            return []

    store = _Store()
    monkeypatch.setattr(act, "_get_store", lambda: store)
    monkeypatch.setattr(act.settings.monitor, "webhook_url", "http://hook", raising=False)

    async def _fake_post(url, payload, *, timeout_s=5.0):
        return payload["key"] == "k1"

    monkeypatch.setattr(act, "post_alert", _fake_post)
    res = await act.deliver_alerts(DeliverIn(cap=100))
    assert res.delivered == 1 and res.failed == 1
    marks = [pm for c, pm in store.calls if "SET a.pushed_at" in c]
    assert len(marks) == 1 and marks[0]["key"] == "k1"


@pytest.mark.asyncio
async def test_deliver_alerts_noop_when_no_url(monkeypatch):
    from src.analytics.contracts import DeliverIn
    from src.workflow.monitor import activities as act

    monkeypatch.setattr(act.settings.monitor, "webhook_url", "", raising=False)
    res = await act.deliver_alerts(DeliverIn())
    assert res.delivered == 0 and res.error == ""
