from datetime import UTC, datetime

import pytest

from scripts.tg_ingest import _message_to_doc, post_ingest


class _FakeMsg:
    def __init__(self, id, message, date):
        self.id = id
        self.message = message
        self.date = date


def test_message_to_doc_maps_fields():
    m = _FakeMsg(42, "  hello world  ", datetime(2024, 3, 1, 12, 0, tzinfo=UTC))
    fn, text, dd = _message_to_doc(m, "@chan")
    assert fn == "tg_chan_42.txt"
    assert text == "hello world"
    assert dd == "2024-03-01"


def test_message_to_doc_skips_empty():
    m = _FakeMsg(1, None, datetime(2024, 1, 1, tzinfo=UTC))
    assert _message_to_doc(m, "@c") is None
    m2 = _FakeMsg(2, "   ", datetime(2024, 1, 1, tzinfo=UTC))
    assert _message_to_doc(m2, "@c") is None


@pytest.mark.asyncio
async def test_post_ingest_true_on_2xx():
    sent = {}

    class _Resp:
        status_code = 202

    class _Client:
        async def post(self, url, headers=None, files=None, data=None):
            sent.update(url=url, headers=headers, files=files, data=data)
            return _Resp()

    ok = await post_ingest(_Client(), "http://api", "k", "f.txt", "hi", "2024-03-01", "q1")
    assert ok is True
    assert sent["url"] == "http://api/api/v1/ingest"
    assert sent["headers"]["X-API-Key"] == "k"
    assert sent["data"]["queue"] == "q1" and sent["data"]["document_date"] == "2024-03-01"
    assert sent["files"]["file"][0] == "f.txt"


@pytest.mark.asyncio
async def test_post_ingest_false_on_error():
    class _Client:
        async def post(self, *a, **k):
            raise RuntimeError("connection refused")

    assert await post_ingest(_Client(), "http://api", "k", "f", "t", "d", "q") is False
