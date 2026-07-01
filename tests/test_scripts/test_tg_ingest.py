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


@pytest.mark.asyncio
async def test_read_and_enqueue_tallies_sent_and_skipped():
    from scripts.tg_ingest import read_and_enqueue

    msgs = [
        _FakeMsg(1, "alpha", datetime(2024, 1, 1, tzinfo=UTC)),
        _FakeMsg(2, "", datetime(2024, 1, 2, tzinfo=UTC)),  # skipped (empty)
        _FakeMsg(3, "gamma", datetime(2024, 1, 3, tzinfo=UTC)),
    ]

    class _TG:
        async def iter_messages(self, channel, limit, reverse):
            assert reverse is True
            for m in msgs:
                yield m

    posted: list[str] = []

    class _Resp:
        status_code = 202

    class _HTTP:
        async def post(self, url, headers=None, files=None, data=None):
            posted.append(files["file"][0])
            return _Resp()

    tally = await read_and_enqueue(
        _TG(),
        _HTTP(),
        channels=["@c"],
        limit=10,
        api_base="http://a",
        api_key="k",
        queue="q",
    )
    assert tally["sent"] == 2 and tally["skipped"] == 1 and tally["failed"] == 0
    assert posted == ["tg_c_1.txt", "tg_c_3.txt"]
