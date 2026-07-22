from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from scripts.tg_ingest import reingest_channels, select_mode


class _FakeMsg:
    def __init__(self, id, message, date):
        self.id = id
        self.message = message
        self.date = date


class _FakeEntity:
    def __init__(self, key, username=None):
        self._key = key
        self.username = username


class _FakeDialog:
    def __init__(self, id, key, *, username=None, is_channel=True, is_group=False):
        self.id = id
        self.entity = _FakeEntity(key, username)
        self.username = username
        self.is_channel = is_channel
        self.is_group = is_group
        self.title = username or str(id)


class _TG:
    """iter_messages(entity, limit, reverse) → prepared newest-`limit` msgs."""

    def __init__(self, per_key):
        self.per_key = per_key  # entity._key -> list[_FakeMsg]

    def iter_messages(self, entity, limit=None, reverse=False):
        msgs = sorted(self.per_key.get(entity._key, []), key=lambda m: -m.id)[:limit]
        ordered = sorted(msgs, key=lambda m: m.id) if reverse else msgs

        async def _gen():
            for m in ordered:
                yield m

        return _gen()


class _RecHTTP:
    def __init__(self):
        self.posted = []

    async def post(self, url, headers=None, files=None, data=None):
        self.posted.append((files["file"][0], dict(data)))

        class _R:
            status_code = 202

        return _R()


@pytest.mark.asyncio
async def test_reingest_success_posts_priority_and_group():
    dialog = _FakeDialog(-100111, "a", username="chan_a")
    tg = _TG({"a": [
        _FakeMsg(1, "alpha", datetime(2024, 1, 1, tzinfo=UTC)),
        _FakeMsg(2, "beta", datetime(2024, 1, 2, tzinfo=UTC)),
    ]})
    http = _RecHTTP()

    tally, errors = await reingest_channels(
        tg, http, dialogs=[dialog], channels=["@chan_a"],
        spec={"include_ids": {-100111}, "exclude_ids": set(),
              "groups": False, "broadcasts": False},
        group_map={-100111: "news"}, limit=50,
        api_base="http://a", api_key="k", queue="ingest.pending", priority=0,
    )

    assert errors == []
    assert tally["sent"] == 2
    assert [f for f, _ in http.posted] == ["tg_chan_a_1.txt", "tg_chan_a_2.txt"]
    assert all(d["priority"] == "0" and d["group"] == "news" for _, d in http.posted)


@pytest.mark.asyncio
async def test_reingest_channel_not_found_errors_no_posts():
    http = _RecHTTP()
    tally, errors = await reingest_channels(
        _TG({}), http, dialogs=[], channels=["@nope"], spec=None,
        group_map={}, limit=10, api_base="http://a", api_key="k",
        queue=None, priority=0,
    )
    assert http.posted == []
    assert len(errors) == 1 and "not found" in errors[0]


@pytest.mark.asyncio
async def test_reingest_channel_not_in_folder_errors_no_posts():
    dialog = _FakeDialog(-100222, "b", username="chan_b")
    http = _RecHTTP()
    tally, errors = await reingest_channels(
        _TG({"b": [_FakeMsg(1, "x", datetime(2024, 1, 1, tzinfo=UTC))]}),
        http, dialogs=[dialog], channels=["@chan_b"],
        spec={"include_ids": set(), "exclude_ids": set(),
              "groups": False, "broadcasts": False},  # not a member
        group_map={}, limit=10, api_base="http://a", api_key="k",
        queue=None, priority=0,
    )
    assert http.posted == []
    assert len(errors) == 1 and "not in" in errors[0]


@pytest.mark.asyncio
async def test_reingest_matches_by_numeric_id_when_no_spec():
    dialog = _FakeDialog(-100333, "c", username=None)
    tg = _TG({"c": [_FakeMsg(7, "hi", datetime(2024, 1, 1, tzinfo=UTC))]})
    http = _RecHTTP()
    tally, errors = await reingest_channels(
        tg, http, dialogs=[dialog], channels=["-100333"], spec=None,
        group_map={}, limit=10, api_base="http://a", api_key="k",
        queue=None, priority=0,
    )
    assert errors == []
    assert [f for f, _ in http.posted] == ["tg_-100333_7.txt"]


def test_select_mode_prefers_reingest():
    args = SimpleNamespace(reingest="@a", channels="@b")
    assert select_mode(args) == "reingest"


def test_select_mode_backfill_when_channels_only():
    assert select_mode(SimpleNamespace(reingest=None, channels="@b")) == "backfill"


def test_select_mode_sync_by_default():
    assert select_mode(SimpleNamespace(reingest=None, channels=None)) == "sync"
