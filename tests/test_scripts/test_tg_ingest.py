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


# ── continuous sync mode (channels+groups, dedup across restarts) ────


class _FakeEntity:
    def __init__(self, username=None):
        self.username = username


class _FakeDialog:
    def __init__(self, id, *, is_user=False, is_group=False, is_channel=False,
                 username=None, title="t"):
        self.id = id
        self.is_user = is_user
        self.is_group = is_group
        self.is_channel = is_channel
        self.entity = _FakeEntity(username)
        self.title = title


def test_select_dialogs_keeps_groups_and_channels_drops_users():
    from scripts.tg_ingest import select_dialogs

    dialogs = [
        _FakeDialog(1, is_user=True),                      # личка — вон
        _FakeDialog(2, is_group=True),                     # группа — берём
        _FakeDialog(3, is_channel=True),                   # канал — берём
        _FakeDialog(4, is_group=True, is_channel=True),    # мегагруппа — берём
        _FakeDialog(5),                                    # ничто — вон
    ]
    assert [d.id for d in select_dialogs(dialogs)] == [2, 3, 4]


def test_dialog_slug_prefers_username_falls_back_to_id():
    from scripts.tg_ingest import dialog_slug

    assert dialog_slug(_FakeDialog(7, username="foo_chan")) == "foo_chan"
    assert dialog_slug(_FakeDialog(-100123, username=None)) == "-100123"


def test_state_roundtrip(tmp_path):
    from scripts.tg_ingest import load_state, save_state

    p = tmp_path / "state.json"
    assert load_state(p) == {}                    # нет файла → пусто
    save_state(p, {"2": {"last_id": 10, "title": "чат"}})
    assert load_state(p) == {"2": {"last_id": 10, "title": "чат"}}


class _SyncTG:
    """iter_messages( entity, min_id=…, reverse=True ) → заготовленные msgs."""

    def __init__(self, per_dialog):
        self.per_dialog = per_dialog     # dialog_id -> list[_FakeMsg]
        self.calls = []                  # (dialog_id, kwargs)

    def _msgs_for(self, entity_key, min_id=0, limit=None):
        msgs = [m for m in self.per_dialog[entity_key] if m.id > min_id]
        if limit is not None:
            msgs = sorted(msgs, key=lambda m: -m.id)[:limit]
        return sorted(msgs, key=lambda m: m.id)

    def iter_messages(self, entity, **kw):
        self.calls.append((entity._key, kw))
        msgs = self._msgs_for(entity._key, kw.get("min_id", 0), kw.get("limit"))

        async def _gen():
            for m in msgs:
                yield m
        return _gen()


def _sync_dialog(id, msgs_key, username=None):
    d = _FakeDialog(id, is_channel=True, username=username)
    d.entity._key = msgs_key
    return d


class _OkHTTP:
    def __init__(self):
        self.posted = []

    async def post(self, url, headers=None, files=None, data=None):
        self.posted.append((files["file"][0], data["document_date"]))

        class _R:
            status_code = 202
        return _R()


@pytest.mark.asyncio
async def test_sync_round_ingests_only_new_and_advances_state():
    from scripts.tg_ingest import sync_round

    tg = _SyncTG({"a": [
        _FakeMsg(1, "old", datetime(2024, 1, 1, tzinfo=UTC)),
        _FakeMsg(2, "new1", datetime(2024, 1, 2, tzinfo=UTC)),
        _FakeMsg(3, "new2", datetime(2024, 1, 3, tzinfo=UTC)),
    ]})
    http = _OkHTTP()
    state = {"10": {"last_id": 1, "title": "A"}}
    saves = []

    tally = await sync_round(
        tg, http, dialogs=[_sync_dialog(10, "a", "chan_a")],
        state=state, save=lambda: saves.append(dict(state)),
        api_base="http://a", api_key="k", queue=None, bootstrap_limit=100,
    )
    # старый (id=1) не переслан — только 2 и 3, state дошёл до 3
    assert [f for f, _ in http.posted] == ["tg_chan_a_2.txt", "tg_chan_a_3.txt"]
    assert state["10"]["last_id"] == 3
    assert tally["sent"] == 2 and tally["failed"] == 0
    assert len(saves) >= 2                        # state сохранялся по ходу
    # запрос шёл с min_id=1 (доборка), не полная история
    assert tg.calls[0][1].get("min_id") == 1


@pytest.mark.asyncio
async def test_sync_round_bootstrap_new_dialog_respects_limit():
    from scripts.tg_ingest import sync_round

    msgs = [_FakeMsg(i, f"m{i}", datetime(2024, 1, 1, tzinfo=UTC)) for i in range(1, 6)]
    tg = _SyncTG({"b": msgs})
    http = _OkHTTP()
    state = {}

    await sync_round(
        tg, http, dialogs=[_sync_dialog(20, "b", "chan_b")],
        state=state, save=lambda: None,
        api_base="http://a", api_key="k", queue=None, bootstrap_limit=2,
    )
    # новый диалог: только 2 НОВЕЙШИХ (4,5), в хронологическом порядке
    assert [f for f, _ in http.posted] == ["tg_chan_b_4.txt", "tg_chan_b_5.txt"]
    assert state["20"]["last_id"] == 5


@pytest.mark.asyncio
async def test_sync_round_failure_stops_dialog_without_advancing():
    from scripts.tg_ingest import sync_round

    tg = _SyncTG({"c": [
        _FakeMsg(2, "ok", datetime(2024, 1, 1, tzinfo=UTC)),
        _FakeMsg(3, "will-fail", datetime(2024, 1, 2, tzinfo=UTC)),
        _FakeMsg(4, "never-reached", datetime(2024, 1, 3, tzinfo=UTC)),
    ]})

    class _FlakyHTTP:
        def __init__(self):
            self.n = 0

        async def post(self, url, headers=None, files=None, data=None):
            self.n += 1
            if self.n >= 2:
                raise RuntimeError("api down")

            class _R:
                status_code = 202
            return _R()

    state = {"30": {"last_id": 1, "title": "C"}}
    tally = await sync_round(
        tg, _FlakyHTTP(), dialogs=[_sync_dialog(30, "c")],
        state=state, save=lambda: None,
        api_base="http://a", api_key="k", queue=None, bootstrap_limit=100,
    )
    # id=2 прошёл (state=2), id=3 упал → state НЕ двигается, id=4 не трогали
    assert state["30"]["last_id"] == 2
    assert tally["sent"] == 1 and tally["failed"] == 1


@pytest.mark.asyncio
async def test_sync_round_empty_message_advances_state():
    from scripts.tg_ingest import sync_round

    tg = _SyncTG({"d": [_FakeMsg(5, "   ", datetime(2024, 1, 1, tzinfo=UTC))]})
    http = _OkHTTP()
    state = {"40": {"last_id": 4, "title": "D"}}

    tally = await sync_round(
        tg, http, dialogs=[_sync_dialog(40, "d")],
        state=state, save=lambda: None,
        api_base="http://a", api_key="k", queue=None, bootstrap_limit=100,
    )
    # пустое сообщение: не шлём, но state двигаем — иначе вечный повтор
    assert http.posted == []
    assert state["40"]["last_id"] == 5
    assert tally["skipped"] == 1


# ── folder (dialog-filter) selection ─────────────────────────────────


class _FakeTitle:
    """Новые слои TL: title = TextWithEntities(.text), старые — str."""

    def __init__(self, text):
        self.text = text


class _FakeFolder:
    def __init__(self, id, title, include=(), pinned=(), exclude=(),
                 groups=False, broadcasts=False):
        self.id = id
        self.title = title
        self.include_peers = list(include)
        self.pinned_peers = list(pinned)
        self.exclude_peers = list(exclude)
        self.groups = groups
        self.broadcasts = broadcasts


def _peer(marked_id):
    return ("peer", marked_id)


def _peer_id(p):
    return p[1]


def test_filter_title_handles_str_and_textwithentities():
    from scripts.tg_ingest import _filter_title

    assert _filter_title(_FakeFolder(1, "Новости")) == "Новости"
    assert _filter_title(_FakeFolder(2, _FakeTitle("Работа"))) == "Работа"
    assert _filter_title(object()) == ""          # DialogFilterDefault без title


def test_resolve_folders_matches_case_insensitive_and_reports_missing():
    from scripts.tg_ingest import resolve_folders

    folders = [
        _FakeFolder(1, "Новости", include=[_peer(-100111)], pinned=[_peer(-100222)]),
        _FakeFolder(2, _FakeTitle("Работа"), include=[_peer(-100333)],
                    exclude=[_peer(-100444)], groups=True),
    ]
    spec, missing = resolve_folders(folders, ["новости", "РАБОТА", "Нет такой"], peer_id=_peer_id)
    assert spec["include_ids"] == {-100111, -100222, -100333}
    assert spec["exclude_ids"] == {-100444}
    assert spec["groups"] is True and spec["broadcasts"] is False
    assert missing == ["Нет такой"]


def test_dialog_in_folders_include_exclude_and_flags():
    from scripts.tg_ingest import dialog_in_folders

    spec = {"include_ids": {-100111}, "exclude_ids": {-100999},
            "groups": False, "broadcasts": True}
    chan = _FakeDialog(-100555, is_channel=True)
    grp = _FakeDialog(-100666, is_group=True)
    listed = _FakeDialog(-100111, is_group=True)
    banned = _FakeDialog(-100999, is_channel=True)

    assert dialog_in_folders(listed, spec) is True     # явно в папке
    assert dialog_in_folders(chan, spec) is True       # флаг «каналы»
    assert dialog_in_folders(grp, spec) is False       # групп-флага нет
    assert dialog_in_folders(banned, spec) is False    # исключён из папки


@pytest.mark.asyncio
async def test_post_ingest_includes_priority_when_set():
    sent = {}

    class _Resp:
        status_code = 202

    class _Client:
        async def post(self, url, headers=None, files=None, data=None):
            sent.update(data=data)
            return _Resp()

    ok = await post_ingest(
        _Client(), "http://api", "k", "f.txt", "hi", "2024-03-01", "q1",
        group="news", priority=0,
    )
    assert ok is True
    assert sent["data"]["priority"] == "0"
    assert sent["data"]["group"] == "news"


@pytest.mark.asyncio
async def test_post_ingest_omits_priority_when_none():
    sent = {}

    class _Resp:
        status_code = 202

    class _Client:
        async def post(self, url, headers=None, files=None, data=None):
            sent.update(data=data)
            return _Resp()

    await post_ingest(_Client(), "http://api", "k", "f.txt", "hi", "2024-03-01", "q1")
    assert "priority" not in sent["data"]
