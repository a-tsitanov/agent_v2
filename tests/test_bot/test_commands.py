"""Command handlers: admission, quota, the gate, and the audit around them.

The order is the thing being pinned — admission, then quota, then the
gate, then the row, then the work — together with the rule that EVERY
attempt leaves a row, refusals included.
"""

from __future__ import annotations

from typing import Any

import pytest

from src.bot.commands import (
    NO_SUCH_REQUEST,
    NOT_ADMIN,
    NOT_YOUR_REQUEST,
    SEARCH_ERROR,
    Ctx,
    handle_agent,
    handle_approve,
    handle_ask,
    handle_channels,
    handle_entity,
    handle_find,
    handle_history,
    handle_repeat,
    handle_users,
    handle_whoami,
)
from src.bot.policy import ConcurrencyGate

USER = 42


class _Repo:
    """Records what the handlers wrote."""

    def __init__(self, user: dict | None = None, used_today: int = 0,
                 requests: list[dict] | None = None) -> None:
        self._user = user if user is not None else {
            "telegram_id": USER, "status": "active", "role": "client", "daily_quota": 20,
        }
        self.used_today = used_today
        self.started: list[dict] = []
        self.finished: list[dict] = []
        self.statuses: list[tuple] = []
        self._requests = {r["id"]: r for r in (requests or [])}
        self._next_id = 100

    async def get_or_create_user(self, telegram_id, username=""):
        return self._user

    async def count_requests_today(self, telegram_id):
        return self.used_today

    async def start_request(self, *, telegram_id, chat_id, command, args="",
                            status="running"):
        self._next_id += 1
        self.started.append({"id": self._next_id, "command": command,
                             "args": args, "status": status})
        return self._next_id

    async def finish_request(self, request_id, *, status, answer="", sources=None,
                             error=""):
        self.finished.append({"id": request_id, "status": status, "answer": answer,
                              "sources": sources or [], "error": error})

    async def recent_requests(self, telegram_id, limit=10):
        return list(self._requests.values())

    async def get_request(self, request_id):
        return self._requests.get(request_id)

    async def list_users(self, status=None):
        return [self._user]

    async def set_status(self, telegram_id, status, *, approved_by=None):
        self.statuses.append((telegram_id, status, approved_by))


def _ctx(repo: _Repo, *, gate_limit: int = 2, search=None, find=None,
         channels=None, timeline=None) -> Ctx:
    async def _search(q):
        return {"answer": f"ответ на {q}", "sources": [{"chunk_id": "c1"}]}

    async def _find(q):
        return {"answer": "", "sources": [{"text": "фрагмент"}]}

    async def _channels():
        return [{"key": "tass", "total": 10, "completed": 10}]

    async def _timeline(**kw):
        return [{"day": "2026-08-01", "count": 3}]

    return Ctx(
        repo=repo, search=search or _search, find=find or _find,
        channels=channels or _channels, timeline=timeline or _timeline,
        gate=ConcurrencyGate(gate_limit),
    )


async def _ask(ctx: Ctx, query: str = "вопрос") -> str:
    return await handle_ask(ctx, user_id=USER, chat_id=1, query=query)


# ── admission ────────────────────────────────────────────────────────


async def test_pending_user_is_refused_without_searching():
    called: list[str] = []

    async def _search(q):
        called.append(q)
        return {"answer": "не должно случиться", "sources": []}

    repo = _Repo(user={"telegram_id": USER, "status": "pending", "role": "client"})
    out = await _ask(_ctx(repo, search=_search))
    assert "не одобрена" in out
    assert called == []


async def test_a_refused_attempt_is_still_recorded():
    """The audit is of what was attempted, not only of what ran."""
    repo = _Repo(user={"telegram_id": USER, "status": "pending", "role": "client"})
    await _ask(_ctx(repo))
    assert [r["status"] for r in repo.started] == ["denied"]


async def test_blocked_user_is_refused():
    repo = _Repo(user={"telegram_id": USER, "status": "blocked", "role": "client"})
    assert "заблокирован" in (await _ask(_ctx(repo))).lower()


# ── quota ────────────────────────────────────────────────────────────


async def test_over_quota_is_refused_without_searching_and_recorded():
    called: list[str] = []

    async def _search(q):
        called.append(q)
        return {"answer": "x", "sources": []}

    repo = _Repo(used_today=20)
    out = await _ask(_ctx(repo, search=_search))
    assert "лимит" in out.lower()
    assert called == []
    assert [r["status"] for r in repo.started] == ["denied"]


async def test_under_quota_proceeds():
    repo = _Repo(used_today=19)
    assert "ответ на" in await _ask(_ctx(repo))


# ── the gate ─────────────────────────────────────────────────────────


async def test_a_full_gate_refuses_without_searching_and_records_it():
    called: list[str] = []

    async def _search(q):
        called.append(q)
        return {"answer": "x", "sources": []}

    repo = _Repo()
    ctx = _ctx(repo, gate_limit=1, search=_search)
    ctx.gate.try_acquire()          # someone else is already searching
    out = await _ask(ctx)
    assert "подождите" in out.lower()
    assert called == []
    assert [r["status"] for r in repo.started] == ["denied"]


async def test_the_slot_is_released_after_a_successful_search():
    repo = _Repo()
    ctx = _ctx(repo, gate_limit=1)
    await _ask(ctx)
    assert ctx.gate.in_flight == 0


async def test_the_slot_is_released_when_the_search_raises():
    """Otherwise N failures wedge the bot permanently."""
    async def _boom(q):
        raise RuntimeError("search blew up")

    repo = _Repo()
    ctx = _ctx(repo, gate_limit=1, search=_boom)
    for _ in range(3):
        assert await _ask(ctx) == SEARCH_ERROR
    assert ctx.gate.in_flight == 0


# ── the happy path and its audit ─────────────────────────────────────


async def test_ask_records_the_row_before_the_work_and_finishes_it_after():
    repo = _Repo()
    out = await _ask(_ctx(repo), "что нового")
    assert "ответ на что нового" in out
    assert repo.started[0]["status"] == "running"
    assert repo.started[0]["args"] == "что нового"
    assert repo.finished[0]["status"] == "done"


async def test_ask_stores_the_sources_with_the_answer():
    """So /history can show an old answer with its provenance without
    re-running anything."""
    repo = _Repo()
    await _ask(_ctx(repo))
    assert repo.finished[0]["sources"] == [{"chunk_id": "c1"}]


async def test_a_failed_search_is_recorded_as_failed_with_the_reason():
    async def _boom(q):
        raise RuntimeError("upstream 500")

    repo = _Repo()
    assert await _ask(_ctx(repo, search=_boom)) == SEARCH_ERROR
    assert repo.finished[0]["status"] == "failed"
    assert "upstream 500" in repo.finished[0]["error"]


async def test_empty_query_asks_for_one_and_writes_nothing():
    repo = _Repo()
    out = await _ask(_ctx(repo), "   ")
    assert "Нужен аргумент" in out
    assert repo.started == []


async def test_find_renders_fragments_and_is_recorded_as_find():
    repo = _Repo()
    out = await handle_find(_ctx(repo), user_id=USER, chat_id=1, query="зерно")
    assert "фрагмент" in out
    assert repo.started[0]["command"] == "/find"


# ── /repeat ──────────────────────────────────────────────────────────


async def test_repeat_reruns_the_stored_query():
    repo = _Repo(requests=[{"id": 7, "telegram_id": USER, "command": "/ask",
                            "args": "старый вопрос"}])
    out = await handle_repeat(_ctx(repo), user_id=USER, chat_id=1, raw_id="7")
    assert "ответ на старый вопрос" in out


async def test_repeat_refuses_another_users_request():
    """The audit trail must not become a way to read other people's
    questions."""
    repo = _Repo(requests=[{"id": 7, "telegram_id": 999, "command": "/ask",
                            "args": "чужой вопрос"}])
    out = await handle_repeat(_ctx(repo), user_id=USER, chat_id=1, raw_id="7")
    assert out == NOT_YOUR_REQUEST
    assert repo.started == []


async def test_repeat_on_a_missing_id():
    repo = _Repo()
    assert await handle_repeat(
        _ctx(repo), user_id=USER, chat_id=1, raw_id="999") == NO_SUCH_REQUEST


async def test_repeat_rejects_a_non_numeric_id():
    repo = _Repo()
    out = await handle_repeat(_ctx(repo), user_id=USER, chat_id=1, raw_id="abc")
    assert "Нужен аргумент" in out


async def test_repeat_costs_quota_again():
    """It re-runs the work; it does not replay a stored answer."""
    repo = _Repo(used_today=20,
                 requests=[{"id": 7, "telegram_id": USER, "command": "/ask",
                            "args": "вопрос"}])
    out = await handle_repeat(_ctx(repo), user_id=USER, chat_id=1, raw_id="7")
    assert "лимит" in out.lower()


# ── the rest ─────────────────────────────────────────────────────────


async def test_channels_and_history_need_admission():
    repo = _Repo(user={"telegram_id": USER, "status": "pending", "role": "client"})
    ctx = _ctx(repo)
    assert "не одобрена" in await handle_channels(ctx, user_id=USER)
    assert "не одобрена" in await handle_history(ctx, user_id=USER)


async def test_channels_fail_soft():
    async def _boom():
        raise RuntimeError("api down")

    assert await handle_channels(
        _ctx(_Repo(), channels=_boom), user_id=USER) == SEARCH_ERROR


async def test_whoami_reports_status_and_remaining_quota():
    out = await handle_whoami(_ctx(_Repo(used_today=3)), user_id=USER)
    assert "active" in out and "3 из 20" in out


# ── admin ────────────────────────────────────────────────────────────


async def test_admin_commands_refuse_a_client():
    repo = _Repo()
    ctx = _ctx(repo)
    assert await handle_users(ctx, user_id=USER) == NOT_ADMIN
    assert await handle_approve(ctx, user_id=USER, raw_id="5") == NOT_ADMIN
    assert repo.statuses == []


async def test_admin_can_approve():
    repo = _Repo(user={"telegram_id": USER, "status": "active", "role": "admin"})
    out = await handle_approve(_ctx(repo), user_id=USER, raw_id="5")
    assert repo.statuses == [(5, "active", USER)]
    assert "5" in out


@pytest.mark.parametrize("bad", ["", "abc", "  "])
async def test_approve_rejects_a_non_numeric_id(bad):
    repo = _Repo(user={"telegram_id": USER, "status": "active", "role": "admin"})
    out = await handle_approve(_ctx(repo), user_id=USER, raw_id=bad)
    assert "Нужен аргумент" in out
    assert repo.statuses == []


# ── the store itself failing ─────────────────────────────────────────


async def test_a_store_outage_denies_rather_than_admits():
    """Fail closed all the way down: if the user cannot be loaded, they
    are not assumed to be fine."""
    class _Broken(_Repo):
        async def get_or_create_user(self, telegram_id, username=""):
            raise RuntimeError("postgres down")

    repo: Any = _Broken()
    out = await _ask(_ctx(repo))
    assert "не зарегистрированы" in out.lower()
    assert repo.started == []


# ── /entity ──────────────────────────────────────────────────────────


def _ctx_with_entities(repo, fn):
    ctx = _ctx(repo)
    ctx.entities = fn
    return ctx


async def test_entity_needs_admission():
    repo = _Repo(user={"telegram_id": USER, "status": "pending", "role": "client"})
    called: list[str] = []

    async def _e(q, **kw):
        called.append(q)
        return {"entities": []}

    out = await handle_entity(_ctx_with_entities(repo, _e), user_id=USER, query="Украина")
    assert "не одобрена" in out
    assert called == []


async def test_entity_returns_the_matches():
    async def _e(q, **kw):
        return {"entities": [{"entity_name": "Украина", "entity_type": "Country"}]}

    out = await handle_entity(_ctx_with_entities(_Repo(), _e), user_id=USER, query="Украина")
    assert "Украина" in out


async def test_entity_costs_no_quota_and_no_slot():
    """It is an index read, not a search. Rationing it would ration the
    only fast content command the bot has."""
    async def _e(q, **kw):
        return {"entities": []}

    repo = _Repo(used_today=999)          # far over any quota
    ctx = _ctx_with_entities(repo, _e)
    ctx.gate.try_acquire()          # and the gate is full
    ctx.gate.try_acquire()
    out = await handle_entity(ctx, user_id=USER, query="Украина")
    assert "лимит" not in out.lower()
    assert "подождите" not in out.lower()
    assert repo.started == []             # not an audited request either


async def test_entity_empty_query_asks_for_one():
    async def _e(q, **kw):
        raise AssertionError("must not be called")

    out = await handle_entity(_ctx_with_entities(_Repo(), _e), user_id=USER, query=" ")
    assert "Нужен аргумент" in out


async def test_entity_fails_soft():
    async def _boom(q, **kw):
        raise RuntimeError("api down")

    out = await handle_entity(_ctx_with_entities(_Repo(), _boom), user_id=USER, query="X")
    assert out == SEARCH_ERROR


# ── follow-up context ────────────────────────────────────────────────


class _Session:
    def __init__(self):
        self.turns = {}

    def load(self, chat_id):
        return list(self.turns.get(chat_id, []))

    def append(self, chat_id, turn):
        self.turns.setdefault(chat_id, []).append(turn)


def _ctx_chat(repo, *, rewrite=None, search=None):
    ctx = _ctx(repo, search=search)
    ctx.session = _Session()
    ctx.rewrite = rewrite
    return ctx


async def test_followup_is_rewritten_before_searching():
    seen = {}

    async def _rw(history, question):
        seen["history"] = list(history)
        return "полный вопрос про урожай"

    async def _search(q):
        seen["asked"] = q
        return {"answer": "ответ", "sources": []}

    ctx = _ctx_chat(_Repo(), rewrite=_rw, search=_search)
    await handle_ask(ctx, user_id=USER, chat_id=1, query="а что ещё?")
    assert seen["asked"] == "полный вопрос про урожай"


async def test_session_keeps_what_the_user_typed_not_the_rewrite():
    """Later rewrites must see the real conversation."""
    async def _rw(history, question):
        return "переписанный"

    ctx = _ctx_chat(_Repo(), rewrite=_rw)
    await handle_ask(ctx, user_id=USER, chat_id=1, query="исходный")
    assert ctx.session.load(1)[0].text == "исходный"


async def test_rewrite_failure_costs_context_not_the_answer():
    async def _boom(history, question):
        raise RuntimeError("llm down")

    ctx = _ctx_chat(_Repo(), rewrite=_boom)
    out = await handle_ask(ctx, user_id=USER, chat_id=1, query="вопрос")
    assert "ответ на вопрос" in out


async def test_a_failed_search_is_not_persisted():
    """A broken turn would poison every later rewrite."""
    async def _boom(q):
        raise RuntimeError("search down")

    ctx = _ctx_chat(_Repo(), search=_boom)
    await handle_ask(ctx, user_id=USER, chat_id=1, query="вопрос")
    assert ctx.session.load(1) == []


# ── /agent (openclaw bridge) ─────────────────────────────────────────


def _ctx_agent(repo, fn):
    ctx = _ctx(repo)
    ctx.agent = fn
    return ctx


async def test_agent_returns_the_prose_and_records_it():
    async def _a(q):
        return f"агент ответил на {q}"

    repo = _Repo()
    out = await handle_agent(_ctx_agent(repo, _a), user_id=USER, chat_id=1, query="вопрос")
    assert "агент ответил на вопрос" in out
    assert repo.started[0]["command"] == "/agent"
    assert repo.finished[0]["status"] == "done"


async def test_agent_costs_a_slot_and_quota_like_ask():
    async def _a(q):
        raise AssertionError("must not run")

    repo = _Repo(used_today=20)
    out = await handle_agent(_ctx_agent(repo, _a), user_id=USER, chat_id=1, query="x")
    assert "лимит" in out.lower()
    assert repo.started[-1]["status"] == "denied"


async def test_agent_fails_soft():
    async def _boom(q):
        raise RuntimeError("openclaw down")

    repo = _Repo()
    out = await handle_agent(_ctx_agent(repo, _boom), user_id=USER, chat_id=1, query="x")
    assert out == SEARCH_ERROR
    assert repo.finished[0]["status"] == "failed"


async def test_agent_needs_admission():
    async def _a(q):
        raise AssertionError("must not run")

    repo = _Repo(user={"telegram_id": USER, "status": "pending", "role": "client"})
    out = await handle_agent(_ctx_agent(repo, _a), user_id=USER, chat_id=1, query="x")
    assert "не одобрена" in out
