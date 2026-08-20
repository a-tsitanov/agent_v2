"""Command handlers, independent of aiogram.

Each returns the reply TEXT and takes its dependencies through ``Ctx``,
so the whole surface is testable without a Telegram server; the aiogram
wiring in ``__main__`` only maps a message to one of these.

Every path through `/ask` and `/find` — including each refusal — writes a
row to `bot_request`. The order is fixed and the tests pin it:
admission → quota → gate → row → work → finish row → release.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from loguru import logger

from src.bot import format as fmt
from src.bot.policy import (
    BUSY_MESSAGE,
    ConcurrencyGate,
    admit,
    check_quota,
    is_admin,
)
from src.bot.session import Turn

SEARCH_ERROR = "Не удалось получить ответ из базы. Попробуйте ещё раз."
NOT_ADMIN = "Команда доступна только администратору."
BAD_ARGS = "Нужен аргумент. Пример: {example}"
NO_SUCH_REQUEST = "Запрос с таким номером не найден."
NOT_YOUR_REQUEST = "Это чужой запрос."

HELP = """Команды:

/ask <вопрос> — поиск с готовым ответом (около минуты)
/agent <вопрос> — умный агент: сам подбирает и комбинирует инструменты (дольше)
/entity <имя> — что за сущность, по началу имени (быстро)
/find <запрос> — те же материалы, но текстом первоисточников, без пересказа
   (чуть быстрее /ask, но не мгновенно — работа та же, кроме финального пересказа)
/channels — что загружено, по каналам
/volume [канал] — динамика по дням
/history — мои последние запросы
/repeat <номер> — повторить запрос из истории
/whoami — мой статус и лимит

Просто написанный текст без команды считается вопросом."""


@dataclass
class Ctx:
    """Everything a handler needs, injected."""

    repo: Any
    search: Callable[[str], Awaitable[dict]]
    find: Callable[[str], Awaitable[dict]]
    channels: Callable[[], Awaitable[list[dict]]]
    timeline: Callable[..., Awaitable[list[dict]]]
    gate: ConcurrencyGate
    entities: Callable[..., Awaitable[dict]] | None = None
    agent: Callable[[str], Awaitable[str]] | None = None
    # Follow-up context. Both already existed, unit-tested, and were left
    # unwired when the pipeline was replaced by these handlers — so "а что
    # ещё про это?" was answered as a fresh question.
    session: Any = None
    rewrite: Callable[..., Awaitable[str]] | None = None
    default_quota: int = 20


async def _load_user(ctx: Ctx, user_id: int, username: str = "") -> dict | None:
    try:
        return await ctx.repo.get_or_create_user(user_id, username)
    except Exception as exc:
        # A store outage must not read as "this user is fine".
        logger.warning("bot: cannot load user {u}: {e}", u=user_id, e=exc)
        return None


async def _gate_checks(
    ctx: Ctx, *, user: dict | None, user_id: int, chat_id: int,
    command: str, args: str,
) -> str | None:
    """Admission + quota. Returns a refusal message, or None to proceed.

    A refusal is recorded as a `denied` row before it is returned — the
    audit is of what was ATTEMPTED, not only of what ran.
    """
    decision = admit(user, user_id=user_id)
    if not decision.allowed:
        # An unknown user has no row to reference yet; `get_or_create_user`
        # made one unless the store itself is down, in which case there is
        # nothing to write to either.
        if user is not None:
            await _record_refusal(ctx, user_id, chat_id, command, args)
        return decision.message

    quota = int(user.get("daily_quota") or ctx.default_quota)
    used = await ctx.repo.count_requests_today(user_id)
    decision = check_quota(used, quota)
    if not decision.allowed:
        await _record_refusal(ctx, user_id, chat_id, command, args)
        return decision.message
    return None


async def _record_refusal(
    ctx: Ctx, user_id: int, chat_id: int, command: str, args: str,
) -> None:
    try:
        await ctx.repo.start_request(
            telegram_id=user_id, chat_id=chat_id, command=command,
            args=args, status="denied",
        )
    except Exception as exc:
        logger.warning("bot: cannot record refusal: {e}", e=exc)


async def _run_search(
    ctx: Ctx, *, user_id: int, chat_id: int, command: str, query: str,
    fn: Callable[[str], Awaitable[dict]], render: Callable[[dict], str],
) -> str:
    """The shared body of `/ask` and `/find`: gate, row, work, finish."""
    if not ctx.gate.try_acquire():
        await _record_refusal(ctx, user_id, chat_id, command, query)
        return BUSY_MESSAGE

    request_id = None
    try:
        request_id = await ctx.repo.start_request(
            telegram_id=user_id, chat_id=chat_id, command=command, args=query,
        )
        result = await fn(query)
        text = render(result)
        await ctx.repo.finish_request(
            request_id, status="done", answer=text,
            sources=result.get("sources") or [],
        )
        return text
    except Exception as exc:
        logger.warning("bot: {c} failed: {e}", c=command, e=exc)
        if request_id is not None:
            try:
                await ctx.repo.finish_request(
                    request_id, status="failed", error=str(exc)[:500],
                )
            except Exception:
                logger.warning("bot: cannot finish request {r}", r=request_id)
        return SEARCH_ERROR
    finally:
        # In `finally`, not after the return: an exception that skipped
        # this would leak the slot, and N of those wedge the bot for good.
        ctx.gate.release()


async def handle_ask(
    ctx: Ctx, *, user_id: int, chat_id: int, query: str, username: str = "",
    command: str = "/ask",
) -> str:
    query = (query or "").strip()
    if not query:
        return BAD_ARGS.format(example="/ask что писали про урожай")
    user = await _load_user(ctx, user_id, username)
    refusal = await _gate_checks(
        ctx, user=user, user_id=user_id, chat_id=chat_id, command=command, args=query,
    )
    if refusal is not None:
        return refusal

    # Resolve the follow-up against this chat's recent turns. Fail-soft:
    # a rewrite outage should cost context, not the answer.
    standalone = query
    if ctx.session is not None and ctx.rewrite is not None:
        try:
            standalone = await ctx.rewrite(ctx.session.load(chat_id), query) or query
        except Exception as exc:
            logger.warning("bot: rewrite failed, asking as-is: {e}", e=exc)

    answer = await _run_search(
        ctx, user_id=user_id, chat_id=chat_id, command=command, query=standalone,
        fn=ctx.search,
        render=lambda r: fmt.format_answer(r.get("answer") or "", r.get("sources")),
    )
    if ctx.session is not None and answer != SEARCH_ERROR:
        # Persist what the user TYPED, not the rewritten query — later
        # rewrites must see the real conversation.
        ctx.session.append(chat_id, Turn(role="user", text=query))
        ctx.session.append(chat_id, Turn(role="assistant", text=answer))
    return answer


async def handle_find(
    ctx: Ctx, *, user_id: int, chat_id: int, query: str, username: str = "",
) -> str:
    query = (query or "").strip()
    if not query:
        return BAD_ARGS.format(example="/find зерновая сделка")
    user = await _load_user(ctx, user_id, username)
    refusal = await _gate_checks(
        ctx, user=user, user_id=user_id, chat_id=chat_id, command="/find", args=query,
    )
    if refusal is not None:
        return refusal
    return await _run_search(
        ctx, user_id=user_id, chat_id=chat_id, command="/find", query=query,
        fn=ctx.find,
        render=lambda r: fmt.format_fragments(r.get("sources") or []),
    )


async def handle_agent(
    ctx: Ctx, *, user_id: int, chat_id: int, query: str, username: str = "",
) -> str:
    """Free-form question to the openclaw agent, which chains MCP tools.
    Same cost as /ask (a slot + quota); openclaw keeps its own memory, so
    no session/rewrite here."""
    query = (query or "").strip()
    if not query:
        return BAD_ARGS.format(example="/agent кто связан с зерновой сделкой")
    if ctx.agent is None:
        return SEARCH_ERROR
    user = await _load_user(ctx, user_id, username)
    refusal = await _gate_checks(
        ctx, user=user, user_id=user_id, chat_id=chat_id, command="/agent", args=query,
    )
    if refusal is not None:
        return refusal
    return await _run_search(
        ctx, user_id=user_id, chat_id=chat_id, command="/agent", query=query,
        fn=lambda q: _wrap_agent(ctx, q, session=f"tg:{chat_id}"),
        render=lambda r: r.get("answer") or SEARCH_ERROR,
    )


async def _wrap_agent(ctx: Ctx, q: str, *, session: str) -> dict:
    # _run_search expects {answer, sources}; the agent returns prose only.
    # `session` = per-chat sessionKey so openclaw carries follow-ups.
    return {"answer": await ctx.agent(q, session=session), "sources": []}


async def handle_channels(ctx: Ctx, *, user_id: int, username: str = "") -> str:
    user = await _load_user(ctx, user_id, username)
    decision = admit(user, user_id=user_id)
    if not decision.allowed:
        return decision.message
    try:
        return fmt.format_channels(await ctx.channels())
    except Exception as exc:
        logger.warning("bot: /channels failed: {e}", e=exc)
        return SEARCH_ERROR


async def handle_volume(
    ctx: Ctx, *, user_id: int, channel: str = "", username: str = "",
) -> str:
    user = await _load_user(ctx, user_id, username)
    decision = admit(user, user_id=user_id)
    if not decision.allowed:
        return decision.message
    try:
        buckets = await ctx.timeline(channel=channel or None)
        return fmt.format_timeline(buckets, channel=channel)
    except Exception as exc:
        logger.warning("bot: /volume failed: {e}", e=exc)
        return SEARCH_ERROR


async def handle_entity(
    ctx: Ctx, *, user_id: int, query: str, username: str = "",
) -> str:
    """Entity lookup by name prefix. Cheap — no concurrency slot, no
    quota: it is an index read, not a search, and rationing it would be
    rationing the one fast content command the bot has."""
    user = await _load_user(ctx, user_id, username)
    decision = admit(user, user_id=user_id)
    if not decision.allowed:
        return decision.message
    query = (query or "").strip()
    if not query:
        return BAD_ARGS.format(example="/entity Украина")
    if ctx.entities is None:
        return SEARCH_ERROR
    try:
        return fmt.format_entities(await ctx.entities(query), query=query)
    except Exception as exc:
        logger.warning("bot: /entity failed: {e}", e=exc)
        return SEARCH_ERROR


async def handle_history(ctx: Ctx, *, user_id: int, username: str = "") -> str:
    user = await _load_user(ctx, user_id, username)
    decision = admit(user, user_id=user_id)
    if not decision.allowed:
        return decision.message
    return fmt.format_history(await ctx.repo.recent_requests(user_id, 10))


async def handle_repeat(
    ctx: Ctx, *, user_id: int, chat_id: int, raw_id: str, username: str = "",
) -> str:
    """Re-run a stored request. Fresh work, and it costs quota again."""
    user = await _load_user(ctx, user_id, username)
    decision = admit(user, user_id=user_id)
    if not decision.allowed:
        return decision.message
    try:
        request_id = int((raw_id or "").strip().lstrip("#"))
    except ValueError:
        return BAD_ARGS.format(example="/repeat 42")

    row = await ctx.repo.get_request(request_id)
    if row is None:
        return NO_SUCH_REQUEST
    if int(row.get("telegram_id") or 0) != user_id:
        # The audit trail must not become a way to read other people's
        # questions.
        return NOT_YOUR_REQUEST

    command = row.get("command") or "/ask"
    args = row.get("args") or ""
    if command == "/find":
        return await handle_find(
            ctx, user_id=user_id, chat_id=chat_id, query=args, username=username,
        )
    return await handle_ask(
        ctx, user_id=user_id, chat_id=chat_id, query=args, username=username,
        command=command,
    )


async def handle_whoami(ctx: Ctx, *, user_id: int, username: str = "") -> str:
    user = await _load_user(ctx, user_id, username)
    if user is None:
        return f"Ваш ID: {user_id}. Хранилище недоступно, попробуйте позже."
    used = await ctx.repo.count_requests_today(user_id)
    quota = int(user.get("daily_quota") or ctx.default_quota)
    limit = "без ограничений" if quota <= 0 else f"{used} из {quota}"
    return (
        f"ID: {user_id}\nСтатус: {user.get('status')}\n"
        f"Роль: {user.get('role')}\nЗапросов сегодня: {limit}"
    )


# ── admin ────────────────────────────────────────────────────────────


async def handle_users(ctx: Ctx, *, user_id: int) -> str:
    user = await _load_user(ctx, user_id)
    if not is_admin(user):
        return NOT_ADMIN
    return fmt.format_users(await ctx.repo.list_users())


async def _set_status(ctx: Ctx, *, user_id: int, raw_id: str, status: str) -> str:
    user = await _load_user(ctx, user_id)
    if not is_admin(user):
        return NOT_ADMIN
    try:
        target = int((raw_id or "").strip())
    except ValueError:
        return BAD_ARGS.format(example="/approve 123456789")
    await ctx.repo.get_or_create_user(target)
    await ctx.repo.set_status(target, status, approved_by=user_id)
    return f"Пользователь {target}: {status}"


async def handle_approve(ctx: Ctx, *, user_id: int, raw_id: str) -> str:
    return await _set_status(ctx, user_id=user_id, raw_id=raw_id, status="active")


async def handle_deny(ctx: Ctx, *, user_id: int, raw_id: str) -> str:
    return await _set_status(ctx, user_id=user_id, raw_id=raw_id, status="blocked")


__all__ = [
    "BAD_ARGS",
    "HELP",
    "NOT_ADMIN",
    "NOT_YOUR_REQUEST",
    "NO_SUCH_REQUEST",
    "SEARCH_ERROR",
    "Ctx",
    "handle_agent",
    "handle_approve",
    "handle_ask",
    "handle_channels",
    "handle_deny",
    "handle_entity",
    "handle_find",
    "handle_history",
    "handle_repeat",
    "handle_users",
    "handle_volume",
    "handle_whoami",
]
