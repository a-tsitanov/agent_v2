"""Telegram Q&A bot entrypoint (`python -m src.bot`).

Long-polls Telegram; every message runs through the KB answer pipeline
(whitelist → session → follow-up rewrite → search API → reply). Answers come
ONLY from the KB. Wiring only — the logic lives in the unit-tested modules.
"""
from __future__ import annotations

import asyncio

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from loguru import logger

from src.bot.access import parse_allowed_users
from src.bot.intent import classify_intent
from src.bot.llm_rewrite import make_rewrite
from src.bot.pipeline import answer_question
from src.bot.router import make_router
from src.bot.search_client import make_analyze, make_search, with_fallback
from src.bot.session import InMemorySessionStore
from src.config import settings

_TG_LIMIT = 4000  # Telegram hard cap is 4096; leave headroom.


def _api_key() -> str:
    cfg = settings.bot
    if cfg.api_key:
        return cfg.api_key
    keys = (settings.api.keys or "").split(",")
    return keys[0].strip() if keys and keys[0].strip() else ""


def _split(text: str) -> list[str]:
    text = text or "(пустой ответ)"
    return [text[i : i + _TG_LIMIT] for i in range(0, len(text), _TG_LIMIT)]


async def main() -> None:
    cfg = settings.bot
    if not cfg.token:
        raise SystemExit("BOT_TOKEN is not set — get one from @BotFather")
    allowed = parse_allowed_users(cfg.allowed_users)
    if not allowed:
        logger.warning(
            "BOT_ALLOWED_USERS is empty — the bot denies EVERYONE until it's set. "
            "Send /start to the bot to see your user id.",
        )

    session = InMemorySessionStore(max_messages=cfg.max_messages)
    rewrite = make_rewrite()
    api_key = _api_key()
    search = make_search(
        api_base=cfg.api_base, api_key=api_key,
        mode=cfg.search_mode, timeout_s=cfg.search_timeout_s,
    )
    # Fast primary (e.g. auto→global) can return empty for niche queries; fall
    # back to a richer mode (drift) only on a miss so the common path stays fast.
    if cfg.fallback_mode and cfg.fallback_mode != cfg.search_mode:
        fallback = make_search(
            api_base=cfg.api_base, api_key=api_key,
            mode=cfg.fallback_mode, timeout_s=cfg.search_timeout_s,
        )
        search = with_fallback(search, fallback)
    # Route analytical questions (counts/distributions/trends/contradictions) to
    # the graph analytics layer, everything else to retrieval search.
    analyze = make_analyze(
        api_base=cfg.api_base, api_key=api_key, timeout_s=cfg.search_timeout_s,
    )
    answer_source = make_router(classify_intent, analyze, search)

    bot = Bot(token=cfg.token)
    dp = Dispatcher()

    @dp.message(Command("start"))
    async def on_start(message: Message) -> None:
        uid = message.from_user.id if message.from_user else "?"
        await message.answer(
            "Привет! Задавай вопрос по базе знаний — отвечаю только по её содержимому.\n"
            f"Твой Telegram user id: `{uid}`",
        )

    @dp.message()
    async def on_message(message: Message) -> None:
        text = (message.text or "").strip()
        if not text or message.from_user is None:
            return
        await bot.send_chat_action(message.chat.id, "typing")
        reply = await answer_question(
            chat_id=message.chat.id,
            user_id=message.from_user.id,
            question=text,
            session=session,
            allowed=allowed,
            rewrite=rewrite,
            search=answer_source,
        )
        for chunk in _split(reply):
            await message.answer(chunk)

    logger.info(
        "kb-bot starting: {n} whitelisted user(s), mode={m} fallback={f}, api={a}",
        n=len(allowed), m=cfg.search_mode, f=cfg.fallback_mode or "off", a=cfg.api_base,
    )
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
