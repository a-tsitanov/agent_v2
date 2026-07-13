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
from src.bot.llm_rewrite import make_rewrite
from src.bot.pipeline import answer_question
from src.bot.search_client import make_search
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
    search = make_search(
        api_base=cfg.api_base, api_key=_api_key(),
        mode=cfg.search_mode, timeout_s=cfg.search_timeout_s,
    )

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
            search=search,
        )
        for chunk in _split(reply):
            await message.answer(chunk)

    logger.info(
        "kb-bot starting: {n} whitelisted user(s), search mode={m}, api={a}",
        n=len(allowed), m=cfg.search_mode, a=cfg.api_base,
    )
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
