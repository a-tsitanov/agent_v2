"""Telegram bot entrypoint (`python -m src.bot`).

Wiring only: builds the dependencies, maps each Telegram command to a
handler in ``src.bot.commands``, and sends what the handler returns. All
the logic — admission, quota, the concurrency cap, the audit trail,
formatting — lives in the unit-tested modules.

Answers come ONLY from the knowledge base, via the search API.

NOT wired: the intent router (`router.py` / `intent.py`), which sent
"тренд"/"динамика"/"статистика" questions to `/analyze`. That layer
answers with errors on the nebula backend — `topic_trend` needs `Chunk`
nodes and `MENTIONS` edges that ingest does not write there by design —
and routing a user into it is worse than not offering the route. The
modules stay in the tree; see the 2026-08-17 spec.
"""
from __future__ import annotations

import asyncio

from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandObject
from aiogram.types import Message
from loguru import logger

from src.bot import commands as cmd
from src.bot.agent_client import make_agent
from src.bot.format import split_for_telegram
from src.bot.llm_rewrite import make_rewrite
from src.bot.policy import ConcurrencyGate
from src.bot.search_client import make_search_full
from src.bot.seed import seed_admins
from src.bot.session import InMemorySessionStore
from src.bot.stats_client import make_channels, make_entities, make_timeline
from src.config import settings
from src.storage.bot import BotRepository

_WORKING = "Принял, работаю… (поиск занимает около полутора минут)"


def _api_key() -> str:
    cfg = settings.bot
    if cfg.api_key:
        return cfg.api_key
    keys = (settings.api.keys or "").split(",")
    return keys[0].strip() if keys and keys[0].strip() else ""


def build_ctx() -> cmd.Ctx:
    cfg = settings.bot
    key = _api_key()
    return cmd.Ctx(
        repo=BotRepository(),
        search=make_search_full(
            api_base=cfg.api_base, api_key=key, mode=cfg.search_mode,
            timeout_s=cfg.search_timeout_s, synthesize=True,
        ),
        # `/find` gets the SAME timeout as `/ask`. It is not a fast path:
        # `synthesize=False` skips only the final synthesis LLM, while
        # planning, the sub-question retrievals, the coverage round and
        # the rerank all still run. Measured 2026-08-17 on one query:
        # 46s without synthesis against 61s with it — a quarter off, not
        # an order of magnitude. A 60s cap was tight enough that a
        # slightly longer question timed out.
        find=make_search_full(
            api_base=cfg.api_base, api_key=key, mode=cfg.search_mode,
            timeout_s=cfg.search_timeout_s, synthesize=False,
        ),
        channels=make_channels(api_base=cfg.api_base, api_key=key),
        timeline=make_timeline(api_base=cfg.api_base, api_key=key),
        entities=make_entities(api_base=cfg.api_base, api_key=key),
        session=InMemorySessionStore(max_messages=cfg.max_messages),
        rewrite=make_rewrite(),
        agent=(make_agent(base_url=cfg.agent_base, token=cfg.agent_token or key,
                          timeout_s=cfg.search_timeout_s * 2)
               if cfg.agent_base else None),
        gate=ConcurrencyGate(cfg.max_concurrent),
        default_quota=cfg.default_daily_quota,
    )


async def main() -> None:
    cfg = settings.bot
    if not cfg.token:
        raise SystemExit("BOT_TOKEN is not set — get one from @BotFather")

    ctx = build_ctx()
    seeded = await seed_admins(ctx.repo, cfg.admin_ids)
    if not seeded:
        logger.warning(
            "BOT_ADMIN_IDS is empty — nobody can approve anybody, so every new "
            "user will stay pending forever. Set it and restart.",
        )

    bot = Bot(token=cfg.token)
    dp = Dispatcher()

    def _who(message: Message) -> tuple[int, str]:
        u = message.from_user
        return (u.id if u else 0), (u.username or "" if u else "")

    async def _reply(message: Message, text: str) -> None:
        for chunk in split_for_telegram(text):
            await message.answer(chunk)

    @dp.message(Command("start", "help"))
    async def on_start(message: Message) -> None:
        uid, username = _who(message)
        await ctx.repo.get_or_create_user(uid, username)
        await _reply(message, f"Ваш Telegram ID: {uid}\n\n{cmd.HELP}")

    @dp.message(Command("whoami"))
    async def on_whoami(message: Message) -> None:
        uid, username = _who(message)
        await _reply(message, await cmd.handle_whoami(ctx, user_id=uid, username=username))

    @dp.message(Command("channels"))
    async def on_channels(message: Message) -> None:
        uid, username = _who(message)
        await _reply(message, await cmd.handle_channels(ctx, user_id=uid, username=username))

    @dp.message(Command("volume"))
    async def on_volume(message: Message, command: CommandObject) -> None:
        uid, username = _who(message)
        await _reply(message, await cmd.handle_volume(
            ctx, user_id=uid, channel=(command.args or "").strip(), username=username))

    @dp.message(Command("entity"))
    async def on_entity(message: Message, command: CommandObject) -> None:
        uid, username = _who(message)
        await _reply(message, await cmd.handle_entity(
            ctx, user_id=uid, query=command.args or "", username=username))

    @dp.message(Command("history"))
    async def on_history(message: Message) -> None:
        uid, username = _who(message)
        await _reply(message, await cmd.handle_history(ctx, user_id=uid, username=username))

    @dp.message(Command("users"))
    async def on_users(message: Message) -> None:
        uid, _ = _who(message)
        await _reply(message, await cmd.handle_users(ctx, user_id=uid))

    @dp.message(Command("approve"))
    async def on_approve(message: Message, command: CommandObject) -> None:
        uid, _ = _who(message)
        await _reply(message, await cmd.handle_approve(
            ctx, user_id=uid, raw_id=command.args or ""))

    @dp.message(Command("deny"))
    async def on_deny(message: Message, command: CommandObject) -> None:
        uid, _ = _who(message)
        await _reply(message, await cmd.handle_deny(
            ctx, user_id=uid, raw_id=command.args or ""))

    @dp.message(Command("agent"))
    async def on_agent(message: Message, command: CommandObject) -> None:
        uid, username = _who(message)
        notice = await message.answer(_WORKING)
        text = await cmd.handle_agent(
            ctx, user_id=uid, chat_id=message.chat.id,
            query=command.args or "", username=username)
        chunks = split_for_telegram(text)
        try:
            await notice.edit_text(chunks[0])
        except Exception:
            await message.answer(chunks[0])
        for extra in chunks[1:]:
            await message.answer(extra)

    @dp.message(Command("find"))
    async def on_find(message: Message, command: CommandObject) -> None:
        uid, username = _who(message)
        await bot.send_chat_action(message.chat.id, "typing")
        await _reply(message, await cmd.handle_find(
            ctx, user_id=uid, chat_id=message.chat.id,
            query=command.args or "", username=username))

    async def _ask(message: Message, query: str) -> None:
        uid, username = _who(message)
        # Say something before a 90-second wait, then edit that message
        # into the answer.
        notice = await message.answer(_WORKING)
        await bot.send_chat_action(message.chat.id, "typing")
        text = await cmd.handle_ask(
            ctx, user_id=uid, chat_id=message.chat.id, query=query, username=username)
        chunks = split_for_telegram(text)
        try:
            await notice.edit_text(chunks[0])
        except Exception:
            # An edit can fail (message too old, identical text); sending
            # is more important than reusing the placeholder.
            await message.answer(chunks[0])
        for extra in chunks[1:]:
            await message.answer(extra)

    @dp.message(Command("ask"))
    async def on_ask(message: Message, command: CommandObject) -> None:
        await _ask(message, command.args or "")

    @dp.message(Command("repeat"))
    async def on_repeat(message: Message, command: CommandObject) -> None:
        uid, username = _who(message)
        notice = await message.answer(_WORKING)
        text = await cmd.handle_repeat(
            ctx, user_id=uid, chat_id=message.chat.id,
            raw_id=command.args or "", username=username)
        chunks = split_for_telegram(text)
        try:
            await notice.edit_text(chunks[0])
        except Exception:
            await message.answer(chunks[0])
        for extra in chunks[1:]:
            await message.answer(extra)

    @dp.message()
    async def on_text(message: Message) -> None:
        """Bare text is a question — the bot stays usable without first
        learning the command list."""
        text = (message.text or "").strip()
        if not text or message.from_user is None:
            return
        await _ask(message, text)

    logger.info(
        "kb-bot starting: admins={a} mode={m} max_concurrent={c} quota={q} api={u}",
        a=seeded, m=cfg.search_mode, c=cfg.max_concurrent,
        q=cfg.default_daily_quota, u=cfg.api_base,
    )
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
