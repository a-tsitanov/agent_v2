"""Rendering replies for Telegram.

Pure functions, so the wording and the bounds can be asserted without a
bot. Every one of them is responsible for its own length: Telegram's
4096-character cap is a hard rejection, not a truncation — an over-long
message is not sent at all — and an EMPTY message is rejected too, so
every formatter returns something even for no input.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from src.bot.answers import NO_RESULT_MESSAGE, is_empty_answer

# The hard cap is 4096; leave headroom for the chunking suffix and for
# any markup the caller adds.
TG_LIMIT = 4000

_CHANNELS_SHOWN = 15
_TIMELINE_SHOWN = 30
_SOURCES_SHOWN = 5
_ANSWER_PREVIEW = 60
_ENTITIES_SHOWN = 10


def _clip(text: str, limit: int) -> str:
    text = " ".join((text or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _when(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%d.%m %H:%M")
    return str(value or "")[:16]


def split_for_telegram(text: str, limit: int = TG_LIMIT) -> list[str]:
    """Cut a reply into sendable pieces. Never returns an empty list —
    Telegram rejects an empty message as firmly as an over-long one."""
    text = text or "(пустой ответ)"
    return [text[i : i + limit] for i in range(0, len(text), limit)]


def format_channels(rows: list[dict[str, Any]]) -> str:
    """Ingest volume per channel, biggest first."""
    if not rows:
        return "Каналов пока нет — база пуста."
    ordered = sorted(rows, key=lambda r: int(r.get("total") or 0), reverse=True)
    shown = ordered[:_CHANNELS_SHOWN]
    lines = ["Загружено по каналам:", ""]
    for r in shown:
        total = int(r.get("total") or 0)
        done = int(r.get("completed") or 0)
        lines.append(f"{r.get('key') or '(без канала)'} — {total} ({done} обработано)")
    hidden = len(ordered) - len(shown)
    if hidden > 0:
        # Say what was dropped. A silently truncated list reads as the
        # whole list.
        lines += ["", f"…и ещё {hidden} канал(ов)"]
    lines += ["", f"Всего: {sum(int(r.get('total') or 0) for r in ordered)}"]
    return "\n".join(lines)


def format_timeline(buckets: list[dict[str, Any]], *, channel: str = "") -> str:
    """Daily counts, newest last, bounded to the most recent days."""
    if not buckets:
        return "За этот период сообщений нет."
    tail = buckets[-_TIMELINE_SHOWN:]
    head = f"Динамика по дням{f' — {channel}' if channel else ''}:"
    lines = [head, ""]
    lines += [f"{b.get('day')}  {int(b.get('count') or 0)}" for b in tail]
    hidden = len(buckets) - len(tail)
    if hidden > 0:
        lines += ["", f"(показаны последние {len(tail)} из {len(buckets)} дней)"]
    return "\n".join(lines)


def format_history(rows: list[dict[str, Any]]) -> str:
    """The user's recent requests, newest first."""
    if not rows:
        return "История пуста — вы ещё ничего не спрашивали."
    lines = ["Последние запросы:", ""]
    for r in rows:
        status = r.get("status") or "?"
        preview = _clip(r.get("args") or "", _ANSWER_PREVIEW)
        lines.append(
            f"#{r.get('id')} · {_when(r.get('started_at'))} · "
            f"{r.get('command')} · {status}\n  {preview}",
        )
    lines += ["", "Повторить: /repeat <номер>"]
    return "\n".join(lines)


def source_text(source: dict[str, Any]) -> str:
    """The chunk's text.

    The search API calls the field ``content``; MCP's `vector_search`
    calls it ``text``. Both are accepted because the bot reads the API
    while the tests and neighbouring tools speak the other shape, and
    guessing wrong renders a list of empty fragments — which is exactly
    what shipped before this was checked against the live API.
    """
    return str(source.get("content") or source.get("text") or "")


def source_name(source: dict[str, Any]) -> str:
    """A human-ish label for provenance.

    The API returns a FLAT source — ``{chunk_id, content, doc_id,
    position, score, …}`` — with no ``metadata`` dict, so reach for the
    nested keys only as a fallback for other shapes.
    """
    meta = source.get("metadata") or {}
    return str(
        meta.get("file_name")
        or source.get("doc_id")
        or meta.get("doc_id")
        or source.get("chunk_id")
        or "?",
    )


def format_answer(answer: str, sources: list[dict[str, Any]] | None) -> str:
    """The answer plus a compact provenance list."""
    answer = (answer or "").strip()
    sources = sources or []
    if is_empty_answer(answer):
        # `Empty Response` is LlamaIndex's marker for "no synthesis", not
        # something to show a user. Distinguish it from "found nothing":
        # an empty synthesis WITH sources is a different failure from no
        # hits at all, and the user can act on the difference.
        answer = (
            NO_RESULT_MESSAGE if not sources
            else "Ответ не сформирован, но найдены источники — посмотрите их ниже."
        )
    if not sources:
        return answer
    names: list[str] = []
    for s in sources[:_SOURCES_SHOWN]:
        name = source_name(s)
        if name not in names:
            names.append(name)
    tail = ["", f"Источники ({len(sources)}):"]
    tail += [f"· {_clip(n, 80)}" for n in names]
    if len(sources) > len(names):
        tail.append(f"· …и ещё {len(sources) - len(names)}")
    return "\n".join([answer, *tail])


def format_fragments(sources: list[dict[str, Any]]) -> str:
    """`/find`: the retrieved text itself, no synthesis."""
    if not sources:
        return "Ничего не найдено."
    lines = [f"Найдено фрагментов: {len(sources)}", ""]
    budget = TG_LIMIT - len(lines[0]) - 100
    for i, s in enumerate(sources, 1):
        piece = f"{i}. {_clip(source_text(s), 400)}"
        if budget - len(piece) < 0:
            lines.append(f"…показаны первые {i - 1}")
            break
        budget -= len(piece)
        lines.append(piece)
    return "\n".join(lines)


def format_users(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "Пользователей нет."
    lines = ["Пользователи:", ""]
    for r in rows:
        lines.append(
            f"{r.get('telegram_id')} · {r.get('status')} · {r.get('role')} · "
            f"@{r.get('username') or '—'}",
        )
    lines += ["", "Одобрить: /approve <id> · Заблокировать: /deny <id>"]
    return "\n".join(lines)


__all__ = [
    "TG_LIMIT",
    "format_answer",
    "format_channels",
    "format_entities",
    "format_fragments",
    "format_history",
    "format_timeline",
    "format_users",
    "source_name",
    "source_text",
    "split_for_telegram",
]


def format_entities(body: dict[str, Any], *, query: str = "") -> str:
    """Entity lookup results, or the reason there are none.

    Takes the whole response body, not just the list, because an empty
    list means two different things and the user can act on the
    difference: "the graph has no such name" is a cue to try another
    spelling, "the lookup could not run" is not.
    """
    error = str(body.get("error") or "")
    rows = body.get("entities") or []
    if error:
        return (
            "Не удалось выполнить поиск по сущностям — это сбой, "
            f"а не отсутствие данных.\nПричина: {_clip(error, 200)}"
        )
    if not rows:
        return (
            f"Сущностей по запросу «{query}» не найдено.\n"
            "Поиск идёт по НАЧАЛУ имени: «Иванов» найдёт «Иванов Иван», "
            "но «Ромаш» не найдёт «ООО Ромашка»."
        )
    lines = [f"Найдено сущностей: {len(rows)}", ""]
    for r in rows[:_ENTITIES_SHOWN]:
        name = r.get("entity_name") or "?"
        kind = r.get("entity_type") or ""
        desc = _clip(r.get("description") or "", 160)
        head = f"{name}" + (f" · {kind}" if kind else "")
        lines.append(head + (f"\n  {desc}" if desc else ""))
    if len(rows) > _ENTITIES_SHOWN:
        lines += ["", f"…и ещё {len(rows) - _ENTITIES_SHOWN}"]
    return "\n".join(lines)
