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

# The hard cap is 4096; leave headroom for the chunking suffix and for
# any markup the caller adds.
TG_LIMIT = 4000

_CHANNELS_SHOWN = 15
_TIMELINE_SHOWN = 30
_SOURCES_SHOWN = 5
_ANSWER_PREVIEW = 60


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


def format_answer(answer: str, sources: list[dict[str, Any]] | None) -> str:
    """The answer plus a compact provenance list."""
    answer = (answer or "").strip()
    sources = sources or []
    if not answer:
        # Distinct from "found nothing": an empty synthesis with sources
        # is a different failure from no hits at all.
        answer = (
            "Ответ не сформирован." if not sources
            else "Ответ не сформирован, но найдены источники."
        )
    if not sources:
        return answer
    names: list[str] = []
    for s in sources[:_SOURCES_SHOWN]:
        meta = s.get("metadata") or {}
        name = meta.get("file_name") or meta.get("doc_id") or s.get("chunk_id") or "?"
        if name not in names:
            names.append(str(name))
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
        piece = f"{i}. {_clip(s.get('text') or '', 400)}"
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
    "format_fragments",
    "format_history",
    "format_timeline",
    "format_users",
    "split_for_telegram",
]
