"""Deterministic event-time resolver: raw phrase + doc date → interval.

Pure module — no I/O, no LLM. Called from ``event_extract.events_to_graph``
with the sanitized ``event_ts_raw`` phrase and the chunk's ``doc_date_epoch``
(epoch DAYS, as stamped by ``parse_and_chunk``). Returns
``(start_epoch_s, end_epoch_s, precision)`` in epoch SECONDS (UTC) or ``None``.

Pipeline: cheap pre-rules for interval shapes Russian news text actually uses
(audited 2026-07-05, see the design spec) → ``dateparser`` for residual point
expressions, anchored on the document date. Anything else ⇒ ``None`` — an
untimed event is honest, an invented date is not.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, timedelta

from loguru import logger

Resolved = tuple[int, int, str]

_MAX_LEN = 64
_MIN_YEAR = 1900
_MAX_YEAR = 2100

_MONTHS = {
    "январь": 1, "января": 1, "январе": 1,
    "февраль": 2, "февраля": 2, "феврале": 2,
    "март": 3, "марта": 3, "марте": 3,
    "апрель": 4, "апреля": 4, "апреле": 4,
    "май": 5, "мая": 5, "мае": 5,
    "июнь": 6, "июня": 6, "июне": 6,
    "июль": 7, "июля": 7, "июле": 7,
    "август": 8, "августа": 8, "августе": 8,
    "сентябрь": 9, "сентября": 9, "сентябре": 9,
    "октябрь": 10, "октября": 10, "октябре": 10,
    "ноябрь": 11, "ноября": 11, "ноябре": 11,
    "декабрь": 12, "декабря": 12, "декабре": 12,
}
_MONTH_RE = "|".join(_MONTHS)

_ISO_POINT_RE = re.compile(r"^(\d{4})-(\d{2})(?:-(\d{2}))?$")
_ISO_RANGE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})$")
_BARE_YEAR_RE = re.compile(r"^(\d{4})(?:\s*год\w*)?$")
_YEAR_RANGE_RE = re.compile(r"^(\d{4})\s*[-–—]\s*(\d{4})(?:\s*год\w*)?$")
_DAY_SPAN_RE = re.compile(rf"^(\d{{1,2}})\s*[-–]\s*(\d{{1,2}})\s+({_MONTH_RE})$")
_DAY_MONTH_RE = re.compile(rf"^(\d{{1,2}})\s+({_MONTH_RE})(?:\s+(\d{{4}}))?(?:\s+года?)?$")
_MONTH_ONLY_RE = re.compile(rf"^({_MONTH_RE})(?:\s+(\d{{4}}))?(?:\s+года?)?$")
_HALF_RE = re.compile(r"^(перв|втор)\w*\s+полугоди\w*$")
_QUARTER_RE = re.compile(r"^(?:q\s*([1-4])|([1-4])\s*-?й?\s*квартал\w*)(?:\s+(\d{4}))?$")
_INTRADAY_RE = re.compile(r"^(.*?)\s*с\s*(\d{1,2}):(\d{2})\s*до\s*(\d{1,2}):(\d{2})(?:\s*\S+)?$")
_PREPOSITION_RE = re.compile(r"^(?:в|на|к|до|около|примерно)\s+")

_EPOCH = date(1970, 1, 1)


def _anchor_date(doc_date_epoch_days: int | None) -> date | None:
    if doc_date_epoch_days is None:
        return None
    return _EPOCH + timedelta(days=int(doc_date_epoch_days))


def _ts(y: int, m: int, d: int, hh: int = 0, mm: int = 0, ss: int = 0) -> int:
    return int(datetime(y, m, d, hh, mm, ss, tzinfo=UTC).timestamp())


def _day_bounds(d: date) -> tuple[int, int]:
    s = _ts(d.year, d.month, d.day)
    return s, s + 86399


def _month_bounds(y: int, m: int) -> tuple[int, int]:
    first = date(y, m, 1)
    last = (date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)) - timedelta(days=1)
    return _day_bounds(first)[0], _day_bounds(last)[1]


def _year_bounds(y: int) -> tuple[int, int]:
    return _day_bounds(date(y, 1, 1))[0], _day_bounds(date(y, 12, 31))[1]


def _nearest_year(day: int, month: int, anchor: date) -> int | None:
    """Year making (day, month) closest to the anchor date."""
    best: tuple[int, int] | None = None
    for y in (anchor.year - 1, anchor.year, anchor.year + 1):
        try:
            delta = abs((date(y, month, day) - anchor).days)
        except ValueError:
            continue
        if best is None or delta < best[0]:
            best = (delta, y)
    return best[1] if best else None


def _resolve_day_expr(text: str, anchor: date | None) -> date | None:
    """A single calendar day from ``text`` (explicit or anchor-relative)."""
    m = _DAY_MONTH_RE.match(text)
    if m:
        day, month = int(m.group(1)), _MONTHS[m.group(2)]
        year = int(m.group(3)) if m.group(3) else (_nearest_year(day, month, anchor) if anchor else None)
        if year is None:
            return None
        try:
            return date(year, month, day)
        except ValueError:
            return None
    iso = _ISO_POINT_RE.match(text)
    if iso and iso.group(3):
        try:
            return date(int(iso.group(1)), int(iso.group(2)), int(iso.group(3)))
        except ValueError:
            return None
    return _dateparser_day(text, anchor)


def _dateparser_day(text: str, anchor: date | None) -> date | None:
    if anchor is None:
        return None
    import dateparser

    base = datetime(anchor.year, anchor.month, anchor.day)
    candidates: list[date] = []
    for pref in ("past", "future"):
        got = dateparser.parse(
            text,
            languages=["ru", "en"],
            settings={"RELATIVE_BASE": base, "PREFER_DATES_FROM": pref, "DATE_ORDER": "DMY"},
        )
        if got:
            candidates.append(got.date())
    if not candidates:
        return None
    return min(candidates, key=lambda d: abs((d - anchor).days))


def resolve(raw: str | None, doc_date_epoch_days: int | None) -> Resolved | None:
    try:
        return _resolve(raw, doc_date_epoch_days)
    except Exception as exc:  # resolver must never break extraction
        logger.debug("event-ts resolve failed for {raw!r}: {exc}", raw=raw, exc=exc)
        return None


def _resolve(raw: str | None, doc_date_epoch_days: int | None) -> Resolved | None:
    text = (raw or "").strip().lower().rstrip(".,")
    if not text or len(text) > _MAX_LEN:
        return None
    anchor = _anchor_date(doc_date_epoch_days)

    m = _ISO_RANGE_RE.match(text)
    if m:
        a = date.fromisoformat(m.group(1))
        b = date.fromisoformat(m.group(2))
        if a <= b:
            return _day_bounds(a)[0], _day_bounds(b)[1], "day"
        return None

    m = _ISO_POINT_RE.match(text)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        if not 1 <= mo <= 12:
            return None
        if m.group(3):
            d = _resolve_day_expr(text, anchor)
            return (*_day_bounds(d), "day") if d else None
        return (*_month_bounds(y, mo), "month")

    m = _YEAR_RANGE_RE.match(text)
    if m:
        y1, y2 = int(m.group(1)), int(m.group(2))
        if not (_MIN_YEAR <= y1 <= _MAX_YEAR and _MIN_YEAR <= y2 <= _MAX_YEAR):
            return None
        if y1 <= y2:
            return _year_bounds(y1)[0], _year_bounds(y2)[1], "year"
        return None

    m = _BARE_YEAR_RE.match(text)
    if m:
        y = int(m.group(1))
        if not _MIN_YEAR <= y <= _MAX_YEAR:
            return None
        return (*_year_bounds(y), "year")

    m = _HALF_RE.match(text)
    if m and anchor:
        y = anchor.year
        if m.group(1) == "перв":
            return _month_bounds(y, 1)[0], _month_bounds(y, 6)[1], "month"
        return _month_bounds(y, 7)[0], _month_bounds(y, 12)[1], "month"

    m = _QUARTER_RE.match(text)
    if m:
        q = int(m.group(1) or m.group(2))
        y = int(m.group(3)) if m.group(3) else (anchor.year if anchor else 0)
        if y:
            return _month_bounds(y, 3 * q - 2)[0], _month_bounds(y, 3 * q)[1], "month"
        return None

    m = _INTRADAY_RE.match(text)
    if m:
        day_part = m.group(1).strip() or None
        day = _resolve_day_expr(_PREPOSITION_RE.sub("", day_part), anchor) if day_part else anchor
        if day is None:
            return None
        start = _ts(day.year, day.month, day.day, int(m.group(2)), int(m.group(3)))
        end = _ts(day.year, day.month, day.day, int(m.group(4)), int(m.group(5)))
        return (start, end, "datetime") if start <= end else None

    stripped = _PREPOSITION_RE.sub("", text)

    m = _DAY_SPAN_RE.match(stripped)
    if m and anchor:
        d1, d2, month = int(m.group(1)), int(m.group(2)), _MONTHS[m.group(3)]
        year = _nearest_year(d1, month, anchor)
        if year is None or d1 > d2:
            return None
        try:
            return _day_bounds(date(year, month, d1))[0], _day_bounds(date(year, month, d2))[1], "day"
        except ValueError:
            return None

    m = _MONTH_ONLY_RE.match(stripped)
    if m:
        month = _MONTHS[m.group(1)]
        year = int(m.group(2)) if m.group(2) else (anchor.year if anchor else None)
        if year is None:
            return None
        return (*_month_bounds(year, month), "month")

    if not re.search(r"[а-яa-z0-9]", stripped):
        return None
    if re.search(r"\bxx\b|xx-|-xx|\.\.", stripped):
        return None  # pseudo-date debris — never feed to dateparser

    day = _resolve_day_expr(stripped, anchor)
    return (*_day_bounds(day), "day") if day else None


__all__ = ["resolve"]
