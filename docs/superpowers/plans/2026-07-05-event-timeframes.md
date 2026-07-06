# E2 Event Time-Frames Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Event time becomes a machine-queryable interval (`event_start_epoch`/`event_end_epoch`/`event_ts_precision`) resolved deterministically from a verbatim raw phrase (`event_ts_raw`), with garbage neutralized at parse time and `event_type` enforced against the configured taxonomy.

**Architecture:** The extraction LLM only copies time words from the text; a new pure resolver module (`src/graph/event_ts_resolver.py`) converts phrase + document date into epoch-second intervals; `events_to_graph` writes the four new node properties; the dedup key and event primitives switch from the legacy `event_ts` string to `event_start_epoch`.

**Tech Stack:** Python 3.12, `dateparser` (new dep), Neo4j via existing store, pytest (+ `pytest-asyncio` already in project).

**Spec:** `docs/superpowers/specs/2026-07-05-event-timeframes-design.md`

## Global Constraints

- Time fields on `:EventOrAction`: `event_ts_raw: str`, `event_start_epoch: int` (epoch **seconds** UTC), `event_end_epoch: int` (inclusive; 23:59:59 for date-granular ends), `event_ts_precision: year|month|day|datetime`. The three resolved fields are set together or not at all; legacy `event_ts` is no longer written.
- `doc_date_epoch` chunk metadata is epoch **DAYS** (as stamped by `parse_and_chunk.py`); the resolver converts internally.
- Resolver is pure, never raises; unresolvable ⇒ `None`.
- Taxonomy: `settings.events.taxonomy ∪ {"other"}`, case-insensitive; off-list types ⇒ `other` with original in `event_type_raw`.
- All work TDD; run `uv run pytest <file> -q`. Full-suite regression gate: only the 13 known pre-existing baseline failures (pipeline/make_env/push_wikibase/search_community) are tolerated.
- Branch: `feat/event-timeframes` off current `main`. Commits are per-task but require the user's standing approval (ask once before Task 1 commit; NEVER push).

---

### Task 1: Deterministic resolver module

**Files:**
- Create: `src/graph/event_ts_resolver.py`
- Test: `tests/test_graph/test_event_ts_resolver.py`
- Modify: `pyproject.toml` (add `dateparser` to `[project] dependencies`)

**Interfaces:**
- Produces: `resolve(raw: str | None, doc_date_epoch_days: int | None) -> tuple[int, int, str] | None` — `(start_epoch_s, end_epoch_s, precision)`; consumed by Tasks 4, 5, 7.

- [ ] **Step 1: Add dependency**

In `pyproject.toml`, append to the main `dependencies` list (alphabetical placement near other `d*` entries):

```toml
    "dateparser>=1.2",
```

Run: `uv sync` — expected: resolves and installs `dateparser`.

- [ ] **Step 2: Write the failing tests**

```python
"""Table-driven tests for the deterministic event-time resolver.

Anchor below = 2026-07-05 (epoch day 20639): date(2026,7,5) - date(1970,1,1) = 20639 days.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.graph.event_ts_resolver import resolve

ANCHOR_DAYS = 20639  # 2026-07-05


def _utc(y, m, d, hh=0, mm=0, ss=0) -> int:
    return int(datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc).timestamp())


# ── resolvable: explicit dates ───────────────────────────────────────

def test_explicit_dmy_date():
    assert resolve("1 марта 2024", ANCHOR_DAYS) == (_utc(2024, 3, 1), _utc(2024, 3, 1, 23, 59, 59), "day")


def test_iso_day():
    assert resolve("2024-07-06", ANCHOR_DAYS) == (_utc(2024, 7, 6), _utc(2024, 7, 6, 23, 59, 59), "day")


def test_iso_month():
    assert resolve("2024-03", ANCHOR_DAYS) == (_utc(2024, 3, 1), _utc(2024, 3, 31, 23, 59, 59), "month")


def test_iso_range():
    assert resolve("2026-01-01..2026-04-30", ANCHOR_DAYS) == (
        _utc(2026, 1, 1), _utc(2026, 4, 30, 23, 59, 59), "day")


# ── resolvable: relative to document date ────────────────────────────

def test_yesterday():
    assert resolve("вчера", ANCHOR_DAYS) == (_utc(2026, 7, 4), _utc(2026, 7, 4, 23, 59, 59), "day")


def test_yearless_day_month_resolves_nearest_to_anchor():
    # 2026-07-06 is 1 day from the anchor; 2025-07-06 is 364 days away.
    assert resolve("6 июля", ANCHOR_DAYS) == (_utc(2026, 7, 6), _utc(2026, 7, 6, 23, 59, 59), "day")


def test_bare_month_uses_anchor_year():
    assert resolve("в марте", ANCHOR_DAYS) == (_utc(2026, 3, 1), _utc(2026, 3, 31, 23, 59, 59), "month")


# ── resolvable: intervals ────────────────────────────────────────────

def test_bare_year():
    assert resolve("2023", ANCHOR_DAYS) == (_utc(2023, 1, 1), _utc(2023, 12, 31, 23, 59, 59), "year")


def test_year_range_with_word():
    assert resolve("2026–2027 годы", ANCHOR_DAYS) == (_utc(2026, 1, 1), _utc(2027, 12, 31, 23, 59, 59), "year")


def test_day_span_in_month():
    assert resolve("1-5 июля", ANCHOR_DAYS) == (_utc(2026, 7, 1), _utc(2026, 7, 5, 23, 59, 59), "day")


def test_first_half_year():
    assert resolve("первое полугодие", ANCHOR_DAYS) == (_utc(2026, 1, 1), _utc(2026, 6, 30, 23, 59, 59), "month")


def test_intraday_span_with_day():
    assert resolve("6 июля с 12:00 до 18:00 мск", ANCHOR_DAYS) == (
        _utc(2026, 7, 6, 12, 0), _utc(2026, 7, 6, 18, 0), "datetime")


# ── unresolvable ⇒ None, never an invention ──────────────────────────

@pytest.mark.parametrize("garbage", [
    None, "", "2024-XX", "2024-XX..2025-XX", "20XX-MM-DD", "..2024",
    "после праздников", "листья", "Константиновка",
    "Упоминается роль Норвегии как крупнейшего донора в программе НАТО PURL.",
])
def test_unresolvable_returns_none(garbage):
    assert resolve(garbage, ANCHOR_DAYS) is None


def test_no_anchor_still_resolves_absolute_dates():
    assert resolve("2024-07-06", None) == (_utc(2024, 7, 6), _utc(2024, 7, 6, 23, 59, 59), "day")


def test_no_anchor_relative_returns_none():
    assert resolve("вчера", None) is None


def test_never_raises_on_weird_input():
    assert resolve("6 " * 30, ANCHOR_DAYS) is None  # >64 chars → None fast-path
    assert resolve("99 микабря 20260", ANCHOR_DAYS) is None
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_graph/test_event_ts_resolver.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.graph.event_ts_resolver'`

- [ ] **Step 4: Write the implementation**

```python
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
from datetime import date, datetime, timedelta, timezone

Resolved = tuple[int, int, str]

_MAX_LEN = 64

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
    return int(datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc).timestamp())


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
    except Exception:  # noqa: BLE001 — resolver must never break extraction
        return None


def _resolve(raw: str | None, doc_date_epoch_days: int | None) -> Resolved | None:
    text = (raw or "").strip().lower().strip(".,")
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
        if y1 <= y2:
            return _year_bounds(y1)[0], _year_bounds(y2)[1], "year"
        return None

    m = _BARE_YEAR_RE.match(text)
    if m:
        return (*_year_bounds(int(m.group(1))), "year")

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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_graph/test_event_ts_resolver.py -q`
Expected: all PASS. If a `dateparser`-backed case fails, debug against the actual library behavior — adjust the pre-rule, not the test expectation (expectations encode the spec).

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock src/graph/event_ts_resolver.py tests/test_graph/test_event_ts_resolver.py
git commit -m "feat(events): deterministic event-time resolver (phrase + doc date -> interval)"
```

---

### Task 2: Parse-side sanity gate for the ts field

**Files:**
- Modify: `src/graph/lightrag_parse.py:266-295` (`_parse_event`)
- Test: `tests/test_graph/test_lightrag_parse.py` (append)

**Interfaces:**
- Consumes: nothing new.
- Produces: `ParsedEvent.event_ts` now carries a *sanitized verbatim phrase or None* (same field name/type). `_sanitize_event_ts(value: str | None) -> str | None` (module-private but unit-tested).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_graph/test_lightrag_parse.py`:

```python
# ── event_ts sanity gate ─────────────────────────────────────────────

import pytest

from src.graph.lightrag_parse import _sanitize_event_ts, parse_lightrag_output

D = "<|#|>"  # keep in sync with TUPLE_DELIM


@pytest.mark.parametrize("bad", [
    "affirmed", "uncertain", "empty", "unknown", "Не указано", "неизвестно",
    "52.164866, 32.929911", "Бразилия;Норвегия",
    "Упоминается роль Норвегии как крупнейшего донора в программе НАТО PURL.",
])
def test_sanitize_event_ts_rejects_non_temporal(bad):
    assert _sanitize_event_ts(bad) is None


@pytest.mark.parametrize("good", ["вчера", "6 июля с 12:00 до 18:00 мск", "1 марта 2024", "2024-07-06"])
def test_sanitize_event_ts_keeps_phrases(good):
    assert _sanitize_event_ts(good) == good


def test_event_line_with_missing_fields_gets_untimed():
    # Only 5 fields: participants slid into the ts position — ts must be dropped.
    line = D.join(["event", "meeting", "провели встречу", "Иванов;Петров", "Москва"])
    out = parse_lightrag_output(line + "\n<|COMPLETE|>")
    assert len(out.events) == 1
    assert out.events[0].event_ts is None


def test_full_event_line_keeps_verbatim_ts():
    line = D.join(["event", "meeting", "провели встречу", "Иванов", "вчера", "Москва", "affirmed"])
    out = parse_lightrag_output(line + "\n<|COMPLETE|>")
    assert out.events[0].event_ts == "вчера"
```

Before running: check the actual `TUPLE_DELIM` / completion sentinel values at the top of `src/graph/lightrag_parse.py` and use those constants (import them) instead of the literals above if they differ.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_graph/test_lightrag_parse.py -q -k sanitize or event_line`
Expected: FAIL — `ImportError: cannot import name '_sanitize_event_ts'`

- [ ] **Step 3: Implement the gate**

In `src/graph/lightrag_parse.py`, add above `_parse_event` (module already imports `re`):

```python
_TS_POLARITY_LITERALS = {"affirmed", "negated", "uncertain"}
_TS_PLACEHOLDERS = {
    "empty", "unknown", "none", "null", "n/a", "-", "not specified",
    "не указано", "не указана", "неизвестно", "дата неизвестна", "дата не указана",
}
_TS_COORD_RE = re.compile(r"^-?\d{1,3}\.\d+\s*[,;]\s*-?\d{1,3}\.\d+$")
_TS_MAX_LEN = 64


def _sanitize_event_ts(value: str | None) -> str | None:
    """Verbatim time phrase or None — reject polarity/location/participant
    debris that slides into the ts position on malformed tuples."""
    v = (value or "").strip()
    if not v or len(v) > _TS_MAX_LEN:
        return None
    if v.lower().strip("().") in _TS_POLARITY_LITERALS | _TS_PLACEHOLDERS:
        return None
    if ";" in v or _TS_COORD_RE.match(v):
        return None
    return v
```

In `_parse_event`, replace the `event_ts` line:

```python
    # ts position is only trustworthy on a full 7-field tuple; on shorter
    # tuples neighboring fields slide into it (audited 2026-07-05).
    event_ts = _sanitize_event_ts(fields[4]) if len(fields) >= 7 else None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_graph/test_lightrag_parse.py -q`
Expected: PASS (including pre-existing tests; if an existing test asserted ts survival on short tuples, update it to the new contract — the spec supersedes).

- [ ] **Step 5: Commit**

```bash
git add src/graph/lightrag_parse.py tests/test_graph/test_lightrag_parse.py
git commit -m "feat(events): sanity-gate event_ts at parse (verbatim phrase or None)"
```

---

### Task 3: Prompt contract — verbatim copy + taxonomy injection

**Files:**
- Modify: `src/graph/lightrag_prompts.py:365-399` (`EVENT_INSTRUCTION`)
- Modify: `src/graph/lightrag_extract.py:182-186` (format call)
- Test: `tests/test_graph/test_event_extract.py` (append)

**Interfaces:**
- Produces: `EVENT_INSTRUCTION` gains a `{taxonomy}` placeholder; the call site formats it with `", ".join(settings.events.taxonomy) + ", other"`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_graph/test_event_extract.py`:

```python
def test_event_instruction_declares_verbatim_and_taxonomy():
    from src.graph.lightrag_prompts import EVENT_INSTRUCTION

    assert "{taxonomy}" in EVENT_INSTRUCTION
    assert "VERBATIM" in EVENT_INSTRUCTION
    assert "NEVER guess" in EVENT_INSTRUCTION
    # the old contract must be gone: no ISO normalization request
    assert "ISO date or range" not in EVENT_INSTRUCTION
    # the few-shot must demonstrate the empty-ts case
    assert "нет времени в тексте" in EVENT_INSTRUCTION or "no time stated" in EVENT_INSTRUCTION


@pytest.mark.asyncio
async def test_system_prompt_carries_configured_taxonomy(monkeypatch) -> None:
    from src.config import settings as _settings

    monkeypatch.setattr(_settings.events, "extraction_enabled", True)
    monkeypatch.setattr(_settings.events, "taxonomy", ["deal", "meeting"])
    captured: dict = {}

    extractor = LightRAGExtractor(llm=_ScriptedLLM(responses=[_event_payload()]), num_workers=1)

    orig = extractor._chat

    async def spy(system_msg, user_msg):
        captured["system"] = system_msg
        return await orig(system_msg, user_msg)

    extractor._chat = spy
    await extractor.acall([TextNode(id_="tax1", text="text")])
    assert "deal, meeting, other" in captured["system"]
```

(`_ScriptedLLM`, `_event_payload`, `TextNode`, `LightRAGExtractor`, `pytest` are already imported/defined in this test file — reuse them.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_graph/test_event_extract.py -q -k "instruction or taxonomy"`
Expected: FAIL — `{taxonomy}` not in EVENT_INSTRUCTION.

- [ ] **Step 3: Rewrite EVENT_INSTRUCTION**

Replace the whole `EVENT_INSTRUCTION` string in `src/graph/lightrag_prompts.py` with:

```python
EVENT_INSTRUCTION = """\
---Events---
Extract *discrete occurrences* described in the text as `event` tuples, on top of the entity and relation output above.

**Output Format — Events:** Output 7 fields per event, delimited by `{tuple_delimiter}`, on a single line.  The first field *must* be the literal string `event`.

  Format: `event{tuple_delimiter}event_type{tuple_delimiter}trigger_phrase{tuple_delimiter}participants{tuple_delimiter}event_timestamp{tuple_delimiter}event_location{tuple_delimiter}event_polarity`

Field definitions:
  * `event_type`: one of: {taxonomy}. Use `other` if none fits — do not invent new labels.
  * `trigger_phrase`: the verb phrase or noun that signals the event in the text (e.g. "signed a contract", "merger announced").
  * `participants`: semicolon-separated list of entity names involved (e.g. `Romashka;Lutik`).  Use the same names as in the entity list.
  * `event_timestamp`: copy the time expression VERBATIM from the text (e.g. "вчера", "в марте", "6 июля с 12:00 до 18:00"). Leave **empty** if the text states no time. NEVER guess or invent a date — do not normalize, do not add a year.
  * `event_location`: place where the event occurred; leave **empty** if not mentioned.
  * `event_polarity`: logical polarity — `affirmed` (event occurred), `negated` (explicitly did NOT occur), or `uncertain` (hedged).  Default `affirmed`.

Output all events after the entity and relation lines, before the `{completion_delimiter}` sentinel.

---Event Examples---
<Input Text>
```
Вчера ООО «Ромашка» и ООО «Лютик» подписали договор поставки.
```
<Output>
event{tuple_delimiter}deal{tuple_delimiter}подписали договор поставки{tuple_delimiter}ООО «Ромашка»;ООО «Лютик»{tuple_delimiter}Вчера{tuple_delimiter}{tuple_delimiter}affirmed
{completion_delimiter}

<Input Text>
```
Совет директоров одобрил слияние компаний Alpha и Beta. (нет времени в тексте)
```
<Output>
event{tuple_delimiter}deal{tuple_delimiter}одобрил слияние{tuple_delimiter}Alpha;Beta{tuple_delimiter}{tuple_delimiter}{tuple_delimiter}affirmed
{completion_delimiter}
"""
```

(Keep the module docstring line under the constant as-is.)

- [ ] **Step 4: Format taxonomy at the call site**

In `src/graph/lightrag_extract.py` replace lines 182-186:

```python
        if events_enabled:
            system_msg = system_msg + EVENT_INSTRUCTION.format(
                tuple_delimiter=TUPLE_DELIM,
                completion_delimiter=COMPLETE_DELIM,
                taxonomy=", ".join(_settings.events.taxonomy) + ", other",
            )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_graph/test_event_extract.py -q`
Expected: PASS (all — including pre-existing gated on/off tests).

- [ ] **Step 6: Commit**

```bash
git add src/graph/lightrag_prompts.py src/graph/lightrag_extract.py tests/test_graph/test_event_extract.py
git commit -m "feat(events): prompt contract — verbatim ts copy, taxonomy injection, empty-ts few-shot"
```

---

### Task 4: Wire resolver + taxonomy into event nodes

**Files:**
- Modify: `src/graph/event_extract.py` (`events_to_graph`)
- Modify: `src/graph/lightrag_extract.py:282-285` (call site) 
- Test: `tests/test_graph/test_event_extract.py` (append)

**Interfaces:**
- Consumes: `resolve(raw, doc_date_epoch_days)` from Task 1.
- Produces: `events_to_graph(events, *, id_by_name, doc_date_epoch_days: int | None = None)`; event node properties now include `event_ts_raw` / `event_start_epoch` / `event_end_epoch` / `event_ts_precision` / `event_type_raw`; legacy `event_ts` key no longer set. Tasks 5-7 depend on these property names.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_graph/test_event_extract.py`:

```python
ANCHOR_DAYS_2026_07_05 = 20639


def _mk_event(ts, event_type="meeting"):
    from src.graph.lightrag_parse import ParsedEvent

    return ParsedEvent(
        event_type=event_type, trigger="провели встречу", participants=["Иванов"],
        event_ts=ts, location=None, polarity="affirmed",
        source_chunk_id="c1", file_path="f",
    )


def test_events_to_graph_resolves_interval():
    from src.graph.event_extract import events_to_graph

    nodes, _ = events_to_graph([_mk_event("вчера")], id_by_name={}, doc_date_epoch_days=ANCHOR_DAYS_2026_07_05)
    ev = [n for n in nodes if n.label == "EventOrAction"][0]
    p = ev.properties
    assert p["event_ts_raw"] == "вчера"
    assert p["event_ts_precision"] == "day"
    assert p["event_end_epoch"] - p["event_start_epoch"] == 86399
    assert "event_ts" not in p  # legacy key gone


def test_events_to_graph_unresolved_stays_untimed():
    from src.graph.event_extract import events_to_graph

    nodes, _ = events_to_graph([_mk_event("после праздников")], id_by_name={}, doc_date_epoch_days=ANCHOR_DAYS_2026_07_05)
    p = [n for n in nodes if n.label == "EventOrAction"][0].properties
    assert p["event_ts_raw"] == "после праздников"
    assert "event_start_epoch" not in p and "event_ts_precision" not in p


def test_events_to_graph_enforces_taxonomy(monkeypatch):
    from src.config import settings as _settings
    from src.graph.event_extract import events_to_graph

    monkeypatch.setattr(_settings.events, "taxonomy", ["deal", "meeting"])
    nodes, _ = events_to_graph(
        [_mk_event(None, event_type="potential_journalists_work")], id_by_name={}, doc_date_epoch_days=None)
    p = [n for n in nodes if n.label == "EventOrAction"][0].properties
    assert p["event_type"] == "other"
    assert p["event_type_raw"] == "potential_journalists_work"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_graph/test_event_extract.py -q -k events_to_graph`
Expected: FAIL — unexpected keyword `doc_date_epoch_days` / `event_ts` still present.

- [ ] **Step 3: Implement**

In `src/graph/event_extract.py`: change the signature and the node build (current lines 29-73):

```python
def events_to_graph(
    events: list[ParsedEvent],
    *,
    id_by_name: dict[str, str],
    doc_date_epoch_days: int | None = None,
) -> tuple[list[EntityNode], list[Relation]]:
```

Add imports at top: `from src.graph.event_ts_resolver import resolve`.

Replace the event-node construction:

```python
    from src.config import settings as _settings

    taxonomy = {t.strip().lower() for t in _settings.events.taxonomy} | {"other"}

    for ev in events:
        event_name = f"{ev.event_type}: {ev.trigger}"[:120]
        etype = (ev.event_type or "event").strip().lower()

        props: dict = {
            "event_type": etype if etype in taxonomy else "other",
            "trigger": ev.trigger,
            "polarity": ev.polarity,
            "participants": list(ev.participants),
            "source_chunks": [ev.source_chunk_id] if ev.source_chunk_id else [],
            "file_paths": [ev.file_path] if ev.file_path else [],
        }
        if etype not in taxonomy:
            props["event_type_raw"] = ev.event_type
        if ev.event_ts:
            props["event_ts_raw"] = ev.event_ts
            resolved = resolve(ev.event_ts, doc_date_epoch_days)
            if resolved:
                props["event_start_epoch"], props["event_end_epoch"], props["event_ts_precision"] = resolved

        event_node = EntityNode(name=event_name, label="EventOrAction", properties=props)
        event_nodes.append(event_node)
```

Update the module docstring's DATED-edge note to mention the four new fields instead of `event_ts`.

In `src/graph/lightrag_extract.py` replace lines 282-285:

```python
        if events_enabled and parsed.events:
            _md = node.metadata or {}
            ev_nodes, ev_rels = events_to_graph(
                parsed.events,
                id_by_name=id_by_name,
                # anchor: document date; fallback ingest date (spec §4.3)
                doc_date_epoch_days=_md.get("doc_date_epoch", _md.get("inserted_at_epoch")),
            )
            parsed.entities.extend(ev_nodes)
            relations.extend(ev_rels)
            n_raw = sum(1 for e in parsed.events if e.event_ts)
            n_resolved = sum(
                1 for n_ in ev_nodes
                if n_.label == "EventOrAction" and "event_start_epoch" in (n_.properties or {})
            )
            logger.info(
                "event-ts chunk={c} events={n} ts_raw={r} ts_resolved={s}",
                c=chunk_id, n=len(parsed.events), r=n_raw, s=n_resolved,
            )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_graph/test_event_extract.py tests/test_workflow/test_merge_and_resolve_events.py -q`
Expected: PASS. `test_merge_and_resolve_events.py` fakes may set `event_ts` props — if they fail, update the fakes to the new property names (contract change is intentional).

- [ ] **Step 5: Commit**

```bash
git add src/graph/event_extract.py src/graph/lightrag_extract.py tests/test_graph/test_event_extract.py tests/test_workflow/test_merge_and_resolve_events.py
git commit -m "feat(events): resolve ts to interval+precision at node build; enforce taxonomy"
```

---

### Task 5: Dedup key on epochs

**Files:**
- Modify: `src/graph/event_merge.py`
- Test: `tests/test_graph/test_event_merge.py`

**Interfaces:**
- Consumes: `event_start_epoch` node property (Task 4).
- Produces: `event_key(event_type: str, participants: list[str], event_start_epoch: int | None, *, bucket_days: int = 7) -> tuple`; `_ts_bucket(event_start_epoch: int | None, bucket_days: int) -> str`. Task 7 (events_eval) depends on this signature.

- [ ] **Step 1: Write the failing tests**

In `tests/test_graph/test_event_merge.py`, update/append (adapt existing tests that pass ISO strings — the third positional arg is now an epoch int):

```python
from datetime import datetime, timezone

from src.graph.event_merge import _ts_bucket, event_key, merge_events


def _epoch(y, m, d):
    return int(datetime(y, m, d, tzinfo=timezone.utc).timestamp())


def test_ts_bucket_same_iso_week_collides():
    assert _ts_bucket(_epoch(2026, 7, 1), 7) == _ts_bucket(_epoch(2026, 7, 3), 7)
    assert _ts_bucket(_epoch(2026, 7, 1), 7) != _ts_bucket(_epoch(2026, 7, 8), 7)


def test_ts_bucket_none_is_sentinel():
    assert _ts_bucket(None, 7) == "∅"


def test_event_key_buckets_on_epoch():
    k1 = event_key("deal", ["Иванов"], _epoch(2026, 7, 1))
    k2 = event_key("deal", ["Иванов"], _epoch(2026, 7, 3))
    assert k1 == k2


def test_merge_events_keeps_earliest_interval():
    from llama_index.core.graph_stores.types import EntityNode

    def node(start, end):
        return EntityNode(name=f"deal: подписали {start}", label="EventOrAction", properties={
            "event_type": "deal", "participants": ["Иванов"],
            "event_ts_raw": "x", "event_start_epoch": start, "event_end_epoch": end,
            "event_ts_precision": "day", "source_chunks": [f"c{start}"],
        })

    early, late = _epoch(2026, 7, 1), _epoch(2026, 7, 2)
    merged, _ = merge_events([node(late, late + 86399), node(early, early + 86399)], [])
    assert len(merged) == 1
    assert merged[0].properties["event_start_epoch"] == early
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_graph/test_event_merge.py -q`
Expected: FAIL — `_ts_bucket` chokes on int (`event_ts[:10]` slicing) / merge reads `event_ts`.

- [ ] **Step 3: Implement**

Replace `_ts_bucket`, `event_key`'s third parameter, and the ts handling in `merge_events`:

```python
from datetime import datetime, timezone

_NO_TS = "∅"
_TS_PROPS = ("event_ts_raw", "event_start_epoch", "event_end_epoch", "event_ts_precision")


def _ts_bucket(event_start_epoch: int | None, bucket_days: int) -> str:
    if event_start_epoch is None:
        return _NO_TS
    d = datetime.fromtimestamp(int(event_start_epoch), tz=timezone.utc).date()
    if bucket_days == 7:
        year, week, _ = d.isocalendar()
        return f"{year}-W{week:02d}"
    return str(d.toordinal() // max(bucket_days, 1))


def event_key(
    event_type: str,
    participants: list[str],
    event_start_epoch: int | None,
    *,
    bucket_days: int = 7,
) -> tuple:
    parts = frozenset(_normalize_entity_name(p) for p in participants if p)
    return ((event_type or "event").strip().lower(), parts, _ts_bucket(event_start_epoch, bucket_days))
```

In `merge_events`: build the group key with `p.get("event_start_epoch")` instead of `p.get("event_ts")`; replace the `ts_vals` logic (lines 84-97) with earliest-member selection:

```python
        earliest = min(
            (m for m in members if (m.properties or {}).get("event_start_epoch") is not None),
            key=lambda m: m.properties["event_start_epoch"],
            default=None,
        )
        props = dict(first.properties or {})
        props["event_type"] = type_votes.most_common(1)[0][0]
        props["source_chunks"] = list(dict.fromkeys(chunks))
        if earliest is not None:
            for k in _TS_PROPS:
                if k in (earliest.properties or {}):
                    props[k] = earliest.properties[k]
```

(drop the `ts_vals` accumulation; update the docstring bullet «earliest `event_ts`» → «interval of the earliest-starting member».)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_graph/test_event_merge.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/graph/event_merge.py tests/test_graph/test_event_merge.py
git commit -m "feat(events): dedup key + canonical merge on event_start_epoch"
```

---

### Task 6: Primitives read the new fields

**Files:**
- Modify: `src/analytics/primitives/events_llm.py` (`_EVENT_CORE`, `event_timeline`)
- Test: `tests/test_analytics/test_events_llm.py` (or wherever `event_timeline` tests live — locate with `grep -rl event_timeline tests/test_analytics/`)

**Interfaces:**
- Consumes: node properties from Task 4.
- Produces: `event_timeline` rows: `name, event_type, event_ts_raw, event_start_epoch, event_end_epoch, event_ts_precision`; ordering untimed-last.

- [ ] **Step 1: Write the failing tests**

Append to the existing `event_timeline` test module (reuse its fake-store fixture pattern — it drives `run_rows` with a canned rows list and asserts on the cypher string):

```python
@pytest.mark.asyncio
async def test_event_timeline_orders_by_start_epoch_untimed_last():
    res = await event_timeline(None, entity="X")
    assert "ORDER BY e.event_start_epoch IS NULL, e.event_start_epoch DESC" in res.cypher
    assert "e.event_ts_raw" in res.cypher
    assert "e.event_ts AS" not in res.cypher  # legacy field gone


@pytest.mark.asyncio
async def test_event_timeline_window_filters_on_start_with_created_fallback():
    res = await event_timeline(None, entity="X", window_days=7)
    assert "coalesce(e.event_start_epoch, e.created_at * 86400) >= $since_secs" in res.cypher
    assert "since_secs" in res.params
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_analytics/ -q -k event_timeline`
Expected: FAIL — old cypher.

- [ ] **Step 3: Implement**

In `src/analytics/primitives/events_llm.py`:

```python
_EVENT_CORE = (
    "MATCH (e:__Entity__:EventOrAction {name:$name}) "
    "RETURN e.name AS name, e.event_type AS event_type, e.event_ts_raw AS event_ts_raw, "
    "e.event_start_epoch AS event_start_epoch, e.event_end_epoch AS event_end_epoch, "
    "e.event_ts_precision AS event_ts_precision, e.polarity AS polarity"
)
```

`event_timeline` body:

```python
    top_n = clamp_top_n(top_n, default=50)
    params: dict[str, Any] = {"entity": entity, "top_n": top_n}
    where = ""
    if window_days is not None:
        params["since_secs"] = (today_epoch_days() - int(window_days)) * 86400
        where = "WHERE coalesce(e.event_start_epoch, e.created_at * 86400) >= $since_secs "
    cypher = (
        "MATCH (p:__Entity__ {name:$entity})-[]-(e:__Entity__:EventOrAction) "
        f"{where}"
        "RETURN e.name AS name, e.event_type AS event_type, e.event_ts_raw AS event_ts_raw, "
        "e.event_start_epoch AS event_start_epoch, e.event_end_epoch AS event_end_epoch, "
        "e.event_ts_precision AS event_ts_precision "
        "ORDER BY e.event_start_epoch IS NULL, e.event_start_epoch DESC LIMIT $top_n"
    )
```

Update the `event_timeline` docstring («ordered by event_start_epoch, untimed last; window filters on event_start_epoch with created_at fallback») and the registered description string to «Events a named entity participated in, ordered by resolved start time (untimed last).»

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_analytics/ -q`
Expected: PASS (all analytics tests, not only the two new ones).

- [ ] **Step 5: Commit**

```bash
git add src/analytics/primitives/events_llm.py tests/test_analytics/
git commit -m "feat(analytics): event primitives read interval fields, chronological ordering"
```

---

### Task 7: Observability — check_ingestion section + eval resolve-rate

**Files:**
- Modify: `scripts/check_ingestion.py` (new `check_events()` + call in main)
- Modify: `tests/eval/events_eval.py` (`_keys_by_type` epoch conversion + resolve-rate report)
- Test: `tests/test_graph/test_event_ts_resolver.py` (append 1 test for ISO-without-anchor reuse — already covered by `test_no_anchor_still_resolves_absolute_dates`; no new test file for scripts)

**Interfaces:**
- Consumes: `resolve()` (Task 1), `event_key` signature (Task 5).

- [ ] **Step 1: check_ingestion events section**

Append to `scripts/check_ingestion.py` (follow the existing `check_*` print style; reuse the Neo4j connection pattern already present in the file — if the file has no Neo4j driver helper, build one exactly like `scripts/wipe_db.py::wipe_neo4j` does):

```python
def check_events() -> None:
    from neo4j import GraphDatabase

    print(_SEP)
    print("Neo4j — E2 events / time-frames")
    print(_SEP)
    nj = settings.neo4j
    try:
        auth = (nj.user, nj.password.get_secret_value())
        with GraphDatabase.driver(nj.uri, auth=auth) as driver, driver.session(database=nj.database) as s:
            row = s.run(
                "MATCH (e:__Entity__:EventOrAction) RETURN count(e) AS total, "
                "count(e.event_ts_raw) AS ts_present, count(e.event_start_epoch) AS ts_resolved"
            ).single()
            print(f"  events {row['total']}  ts_present {row['ts_present']}  ts_resolved {row['ts_resolved']}")
            for r in s.run(
                "MATCH (e:__Entity__:EventOrAction) "
                "WHERE e.event_ts_raw IS NOT NULL AND e.event_start_epoch IS NULL "
                "RETURN e.event_ts_raw AS raw, count(*) AS n ORDER BY n DESC LIMIT 15"
            ):
                print(f"    unresolved ×{r['n']}: {r['raw']!r}")
    except Exception as exc:
        print(f"  unreachable: {exc}\n")
```

Wire it into the script's `main`/`__main__` flow right after the existing Neo4j check.

- [ ] **Step 2: events_eval epoch keys + resolve-rate**

In `tests/eval/events_eval.py::_keys_by_type`, golden/predicted `event_ts` are ISO strings — convert before keying, and count resolvability:

```python
from src.graph.event_ts_resolver import resolve as resolve_ts


def _ts_epoch(ev: dict) -> int | None:
    got = resolve_ts(ev.get("event_ts"), None)  # golden ts are absolute ISO — no anchor needed
    return got[0] if got else None
```

and in the `event_key(...)` call replace `ev.get("event_ts")` with `_ts_epoch(ev)`. After the scoring loop in the report section, print resolve-rate over predicted events:

```python
    ts_present = sum(1 for ev in all_predicted if ev.get("event_ts"))
    ts_resolved = sum(1 for ev in all_predicted if _ts_epoch(ev) is not None)
    print(f"ts resolve-rate: {ts_resolved}/{ts_present}"
          f" ({(ts_resolved / ts_present * 100) if ts_present else 0:.0f}%)")
```

(`all_predicted`: accumulate predicted event dicts in the existing per-case loop — add `all_predicted: list[dict] = []` before it and `all_predicted.extend(predicted)` inside.)

- [ ] **Step 3: Verify**

Run: `uv run pytest tests/test_graph/test_event_merge.py tests/test_graph/test_event_ts_resolver.py -q` — PASS.
Run: `uv run python -m scripts.check_ingestion` (stack up) — the events section prints counts without traceback.
Run: `uv run python -c "import tests.eval.events_eval"` — imports clean.

- [ ] **Step 4: Commit**

```bash
git add scripts/check_ingestion.py tests/eval/events_eval.py
git commit -m "feat(events): resolve-rate observability in check_ingestion + events_eval"
```

---

### Task 8: Full regression gate

**Files:** none (verification only)

- [ ] **Step 1: Full suite**

Run: `uv run pytest tests -q 2>&1 | tail -5`
Expected: only the 13 known pre-existing baseline failures (pipeline/make_env/push_wikibase/search_community families). Any NEW failure = fix before proceeding.

- [ ] **Step 2: Grep for legacy readers**

Run: `grep -rn "event_ts\b" src/ --include="*.py" | grep -v "event_ts_raw\|event_ts_precision\|event_ts_resolver"`
Expected: no remaining reader/writer of the legacy `event_ts` property outside historical comments. Fix any stragglers.

- [ ] **Step 3: Commit (if fixes were needed) and report**

Summarize: tests green, resolve-rate wiring in place. Remind the user: wipe + re-ingest is the rollout step (user-driven), then `python -m scripts.check_ingestion` shows the live resolve-rate.
