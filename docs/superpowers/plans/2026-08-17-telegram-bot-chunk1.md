# Telegram Bot — Commands, Users, History Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Turn `src/bot` from a whitelist-gated Q&A relay into a commanded bot with database-backed users, an audit trail, and bounded work.

**Architecture:** The bot keeps talking only to the API over HTTP — every command maps to a route that already exists and was verified live. New: a Postgres user/request store, pure admission-and-quota policy, and aiogram command handlers. Spec: [`../specs/2026-08-17-telegram-bot-users-history-design.md`](../specs/2026-08-17-telegram-bot-users-history-design.md).

**Tech Stack:** aiogram 3, httpx, psycopg 3 (async pool), pydantic-settings, pytest.

## Global Constraints

- **Fail closed.** An unknown or unapproved user gets their id and nothing else. This is the existing guarantee in `access.py` and must not weaken.
- The bot talks to **the API only** — never to MCP, Milvus, Nebula or the graph directly. Every command below maps to a route verified live on 2026-08-17.
- **Every request is a row**, written BEFORE the work starts, including refusals (quota, busy, denied). An audit that only records successes is not an audit.
- No foreign key from `bot_request` to `bot_user`: the audit record outlives the user.
- Ruff: `line-length = 100`, `py312`, `select = ["E","F","I","B","UP","SIM","RUF"]`. pytest `asyncio_mode = "auto"`.
- Do NOT wire `topic_trend`, `polarity_evolution`, `find_entity_by_name`, `graph_pagerank` or any `stat_*` tool. They do not answer — see the spec for each.
- Do NOT fix those tools here.

## Verified API surface

```
POST /api/v1/search/{auto,local,global,drift}   {query, synthesize, top_k, …} → {answer, sources, …}
GET  /api/v1/stats/messages?group_by=channel    → {group_by, rows:[{key,total,completed,…}]}
GET  /api/v1/stats/timeline?date_field=doc_date → {date_field, buckets:[{day,key,count}]}
```

`synthesize` is a real field (`src/models/search.py:93`) threaded to the orchestrator, so `/find` is `synthesize=false`.

---

### Task 1: The store

**Files:**
- Modify: `scripts/setup_db.py`
- Create: `src/storage/bot.py`
- Test: `tests/test_storage/test_bot_repository.py`, `tests/test_scripts/test_setup_db_bot.py`

**Interfaces:**
- `BotRepository(dsn=None)` with async methods: `get_or_create_user`, `set_status`, `list_users`, `count_requests_today`, `start_request`, `finish_request`, `recent_requests`, `get_request`.
- Pure builders `build_recent_requests_query`, `build_list_users_query` returning `(sql, params)`.

- [ ] **Step 1: DDL**

Add to `scripts/setup_db.py`, in the idempotent style of the `stat_*` tables, exactly the two tables from the spec plus:

```sql
CREATE INDEX IF NOT EXISTS bot_request_user_time_idx
    ON bot_request (telegram_id, started_at DESC);
```

That index serves both `/history` and the quota count; no other index is wanted.

- [ ] **Step 2: Write the failing tests**

Follow `tests/test_storage/test_stats_repository.py` — in particular its stub **honours `row_factory`**, returning dicts only when `dict_row` was asked for. Keep that: it is what makes a missing `row_factory=dict_row` fail here instead of on live psycopg3.

1. `get_or_create_user` inserts a `pending` row for an unknown id and returns the existing row for a known one, without overwriting `status` or `role` (an approved user sending `/start` again must not be reset to pending — that would silently revoke access).
2. `set_status` writes `status`, `approved_at`, `approved_by`.
3. `count_requests_today` counts only that user and only today (a row from yesterday does not count).
4. `start_request` returns the new id and the row is `running`.
5. `finish_request` sets status, answer, sources, `finished_at`.
6. `recent_requests` is newest-first and bounded by the limit.
7. Every read asks for `row_factory=dict_row`.

- [ ] **Step 3: Run to verify they fail**

`uv run pytest tests/test_storage/test_bot_repository.py tests/test_scripts/test_setup_db_bot.py -v`

- [ ] **Step 4: Implement**

`src/storage/bot.py`, async over `get_pg_pool()` (the bot is async — unlike the ER verdict cache, which needed the sync pool). Mirror `src/storage/stats.py`: pure query builders, thin async methods, `row_factory=dict_row` on every read.

`get_or_create_user` is `INSERT ... ON CONFLICT (telegram_id) DO UPDATE SET username = EXCLUDED.username RETURNING *` — the username refreshes, status and role do not.

- [ ] **Step 5: Run, lint, commit**

```bash
uv run pytest tests/test_storage tests/test_scripts -q -p no:randomly
uv run ruff check src/storage/bot.py scripts/setup_db.py
git commit -m "feat(bot): add the user and request store"
```

---

### Task 2: Policy — admission, quota, concurrency

**Files:**
- Create: `src/bot/policy.py`
- Test: `tests/test_bot/test_policy.py`

**Interfaces:**
- `Decision` — a frozen dataclass `(allowed: bool, reason: str, message: str)`.
- `admit(user: dict | None) -> Decision`
- `check_quota(used: int, quota: int) -> Decision`
- `ConcurrencyGate(limit: int)` with `try_acquire() -> bool` and `release()`.

- [ ] **Step 1: Write the failing tests**

1. `admit(None)` → not allowed, reason `"unknown"`.
2. `admit({"status": "pending"})` → not allowed, reason `"pending"`.
3. `admit({"status": "blocked"})` → not allowed, reason `"blocked"`.
4. `admit({"status": "active"})` → allowed.
5. `check_quota(19, 20)` allows, `check_quota(20, 20)` refuses with reason `"quota"`; `quota <= 0` means unlimited.
6. `ConcurrencyGate(2)`: two acquires succeed, the third fails, and after one `release()` the next succeeds.
7. `ConcurrencyGate` releases on exception when used as a context manager — a search that raises must not leak a slot, or the bot wedges permanently after N errors.

- [ ] **Step 2: Run to verify they fail**

`uv run pytest tests/test_bot/test_policy.py -v`

- [ ] **Step 3: Implement**

Pure and synchronous except the gate. The gate wraps `asyncio.Semaphore` but must expose a NON-blocking attempt — the spec refuses over the cap rather than queueing, so `await acquire()` is wrong here:

```python
class ConcurrencyGate:
    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._in_flight = 0

    def try_acquire(self) -> bool:
        if self._limit > 0 and self._in_flight >= self._limit:
            return False
        self._in_flight += 1
        return True

    def release(self) -> None:
        self._in_flight = max(0, self._in_flight - 1)
```

Single-threaded asyncio, so a plain counter is correct and a lock is not needed; say so in a comment.

User-facing messages live here as constants, in Russian, matching the existing `DENIED_MESSAGE` style in `pipeline.py`.

- [ ] **Step 4: Run, lint, commit**

```bash
uv run pytest tests/test_bot -q -p no:randomly
uv run ruff check src/bot/policy.py
git commit -m "feat(bot): admission, quota and concurrency policy"
```

---

### Task 3: API clients for the new commands

**Files:**
- Modify: `src/bot/search_client.py`
- Create: `src/bot/stats_client.py`
- Test: `tests/test_bot/test_search_client_sources.py`, `tests/test_bot/test_stats_client.py`

**Interfaces:**
- `make_search_full(*, api_base, api_key, mode, timeout_s, synthesize=True) -> Callable[[str], Awaitable[dict]]` returning `{"answer": str, "sources": list}`.
- `make_channels(*, api_base, api_key, timeout_s)` → `list[dict]`
- `make_timeline(*, api_base, api_key, timeout_s)` → `list[dict]`, optional `channel` / `since`.

- [ ] **Step 1: Write the failing tests**

Use `httpx.MockTransport` so no server is needed.

1. `make_search_full` posts to `/api/v1/search/<mode>` and returns BOTH answer and sources. (Today's `make_search` drops `sources` on the floor; the history table needs them.)
2. `synthesize=False` is sent in the body — this is what `/find` is.
3. A non-2xx raises, so the caller's fail-soft path can report it.
4. `make_channels` hits `/api/v1/stats/messages?group_by=channel` and returns the rows.
5. `make_timeline` hits `/api/v1/stats/timeline` with `date_field=doc_date`, and passes `channel` / `since` only when given.

- [ ] **Step 2: Run to verify they fail**

`uv run pytest tests/test_bot -v`

- [ ] **Step 3: Implement**

Keep the existing `make_search` untouched — `pipeline.py` still uses it and its tests pin it. Add `make_search_full` alongside.

- [ ] **Step 4: Run, lint, commit**

```bash
uv run pytest tests/test_bot -q -p no:randomly
uv run ruff check src/bot
git commit -m "feat(bot): API clients that keep sources and reach the stats routes"
```

---

### Task 4: Commands

**Files:**
- Create: `src/bot/commands.py`, `src/bot/format.py`
- Test: `tests/test_bot/test_commands.py`, `tests/test_bot/test_format.py`

**Interfaces:** each handler is `async def handle_x(ctx: Ctx, ...) -> str` where `Ctx` carries the repository, clients, gate and settings. Handlers return the reply TEXT — aiogram wiring is Task 5, so handlers stay testable without a Telegram server.

- [ ] **Step 1: Write the failing tests**

Formatting (`format.py`, pure):

1. `format_channels(rows)` renders a short table and truncates to the top 15 by total, saying how many were omitted.
2. `format_timeline(buckets)` renders day/count and holds under Telegram's 4096-char cap for a year of daily rows.
3. `format_history(rows)` shows id, command, when, and a trimmed first line of the answer.
4. `format_answer(answer, sources)` appends a compact source list; with no sources it says so rather than rendering an empty section.
5. Every formatter returns a non-empty string for empty input (an empty reply is not a valid Telegram message).

Handlers (`commands.py`, deps injected):

6. `/ask` on an unapproved user: refuses, does NOT call search, and still writes a `denied` row.
7. `/ask` over quota: refuses, no search call, row written with status `denied`.
8. `/ask` when the gate is full: refuses with the busy message, no search call, row written.
9. `/ask` happy path: writes a `running` row, calls search, finishes the row with answer AND sources, releases the gate.
10. `/ask` when search raises: row finished with status `failed` and the error, gate released, user gets the error message.
11. `/repeat <id>` re-runs the stored args; `/repeat` on another user's id refuses (an audit trail must not become a way to read other people's questions).
12. `/approve` from a non-admin refuses; from an admin it flips status to `active`.

- [ ] **Step 2: Run to verify they fail**

`uv run pytest tests/test_bot -v`

- [ ] **Step 3: Implement**

Order inside `/ask`, which the tests above pin: admission → quota → gate → write row → search → finish row → release. The gate is released in a `finally`.

- [ ] **Step 4: Run, lint, commit**

```bash
uv run pytest tests/test_bot -q -p no:randomly
uv run ruff check src/bot
git commit -m "feat(bot): commands, formatting, and the audit trail around them"
```

---

### Task 5: Wiring, config, container

**Files:**
- Modify: `src/bot/__main__.py`, `src/config.py`, `scripts/make_env.py`, `docker-compose.bot.yml`
- Test: `tests/test_config/test_bot_settings.py`

- [ ] **Step 1: Settings**

`BotSettings` gains, next to the existing fields:

- `admin_ids: str = ""` — comma-separated, seeds admins on startup. Without it nobody can approve anybody; say that in the description.
- `max_concurrent: int = Field(default=2, ge=0)` — 0 means unlimited.
- `default_daily_quota: int = Field(default=20, ge=0)` — 0 means unlimited.

Keep `allowed_users` for now but mark it superseded in its description — the store is the source of truth. Describe all three in `scripts/make_env.py`.

Test: bounds are enforced, and `admin_ids` parses with the existing `parse_allowed_users`.

- [ ] **Step 2: Wire aiogram**

`__main__.py`: register one handler per command via `Command(...)`, plus a catch-all that routes bare text to `/ask`. On startup, seed `admin_ids` into `bot_user` as `active` + `admin`.

`/ask` sends "Принял, работаю…" first, then edits that message with the answer (spec). Keep `_split` for the 4096 cap — an edited message has the same limit, so a long answer edits the first chunk and sends the rest.

Retire the intent router from the default path: `router.py` / `intent.py` stay in the tree but are no longer wired, because `/analyze` answers with errors. Leave a comment saying why, pointing at the spec.

- [ ] **Step 3: Compose**

`docker-compose.bot.yml`: add `BOT_ADMIN_IDS`, `BOT_MAX_CONCURRENT`, `BOT_DEFAULT_DAILY_QUOTA`, and the Postgres env the store needs. Add a `mem_limit` in the style of the other services in `docker-compose.prod.yml` — this host is memory-constrained and every service there carries one.

- [ ] **Step 4: Run the whole suite, lint, commit**

```bash
uv run pytest tests -q -p no:randomly
uv run ruff check src/bot src/config.py
git commit -m "feat(bot): wire the commands, settings and container"
```

- [ ] **Step 5: Deploy and verify by hand**

Create the tables on the live database, build, start the bot, and walk the spec's Verification section: an unapproved user, an approved one, `/history`, `/repeat`, the quota refusal, the busy refusal. Confirm each attempt appears in `bot_request`.

---

## Verification

The spec's Verification section, run against the live bot.

## Notes for the implementer

- The 4096-char Telegram cap is a hard failure, not a truncation: a longer message is rejected outright. Every formatter is responsible for its own bound.
- `/repeat` re-runs, it does not replay a stored answer. Same command, same args, fresh work — and it costs quota again.
- Writing the request row before the work means a crash mid-search leaves a `running` row. That is correct: it is evidence the work started. Chunk 2's task machinery is what will reconcile them.
