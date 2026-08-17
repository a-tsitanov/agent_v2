# Telegram bot: commands, users, history

Date: 2026-08-17. First of three; see "Out of scope" for the other two.

## Problem

The Telegram surface today is openclaw itself: a free-form chat with an
agent. That gives no control over what a user can ask for, no notion of
who the user is beyond one `ownerAllowFrom` list in a config file, no
record of what was asked, and no way to bound how much work one person
can start.

The service is to be opened to **external clients**. Those four gaps are
the reason for a real bot in front.

Data isolation is explicitly NOT one of the gaps: every client sees the
same corpus the operator does. The boundary is admission — who gets in —
not what they can see once in. This matters because the service has no
access-control boundary to build on: `SearchRequest.department` /
`user_id` / `doc_type_filter` are marked RESERVED and not applied, and
the group filters carry an explicit "NOT an access-control boundary"
warning in the code.

## What the bot can actually answer

Measured 2026-08-17, not assumed. Only these are wired into commands:

```
full search (synthesis)      89 s    20 sources, reranked
vector_search               0.7 s    raw fragments
channel_message_stats       0.2 s    volume per channel
channel_message_timeline    0.2 s    daily volume, back to 2017
graph_stats                 4.9 s    graph size
```

Deliberately NOT wired, because they do not answer:

- `topic_trend` — dead on the nebula backend, and not for the known
  parameter-binding reason: it needs `Chunk` nodes and `MENTIONS` edges,
  which nebula ingest does not write by design.
- `polarity_evolution` — returns an empty series.
- `find_entity_by_name` — a defect, not missing data: 10.6 s and
  `{"entities":[]}` for "Украина", which an index lookup finds instantly.
- `graph_pagerank` — empty.
- the `stat_*` tools — the indicator registry has never had a row.

Consequence for the design: `src/bot/intent.py` currently routes
"тренд"/"динамика"/"статистика" questions to `/analyze`. That path is
retired from the default flow. Sending a user's question to a layer that
answers with errors is worse than not offering it.

## Design

### Base

Extend `src/bot` (454 lines, 11 modules: whitelist, session, intent
router, query rewrite, search client, pipeline). It already has the
aiogram wiring, the API client with fallback, and the rolling session
window. `docker-compose.bot.yml` and the image's `bot` extra exist.

Replaced: `access.py`'s env-var whitelist, by a database-backed user
store. Retired from the default path: `router.py` / `intent.py`.
Everything else is kept.

### Commands

```
/ask <вопрос>     full search WITH synthesis        ~90 s
/find <запрос>    raw fragments, no synthesis        ~1 s
/channels         ingested volume per channel        ~1 s
/volume [канал]   daily volume, optional channel     ~1 s
/history          this user's last 10 requests
/repeat <id>      re-run a request from history
/start /help /whoami
```

Admin only: `/users`, `/approve <telegram_id>`, `/deny <telegram_id>`.

A bare message with no command is treated as `/ask`, so the bot stays
usable without learning the command list.

`/ask` uses `BOT_SEARCH_MODE` (default `auto`, unchanged) so the operator
keeps the existing tuning.

### Users and admission

```sql
CREATE TABLE IF NOT EXISTS bot_user (
    telegram_id  BIGINT PRIMARY KEY,
    username     TEXT        NOT NULL DEFAULT '',
    status       TEXT        NOT NULL DEFAULT 'pending',  -- pending|active|blocked
    role         TEXT        NOT NULL DEFAULT 'client',   -- client|admin
    daily_quota  INTEGER     NOT NULL DEFAULT 20,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    approved_at  TIMESTAMPTZ,
    approved_by  BIGINT
);
```

An unknown user sending anything gets a `pending` row and is told their
id; nothing else works until an admin runs `/approve`. Fail closed, as
today.

Bootstrap: `BOT_ADMIN_IDS` seeds the first admins on startup — without
it there is nobody who can approve anybody.

### History

```sql
CREATE TABLE IF NOT EXISTS bot_request (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    telegram_id  BIGINT      NOT NULL,
    chat_id      BIGINT      NOT NULL,
    command      TEXT        NOT NULL,
    args         TEXT        NOT NULL DEFAULT '',
    status       TEXT        NOT NULL DEFAULT 'running', -- running|done|failed|denied
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    answer       TEXT        NOT NULL DEFAULT '',
    sources      JSONB       NOT NULL DEFAULT '[]'::jsonb,
    error        TEXT        NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS bot_request_user_time_idx
    ON bot_request (telegram_id, started_at DESC);
```

One row per request, written before the work starts and completed after.
It backs `/history`, `/repeat`, the daily quota count, and the audit of
who asked what. No foreign key to `bot_user`: an audit record must
survive its user being deleted.

`sources` stores what the search returned, so `/history` can show an old
answer with its provenance without re-running anything.

### Bounding the work

Two limits, both in this first chunk rather than deferred, because the
box is a 14 GiB host at ~99% swap and the clients are external:

- **Per-user daily quota** (`bot_user.daily_quota`, default 20), counted
  from `bot_request`.
- **Global concurrency cap** on `/ask` (`BOT_MAX_CONCURRENT`, default 2).
  A 90-second search is not free; several at once are felt by everything
  else on the host. Over the cap the user is told to wait, not queued —
  queueing is chunk 2's job.

### Long answers

"Принял, работаю" plus a typing indicator, then the message is edited
into the answer. Telegram does not time out; the user needs feedback, not
machinery. Cancel, status polling and completion notifications are chunk
2.

## Out of scope

**Chunk 2 — tasks.** Background execution, status, cancel, notify on
completion, queueing past the concurrency cap.

**Chunk 3 — the openclaw bridge.** `/ask` going to an agent turn over the
gateway instead of straight to the API. Deferred deliberately: the agent's
value is choosing among tools, and most of the tools it would choose from
do not currently answer.

**Not planned here:** per-client data isolation (explicitly not wanted),
billing, and any fix to the broken tools listed above — those are their
own work.

## Verification

An unapproved user gets only their id and a pending notice. An approved
one gets an answer to `/ask` with sources, and it appears in `/history`.
`/repeat` on that id produces a fresh answer. The 21st request in a day is
refused with the quota message. A third simultaneous `/ask` is refused
with the busy message while two run. Every one of those attempts,
including the refusals, is a row in `bot_request`.
