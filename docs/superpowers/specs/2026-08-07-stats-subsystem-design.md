# Statistics subsystem (MCP-3)

Date: 2026-08-07
Status: design, approved for planning

## Problem

We want to bring external statistics into the service — starting with ФОМ
poll data (weekly «Доминанты» waves), then other sources (Росстат, ЦБ, …).
The driving use case: **compare what Telegram channels talk about against
what the polls measure**, and treat the divergence as a signal.

Two properties make this awkward for the existing service:

1. **Exact vs approximate.** The current service is an approximation
   machine — embeddings, LLM entity extraction, entity resolution,
   community summaries. Its degradation is soft: a worse answer is still an
   acceptable answer. A statistic has the opposite contract — a wrong number
   is worse than a missing number.
2. **Heterogeneity.** Sources disagree on time grid (weekly / monthly /
   daily), dimensions (region, age, sex, industry), units (%, ₽, index),
   value semantics (share of answers / level / rate / index), revisions
   (Росстат restates history) and error bars (polls have a sample size,
   administrative statistics do not). A per-source table does not survive
   the second source.

A third problem is structural: the search surface synthesizes an answer
with a large model before returning anything, so a caller cannot get the
underlying data without paying for synthesis first. Numbers must not travel
through a synthesis prompt — the model will restate them.

## Background (current state)

- **Two MCP servers already split along the right line.**
  - MCP-1 `src/mcp/search_server.py` — four orchestrated tools returning a
    finished answer. `_outcome_to_dict` (`:160`) does return `sources` with
    chunk text plus `citations`, but only *after* the synthesis step has
    run: there is no data-only mode. Its own docstring points callers at
    the atomic tools "for simple lookups" (`:232`).
  - MCP-2 `src/mcp/tools_server.py` — 15 atomic tools, none of which
    synthesize. The trailing note (`:493`) states the model explicitly:
    atomic clients "pass each tool call as a fresh request and assemble
    context themselves".
- **A numeric-statistics tool already lives in MCP-2.**
  `channel_message_stats` (`tools_server.py:449`) and
  `channel_message_timeline` (`:472`) return raw buckets from Postgres with
  a 120 s timeout instead of the 1800 s used by the retrieval tools. Their
  helpers `_stats_by` / `_timeline` (`:402`, `:421`) hold the validation so
  they stay unit-testable outside FastMCP, need no retriever/graph
  bootstrap, and call the same `AsyncPostgres` aggregation methods that
  `/api/v1/stats` and the `message_stats` CLI use — deliberately, so all
  three surfaces report identical numbers. This is the template for MCP-3.
- **Postgres access** goes through the process-global pool
  (`src/storage/pg_pool.py`, `get_pg_pool()`); `AsyncPostgres`
  (`src/storage/postgres.py:57`) is the thin wrapper over `documents`.
  Schema is provisioned idempotently by `scripts/setup_db.py`
  (`CREATE TABLE IF NOT EXISTS` + `ALTER TABLE … ADD COLUMN IF NOT
  EXISTS`), the pattern used by the channel-message-stats work.
- **Synthesis is mandatory in the search path.** `synthesize_answer` is
  step 4 of `SearchOrchestratorWorkflow`, pinned to `large_task_queue`
  (`src/workflow/search/orchestrator.py:334-348`). `SearchRequest`
  (`src/models/search.py`) has no skip-synthesis flag — `include_references`
  and `answer_template` shape the answer, they do not remove it.
- **Analytics primitives cannot reach Postgres.** The single call site is
  `prim.fn(store, **params)` (`src/workflow/analytics/activities.py:61`),
  where `store` is the graph store. A catalog primitive has no way to get a
  Postgres pool.
- **Channel-side time series exist but only as primitives.**
  `topic_trend` (`src/analytics/primitives/dynamics.py:113`) counts chunks
  mentioning an entity per period; `polarity_evolution` (`:135`) tracks edge
  polarity over time. Neither is exposed via MCP.
  `channel_message_timeline` is *ingest volume*, not topic attention — it
  cannot substitute.
- **Separate-loader precedent.** `tg_ingest` runs as its own container
  (`docker-compose.tg-ingest.yml`) with its own state files, but owns no
  storage: it POSTs to `/api/v1/ingest`. The boundary is cut at
  acquisition, not at storage.
- **Extra Milvus collections precedent.** Beyond the main `kb_llamaindex`
  collection (`src/config.py:82`) the project already added `entity_er_vec`
  (`:667`) and `community_report_vec` (`:673`), each via its own spec.

Nothing in the repo references ФОМ, ВЦИОМ, Росстат or any external
statistical source today. This is greenfield.

## Design

### Chosen approach

Split responsibility by **guarantee**, not by feature: exact numbers live in
their own subsystem with its own schema and its own MCP server, and are
joined to the semantic side **by the agent**, not by the service.

This works only because of the atomic-tool model MCP-2 already establishes.
An agent composing context from many tools does not care that
`vector_search` and `stat_series` are served by different processes — so a
boundary that would normally be expensive is nearly free. Conversely, the
split makes the tool model mandatory: with synthesis owned by the service
there would be nothing to join the two sides.

Rejected alternatives:

- **Folding statistics into the existing analytics catalog.** Requires
  widening the `prim.fn(store, …)` contract seen by 40+ primitives, and
  inherits the LLM-tier timeouts and soft-degradation semantics of a
  subsystem whose guarantees do not fit exact numbers.
- **One wide observations table with no registry.** Cheapest to start, but
  there is no object to search: "which indicators exist about X" cannot be
  answered over rows of values. Discovery is the whole point of the
  registry.
- **A per-source table (`fom_*`, `rosstat_*`).** DDL, loader and tools per
  source. Fails the stated goal of adding sources cheaply.
- **A separate deployed service now.** The signal's value is unproven;
  paying for a second deploy, auth surface and monitoring before that is
  established is premature — and the host is memory-constrained. The
  boundaries below are drawn so that extraction later is mechanical.

### 1. Boundary

New server `src/mcp/stats_server.py` (MCP-3), a sibling of MCP-1/MCP-2:

- Returns data only. No synthesis, no LLM call anywhere in its path.
- Owns the `stat_*` tables. Does not read `documents`, the graph, or Milvus.
- Links to the semantic side by two **weak references**, deliberately not
  foreign keys, so a graph rebuild or a re-ingest cannot break statistics:
  - `stat_indicator.entity_vid` → a graph entity
  - `stat_observation.source_doc_id` → the ingested bulletin the number
    came from
- Does not touch `/search/*`, the orchestrator, or synthesis.

Bulletins still flow through the normal ingest as **text** (that is the
semantic service's job). The **numbers** extracted from them are loaded
separately. One source document, two records, joined by `doc_id`.

### 2. Schema (`scripts/setup_db.py`)

Two tables, added idempotently alongside `documents` and `ingest_metrics`.
Requires `CREATE EXTENSION IF NOT EXISTS pg_trgm;` for registry search.

```sql
CREATE TABLE IF NOT EXISTS stat_indicator (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source         TEXT    NOT NULL,           -- 'fom' | 'rosstat' | …
    code           TEXT    NOT NULL,           -- source-native identifier
    title          TEXT    NOT NULL,
    question_text  TEXT    NOT NULL DEFAULT '',-- poll wording, '' if N/A
    unit           TEXT    NOT NULL,           -- '%', 'RUB', 'index', …
    value_kind     TEXT    NOT NULL,           -- share|level|rate|index
    granularity    TEXT    NOT NULL,           -- day|week|month|quarter|year
    dims_schema    JSONB   NOT NULL DEFAULT '{}'::jsonb,
    entity_vid     TEXT,                       -- weak link to graph entity
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, code),
    CONSTRAINT stat_indicator_value_kind_check
        CHECK (value_kind IN ('share','level','rate','index')),
    CONSTRAINT stat_indicator_granularity_check
        CHECK (granularity IN ('day','week','month','quarter','year'))
);

CREATE TABLE IF NOT EXISTS stat_observation (
    indicator_id   BIGINT  NOT NULL REFERENCES stat_indicator(id) ON DELETE CASCADE,
    period_start   DATE    NOT NULL,
    period_end     DATE    NOT NULL,
    dims           JSONB   NOT NULL DEFAULT '{}'::jsonb,
    value          NUMERIC NOT NULL,
    sample_n       INTEGER,                    -- NULL for admin statistics
    revision       INTEGER NOT NULL DEFAULT 0,
    source_doc_id  UUID,                       -- weak link to documents.id
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (indicator_id, period_start, dims, revision)
);

CREATE INDEX IF NOT EXISTS stat_observation_series_idx
    ON stat_observation (indicator_id, period_start);
CREATE INDEX IF NOT EXISTS stat_observation_dims_idx
    ON stat_observation USING GIN (dims);
CREATE INDEX IF NOT EXISTS stat_indicator_title_trgm_idx
    ON stat_indicator USING GIN (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS stat_indicator_question_trgm_idx
    ON stat_indicator USING GIN (question_text gin_trgm_ops);
```

Notes that matter:

- `dims` is `NOT NULL DEFAULT '{}'` on purpose. With a nullable column the
  `UNIQUE` constraint would not deduplicate undimensioned rows, because
  `NULL` never compares equal. `jsonb` normalises key order, so equality is
  well-defined.
- `revision` handles restatements. A read returns the highest revision per
  `(indicator_id, period_start, dims)` unless a specific revision is asked
  for. History is retained, never overwritten.
- `period_end` is stored rather than derived: poll field periods are
  irregular and do not always align to calendar weeks.
- A new source costs rows in `stat_indicator` plus a loader adapter. No DDL,
  no new tools.

### 3. Storage layer (`src/storage/stats.py`)

`StatsRepository`, using `get_pg_pool()` exactly as `AsyncPostgres` does.
One query layer, so MCP-3 and the CLI report identical numbers — the same
discipline `_stats_by` follows today.

- `list_sources()` — one row per source: indicator count and the earliest /
  latest period any of its indicators covers. This is the entry point for a
  caller who does not yet know what the subsystem holds.
- `list_indicators(source=None, limit=100)` — the registry itself, optionally
  scoped to one source.
- `search_indicators(query, source=None, limit=20)` — trigram similarity
  over `title` and `question_text`, ordered by score.
- `get_indicator(indicator_id)`
- `series(indicator_id, since=None, until=None, dims=None, revision=None)` —
  latest revision per period by default; returns rows ordered by
  `period_start`.
- `upsert_indicator(...)` / `upsert_observations(rows)` — `ON CONFLICT DO
  UPDATE`, used by the loader.

### 4. Alignment math (`src/stats/align.py`)

Pure functions, no I/O, no LLM. This is where the comparison actually
happens, and it must be deterministic — an agent must never be asked to do
the arithmetic.

`align(series_a, series_b, *, granularity, normalize="zscore", max_lag=0)`
takes two `[{period_start, value}]` lists and returns:

- `grid` — both series resampled onto the common `granularity`.
  Down-aggregation only, chosen by `value_kind`: `mean` for `share`/`rate`/
  `index`, `last` for `level`. **Never interpolate upward** — a coarser
  series against a finer grid yields `null` cells plus a `sparse` flag.
- `normalized` — z-score within the requested window, so a percentage and a
  ruble amount become comparable.
- `gap` — per-period difference of the normalized series.
- `divergence` — scalar summary: mean absolute `gap`.
- `best_lag` / `correlation` — Pearson correlation over lags in
  `[-max_lag, +max_lag]`, reporting the best-fitting shift. Polls describe
  what already happened while channels react earlier; without a lag search
  the divergence is an artefact of timing.
- `warnings` — sparsity, too few overlapping periods (below 8, correlation
  is not reported), unit or `value_kind` mismatch.

### 5. MCP-3 tools (`src/mcp/stats_server.py`)

Built on the `channel_message_stats` template: thin validating helpers that
are unit-testable without FastMCP, no retriever/graph bootstrap, plain
Postgres, and a **120 s** timeout rather than 1800 s.

- `stat_indicators_search(query=None, source=None, limit=20)` — discovery.
  **`query` is optional on purpose.** Called with no arguments it returns the
  catalog — the sources, how many indicators each holds, and the period they
  cover. Called with `source` alone it lists that source's indicators. Called
  with `query` it runs the trigram search. Without the no-argument mode a
  caller has to guess a search term before it can learn anything, and a
  trigram miss is indistinguishable from "no such data" — so an agent that
  guessed wrong would confidently report that the statistic does not exist.
  Every hit carries unit / value_kind / granularity, which is what tells the
  caller whether two indicators are comparable.

  The server's FastMCP `instructions` must state the discovery-first order
  explicitly: call `stat_indicators_search` with no arguments to see what
  exists, then narrow. The tool list alone does not teach that.
- `stat_series(indicator_id, since=None, until=None, dims=None)` — the
  values, plus indicator metadata and `source_doc_id` per point for
  provenance back to the bulletin.
- `stat_align(series_a, series_b, granularity, normalize=…, max_lag=…)` —
  the pure function above, exposed directly. It takes two series **as
  arguments** and reads nothing, which is what keeps the boundary clean: the
  agent fetches the channel series from MCP-2 and the indicator series from
  MCP-3, then hands both to this tool.

Errors follow the existing convention: return `{"error": "..."}`, do not
raise (`_stats_by`, `tools_server.py:402`).

### 6. Required change on the semantic side (MCP-2)

The channel-side series is not reachable over MCP today. Expose two
existing functions as atomic tools in `src/mcp/tools_server.py`:

- `topic_trend(topic, granularity, since, until)` → attention series
- `polarity_evolution(name, rel_type)` → valuation series

Both wrap `src/analytics/primitives/dynamics.py` functions that already
exist; the work is the tool surface, docstrings and validation, not the
computation. This lands in the semantic service, not MCP-3 — but without it
the comparison use case does not assemble.

### 7. Write path (`src/api/routes/stats_data.py`)

`POST /api/v1/statistics/load` takes one indicator and its observations as
JSON and upserts both. Row volumes are small and curated, so there is no file
upload, no CSV parsing and no per-source adapter — whoever has the numbers
posts them.

The prefix is `/statistics` rather than `/stats` because `/stats` already
means ingest-pipeline statistics over the `documents` table
(`src/api/routes/stats.py`). Two different meanings of the word must not share
a URL space.

Raw values are stored **as loaded** — alignment and normalisation are
recomputed on read, so changing the normalisation method never requires
reloading a source. The call is idempotent: re-posting the same payload
changes nothing, while re-posting a period at a higher `revision` adds a row
rather than overwriting one.

Reads do not live here. They are served by MCP-3, which is the surface agents
talk to; this endpoint exists for whoever feeds the subsystem.

`entity_vid` is supplied by the caller (a curated value), not inferred.
Inference is a later, separate question.

A scraper is out of scope here. When it comes, it runs as its own process
following the `tg_ingest` precedent — never inside an ingest worker.

### 8. Config

A `StatsSettings` block in `src/config.py`: default granularity, default
`max_lag`, search result cap, minimum overlapping periods for correlation.
MCP-3 transport/port flags reuse `src/mcp/_shared.py` (`parse_args`,
`build_sse_auth`, `assert_api_key_env_set`), same as MCP-1/MCP-2; suggested
default port 9003.

### 9. Testing

The whole subsystem is deterministic, so tests assert exact equality — the
main practical benefit of keeping it out of the semantic contour.

- `tests/test_stats/test_align.py` — resampling per `value_kind`, no upward
  interpolation, z-score, gap, lag search, every `warnings` trigger, and the
  under-8-overlap guard.
- `tests/test_api/test_stats_data.py` — the load endpoint against the real
  FastAPI app with `StatsRepository` patched: auth, rejection of an unknown
  `value_kind` / `granularity` and of a reversed period, `dims` and `revision`
  carried through, and an indicator registered with no observations yet.
- `tests/test_storage/test_stats_repository.py` — upsert idempotency,
  latest-revision selection, `dims` uniqueness including the empty-dims
  case, trigram search ordering.
- `tests/test_mcp/test_stats_server.py` — helper-level validation and error
  shapes, mirroring `tests/test_mcp/` conventions.

## Out of scope (YAGNI)

- Registering statistics primitives in the `/analyze` catalog, and the
  `prim.fn(store, …)` contract change that would require.
- A Milvus collection over the registry. Trigram search ships first;
  vectors are added only if synonym recall proves insufficient
  (`entity_er_vec` is the template when that day comes).
- Any scraper.
- Making synthesis optional on `/api/v1/search/*`.
- Materialising aligned series; alignment is computed on read.
- A separately deployed service, its own database, or its own repository.
- Confidence intervals from `sample_n` — the column is stored now, used
  later.

## Risks

- **Indicator ↔ topic mapping is manual.** `entity_vid` is curated. With
  many sources this becomes the maintenance cost of the subsystem, and a
  wrong mapping produces a confident, wrong divergence. Mitigation: mapping
  lives in the registry file under review, and `stat_align` reports unit /
  `value_kind` mismatches.
- **Trigram search misses synonyms.** "настроения" will not find
  "социальное самочувствие". Accepted for the first iteration; the vector
  collection is the known escape hatch.
- **Divergence is not causation.** The output is a described gap with a
  lag, not an explanation. The tool returns numbers and warnings; any
  interpretation is the agent's, with both sides cited.
- **Two surfaces can drift.** If a second query path is ever added, it must
  go through `StatsRepository` — the same rule `_stats_by` follows for
  `/api/v1/stats`.

## Files touched

New:
- `src/mcp/stats_server.py`
- `src/storage/stats.py`
- `src/stats/__init__.py`, `src/stats/align.py`
- `src/api/routes/stats_data.py`
- `tests/test_stats/`, `tests/test_storage/test_stats_repository.py`,
  `tests/test_mcp/test_stats_server.py`

Modified:
- `scripts/setup_db.py` — `pg_trgm` extension, two tables, four indexes
- `src/config.py` — `StatsSettings`
- `src/mcp/tools_server.py` — expose `topic_trend`, `polarity_evolution`
- `docs/runbook/mcp.md` — document MCP-3 and its transports
- `README.md:141`, `README.md:178`, `docs/FEATURES.md:101` — these say "two
  MCP servers"; they become three
