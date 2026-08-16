# Move the ER Verdict Cache to Postgres Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Take the 1 395 491-vertex `ERVerdict` KV cache out of the Nebula graph and put it in a Postgres table, dropping the graph from ~1.56M vertices to ~163k.

**Architecture:** A composite `ERGraphOps` routes the three verdict-cache methods to a new synchronous Postgres implementation and leaves `merge_loser_into_canonical` on the graph backend. No call site and no signature changes. Spec: [`../specs/2026-08-16-er-verdict-cache-postgres-design.md`](../specs/2026-08-16-er-verdict-cache-postgres-design.md).

**Tech Stack:** psycopg 3 (sync + pool), pydantic-settings, Nebula nGQL, pytest.

## Global Constraints

- The verdict cache stays **OPTIONAL and FAIL-SAFE**. Any storage error is logged and swallowed; ER falls back to pure LLM judging with identical results. Do not narrow this.
- No change to what ER decides or when it judges. `merge_loser_into_canonical` is untouched.
- `_load_verdict_cache` and `_store_verdicts` in `entity_resolution.py` keep their current signatures and bodies.
- `AGENT_ER_VERDICT_CACHE_BACKEND=graph` must restore today's behaviour exactly, with no image rebuild.
- Ruff: `line-length = 100`, `py312`, `select = ["E","F","I","B","UP","SIM","RUF"]`. pytest `asyncio_mode = "auto"`.
- Never drop `er_verdict_key_idx` before the migration has been verified — it is the only enumeration path.

---

### Task 1: Postgres-backed verdict cache behind the existing seam

**Files:**
- Create: `src/storage/pg_sync_pool.py`
- Modify: `scripts/setup_db.py`, `src/graph/er_graph_ops.py`, `src/config.py`
- Test: `tests/test_graph/test_er_verdict_cache_postgres.py`, `tests/test_scripts/test_setup_db_er_verdict.py`

**Interfaces:**
- `get_pg_sync_pool() -> ConnectionPool` — process-global, `min_size=0`, mirrors `get_pg_pool()`.
- `PostgresERVerdictCache(dsn=None)` with `ensure_verdict_schema()`, `load_verdicts(keys) -> dict[str, bool]`, `store_verdicts(entries) -> None`.
- `build_er_graph_ops(store) -> ERGraphOps` — unchanged signature, now may return a composite.

- [ ] **Step 1: The table**

Add to `scripts/setup_db.py`, following the idempotent style already used for the `stat_*` tables:

```sql
CREATE TABLE IF NOT EXISTS er_verdict (
    er_key   TEXT PRIMARY KEY,
    same     BOOLEAN     NOT NULL,
    updated  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

No secondary index — the primary key is the only access path.

- [ ] **Step 2: The sync pool**

`src/storage/pg_sync_pool.py`, mirroring `src/storage/pg_pool.py` but with `psycopg_pool.ConnectionPool` and a `threading.Lock`. `min_size=0` so import opens nothing. Reuse `settings.postgres` sizing. Include `close_pg_sync_pool()`.

- [ ] **Step 3: Write the failing tests**

In `tests/test_graph/test_er_verdict_cache_postgres.py`, following the stub conventions in `tests/test_storage/test_stats_repository.py` (whose stub **honours `row_factory`** — keep that property; a stub that always yields dicts hides a real class of bug):

1. `load_verdicts([])` returns `{}` and issues no query.
2. `load_verdicts(keys)` issues ONE query for the whole batch (not one per key) and maps `{er_key: same}`.
3. `load_verdicts` returns only the keys Postgres knew — a miss is absent, not `False`. (A miss returned as `False` would silently mean "judged DIFFERENT" and suppress a real LLM judgement.)
4. `store_verdicts({})` is a no-op.
5. `store_verdicts` upserts by `er_key` and is idempotent: storing the same key twice leaves one row and the later value wins.
6. Round-trip: `True` and `False` both survive, distinctly.
7. The composite from `build_er_graph_ops` sends the three cache methods to Postgres and `merge_loser_into_canonical` to the graph implementation.
8. `AGENT_ER_VERDICT_CACHE_BACKEND=graph` returns the plain backend ops, with no Postgres involvement.
9. Fail-safe end to end: with the cache raising on load and on store, `_load_verdict_cache` returns `{}` and `_store_verdicts` does not raise.

In `tests/test_scripts/test_setup_db_er_verdict.py`: the DDL is emitted and running `setup_db` twice both times exits 0.

- [ ] **Step 4: Run to verify they fail**

```bash
uv run pytest tests/test_graph/test_er_verdict_cache_postgres.py tests/test_scripts/test_setup_db_er_verdict.py -v
```

- [ ] **Step 5: Implement**

`PostgresERVerdictCache` in `src/graph/er_graph_ops.py`:

- `ensure_verdict_schema()` — `CREATE TABLE IF NOT EXISTS` (same DDL as `setup_db.py`), so a worker on a fresh database is not blocked on the setup script.
- `load_verdicts(keys)` — one `SELECT er_key, same FROM er_verdict WHERE er_key = ANY(%s)`, `row_factory=dict_row`.
- `store_verdicts(entries)` — `executemany` / `execute_values`-style batched `INSERT ... ON CONFLICT (er_key) DO UPDATE SET same = EXCLUDED.same, updated = now()`.

Add `er_verdict_cache_backend: str = "postgres"` next to `er_verdict_cache_enabled` in `src/config.py`, validated against `{"postgres", "graph"}`, and describe it in `scripts/make_env.py` next to the existing `AGENT_ER_VERDICT_CACHE_ENABLED` entry.

`build_er_graph_ops` returns a composite when the setting is `postgres`, else today's backend ops unchanged.

- [ ] **Step 6: Run, lint, commit**

```bash
uv run pytest tests/test_graph tests/test_scripts tests/test_storage -q -p no:randomly
uv run ruff check src/graph/er_graph_ops.py src/storage/pg_sync_pool.py src/config.py scripts/setup_db.py
git commit -m "feat(er): back the verdict cache with Postgres instead of the graph"
```

---

### Task 2: The migration script

**Files:**
- Create: `scripts/migrate_er_verdicts.py`
- Test: `tests/test_scripts/test_migrate_er_verdicts.py`

**Interfaces:**
- `build_page_query(last_key: str | None, page: int) -> str` — pure.
- `migrate(store, cache, *, page: int, start_after: str | None) -> tuple[int, str | None]` — returns `(rows_copied, last_key)`.

- [ ] **Step 1: Write the failing tests**

The pure query builder is the part worth pinning, because the wrong shape here is what fails on the live store:

1. The first page has no `WHERE` clause and carries `ORDER BY $-.k` and `LIMIT <page>`.
2. A resumed page filters `WHERE ... er_key > "<last>"` and keeps the same `ORDER BY`/`LIMIT`.
3. A key containing `"` is escaped so the generated nGQL is not broken by it. (Keys are JSON text and really do contain quotes — the sample from production is `[["#ОсторожноСобчак", "Organization"], …]`.)
4. `migrate` stops when a page comes back short, and returns the last key seen.
5. `migrate` passes each page to `store_verdicts` in one call, and copies `True`/`False` faithfully.
6. An empty store copies 0 rows and returns `None`, without raising.

- [ ] **Step 2: Run to verify they fail**

`uv run pytest tests/test_scripts/test_migrate_er_verdicts.py -v`

- [ ] **Step 3: Implement**

Key-range pagination only — offset pagination fails on the live store with `StorageMemoryExceeded (-3600)`:

```
LOOKUP ON `ERVerdict` WHERE `ERVerdict`.er_key > "<last>"
  YIELD `ERVerdict`.er_key AS k, `ERVerdict`.same AS s
  | ORDER BY $-.k | LIMIT <page>
```

CLI: `--page` (default 2000), `--start-after`, `--dry-run`, `--limit-pages`. Print progress every page: rows so far, elapsed, last key. On any page failure, print the last key so the run can resume.

- [ ] **Step 4: Run, lint, commit**

```bash
uv run pytest tests/test_scripts -q -p no:randomly
uv run ruff check scripts/migrate_er_verdicts.py
git commit -m "feat(er): add the verdict-cache migration script"
```

---

### Task 3: Execute the migration and switch over

Operational. Nothing here is committed except a short runbook note.

- [ ] **Step 1: Stop the ingest pipeline**

`docker stop agent_v2-tg-ingest-1 agent_v2-ingest-consumer-1 agent_v2-worker-1`. ER writes verdicts; migrating a table that is being written to loses the writes that land after their page has passed. Authorized by the user.

- [ ] **Step 2: Create the table**

Run the `er_verdict` DDL against the live database.

- [ ] **Step 3: Migrate**

Start with `--page 500 --limit-pages 2` and measure. Raise the page size only while pages stay comfortably inside memory. Then run to completion.

- [ ] **Step 4: Verify**

Row count in `er_verdict` equals `ERVerdict` in `SHOW STATS`. Spot-check ~20 keys for identical `same` values on both sides.

- [ ] **Step 5: Roll out**

Rebuild the app images and restart. Confirm the worker starts clean and an ingest round produces no `ER verdict cache load failed` warnings.

- [ ] **Step 6: Restart ingest, watch one round**

Confirm ER is hitting the cache rather than re-judging everything.

---

### Task 4: Reclaim the graph

Only after Task 3 verifies. Each step is measured, not assumed.

- [ ] **Step 1: Drop the unused index**

`DROP TAG INDEX er_verdict_key_idx`. Nothing reads it (the `CREATE` in `nebula_schema.py:106` is its only mention in the repo), and after Task 3 nothing needs it for enumeration either.

- [ ] **Step 2: Remove the tag from the schema**

Delete the `ERVerdict` `CREATE TAG` and `CREATE TAG INDEX` statements from `src/graph/nebula_schema.py`, so a fresh space never recreates them. Commit.

- [ ] **Step 3: Delete the vertices, carefully**

Attempt in small batches, measuring memory. If it hits the ceiling, stop and record it for a maintenance window — the migration's correctness does not depend on this step.

- [ ] **Step 4: Re-measure**

`SUBMIT JOB STATS` then `SHOW STATS`. Record the new vertex total. Re-run the `graph_stats` tool and note whether the full-scan queries that failed today now succeed.

---

## Verification

`SHOW STATS` vertex total falls from ~1 558 997 toward ~163 000. `er_verdict` holds the same number of rows `ERVerdict` held. An ingest round after the switch judges only genuinely new pairs. `AGENT_ER_VERDICT_CACHE_BACKEND=graph` still works.

## Notes for the implementer

- A cache miss must be *absent* from the returned dict, never `False`. `False` means "judged DIFFERENT" and would suppress a real judgement.
- Do not add an index on `er_verdict.updated`. Nothing queries by age, and the pruning idea it would serve is a trap: the oldest verdicts are the most valuable, because stable name pairs recur indefinitely.
- The graph's own `NebulaERGraphOps.load_verdicts` uses `FETCH PROP ON ERVerdict "<vid>"` — VID-addressed. That is why the key index has no reader.
