# Close the Statistics Subsystem Gaps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Close four gaps recorded in `docs/BACKLOG.md` — three settings that are documented but dead, a test stub that hides a class of runtime bugs, an enum duplicated into SQL where it cannot be extended, and one missing test.

**Architecture:** All four are "the promise and the code disagree". Nothing here changes behaviour a user can see; each makes an existing claim true.

**Tech Stack:** pydantic-settings, FastMCP, psycopg 3, Temporal, pytest.

## Global Constraints

- No user-visible behaviour change. Defaults must resolve to exactly today's values — `20`, `"week"`, `4`, `8`.
- Additive and backward compatible: existing callers, existing rows, existing API shape.
- Ruff: `line-length = 100`, `py312`, `select = ["E","F","I","B","UP","SIM","RUF"]`. pytest `asyncio_mode = "auto"`.
- Do NOT touch `src/config.py:148-154`'s graph-backend drift. That is a pre-existing defect needing the migration's history; it is recorded in the backlog and belongs to whoever owns that decision.
- Do not connect to the live Temporal or namespace `default`; do not rebuild images or start containers.

---

### Task 1: Make the documented settings real, and cover the untested fallback

**Files:**
- Modify: `src/mcp/stats_server.py`, `src/config.py`
- Test: `tests/test_mcp/test_stats_server.py`, `tests/test_config/test_settings.py`, `tests/test_workflow/test_search_orchestrator_synthesize.py`

**Interfaces:** no signature changes beyond parameter defaults; `StatsSettings` fields gain bounds.

- [ ] **Step 1: Confirm what is actually dead**

`grep -rn "settings.stats\." --include=*.py src/` returns exactly one hit today: `min_overlap`. Confirm that, and confirm the literals shadowing the other three: `limit: int = 20` (`stats_server.py:197`), `granularity: str = "week"` (`:248`), `max_lag: int = 4` (`:251`). If the picture differs, stop and report.

- [ ] **Step 2: Write the failing tests**

1. Each of `default_granularity`, `default_max_lag`, `search_limit` actually reaches the tool's behaviour — set a non-default value and assert the helper uses it. Read `tests/test_mcp/test_stats_server.py` and follow its helper-level conventions.
2. `StatsSettings` rejects values its own documentation forbids: `default_max_lag` below 0, `min_overlap` below 1, `search_limit` below 1. `scripts/make_env.py` states those bounds; nothing enforces them.
3. The orchestrator's own `except` branch: when the `rerank_sources` activity itself raises, `SearchOutcome.sources` still comes back as the full merged pool. The activity's internal "reranker unavailable" fallback is already covered in `tests/test_workflow/test_search_rerank.py`; this is the outer one, in `orchestrator.py`. Use the `WorkflowEnvironment.start_time_skipping()` harness already in `test_search_orchestrator_synthesize.py`.

- [ ] **Step 3: Run to verify they fail**

`uv run pytest tests/test_mcp/test_stats_server.py tests/test_config/test_settings.py tests/test_workflow/test_search_orchestrator_synthesize.py -v`

- [ ] **Step 4: Implement**

Add bounds to `StatsSettings` matching what `make_env.py` already promises — `Field(ge=0)` on `default_max_lag`, `Field(ge=1)` on `min_overlap` and `search_limit`, and validate `default_granularity` against `GRANULARITIES`.

Then wire the three into `src/mcp/stats_server.py`.

**A decision you must make and justify in your report:** a tool signature default like `limit: int = settings.stats.search_limit` is evaluated at import, so the value freezes when the module loads and the FastMCP schema shows the configured number. The alternative is `limit: int | None = None`, resolved inside the helper, which keeps the schema stable and stays trivially testable but makes the parameter nullable in the tool's public contract. Pick one, apply it consistently to all three, and say why. Whichever you choose, the effective default must stay `20` / `"week"` / `4` for a caller that passes nothing.

Leave `_MAX_SEARCH_LIMIT = 100` a constant — it is a hard ceiling, not a preference, and `make_env.py` does not advertise it.

- [ ] **Step 5: Run, lint, commit**

```bash
uv run pytest tests/test_mcp tests/test_config tests/test_workflow -q
uv run ruff check src/mcp/stats_server.py src/config.py
git commit -m "fix(stats): wire the documented settings and bound them"
```

---

### Task 2: One source for the enums, and a stub that cannot lie

**Files:**
- Modify: `scripts/setup_db.py`, `tests/test_storage/test_stats_repository.py`, `src/mcp/tools_server.py`
- Test: `tests/test_scripts/test_setup_db_stats.py`, `tests/test_storage/test_stats_repository.py`

- [ ] **Step 1: The stub**

`_StubConn.cursor()` in `tests/test_storage/test_stats_repository.py` swallows kwargs and always yields dict rows. Consequence: deleting `row_factory=dict_row` from any read in `src/storage/stats.py` breaks nothing in the suite, while on live psycopg3 the pool sets no row factory, so reads become tuples and `r["score"]` raises.

Make the stub honour `row_factory`: return dict rows only when asked, tuples otherwise. Then confirm the existing tests still pass — if any now fails, that failure is real and the production code needs the fix, not the test.

Add a test that pins it: a read called without `dict_row` must not silently work.

- [ ] **Step 2: The enums**

`VALUE_KINDS` / `GRANULARITIES` live in `src/stats/align.py`. Two copies exist: the SQL `CHECK` in `scripts/setup_db.py:142` and `_TREND_GRANULARITIES` in `src/mcp/tools_server.py:451`.

Build the SQL `CHECK` clauses from the imported constants instead of literal text, so the DDL cannot drift.

**The part that matters more than the duplication:** the constraint lives inside `CREATE TABLE IF NOT EXISTS`, and the table already exists in the live database. Adding a value to `VALUE_KINDS` later would be accepted by Python and rejected by Postgres, and `setup_db.py` would not fix it because the table is already there. Add an idempotent `ALTER TABLE ... DROP CONSTRAINT IF EXISTS ... ADD CONSTRAINT ...` so an existing table's constraint is brought in line on every run. Follow the existing idempotent-DDL style in that file.

Verify the ALTER is genuinely idempotent — running `setup_db` twice must both times exit 0.

`_TREND_GRANULARITIES`: check what it actually needs to be. `topic_trend` supports day/week/month/quarter/year, which is `GRANULARITIES` exactly — if so, import it rather than keeping a parallel tuple. If it is deliberately a subset, leave it and say why in your report.

- [ ] **Step 3: Tests**

`tests/test_scripts/test_setup_db_stats.py` currently pins the CHECK clause as literal text. Change it to assert the DDL is generated from the constants — a test that hard-codes the same string it is guarding proves nothing.

- [ ] **Step 4: Run, lint, commit**

```bash
uv run pytest tests/test_storage tests/test_scripts tests/test_stats tests/test_mcp -q
uv run ruff check scripts/setup_db.py src/mcp/tools_server.py
git commit -m "fix(stats): generate the DDL enums from one source, make the DB stub honest"
```

Do NOT run `setup_db` against the live database in this task — the ALTER path should be exercised by tests here; applying it to production is a deployment step, not part of the change.

---

## Verification

Setting `STATS_SEARCH_LIMIT=5` changes what `stat_indicators_search` returns. `STATS_MIN_OVERLAP=0` is rejected at startup rather than silently accepted. Removing `row_factory=dict_row` from a read in `src/storage/stats.py` now fails a test. Adding a value to `VALUE_KINDS` updates the SQL constraint on the next `setup_db` run.
