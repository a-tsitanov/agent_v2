# Entity Search Table Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** A flat, trigram-indexed `entity` table in Postgres that mirrors the graph's entities, giving graph-search a lexical entry (exact / prefix / substring name lookup with label filters) alongside its existing vector kNN.

**Architecture:** The table is a search MIRROR, not a second source of truth — the graph stays canonical. It is filled by a fail-soft upsert grafted onto the existing entity write (`NebulaGraphStore.upsert_nodes`), backfilled once from the graph, queried by a repository shaped like `StatsRepository`, and joined into the graph-search entry union.

**Tech Stack:** psycopg 3 (sync pool for writes, async for reads), pg_trgm, pytest.

**Spec:** [`../specs/2026-08-21-entity-search-table-design.md`](../specs/2026-08-21-entity-search-table-design.md)

## Global Constraints

- The table is a MIRROR. The graph is canonical. Writes are FAIL-SOFT: any Postgres error is logged and swallowed, and the graph write must not fail because of it.
- `vid = entity_vid(name)` — the SAME deterministic key the graph vertex uses (`src/graph/nebula_store.py:63`). Upserts are keyed on it and are idempotent.
- No `pagerank` / `betweenness` in the table — those are offline (`AnalyticsMaterialize`), would drift. Not in scope.
- Read path async over `get_pg_pool()`; write path SYNC over `get_pg_sync_pool()`, because `upsert_nodes` is synchronous (same reason the ER verdict cache uses the sync pool).
- Ruff: `line-length = 100`, `py312`, `select = ["E","F","I","B","UP","SIM","RUF"]`. pytest `asyncio_mode = "auto"`.
- Do NOT run the backfill or touch the live database from a test. Backfill is a deployment step (Task 5), exercised by tests against stubs only.

---

### Task 1: The table

**Files:**
- Modify: `scripts/setup_db.py`
- Test: `tests/test_scripts/test_setup_db_entity.py`

**Interfaces:**
- Produces: table `entity(vid PK, name, label, description, mention_count, updated_at)` + `entity_name_trgm_idx` (GIN trgm) + `entity_label_idx`.

- [ ] **Step 1: Write the failing test**

`tests/test_scripts/test_setup_db_entity.py`, following `tests/test_scripts/test_setup_db_er_verdict.py`:

```python
from scripts.setup_db import _ENTITY_DDL, _ENTITY_INDEXES_DDL


def test_table_is_created_idempotently():
    assert "CREATE TABLE IF NOT EXISTS entity" in _ENTITY_DDL


def test_vid_is_the_primary_key():
    assert "vid           TEXT PRIMARY KEY" in _ENTITY_DDL


def test_name_is_not_nullable():
    assert "name          TEXT NOT NULL" in _ENTITY_DDL


def test_no_pagerank_column():
    """Centrality is offline and would drift — deliberately absent."""
    assert "pagerank" not in _ENTITY_DDL
    assert "betweenness" not in _ENTITY_DDL


def test_trigram_index_on_name_for_substring():
    assert "entity_name_trgm_idx" in _ENTITY_INDEXES_DDL
    assert "gin_trgm_ops" in _ENTITY_INDEXES_DDL


def test_label_index_for_filtering():
    assert "entity_label_idx" in _ENTITY_INDEXES_DDL
```

- [ ] **Step 2: Run to verify it fails**

`uv run pytest tests/test_scripts/test_setup_db_entity.py -v`
Expected: FAIL, `ImportError: cannot import name '_ENTITY_DDL'`.

- [ ] **Step 3: Implement**

In `scripts/setup_db.py`, next to `_ER_VERDICT_DDL`, add:

```python
# Search mirror of the graph's entities. NOT a second source of truth —
# the graph stays canonical; this is a trigram-indexed flat copy so name
# lookup (exact/prefix/substring + label filter) runs in Postgres instead
# of scanning Nebula, which falls over on full scans. `vid` is the same
# entity_vid(name) key the graph vertex uses, so the mirror lines up by
# key. No pagerank/betweenness: those are offline and would drift.
_ENTITY_DDL = """
CREATE TABLE IF NOT EXISTS entity (
    vid           TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    label         TEXT NOT NULL DEFAULT '',
    description   TEXT NOT NULL DEFAULT '',
    mention_count INTEGER NOT NULL DEFAULT 1,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

_ENTITY_INDEXES_DDL = """
CREATE INDEX IF NOT EXISTS entity_name_trgm_idx
    ON entity USING GIN (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS entity_label_idx ON entity (label);
"""
```

The `gin_trgm_ops` operator class needs the `pg_trgm` extension, which
`_PG_TRGM_DDL` already creates earlier in this file — run these AFTER it.

Add the executes in `main()` after the `_ER_VERDICT_DDL` execute:

```python
        cur.execute(_ENTITY_DDL)
        cur.execute(_ENTITY_INDEXES_DDL)
```

- [ ] **Step 4: Run to verify it passes**

`uv run pytest tests/test_scripts/test_setup_db_entity.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/setup_db.py tests/test_scripts/test_setup_db_entity.py
git commit -m "feat(entity): add the Postgres entity search table DDL"
```

---

### Task 2: The search repository

**Files:**
- Create: `src/storage/entity_search.py`
- Test: `tests/test_storage/test_entity_search.py`

**Interfaces:**
- Consumes: table from Task 1; `get_pg_pool` (`src/storage/pg_pool.py`).
- Produces:
  - `build_entity_search_query(query: str, *, mode: str, label: str | None, limit: int) -> tuple[str, list[Any]]` — pure. `mode` ∈ `{"exact","prefix","substring"}`.
  - `EntitySearchRepository(dsn=None)` with `async search(query, *, mode="substring", label=None, limit=10) -> list[dict]`, each `{vid, name, label, description, mention_count}`.

- [ ] **Step 1: Write the failing test**

`tests/test_storage/test_entity_search.py`. Query builders are asserted exactly; row mapping uses the honest stub from `tests/test_storage/test_stats_repository.py` (it HONOURS `row_factory` — keep that).

```python
from src.storage.entity_search import build_entity_search_query


def test_exact_matches_the_whole_name():
    sql, params = build_entity_search_query("Украина", mode="exact", label=None, limit=10)
    assert "name = %s" in sql
    assert params[0] == "Украина"
    assert params[-1] == 10


def test_prefix_uses_ilike_anchored_left():
    sql, params = build_entity_search_query("Украин", mode="prefix", label=None, limit=10)
    assert "name ILIKE %s" in sql
    assert params[0] == "Украин%"


def test_substring_uses_trigram_and_orders_by_similarity():
    sql, params = build_entity_search_query("Ромаш", mode="substring", label=None, limit=10)
    # `%%` is the psycopg-escaped `%` trigram operator.
    assert "name %% %s" in sql
    assert "similarity(name, %s)" in sql
    assert "ORDER BY" in sql


def test_label_filter_is_added_only_when_given():
    sql_no, p_no = build_entity_search_query("x", mode="exact", label=None, limit=5)
    assert "label = %s" not in sql_no
    sql_yes, p_yes = build_entity_search_query("x", mode="exact", label="Person", limit=5)
    assert "label = %s" in sql_yes
    assert "Person" in p_yes


def test_mention_count_breaks_ties():
    """Frequent entities surface first among equal matches."""
    sql, _ = build_entity_search_query("x", mode="prefix", label=None, limit=5)
    assert "mention_count DESC" in sql


def test_unknown_mode_is_rejected():
    import pytest
    with pytest.raises(ValueError, match="unknown mode"):
        build_entity_search_query("x", mode="fuzzy", label=None, limit=5)
```

- [ ] **Step 2: Run to verify it fails**

`uv run pytest tests/test_storage/test_entity_search.py -v`
Expected: FAIL, module missing.

- [ ] **Step 3: Implement**

`src/storage/entity_search.py`, mirroring `src/storage/stats.py`:

```python
"""Postgres access for the entity search mirror.

Pure query builders (asserted exactly, no live DB) + a thin async
repository over the process pool. The graph is canonical; this table is
a trigram-indexed copy so name lookup runs here instead of scanning
Nebula.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

from src.storage.pg_pool import get_pg_pool

_COLUMNS = "vid, name, label, description, mention_count"
_MODES = ("exact", "prefix", "substring")


def build_entity_search_query(
    query: str, *, mode: str, label: str | None, limit: int,
) -> tuple[str, list[Any]]:
    """One name-search query. `mode`: exact / prefix / substring."""
    if mode not in _MODES:
        raise ValueError(f"unknown mode {mode!r}, expected one of {list(_MODES)}")
    params: list[Any] = []
    order = "mention_count DESC"
    if mode == "exact":
        where = "name = %s"
        params.append(query)
    elif mode == "prefix":
        where = "name ILIKE %s"
        params.append(f"{query}%")
    else:  # substring — trigram; `%%` is the escaped `%` operator
        where = "name %% %s"
        params.append(query)
        order = "similarity(name, %s) DESC, mention_count DESC"
    if label is not None:
        where += " AND label = %s"
        params.append(label)
    # similarity() in ORDER BY needs its own bound param, appended last so
    # it lands after the WHERE params.
    if mode == "substring":
        params.append(query)
    params.append(int(limit))
    sql = (
        f"SELECT {_COLUMNS} FROM entity WHERE {where} "
        f"ORDER BY {order} LIMIT %s"
    )
    return sql, params


class EntitySearchRepository:
    """Async wrapper over the `entity` table."""

    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn

    @asynccontextmanager
    async def _conn(self) -> AsyncIterator[psycopg.AsyncConnection]:
        if self._dsn is None:
            pool = await get_pg_pool()
            async with pool.connection() as conn:
                yield conn
        else:
            async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
                yield conn

    async def search(
        self, query: str, *, mode: str = "substring",
        label: str | None = None, limit: int = 10,
    ) -> list[dict[str, Any]]:
        if not (query or "").strip():
            return []
        sql, params = build_entity_search_query(
            query, mode=mode, label=label, limit=limit,
        )
        async with self._conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            return list(await cur.fetchall())


__all__ = ["EntitySearchRepository", "build_entity_search_query"]
```

- [ ] **Step 4: Run to verify it passes**

`uv run pytest tests/test_storage/test_entity_search.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/storage/entity_search.py tests/test_storage/test_entity_search.py
git commit -m "feat(entity): search repository with exact/prefix/substring modes"
```

---

### Task 3: Fail-soft upsert grafted onto the entity write

**Files:**
- Create: `src/graph/entity_table.py`
- Modify: `src/graph/nebula_store.py` (inside `upsert_nodes`, around line 200)
- Test: `tests/test_graph/test_entity_table.py`

**Interfaces:**
- Consumes: `entity_vid` (`src/graph/nebula_store.py:63`); `get_pg_sync_pool` (`src/storage/pg_sync_pool.py:41`).
- Produces: `mirror_entities(rows: list[dict]) -> None` where each row is `{vid, name, label, description, mention_count}`. Fail-soft: logs and swallows any error.

- [ ] **Step 1: Write the failing test**

`tests/test_graph/test_entity_table.py`, with the honest `row_factory` stub from `tests/test_graph/test_er_verdict_cache_postgres.py`:

```python
from src.graph.entity_table import mirror_entities


def test_upsert_is_keyed_on_vid(mirror_with_pool):
    fn, pool = mirror_with_pool()
    fn([{"vid": "v1", "name": "Украина", "label": "Location",
         "description": "государство", "mention_count": 5}])
    sql, params = pool.executed[0]
    assert "INSERT INTO entity" in sql
    assert "ON CONFLICT (vid) DO UPDATE" in sql
    assert "name = EXCLUDED.name" in sql
    assert "updated_at = now()" in sql


def test_empty_rows_is_a_noop(mirror_with_pool):
    fn, pool = mirror_with_pool()
    fn([])
    assert pool.executed == []


def test_a_postgres_error_is_swallowed(mirror_with_pool):
    """FAIL-SOFT: the mirror must never break the graph write."""
    fn, pool = mirror_with_pool(raise_on_execute=True)
    fn([{"vid": "v1", "name": "n", "label": "", "description": "", "mention_count": 1}])
    # no exception propagated
```

Add the `mirror_with_pool` fixture (a stub sync pool exposing `.executed`
and an optional `raise_on_execute`), modelled on the `_StubPool` in
`tests/test_graph/test_er_verdict_cache_postgres.py`.

- [ ] **Step 2: Run to verify it fails**

`uv run pytest tests/test_graph/test_entity_table.py -v`
Expected: FAIL, module missing.

- [ ] **Step 3: Implement**

`src/graph/entity_table.py`:

```python
"""Fail-soft mirror of graph entities into the Postgres `entity` table.

Called from the same place the graph write happens. The mirror is a
search accelerator, never a critical path: any Postgres error is logged
and swallowed so the graph write still succeeds. A drifted mirror is
re-fillable (scripts/backfill_entity_table.py).
"""

from __future__ import annotations

from typing import Any

from loguru import logger

_UPSERT = (
    "INSERT INTO entity (vid, name, label, description, mention_count) "
    "VALUES (%s, %s, %s, %s, %s) "
    "ON CONFLICT (vid) DO UPDATE SET "
    "name = EXCLUDED.name, label = EXCLUDED.label, "
    "description = EXCLUDED.description, "
    "mention_count = EXCLUDED.mention_count, updated_at = now()"
)


def mirror_entities(rows: list[dict[str, Any]]) -> None:
    """Upsert entity rows into Postgres. Fail-soft."""
    if not rows:
        return
    try:
        from src.storage.pg_sync_pool import get_pg_sync_pool

        values = [
            (r["vid"], r["name"], r.get("label") or "",
             r.get("description") or "", int(r.get("mention_count") or 1))
            for r in rows
        ]
        with get_pg_sync_pool().connection() as conn, conn.cursor() as cur:
            cur.executemany(_UPSERT, values)
    except Exception as exc:
        logger.warning("entity mirror upsert failed (search only): {e}", e=exc)


__all__ = ["mirror_entities"]
```

- [ ] **Step 4: Graft the call into `upsert_nodes`**

In `src/graph/nebula_store.py`, `upsert_nodes` already builds a per-node
value string and writes the graph. After the graph `INSERT VERTEX`
succeeds for a chunk, collect the same nodes' fields and mirror them.
Find where the chunk's rows are assembled (near line 200, after the value
list is built) and add, at the END of processing a chunk, still inside
the method:

```python
            # Search mirror (fail-soft). Same nodes, same entity_vid key.
            from src.graph.entity_table import mirror_entities
            mirror_entities([
                {
                    "vid": entity_vid(getattr(n, "name", "")),
                    "name": getattr(n, "name", ""),
                    "label": getattr(n, "label", "") or "",
                    "description": (getattr(n, "properties", {}) or {}).get("description", ""),
                    "mention_count": (getattr(n, "properties", {}) or {}).get("mention_count", 1),
                }
                for n in chunk
            ])
```

Place it so it runs only after the graph write for that chunk did NOT
raise — the mirror follows the canonical write, never precedes it.

- [ ] **Step 5: Run to verify it passes**

`uv run pytest tests/test_graph/test_entity_table.py -q`
Expected: PASS. Then `uv run pytest tests/test_graph -q -p no:randomly` to confirm `upsert_nodes`' own tests still pass (the graft is additive and fail-soft, so they must).

- [ ] **Step 6: Commit**

```bash
git add src/graph/entity_table.py src/graph/nebula_store.py tests/test_graph/test_entity_table.py
git commit -m "feat(entity): mirror entities to Postgres on graph write, fail-soft"
```

---

### Task 4: One-off backfill script

**Files:**
- Create: `scripts/backfill_entity_table.py`
- Test: `tests/test_scripts/test_backfill_entity_table.py`

**Interfaces:**
- Consumes: `mirror_entities` (Task 3); a graph store with `structured_query`.
- Produces:
  - `build_page_query(last_name: str | None, page: int) -> str` — pure, key-range pagination over `name`.
  - `backfill(store, *, page, start_after) -> tuple[int, str | None]` — returns `(copied, last_name)`.

- [ ] **Step 1: Write the failing test**

`tests/test_scripts/test_backfill_entity_table.py`, modelled on `tests/test_scripts/test_migrate_er_verdicts.py`:

```python
from scripts.backfill_entity_table import backfill, build_page_query


def test_first_page_has_no_range_filter():
    q = build_page_query(None, 500)
    assert "WHERE" not in q
    assert "ORDER BY $-.name" in q
    assert "LIMIT 500" in q


def test_resumed_page_filters_past_the_last_name():
    q = build_page_query("Кремль", 500)
    assert 'WHERE `Entity`.name > "Кремль"' in q
    assert "ORDER BY $-.name" in q


def test_page_query_never_uses_offset_pagination():
    """Offset pagination fails on the live store with StorageMemoryExceeded."""
    for q in (build_page_query(None, 500), build_page_query("x", 500)):
        assert "," not in q.split("LIMIT")[-1]


def test_page_query_escapes_quotes():
    q = build_page_query('ООО "Ромашка"', 10)
    assert '\\"' in q


class _StubStore:
    def __init__(self, names):
        self.rows = [{"vid": f"v{i}", "name": n, "label": "X",
                      "description": "", "mc": 1} for i, n in enumerate(sorted(names))]
    def structured_query(self, q):
        page = int(q.rsplit("LIMIT", 1)[1])
        after = None
        if "name > " in q:
            after = q.split('name > "', 1)[1].split('" ', 1)[0]
        rest = [r for r in self.rows if after is None or r["name"] > after]
        return rest[:page]


def test_backfill_copies_every_row_across_pages():
    seen = []
    store = _StubStore(["Аня", "Борис", "Вера", "Глеб", "Дима"])
    copied, last = backfill(store, page=2, sink=seen.extend)
    assert copied == 5
    assert last == "Дима"
    assert {r["name"] for r in seen} == {"Аня", "Борис", "Вера", "Глеб", "Дима"}


def test_backfill_empty_store():
    copied, last = backfill(_StubStore([]), page=10, sink=lambda rows: None)
    assert (copied, last) == (0, None)
```

- [ ] **Step 2: Run to verify it fails**

`uv run pytest tests/test_scripts/test_backfill_entity_table.py -v`
Expected: FAIL, module missing.

- [ ] **Step 3: Implement**

`scripts/backfill_entity_table.py`, following `scripts/migrate_er_verdicts.py` (key-range pagination — offset pagination is measured to fail on the live store with `StorageMemoryExceeded`):

```python
"""One-off fill of the Postgres `entity` table from the graph.

Resumable, idempotent. Key-range pagination over `name` (NOT offset —
offset fails on the live store with StorageMemoryExceeded, per the ER
verdict migration). Reads by index, never a full scan.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Callable


def escape_ngql(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def build_page_query(last_name: str | None, page: int) -> str:
    where = (
        f'WHERE `Entity`.name > "{escape_ngql(last_name)}" '
        if last_name is not None else ""
    )
    return (
        f"LOOKUP ON `Entity` {where}"
        "YIELD id(vertex) AS vid, `Entity`.name AS name, "
        "`Entity`.label AS label, `Entity`.description AS description, "
        "`Entity`.mention_count AS mc "
        f"| ORDER BY $-.name | LIMIT {int(page)}"
    )


def backfill(
    store: Any, *, page: int = 2000, start_after: str | None = None,
    sink: Callable[[list[dict]], None] | None = None,
    progress: Any = None,
) -> tuple[int, str | None]:
    """Copy every entity into the sink. Returns (copied, last_name)."""
    from src.graph.entity_table import mirror_entities
    write = sink if sink is not None else mirror_entities

    copied, last_name, pages = 0, start_after, 0
    while True:
        rows = store.structured_query(build_page_query(last_name, page)) or []
        rows = [r for r in rows if isinstance(r, dict) and r.get("name")]
        if not rows:
            break
        write([
            {"vid": r["vid"], "name": r["name"], "label": r.get("label") or "",
             "description": r.get("description") or "",
             "mention_count": int(r.get("mc") or 1)}
            for r in rows
        ])
        copied += len(rows)
        last_name = str(rows[-1]["name"])
        pages += 1
        if progress:
            progress(f"page {pages}: +{len(rows)}, {copied} total, last={last_name!r}")
    return copied, last_name


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--page", type=int, default=2000)
    ap.add_argument("--start-after", default=None)
    args = ap.parse_args()
    from src.graph.store import build_graph_store
    store = build_graph_store()
    try:
        copied, last = backfill(
            store, page=args.page, start_after=args.start_after,
            progress=lambda m: print(m, flush=True),
        )
    except Exception as exc:
        print(f"FAILED: {exc}")
        print("resume with --start-after '<last name printed above>'")
        sys.exit(1)
    print(f"copied={copied} last_name={last!r}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run to verify it passes**

`uv run pytest tests/test_scripts/test_backfill_entity_table.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/backfill_entity_table.py tests/test_scripts/test_backfill_entity_table.py
git commit -m "feat(entity): resumable backfill of the entity table from the graph"
```

---

### Task 5: Deploy — table, backfill, verify

Operational; nothing committed except a runbook line if one is warranted.

- [ ] **Step 1: Create the table on the live database**

Run the `_ENTITY_DDL` + `_ENTITY_INDEXES_DDL` against the live database
(via `psql -f` on a copied file, since `docker exec` without `-i` does not
pass stdin — a known gotcha in this repo).

- [ ] **Step 2: Backfill**

```bash
docker exec -w /app agent_v2-worker-1 sh -lc \
  '/app/.venv/bin/python -m scripts.backfill_entity_table --page 2000'
```

Start with `--page 500` and watch host memory (the box is memory-tight
under ingest); raise the page only while memory holds.

- [ ] **Step 3: Verify count matches the graph**

`SELECT count(*) FROM entity` should equal the `Entity` tag count from
`SHOW STATS` (submit a fresh `SUBMIT JOB STATS` first if stale).

- [ ] **Step 4: Verify substring works**

```sql
SELECT name FROM entity WHERE name % 'Ромаш' ORDER BY similarity(name,'Ромаш') DESC LIMIT 5;
```
Expect names CONTAINING the fragment mid-string, which prefix search could not reach.

---

### Task 6: Join the table into the graph-search entry

**Files:**
- Modify: `src/graph/retriever.py` (`_aretrieve_nebula`, ~line 462)
- Test: `tests/test_retrieval/test_nebula_read_slice.py`

**Interfaces:**
- Consumes: `EntitySearchRepository.search` (Task 2); the existing vector kNN.
- Produces: `_aretrieve_nebula` seeds the walk from the UNION of vector kNN names and entity-table substring hits, deduped.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_retrieval/test_nebula_read_slice.py`, in the nebula
section:

```python
@pytest.mark.asyncio
async def test_aretrieve_unions_vector_and_table_seeds(monkeypatch):
    """The walk seeds from BOTH the vector kNN and the entity-table lexical
    hit — a named entity the vector misses still gets walked."""
    monkeypatch.setattr(
        "src.graph.retriever.settings.graph.backend", "nebula", raising=False,
    )
    walked: list[str] = []

    class _Store:
        def structured_query(self, q, param_map=None):
            return []

    r = GraphRetriever.for_store(_Store())

    async def _fake_knn_names(_q):
        return ["Вектор-сущность"]

    async def _fake_table(_q):
        return [{"name": "Таблица-сущность"}]

    async def _fake_walk(name, *, hops=1):
        walked.append(name)
        return RoundGraphData()

    monkeypatch.setattr(r, "_nebula_knn_names", _fake_knn_names, raising=False)
    monkeypatch.setattr(r, "_entity_table_names", _fake_table, raising=False)
    monkeypatch.setattr(r, "awalk", _fake_walk)
    await r.aretrieve("зерно")
    assert "Вектор-сущность" in walked
    assert "Таблица-сущность" in walked
```

This test defines the two seams the implementation must expose:
`_nebula_knn_names(query) -> list[str]` and
`_entity_table_names(query) -> list[dict]`.

- [ ] **Step 2: Run to verify it fails**

`uv run pytest tests/test_retrieval/test_nebula_read_slice.py::test_aretrieve_unions_vector_and_table_seeds -v`
Expected: FAIL.

- [ ] **Step 3: Refactor `_aretrieve_nebula` to two named seams + union**

Extract today's kNN block into `_nebula_knn_names(query)` (returns the
list of names it already computes), add `_entity_table_names`:

```python
    async def _entity_table_names(self, query: str) -> list[dict]:
        """Lexical seeds from the Postgres entity mirror. Fail-soft: an
        outage here just means the vector path stands alone."""
        try:
            from src.storage.entity_search import EntitySearchRepository
            return await EntitySearchRepository().search(
                query, mode="substring", limit=self._similarity_top_k,
            )
        except Exception as exc:
            logger.warning("entity-table seed failed (vector-only): {e}", e=repr(exc))
            return []
```

Then union in `_aretrieve_nebula`:

```python
        knn_names = await self._nebula_knn_names(query)
        table_rows = await self._entity_table_names(query)
        names = list(dict.fromkeys(
            [*knn_names, *[r["name"] for r in table_rows if r.get("name")]]
        ))
        hops = path_depth if path_depth is not None else 1
        out = RoundGraphData()
        for name in names:
            sub = await self.awalk(name, hops=hops)
            out.entities.extend(sub.entities)
            out.relations.extend(sub.relations)
        out.entities = _dedupe_entities(out.entities)
        return out
```

Keep the whole thing fail-soft: if `_nebula_knn_names` raises (embed /
vector-store failure), catch as today and still try the table seeds — a
vector outage should not blank the lexical path.

- [ ] **Step 4: Run to verify it passes**

`uv run pytest tests/test_retrieval/test_nebula_read_slice.py -q`
Expected: PASS. Then `uv run pytest tests/test_retrieval -q -p no:randomly`.

- [ ] **Step 5: Commit**

```bash
git add src/graph/retriever.py tests/test_retrieval/test_nebula_read_slice.py
git commit -m "feat(entity): seed graph-search from the entity table alongside vector kNN"
```

---

## Verification

- `entity` holds ~163k rows, matching the `Entity` tag count in `SHOW STATS`.
- `EntitySearchRepository().search("Ромаш", mode="substring")` finds "ООО Ромашка" — which prefix search could not.
- An ingest round after Task 3 adds new entities to the table, with no Postgres errors in the worker log, and a forced Postgres failure does NOT fail the graph write.
- `_aretrieve_nebula` on a named query walks the exact entity even when the vector kNN does not rank it first.

## Notes for the implementer

- `%` in a psycopg SQL string must be written `%%` — the trigram operator `name % ?` becomes `name %% %s`. The builder tests pin this.
- The mirror follows the canonical graph write; it never precedes it. If the graft in Task 3 runs before the `INSERT VERTEX`, a graph write that later fails would leave a row for an entity not in the graph.
- Do not add a foreign key or any cross-DB constraint. The table is a mirror; consistency is eventual and re-fillable, by design.
