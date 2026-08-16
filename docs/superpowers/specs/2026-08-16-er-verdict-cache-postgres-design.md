# Move the ER verdict cache out of the graph

Date: 2026-08-16. Related: [`BACKLOG.md`](../../BACKLOG.md).

## Problem

`SHOW STATS` on the production Nebula space (job 3, first ever run):

```
Tag    Entity          161 204
Tag    Community         2 302
Tag    ERVerdict     1 395 491
Edge   RELATED         310 989
Edge   IN_COMMUNITY    351 384
Edge   PARENT_OF         1 581
Edge   MENTIONS              0
Space  vertices      1 558 997
Space  edges           663 954
```

`ERVerdict` is **89.5% of all vertices**. It is not knowledge — it is a
key/value cache of LLM judgements: one vertex per borderline candidate
pair, keyed on the order-insensitive `(norm, label)|(norm, label)` pair,
valued `same: bool`. It lives in the graph only because entity
resolution was first written against Neo4j.

Three costs follow.

**Full vertex scans pay 10x.** Measured today: `MATCH (n) RETURN
count(n)` fails with `GraphMemoryExceeded (-2600)`, and even
`MATCH (a)-[e:RELATED]->(b) ... LIMIT 1` fails on the memory
high-watermark. Index lookups and `SHOW STATS` return instantly. The
graph is not too big; the cache riding along inside it is.

**One index is pure loss.** `nebula_schema.py:106` creates
`er_verdict_key_idx ON ERVerdict(er_key(256))`. Nothing reads it: the
only occurrence of that name in the repository is the `CREATE`, and the
Nebula read path is `FETCH PROP ON ERVerdict "<vid>"` — VID-addressed,
index-free. The index exists because the *Neo4j* path matches on
`v.key`. On the production backend it is maintained on every write for
no reader.

**Wrong home.** A unique-keyed KV cache is what a relational table is
for. Postgres gives the lookup index with the primary key, for free.

## Non-goals

Not changing what ER decides. Not changing when pairs are judged. Not
touching `merge_loser_into_canonical`, which is a genuine graph
operation and stays on the graph backend.

## Design

### Storage

```sql
CREATE TABLE IF NOT EXISTS er_verdict (
    er_key   TEXT PRIMARY KEY,
    same     BOOLEAN     NOT NULL,
    updated  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

The primary key is the lookup index. No secondary index — the only
access pattern is `WHERE er_key = ANY(...)`, which the PK serves.

Created by `scripts/setup_db.py` alongside the other tables, idempotent
in the same style.

### Seam

`ERGraphOps` (`src/graph/er_graph_ops.py`) is a `Protocol` with four
methods: three are the verdict cache (`ensure_verdict_schema`,
`load_verdicts`, `store_verdicts`) and one is a graph merge
(`merge_loser_into_canonical`).

The Protocol is **not** split. `build_er_graph_ops` returns a composite
that delegates the three cache methods to a Postgres implementation and
the merge to the existing backend implementation. Both callers in
`entity_resolution.py` (`_load_verdict_cache`, `_store_verdicts`) are
untouched, and so is every signature.

Rationale for the composite over splitting the Protocol: the split is
the tidier long-term shape, but it edits call sites for no behavioural
gain, and this change is already touching production ingest. The
composite keeps the diff to the factory.

### Synchronous access

`load_verdicts` / `store_verdicts` are synchronous — they were written
against a synchronous `structured_query`. The Postgres implementation
stays synchronous rather than pushing `async` up through
`entity_resolution.py`.

A process-global **sync** pool (`src/storage/pg_sync_pool.py`) mirrors
the existing async `get_pg_pool()`, including `min_size=0` so importing
it opens no connection. Per-call `psycopg.connect` is deliberately
avoided: `pg_pool.py`'s own docstring records that a connect/close storm
against this Postgres was a confirmed contributor to a merge-phase
freeze. Cache calls are per-batch, not per-pair, so demand is low, but
the pool costs nothing and removes the question.

### Backend selection

New setting alongside `er_verdict_cache_enabled`:

```
AGENT_ER_VERDICT_CACHE_BACKEND = postgres | graph      (default: postgres)
```

`graph` restores today's behaviour exactly, as a rollback that needs no
image rebuild.

The existing guarantee is unchanged and must stay: the cache is OPTIONAL
and FAIL-SAFE. Any storage error is logged and swallowed by
`_load_verdict_cache` / `_store_verdicts`, and ER falls back to pure LLM
judging with identical results. Moving the store does not narrow that.

### Migration

`scripts/migrate_er_verdicts.py`, one-off, resumable.

Enumeration must use **key-range pagination**:

```
LOOKUP ON `ERVerdict` WHERE `ERVerdict`.er_key > "<last>"
  YIELD `ERVerdict`.er_key AS k, `ERVerdict`.same AS s
  | ORDER BY $-.k | LIMIT <page>
```

Offset pagination is not available — measured: `| LIMIT 100000, 5` fails
with `StorageMemoryExceeded (-3600)`. A bare `LOOKUP ... | LIMIT 5000`
returns in 2.2 s, a keyed page of 3 in 2.6 s; page size is tuned by
measurement at run time, starting small.

**The index must not be dropped before the migration completes** — it is
the only way to enumerate these vertices.

Writes are batched `INSERT ... ON CONFLICT (er_key) DO NOTHING`, so a
re-run is safe and resumption needs only the last key.

### Cleanup

After the row counts are verified equal, in this order:

1. `DROP TAG INDEX er_verdict_key_idx`
2. remove the `ERVerdict` tag and index statements from
   `nebula_schema.py`
3. delete the vertices

Step 3 is **not** assumed to be cheap. Deleting 1.4M vertices may hit
the same memory ceiling as any other bulk operation, and Nebula reclaims
space at compaction, not at delete. The step is executed and measured,
and if it proves too expensive it is left for a maintenance window —
the correctness of everything above does not depend on it.

## Verification

- Row count in `er_verdict` equals the `ERVerdict` count from
  `SHOW STATS`.
- A spot sample of keys returns the same `same` value from Postgres as
  from Nebula.
- An ingest run after the switch shows cache hits (ER does not re-judge
  every pair) and no `ER verdict cache load failed` warnings.
- With `AGENT_ER_VERDICT_CACHE_BACKEND=graph` the old path still works.
- After cleanup, `SHOW STATS` shows the vertex total dropping from
  ~1.56M to ~163k.

## Risks

**Ingest is live.** ER runs inside `merge_and_resolve`. The migration
reads a table that ingest writes to; new verdicts written during the
migration may be missed. Mitigation: stop the ingest pipeline for the
migration window (authorized), or accept that missed keys cost repeat
LLM judgements, not correctness.

**Rollout touches ingest.** The change ships in the worker image and
requires a rebuild and a worker restart.

**Host memory.** The box is at ~99% swap. Every bulk operation here is
executed with that in mind, smallest-first, measured rather than
assumed.
