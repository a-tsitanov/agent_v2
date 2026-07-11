# Nebula ingest write batching — throughput scale-blocker

**Status:** approved (autonomous, delegated — user reprioritised to scale-blockers) 2026-07-11. Sub-project of the NebulaGraph migration (nebula-becomes-default). Stage-4 first slice: the nebula write-path throughput wall. Branch `feat/nebula-ingest-batch` off `main` (base `adec19c`).

## Goal

`NebulaGraphStore.upsert_nodes` / `upsert_relations` currently emit ONE `session.execute` round-trip PER node and PER edge (`nebula_store.py:53-86`). At millions of entities/edges (the billion-scale target) this per-statement loop is the write wall. Batch the INSERTs into multi-VALUES statements (config-sized), amortising round-trips ~`batch_size`×. **Pure throughput change — identical per-item write semantics, nebula-only.** Neo4j path untouched (it already batches via UNWIND).

## Background (grounded)

`build_property_graph.py:121-124` calls `graph_store.upsert_nodes(entities)` / `upsert_relations(relations)` (via `write_with_retry`) with the full lists. The nebula impls loop `for n in nodes: self._exec("INSERT VERTEX ...single VALUES...")`. Entities are deduped by name upstream (`identifier_transform.py:200` passes `seen.values()`), so within one call VIDs are unique (no intra-batch collision). `entity_vid`/`_q` as today. `INSERT` is upsert-by-VID; grouping N rows into one `INSERT ... VALUES a:(...), b:(...), ...` is semantically identical to N sequential single-row INSERTs when the VIDs differ (they do).

## Global Constraints

- **Nebula-only, semantics-preserving.** The exact column values written per node/edge are byte-identical to today; only the statement grouping changes. Neo4j path untouched. Default backend still neo4j.
- Local commits only (no push). Never stage `docs/bruno/collection.bru`. Unit tests DB-free (fake session recording statements). `_q` quoting unchanged.
- Batch size from config (`NebulaSettings.write_batch_size`, env `NEBULA_WRITE_BATCH_SIZE`, default 256, ge=1). An empty input list emits ZERO statements (today emits zero too).

## Design

### 1. Config
`NebulaSettings.write_batch_size: int = Field(default=256, ge=1)` (env `NEBULA_WRITE_BATCH_SIZE`), documented in `scripts/make_env.py` if it lists nebula vars.

### 2. `nebula_store.py` — batched upserts
Extract each per-item VALUES-tuple builder, then chunk:
- `upsert_nodes(nodes)`: for each `chunk` of `settings.nebula.write_batch_size` nodes, build `INSERT VERTEX \`Entity\` (name, description, mention_count, created_at, label) VALUES ` + `", ".join(row(n) for n in chunk)` + `;` where `row(n)` = `f"{_q(vid)}:({_q(name)}, {_q(desc)}, {int(mention_count)}, {int(created_at)}, {_q(label)})"` (identical field expressions to today). One `_exec` per chunk.
- `upsert_relations(relations)`: same, one `INSERT EDGE \`RELATED\` (rel_type, polarity, valid_from, valid_to) VALUES ` + `", ".join(f"{_q(src)} -> {_q(tgt)}:({_q(rel_type)}, {_q(polarity)}, {int(valid_from)}, {int(valid_to)})" ...)` per chunk.
- Empty list → no statement. A chunk helper (local `_chunks(seq, n)` or inline slicing) keeps it simple.

### 3. Tests (DB-free)
- Fake session records statements. `upsert_nodes` of 5 nodes with `write_batch_size=2` → 3 statements (2+2+1); each is a single `INSERT VERTEX` with the right number of comma-joined VALUES; field values match `_q`/int expressions; VIDs = `entity_vid(name)`. Same for `upsert_relations` (endpoints `src -> tgt`, rel_type property). Empty list → 0 statements. A batch_size >= len → 1 statement.
- Monkeypatch `settings.nebula.write_batch_size` in tests to force multiple chunks.

### 4. Manual gate (live-verify)
On the running cluster: `upsert_nodes` of ~1000 synthetic entities in one call, verify vertex count (`LOOKUP`/`FETCH`) and a few props round-trip; compare wall-clock vs the pre-batch per-statement loop (expect a large reduction). Controller-run.

## Out of scope (deferred)

- **True bulk-import** (nebula-importer / SST offline bulk load) for the initial billion-scale backfill — a separate, heavier Phase-5 effort; multi-VALUES batching is the online-write win.
- **Merge-vs-overwrite correctness** of re-ingested entities (INSERT VERTEX overwrites all columns; neo4j MERGE accumulates e.g. mention_count) — a SEPARATE correctness slice, NOT changed here (this slice preserves today's overwrite semantics, just batched).
- Batching `MENTIONS` / other edge types (only `RELATED` + `Entity` are on the current write path).

## Interfaces produced

- `src/config.py`: `NebulaSettings.write_batch_size`.
- `src/graph/nebula_store.py`: `upsert_nodes` / `upsert_relations` batched (multi-VALUES).
