# Search date filters — implementation plan (Rev 2)

Date: 2026-06-23 · derived from
`docs/superpowers/specs/2026-06-22-search-date-filters-design.md` (Rev 2)
Branch: `feat/search-date-filters-v2` (off `main@baa0428`)
Supersedes the stale 1175-line `2026-06-22-search-date-filters.md` (old branch,
push-down design). Mechanism (Rev 2): **uniform post-filter + over-fetch**,
both stores. Milvus push-down → Backlog.

Each phase ends green (ruff + targeted tests). Commit only on explicit
go-ahead. Infra-dependent steps (Milvus/Neo4j live) flagged — unit-tested
with fakes, real validation deferred to a live stack.

## Phase 0 — branch + clean port  ✅ DONE (uncommitted)
- [x] Branch `feat/search-date-filters-v2` off `main`.
- [x] Updated spec → Rev 2.
- [x] Port `src/retrieval/date_filters.py` + `tests/test_retrieval/test_date_filters.py`
      (verbatim from old branch; ruff-modernised). Helpers already cover v1:
      `bounds_from_iso`, `DateBounds`, `node_metadata_in_range`, `filter_nodes`,
      `overfetch_top_k`; `to_metadata_filters` kept for the Backlog push-down.
- [x] Excluded: `vl` junk, the 3 already-merged Tier-0 commits, lock/req churn.
- 6 helper tests green, ruff clean.

## Phase 1 — data model + ingest stamping  ✅ DONE (uncommitted → committed)
- [x] `contracts.py`: `IngestParams` += `doc_date`, `doc_date_epoch`,
      `inserted_at_epoch`; `Ctx` += `doc_date_epoch`, `inserted_at_epoch`.
- [x] `scripts/setup_db.py`: `documents.doc_date DATE` + `documents_doc_date_idx`
      + `ALTER TABLE … ADD COLUMN IF NOT EXISTS` (for pre-existing DBs).
- [x] `src/storage/postgres.py`: `insert_pending(..., doc_date=None)` writes it.
- [x] `/ingest` (`routes/ingest.py`): `document_date` Form field → validate
      (422 via `iso_to_epoch_days` ValueError) → `doc_date_epoch`;
      `inserted_at_epoch = today_epoch_days()`; into `IngestParams`; `insert_pending`.
- [x] `fetch_source.py`: propagate epochs `IngestParams → Ctx` (both sites).
- [x] `parse_and_chunk.py`: stamp `doc_date_epoch` (omit if None) +
      `inserted_at_epoch` from `ctx` alongside position/doc_id.
- [x] `graph/index.py`: `ensure_chunk_date_indexes` (range idx, fail-open) +
      called off-loop in `build_property_graph`.
- [x] Tests: /ingest 422 + epoch propagation; parse_and_chunk stamping;
      ensure_chunk_date_indexes idempotent+fail-open. 620 green, ruff clean.

## Phase 2 — search API + retrieval post-filter
## Phase 2 — search API + retrieval post-filter  ✅ DONE (uncommitted)
- [x] `models/search.py`: `created_after/before` → `str|None` ISO; add
      `doc_date_after/before`; field-validator (422 on bad ISO).
- [x] `search_v2.py`: dropped `created_*` from `_RESERVED_FILTER_FIELDS`;
      `_local_params` → `bounds_from_iso` → 4 epoch ints into `OrchestratorParams`.
- [x] `contracts.py`: 4 epoch-bound `int|None` on `OrchestratorParams`,
      `SubQueryParams`, `RetrieveParams`; orchestrator (fan-out + coverage gap)
      + `subquery_wf` thread them down.
- [x] `_search_deps.py`: cache the vector index + `get_vector_retriever(top_k)`
      (per-request, in-memory `as_retriever` — no Milvus rebuild).
- [x] `retrieve_subquestion`: build `DateBounds`; `any_set` → over-fetch
      retriever at `overfetch_top_k`; merged pool `filter_nodes(...)`. NO
      truncate here (the downstream rerank does the final top-N cut — this
      stage returns a pool, unchanged for non-filtered queries).
- [x] `drift` inherits local. NOTE deviation: `global` no longer warns on date
      fields (created_* left the reserved tuple); global date-filtering stays Backlog.
- [x] Tests: SearchRequest 422 + accept; `_local_params` conversion (set/unset);
      retrieve over-fetch ×3 + post-filter drops out-of-range/missing; no-bound
      path uses cached retriever + no filter. 630 green, ruff clean.

## Phase 3 — verify + hand off
- [x] Full ruff clean on changed code; `tests/test_workflow test_api
      test_retrieval test_graph test_storage test_config` → 630 pass (mod the
      2 known pre-existing `test_search_community` stub failures).
- [ ] Live-only (needs stack): Milvus epoch metadata round-trips + is
      filterable; Neo4j `:Chunk` date indexes created; e2e date query.
- [ ] Commit per phase; push only on explicit command.

## Risks / watch
- Spec written against older main; re-verify `retrieve_subquestion` /
  `_local_params` / `OrchestratorParams` shapes at edit time.
- Docs without `doc_date` + pre-feature chunks → excluded by any date filter
  (backfill = Backlog).
- Over-fetch ×3 raises retrieval volume only when a bound is set.
- Build the per-request retriever off the event loop if it does blocking I/O
  (Track A lesson) — verify whether `index.as_retriever` is cheap/sync.
