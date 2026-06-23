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
- [ ] `models/search.py`: `created_after/before` → `str|None` ISO (insertion
      range); add `doc_date_after/before: str|None`; field-validate (422).
- [ ] `search_v2.py`: drop `created_*` from `_RESERVED_FILTER_FIELDS`;
      `_local_params`: `bounds_from_iso(...)` → 4 epoch ints into `OrchestratorParams`.
- [ ] `contracts.py`: add 4 epoch-bound `int|None` to `OrchestratorParams`,
      `SubQueryParams`, `RetrieveParams`; orchestrator + subquery workflows
      thread them down (mechanical).
- [ ] `retrieve_subquestion` (`search/activities/retrieve.py`):
      reconstruct `DateBounds`; if `any_set` → per-request vector retriever with
      `similarity_top_k = overfetch_top_k(top_k, bounds)` passed into
      `dispatch("vector_search", …, retriever=…)`; else cached retriever.
      After merge/dedup → `filter_nodes(sources, bounds)` → truncate to top_k.
- [ ] `drift` inherits; `global` keeps reserved-warning for date fields.
- [ ] Tests (fakes): 422 paths; `_local_params` conversion; per-request retriever
      built with raised top_k only when bound set; merged sources post-filtered;
      truncate to top_k; no-bound path byte-identical (cached retriever, no filter).

## Phase 3 — verify + hand off
- [ ] Full ruff + `tests/test_retrieval tests/test_storage tests/test_api
      tests/test_workflow` green (mod known pre-existing test_search_community).
- [ ] Note live-only checks: Milvus epoch metadata round-trips + filterable in
      `_node_content`; Neo4j `:Chunk` date indexes created; e2e date query.
- [ ] Commit per phase; push only on explicit command.

## Risks / watch
- Spec written against older main; re-verify `retrieve_subquestion` /
  `_local_params` / `OrchestratorParams` shapes at edit time.
- Docs without `doc_date` + pre-feature chunks → excluded by any date filter
  (backfill = Backlog).
- Over-fetch ×3 raises retrieval volume only when a bound is set.
- Build the per-request retriever off the event loop if it does blocking I/O
  (Track A lesson) — verify whether `index.as_retriever` is cheap/sync.
