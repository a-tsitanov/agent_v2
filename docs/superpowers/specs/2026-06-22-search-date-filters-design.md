# Search date filters — design

Date: 2026-06-22 · **Rev 2: 2026-06-23** (reconciled with `main@baa0428`)
Status: approved (Rev 2 supersedes Rev 1; differences in **§ Rev-2 changes**)
Branch: `feat/search-date-filters-v2` (off `main`, NOT the stale
`feat/search-date-filters`)

## Goal

Let search refine results by two dates per document:
- **document date** — the date of the document itself, provided by the
  caller at `/ingest`.
- **insertion date** — when the document was ingested (= `documents.created_at`).

Both filterable at search time; both propagate into Neo4j so the **graph**
retriever's chunks can be filtered too (not only the vector store).

Scope: filters apply to **local** and **drift** search modes. `global`
(community-summary) mode ignores date filters for now (Backlog).
Granularity: **DATE** (day). Document date is **client-provided** (no
auto-extraction this iteration).

## Rev-2 changes (what differs from the 2026-06-22 draft)

Rev 1 was written against an older `main`. Reconciled against current code
(recon 2026-06-23):

1. **Filter mechanism = UNIFORM POST-FILTER for BOTH stores** (decided
   2026-06-23). Rev 1 split it (Milvus push-down + graph post-filter). The
   project's `MilvusVectorStore` stores metadata as a JSON field
   (`_node_content` via `node_to_metadata_dict`), NOT declared scalar
   columns, so `MetadataFilters` push-down is **unverified** and risky.
   v1 over-fetches and drops out-of-range hits in `retrieve_subquestion`
   for vector + graph + walk uniformly. **Milvus push-down → Backlog**
   (optimization, behind a verification spike).
2. **Contract chain is longer than Rev 1 assumed.** Real path:
   `SearchRequest → _local_params() → OrchestratorParams →
   SearchOrchestratorWorkflow → SubQueryParams → SubQueryRetrievalWorkflow
   → RetrieveParams → retrieve_subquestion`. Epoch bounds thread through
   **three** contracts (`OrchestratorParams`, `SubQueryParams`,
   `RetrieveParams`) plus `_local_params()`.
3. **Retriever is a cached singleton** (`_search_deps.get_retriever()`),
   and `vector_search` (`atomic_tools.py`) calls `retriever.aretrieve(query)`
   with no filters and no per-query `top_k`. Over-fetch is therefore wired
   by building a **per-request retriever** with a raised `similarity_top_k`
   and routing it into `dispatch("vector_search", …, retriever=…)`.
4. **Track A alignment** (post-`main` changes): `build_neo4j_graph_store()`
   is now a per-process cached singleton that MUST be called off the event
   loop (`await asyncio.to_thread(build_neo4j_graph_store)`). New `:Chunk`
   date indexes go through the existing ensure-index step in
   `build_property_graph`, which already runs via `asyncio.to_thread`.

## Chosen approach (A) — unchanged

Entities (`__Entity__`) are cross-document and merged → no single date.
Dates live on per-document **`:Chunk`** nodes; an entity is in-range iff it
has ≥1 in-range mention (chunk). Alternatives B (`:Document` node) and C
(date-denormalized entities) deferred — Backlog.

Canonical filterable value: **epoch-days (int)** — one numeric form that
compares identically everywhere. Fields: `doc_date_epoch`,
`inserted_at_epoch`. (An ISO string may ride alongside for readability; it
is NOT the filter field.)

## Data flow — single stamping point

```
/ingest (client document_date + inserted_at = today UTC)
   → IngestParams{doc_date, doc_date_epoch, inserted_at_epoch}
   → Ctx{doc_date_epoch, inserted_at_epoch}
   → parse_and_chunk: stamp node.metadata[doc_date_epoch, inserted_at_epoch]
       → index_vector            → Milvus (_node_content JSON metadata)
       → build_property_graph    → :Chunk properties (Neo4j)
search:
   SearchRequest{doc_date_after/before, created_after/before (ISO)}
     → epoch bounds → OrchestratorParams → SubQueryParams → RetrieveParams
     → retrieve_subquestion: over-fetch, then post-filter merged sources
```

Stamping happens once, in `parse_and_chunk` (today stamps `position` +
`doc_id` at `parse_and_chunk.py:73-78`), and propagates to both stores.

## Components & changes

### Data model
- **Postgres `documents`** (`scripts/setup_db.py`): add `doc_date DATE`
  (nullable) + `CREATE INDEX documents_doc_date_idx`. Insertion date stays
  `created_at`. *(Ported from the old branch; reconcile with main's current
  `setup_db.py`.)*
- **`IngestParams`** (`src/workflow/contracts.py`, `_Frozen`): add
  `doc_date: str = ""` (ISO `YYYY-MM-DD`), `doc_date_epoch: int | None = None`,
  `inserted_at_epoch: int | None = None`. Snapshotted at `/ingest` (outside
  the Temporal sandbox — same pattern as `wiki_enabled`).
- **`Ctx`** (`contracts.py`): add `doc_date_epoch: int | None`,
  `inserted_at_epoch: int | None` so `parse_and_chunk` can stamp them.

### Ingest
- **`/ingest`** (`src/api/routes/ingest.py`): accept `document_date` Form
  field (`YYYY-MM-DD`); validate → 422 on bad format; compute
  `doc_date_epoch`; set `inserted_at_epoch = today (UTC)`; pass into
  `IngestParams`. `insert_pending` writes `doc_date`.
- **`AsyncPostgres.insert_pending`** (`src/storage/postgres.py`): add the
  `doc_date` column to the INSERT *(port the doc_date bits only — main's
  `postgres.py` already has the Tier-0 pg_pool changes; do NOT re-introduce
  those)*.
- **`parse_and_chunk`** (`src/workflow/activities/parse_and_chunk.py:73-78`):
  alongside `position`/`doc_id`, stamp `md["doc_date_epoch"]` (omit if None)
  and `md["inserted_at_epoch"]` on every chunk, read from `ctx`.
- **`index_vector`** (`index_vector.py`): no change. The epoch ints are tiny
  and survive `_snapshot_for_milvus` (which only peels the LARGEST metadata
  values to fit the 64 KB budget). Add a test asserting they survive.
- **`build_property_graph`**: write path unchanged (PropertyGraphIndex
  persists `node.metadata` onto `:Chunk`). Add `ensure_chunk_date_indexes`
  to `src/graph/index.py` — range indexes on `:Chunk(doc_date_epoch)` and
  `:Chunk(inserted_at_epoch)`, fail-open like the existing
  `ensure_entity_lookup_indexes`; call it in `build_property_graph`'s
  ensure-step via `asyncio.to_thread` (already off-loop there).

### Search
- **`SearchRequest`** (`src/models/search.py:52-53`):
  `created_after`/`created_before` are currently `int | None` RESERVED
  (never applied). Change to `str | None` ISO `YYYY-MM-DD` (insertion-date
  range). Add `doc_date_after`/`doc_date_before` (`str | None`, ISO,
  document-date range). Validate → 422 on bad format.
- **`search_v2.py`**: drop `created_after`/`created_before` from
  `_RESERVED_FILTER_FIELDS` (line 43-45; now implemented). In `_local_params`
  (line 60-75): convert the four ISO request dates → epoch-day bounds
  (4× `int | None`) and forward them into `OrchestratorParams`. `global`
  keeps the reserved-warning for date fields.
- **Contracts** (`contracts.py`): add `doc_date_after/before_epoch` and
  `inserted_after/before_epoch` (`int | None`) to `OrchestratorParams`
  (433-452), `SubQueryParams` (401-405), and `RetrieveParams` (361-369).
  `SearchOrchestratorWorkflow` / `SubQueryRetrievalWorkflow` pass them down
  unchanged (mechanical thread-through).
- **Retrieval mechanism (Rev-2: uniform post-filter)** in
  `retrieve_subquestion` (`src/workflow/search/activities/retrieve.py:100-186`):
  - **Over-fetch:** when ANY date bound is set, build a per-request vector
    retriever with `similarity_top_k = top_k × OVER_FETCH_FACTOR` (default
    ×3) and pass it into `dispatch("vector_search", …, retriever=…)` instead
    of the cached singleton. (No bound set → keep the cached retriever, zero
    overhead.)
  - **Post-filter:** after sources are merged/deduped by chunk_id, apply one
    shared helper that drops every `NodeWithScore` whose `node.metadata`
    epoch dates fall outside the set bounds — covering vector + graph +
    walk uniformly (chunks carry the epoch fields from the stamping step).
    A node MISSING the field when a bound is set is **dropped**.
  - **Truncate:** after filtering, cut back to `top_k`.
  - Helpers live in `src/retrieval/date_filters.py` (ported from the old
    branch: ISO↔epoch, bound assembly, the post-filter predicate).
- **drift** inherits the local retrieval path → same filter. **global**
  ignores (reserved-warning).

## Error handling & compatibility
- Invalid date format → 422 at `/ingest` and `/search`.
- A document without `doc_date` → its chunks have no `doc_date_epoch`; a
  document-date filter **excludes** them. Same for pre-feature chunks
  lacking the fields → excluded by any date filter. Backfill is Backlog.
- Filters combine with AND; each bound (after/before) independent + optional.
- No bound set anywhere → behaviour byte-identical to today (cached
  retriever, no post-filter, no over-fetch).

## Testing (repo style — pure, infra-free helpers preferred)
- ISO→epoch-days conversion + validation (round-trip, bad input → error).
- `SearchRequest` date validation (422 paths) + `created_*` type change.
- Post-filter predicate: keeps in-range, drops out-of-range, drops
  missing-field-when-bound-set, no-op when no bound.
- Over-fetch `top_k` math (×factor only when a bound is set).
- `_local_params` ISO→epoch conversion + forwarding into `OrchestratorParams`.
- `retrieve_subquestion` wiring with fakes: per-request retriever built with
  the raised top_k when a bound is set; cached retriever otherwise; merged
  sources post-filtered; truncated to top_k.
- `insert_pending` writes `doc_date`; epoch ints survive `_snapshot_for_milvus`.

## Backlog (explicitly out of scope here)
- **Milvus push-down** (`MetadataFilters` GTE/LTE → Milvus boolean expr) as
  an over-fetch-eliminating optimization, behind a filter-syntax spike.
- Approach B (`:Document` node) and C (date-denormalized entities).
- Date filtering for `global` (community-summary) search.
- `datetime` granularity (currently DATE).
- Backfill of epoch fields onto existing Milvus chunks + Neo4j `:Chunk`.
- Auto-extraction of document date from content/metadata/filename.
