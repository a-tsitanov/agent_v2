# Search date filters — design

Date: 2026-06-22
Status: approved (pending spec review)

## Goal

Let search refine results by two dates per document:
- **document date** — the date of the document itself, provided by the
  caller at `/ingest`.
- **insertion date** — when the document was ingested (= `documents.created_at`).

Both must be filterable at search time, and both must propagate into Neo4j
so the **graph** retriever can filter too (not only the vector store).

Scope: filters apply to **local** and **drift** search modes. `global`
(community-summary) mode ignores date filters for now (see Backlog).
Granularity: **DATE** (day). Document date is **client-provided** (no
auto-extraction in this iteration).

## Chosen approach (A)

Entities (`__Entity__`) are cross-document and merged, so they carry no
single date. Dates live on per-document **`:Chunk`** nodes; graph-local
retrieval filters at the chunk level (an entity is in-range iff it has at
least one in-range mention). Alternatives B (`:Document` node) and C
(date-denormalized entities) are deferred — see Backlog.

Canonical filterable value: **epoch-days (int)** — one numeric
representation that range-filters identically in Milvus and Neo4j.
Fields: `doc_date_epoch`, `inserted_at_epoch`. (An ISO string may be kept
alongside for readability; it is NOT the filter field.)

## Data flow — single stamping point

```
/ingest (client doc_date + inserted_at=now)
   → IngestParams{doc_date, doc_date_epoch, inserted_at_epoch}
   → Ctx{doc_date_epoch, inserted_at_epoch}
   → parse_and_chunk: stamp node.metadata[doc_date_epoch, inserted_at_epoch]
       → index_vector  → Milvus scalar fields   (vector filter)
       → build_property_graph (PropertyGraphIndex) → :Chunk properties (graph filter)
```

One stamping point (`parse_and_chunk`) propagates to both stores.

## Components & changes

### Data model
- **Postgres `documents`** (`scripts/setup_db.py`): add `doc_date DATE`
  (nullable) + `CREATE INDEX documents_doc_date_idx ON documents (doc_date)`.
  Insertion date stays as `created_at`.
- **`IngestParams`** (`src/workflow/contracts.py`): add `doc_date: str = ""`
  (ISO `YYYY-MM-DD`), `doc_date_epoch: int | None = None`,
  `inserted_at_epoch: int`. Snapshotted at `/ingest` (outside the Temporal
  sandbox — same pattern as `wiki_enabled`).
- **`Ctx`** (`src/workflow/contracts.py`): add `doc_date_epoch: int | None`,
  `inserted_at_epoch: int` so `parse_and_chunk` can stamp them.

### Ingest
- **`/ingest`** (`src/api/routes/ingest.py`): accept `document_date` form
  field/header (`YYYY-MM-DD`); validate → 422 on bad format; compute
  `doc_date_epoch`; set `inserted_at_epoch = today (UTC)`; pass into
  `IngestParams`. `AsyncPostgres.insert_pending` writes `doc_date`.
- **`AsyncPostgres.insert_pending`** (`src/storage/postgres.py`): add the
  `doc_date` column to the INSERT.
- **`parse_and_chunk`** (`src/workflow/activities/parse_and_chunk.py`): stamp
  `node.metadata["doc_date_epoch"]` (omit if None) and
  `node.metadata["inserted_at_epoch"]` on every chunk, from `ctx`.
- **`index_vector`**: no change expected — it serialises `node.metadata` to
  Milvus. Verify the small int fields survive the metadata-truncation logic.
- **`build_property_graph`**: no change to write path (PropertyGraphIndex
  persists `node.metadata` onto `:Chunk`). Add Neo4j range indexes on
  `:Chunk(doc_date_epoch)` and `:Chunk(inserted_at_epoch)` in the
  ensure-index step (`src/graph/index.py`), fail-open like the others.

### Search
- **`SearchRequest`** (`src/models/search.py`): `created_after`/`created_before`
  currently exist as `int | None` (RESERVED, never applied) — change them to
  `str | None` ISO `YYYY-MM-DD` (insertion-date range) for a consistent
  date-filter API; they were never functional so this is safe. Add
  `doc_date_after`/`doc_date_before` (`str | None`, ISO, document-date range).
  Validate → 422 on bad format.
- **`search_v2.py`**: drop `created_after`/`created_before` from
  `_RESERVED_FILTER_FIELDS` (now implemented). Convert the four request dates
  → epoch-day bounds; thread through `OrchestratorParams` → `RetrieveParams`
  → `retrieve_subquestion` (add four `int | None` epoch-bound fields to each
  contract). `global` keeps the reserved-warning behaviour for date fields.

  **Retrieval mechanism (refined after reading the code):** `_WALK_CYPHER`
  returns entities/relations, NOT chunks — graph chunk retrieval goes through
  the base PropertyGraphIndex retriever, which has no clean per-query chunk
  `WHERE` hook. So:
  - **Vector (Milvus): push-down.** Build `MetadataFilters` with `GTE`/`LTE`
    on `doc_date_epoch` / `inserted_at_epoch` for whichever bounds are set,
    and build a per-request retriever from the cached vector index
    (`index.as_retriever(similarity_top_k=k, filters=...)`) — the singleton
    `get_retriever()` can't carry per-query filters.
  - **Graph (graph_search + graph_walk): post-filter.** In
    `retrieve_subquestion`, drop any collected `NodeWithScore` whose
    `node.metadata` dates fall outside the set bounds — one shared helper
    applied to the merged sources, covering vector + graph + walk uniformly
    (chunks already carry the epoch dates from the stamping step).
  - **Over-fetch when filtered:** when any date bound is set, raise the
    effective `top_k` (e.g. ×3) before post-filter so dropped out-of-range
    hits don't starve the in-range result count.
- **drift** inherits the local filter (it reuses local retrieval).

## Error handling & compatibility
- Invalid date format → 422 at `/ingest` and `/search`.
- A document without `doc_date` → its chunks have no `doc_date_epoch`; a
  document-date filter **excludes** them (filter matches only in-range
  values). Same for any pre-feature chunks lacking the fields → excluded by
  any date filter. Backfill is Backlog.
- Filters combine with AND; each bound (after/before) is independent and
  optional.

## Testing (repo style — pure, infra-free helpers preferred)
- date(ISO)→epoch-days conversion + validation (round-trip, bad input).
- `SearchRequest` date validation (422 paths).
- `MetadataFilters` assembly from epoch bounds (only set bounds present;
  empty when none set).
- Post-filter helper: keeps in-range `NodeWithScore`, drops out-of-range and
  drops nodes missing the date field when a bound is set; no-op when no bound.
- Over-fetch top_k math (×factor only when a bound is set).
- Filter wiring through `retrieve_subquestion` with fakes (vector retriever
  built with filters; graph results post-filtered).

## Backlog (explicitly out of scope here)
- Approach B (`:Document` node) and C (date-denormalized entities).
- Date filtering for `global` (community-summary) search — needs community
  re-scoping by source-chunk dates.
- `datetime` granularity (currently DATE).
- Backfill of `doc_date_epoch` / `inserted_at_epoch` onto existing Milvus
  chunks and Neo4j `:Chunk` nodes (pre-feature data).
- Auto-extraction of document date from content/metadata/filename.
