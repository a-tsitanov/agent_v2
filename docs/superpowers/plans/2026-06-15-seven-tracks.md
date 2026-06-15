# Plan — Seven Tracks (2026-06-15)

Companion to `specs/2026-06-15-seven-tracks.md`. Each task is TDD where it touches
logic: write the failing test first, then implement. One feature branch per track,
merged to main with `--no-ff` after a quality review.

---

## Track 3 — doc-by-id (branch `fix/doc-by-id-load`)  ← START HERE

1. **Test (red):** `tests/test_ingestion/` — ingest a small doc through the real
   path (or a focused unit), then assert `chunk_repository.aget_chunks_by_doc_id`
   returns chunks and `aread_document_text` returns text for that doc_id. Add a
   `chunk_repository` unit test that `row.path = "s3://bucket/key"` routes to MinIO
   (mock `build_minio_storage`), not `Path.is_file`.
2. **Fix A:** in `index_vector.index_vector` (before `index_nodes`, ~`:167`), set
   `node.metadata["doc_id"] = parsed.ctx.doc_id` for every node. Verify `doc_id`
   survives `_snapshot_for_milvus` (small field, must not be stripped/dropped).
3. **Fallback A:** in `_query_chunks`/`_normalise_chunk_row` (`chunk_repository.py:199`)
   add `doc_id → file_path` fallback so the already-indexed corpus still resolves.
4. **Fix B:** make `aget_document_path`/`aread_document_text` MinIO-aware — mirror
   `documents.py:40-81`: if `row.path` starts with `s3://`, stream/download via
   `build_minio_storage()`; else local file. Reuse `MINIO_DOWNLOAD_DIR` cache.
5. **Green:** run `tests/test_ingestion`, `tests/test_storage`, `tests/test_retrieval`.

## Track 7a — relation weight + tags (branch `feat/relation-weight-tags`)

1. Test: merge of two chunks asserting `weight` reflects `mention_count`,
   `source_chunk_ids` aggregated, `tags` populated.
2. `src/graph/schema.py` — add `tags` + tag vocabulary; `mention_count`,
   `source_chunk_ids` on the relation model.
3. `src/graph/lightrag_parse.py:88,233` — stop hardcoding `weight=1.0`; carry
   confidence; collect `source_chunk_id` into a list.
4. `src/graph/merge.py` — on cross-chunk merge, sum `mention_count`, union
   `source_chunk_ids`, set `weight` from mention_count/confidence, merge `tags`.
5. `build_property_graph` — persist new properties; add index on `tags`.

## Track 4 — Leiden (branch `feat/leiden-weighted-instrumented`)

1. Test: `detect_communities` returns a typed result with projected node/rel
   counts + raw community count; empty-graph vs error distinguished (mock GDS).
2. `src/graph/communities.py:70-106` — log projection node/rel counts + memory
   estimate; capture `communityCount` pre-`min_size`; re-raise genuine GDS errors
   (typed result), fail-soft only on empty projection.
3. Weighted projection: pass edge `weight` as relationship property; weighted
   Leiden config.
4. Knobs: `gamma`/resolution, `concurrency`, wire `AGENT_COMMUNITY_MAX_LEVELS`;
   heartbeat in `detect_communities_activity`.
5. Operator diagnostics snippet committed to `docs/runbook/leiden-diagnostics.md`.

## Track 7b — graph-analysis toolkit (branch `feat/graph-analysis-tools`)

1. Tests per tool (seeded/mocked GDS): pagerank, personalized pagerank, WCC/SCC,
   shortest-path / k-shortest.
2. Implement read-only GDS calls in `src/graph/` + expose via
   `src/retrieval/atomic_tools.py` and admin endpoints.
3. `graph-stats` admin endpoint: counts, degree p50/p99, dup groups, component &
   community size distribution.

## Track 2 — classifier + force (branch `feat/ingest-classifier`)

1. Test: rules skip junk; LLM gate skip; `force=true` bypasses rules; `skipped`
   terminal status + reason persisted; determinism (params snapshot).
2. `IngestParams` — add `force: bool`, classifier prompt-version + model snapshot.
3. New `classify_document` activity (rules layer + `astructured_predict` LLM layer
   over bounded preview); register on `kb-ingest`.
4. `document_ingest.py` — call after `fetch_source`; on skip short-circuit to
   `finalize(status="skipped", reason=...)`; honour `force`.
5. Config `CLASSIFIER_*`; ingest route sets `force` + snapshots.
6. `tests/eval/` — labelled keep/skip set + precision/recall report.

## Track 5 — admission scheduler (branch `feat/ingest-admission-control`)

1. Test (Temporal `WorkflowEnvironment`): with K=1 a 2nd doc waits; with K=2 at
   most 2 run; FIFO ordering; completion releases a slot.
2. `IngestSchedulerWorkflow` — long-lived resource-pool/mutex; signal-driven
   pending queue; admits ≤ K, starts `DocumentIngestWorkflow` child, awaits.
3. Ingest route registers the doc with the scheduler instead of starting the
   workflow directly.
4. Config `MAX_INFLIGHT_DOCS` (default 1).

## Track 6 — answer template (branch `feat/answer-template`)

1. Test: request with inline + named template shapes the prompt; absent → current
   RU behaviour byte-identical; size cap + injection guard.
2. Plumb `answer_template` through the 9 touch points (`models/search.py` →
   `search_v2.py` builders → `contracts.py` Orchestrator/Global/Synthesize params →
   `orchestrator.py`/`global_wf.py` → `synthesize_answer.py`).
3. `prompts/answer_templates/*.md` loader by name; inline override; snapshot at
   submit.
4. `synthesize_answer.py:21-38` — template-driven prompt when present.

## Track 1 — prod compose (branch `feat/prod-compose`, parallelizable)

1. `Dockerfile` (uv-based, one image, role via CMD).
2. `docker-compose.prod.yml` — full stack minus litellm/ollama; worker split by
   `WORKER_GROUPS`; redis; wikibase behind a profile.
3. Prometheus scrape → compose DNS; `temporalio/server` + schema migration job;
   `service_healthy` deps; pinned images; `.env.prod.example`.
4. Smoke: `up` → API health → one ingest + one search against external LLM.
