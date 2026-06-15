# Spec — Seven Tracks (2026-06-15)

Status: decisions locked with user 2026-06-15. Source: planning thread reviewing
backlog + 7 user-requested capabilities. Build order and per-track scope below.

Cross-cutting constraints (from [[feedback_decisions]]):
- Additive / opt-in; never blind replacement.
- Benchmark before adopting where behaviour changes (classifier, weighted Leiden).
- Extend `tests/eval/` for quality-affecting changes.
- Temporal determinism: snapshot any env/config-derived decision into the workflow
  params at submit time (pattern from commit `6721cfc`).

---

## Track 3 — Source-document-by-id is broken (FIRST, quick, independent)

**Problem (verified in code):** two independent bugs.
- **Bug A — Milvus chunks have no `doc_id`.** `index_vector` (`src/workflow/activities/index_vector.py:150-183`) never writes `doc_id` into `node.metadata` before insert; nothing else in ingest writes it (`identifier_transform.py:124-126` only *reads* it). So `chunk_repository._query_chunks` filter `doc_id == "..."` (and JSON fallback `metadata["doc_id"]`) matches nothing → MCP `get_chunks_by_doc_id` returns empty.
- **Bug B — `chunk_repository` is not MinIO-aware (the one the user hits).** `aget_document_path`/`aread_document_text` (`src/storage/chunk_repository.py:137-168`) do `Path(row.path).is_file()`, but `documents.path` is an `s3://…` URI (`ingest.py:105`), so it is always `False` → `read_full_document` always "not found". The HTTP route `GET /documents/{id}` (`documents.py:40-81`) handles `s3://` correctly — chunk_repository must mirror it.

**Scope (in):** fix both. Inject `doc_id` (+ optional doc-level fields) into node metadata in `index_vector`; add a soft `doc_id → file_path` fallback in `_query_chunks`/`_normalise_chunk_row:199` for the already-indexed corpus. Make `chunk_repository` MinIO-aware (download/stream via `build_minio_storage`, mirroring `documents.py`). Add a regression test that ingests a doc and reads it back by id (no such test exists today — why this was missed).

**Acceptance:** new test ingests → `get_chunks_by_doc_id` and `read_full_document` both return content for the new doc_id; existing tests green.

---

## Track 7a — Meaningful relation weight + tags (graph foundation)

**Problem:** relation `weight` is hardcoded `1.0` (`src/graph/lightrag_parse.py:88,233`); this starves weighted Leiden (#4) and ranking. No tag vocabulary on edges; provenance is a single `source_chunk_id`.

**Scope (in):**
- Populate `weight` with a meaningful signal: extraction confidence and/or `mention_count` (co-occurrence frequency across merged chunks).
- Aggregate provenance on merge: `source_chunk_ids: list` + `mention_count: int` on the edge (instead of a single chunk id).
- Add `tags: list[str]` (controlled vocabulary) on relations + an index.

**Backlog (deferred):** `polarity`, `temporal_validity` edge fields (stretch).

**Acceptance:** ingest produces edges with `weight != 1.0` reflecting mention_count/confidence, `mention_count`, aggregated `source_chunk_ids`, and `tags`; merge sums provenance correctly; unit tests cover weight/provenance aggregation.

---

## Track 4 — Leiden at 50k: instrumentation + weighted + knobs

**Problem:** `src/graph/communities.py:70-106` projects `__Entity__` + all rel types undirected, runs `gds.leiden.stream(includeIntermediateCommunities, randomSeed:19)`, and **swallows GDS errors → returns `[]`** ("0 communities"). At 50k the likely cause of "no communities" is a sparse/disconnected entity graph (singletons dropped by `community_min_size=3`), masked by the silent fallback.

**Scope (in):**
- Stop silently swallowing GDS errors: log projection node/rel counts, memory estimate, and `communityCount` **before** the `min_size` filter, so empty-graph vs error vs all-singletons are distinguishable. Re-raise genuine errors (or surface a typed result), keep fail-soft only for truly-empty.
- Feed a **weighted** projection to Leiden using the edge `weight` from 7a.
- Expose knobs: `gamma`/resolution, `concurrency`, `maxLevels` (wire `AGENT_COMMUNITY_MAX_LEVELS`), heartbeat in `detect_communities_activity`.
- Provide an operator diagnostics snippet (rel count between entities, WCC distribution, `gds.leiden.stats`) — for the user to run, not code.

**Acceptance:** rebuild logs now report projected nodes/rels + raw community count; weighted Leiden runs; on a graph with real edges, communities are detected; unit tests cover the instrumentation/typed-result path.

---

## Track 7b — Graph-analysis toolkit (GDS-backed, read-only)

**Scope (in), in order of payoff:** PageRank / personalized (seeded) PageRank; WCC/SCC components; shortest-path / k-shortest between two entities. Surface as atomic tools (`src/retrieval/atomic_tools.py`) + admin endpoints. Add a **graph-stats** admin endpoint (counts, degree p50/p99, dup groups by `toLower(name)`, component & community size distribution) — also the "live diagnostics" from [[graph_scale_250k]].

**Backlog (deferred):** betweenness/closeness, structural embeddings (Node2Vec), node-similarity.

**Acceptance:** each tool returns sane results on the dev graph; read-only; covered by tests with a mocked/seeded GDS.

---

## Track 2 — Input document classifier (skip) with force-override

**Scope (in):** new activity `classify_document` as the first workflow step after `fetch_source`, before `parse_and_chunk`. Two layers:
1. Deterministic rules (extension/MIME, size bounds, empty/corrupt, content-hash dup, filename patterns) — free, fast.
2. LLM classifier (small tier) over a bounded preview (first N KB) → `astructured_predict` → `{decision, reason, doc_type?}`.

On skip → short-circuit to `finalize` with new terminal status `skipped` + persist `reason` (audit). No partial Milvus/Neo4j writes.

**Force-override (user requirement):** a `force: bool` ingest flag that **bypasses the deterministic rules** (and optionally the LLM gate) so an operator can force-ingest a document the rules would skip. Threaded via `IngestParams.force` (snapshot at submit for determinism).

**Quality:** opt-in (`CLASSIFIER_ENABLED`); add a labelled keep/skip eval set in `tests/eval/`; optimise for high recall on "keep" (false-skip is the costly error). Snapshot prompt version + model into `IngestParams`.

**Acceptance:** rule + LLM gate skip junk with `status=skipped` + reason; `force=true` bypasses rules and ingests; eval set reports precision/recall; determinism preserved.

---

## Track 5 — Document-level admission control (variant A)

**Problem:** documents interleave because each stage is a separate FIFO queue with concurrency >1 (`kb-ingest=4`, `kb-ingest-llm=18`, `kb-ingest-merge=14`); a document's tail (merge) queues behind many newer docs' extracts. No priority/ordering.

**Scope (in):** an admission-control gatekeeper that admits at most `MAX_INFLIGHT_DOCS` (K) documents at once; an admitted document runs the full existing multi-queue pipeline to completion before the next is admitted (FIFO). Implement as a long-lived `IngestSchedulerWorkflow` (Temporal resource-pool / mutex pattern): the ingest route registers the doc into a pending set/signal; the scheduler starts `DocumentIngestWorkflow` as a child (or via lease), awaiting completion before admitting the next. Internal per-document parallelism is unchanged.

**Config:** `MAX_INFLIGHT_DOCS` (start K=1 for strict "finish before next"; raise to overlap I/O vs GPU). This also relieves the queue-saturation described in [[worker_hang_congestion_collapse]].

**Acceptance:** with K=1 a second upload does not start until the first completes; with K>1 at most K run; ordering FIFO; no regression to per-document throughput internals.

---

## Track 6 — Templated answers (variant a: text template)

**Scope (in):** thread `answer_template: str | None` (named or inline) request → `OrchestratorParams`/`GlobalSearchParams`/`SynthesizeParams` → `synthesize_answer`, following the `top_k` plumbing pattern (9 touch points enumerated in plan). In `synthesize_answer.py:21-38`, when a template is present, build the prompt from it instead of the hardcoded RU preamble. Named templates stored server-side (`prompts/answer_templates/*.md`), referenced by name; inline override allowed. Snapshot template content at submit; cap size + basic injection guard. Applies to all modes (shared synthesis).

**Backlog (deferred):** variant (b) structured/JSON output via `astructured_predict` + `SynthesizeResult.parsed_answer`.

**Acceptance:** request with `answer_template` shapes the answer to the template; absent → current RU behaviour unchanged; templates loadable by name; size/inject guards tested.

---

## Track 1 — Production docker-compose, everything in compose (LAST / parallelizable)

**Scope (in):** a single `docker-compose.prod.yml` bringing up the WHOLE app, **excluding litellm + ollama** (external via `LITELLM_BASE_URL`).
- **Dockerfile** for the app (one image, role via CMD): `api` (uvicorn :8000), `worker` (split into `worker-ingest` = `WORKER_GROUPS=main,llm,merge`, `worker-search` = `search,large`, `worker-graph` = `graph_build`, opt. `worker-wiki`), `mcp-search` (:9001), `mcp-tools` (:9002).
- Backends: postgres, temporal(+ui), etcd, minio, milvus, neo4j(apoc+gds), prometheus, grafana, redis (LLM cache :6380), wikibase stack behind a compose profile (opt-in).
- Prometheus scrape → compose DNS names of worker services (not `host.docker.internal`).
- Hardening: `temporalio/server` + one-time schema migration (drop auto-setup), separate Temporal Postgres, `depends_on: service_healthy`, `restart: unless-stopped`, pinned images, rotated default creds / secrets.

**Acceptance:** `docker compose -f docker-compose.prod.yml up` brings up the full stack (sans litellm/ollama), API healthy, a real ingest + search works end-to-end against external LLM.

---

## Build order

1. **Track 3** — bugfix (quick, independent).
2. **Track 7a** → **Track 4** → **Track 7b** — graph foundation (weight feeds Leiden + tools).
3. **Track 2** → **Track 5** — ingest control (classifier first, then scheduler; both touch the ingest workflow).
4. **Track 6** — answer templates.
5. **Track 1** — containerise the finished app (independent; can run in parallel anytime).
