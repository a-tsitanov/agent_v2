# CONCEPTS — every technique used in kb-llamaindex, from scratch

This is the educational reference for the service: **what** each technique is
(assuming no prior knowledge), **how** the algorithm works, **why** we chose it,
and **where** it lives in our code. It is the companion to the decision log in
[`adr/`](adr/README.md) (the *why* in record form) and the operational docs
([`ARCHITECTURE.md`](ARCHITECTURE.md), [`INGEST.md`](INGEST.md),
[`SEARCH.md`](SEARCH.md), [`QUEUES.md`](QUEUES.md), [`MODELS.md`](MODELS.md)).

> If you only read one doc to understand the moving parts of this service —
> from "what is the Leiden algorithm" to "why entity resolution uses an LLM
> judge" — read this one.

> Verification note: there is no live Neo4j/GDS in CI, so GDS/Leiden Cypher is
> documented from source + tests, not from execution (flagged inline where it
> matters).

---

## Table of contents

**Part 1 — Foundations & Ingestion**

- [1. Durable execution & Temporal](#1-durable-execution--temporal)
- [2. The claim-check pattern](#2-the-claim-check-pattern)
- [3. Task-queue isolation & head-of-line blocking](#3-task-queue-isolation--head-of-line-blocking)
- [4. LLM concurrency pooling (LLMPool)](#4-llm-concurrency-pooling-llmpool)
- [5. Document parsing & chunking](#5-document-parsing--chunking)
- [6. Deterministic identifier canonicalization (pre-LLM)](#6-deterministic-identifier-canonicalization-pre-llm)
- [7. Knowledge-graph extraction (LightRAG-style)](#7-knowledge-graph-extraction-lightrag-style)
- [8. Text embeddings](#8-text-embeddings)

**Part 2 — Vectors, Graph & Entity Resolution**

- [Vector search & approximate nearest neighbour (ANN)](#vector-search--approximate-nearest-neighbour-ann)
- [FLAT vs HNSW (Milvus)](#flat-vs-hnsw-milvus)
- [Neo4j as a property graph + its indexes](#neo4j-as-a-property-graph--its-indexes)
- [Entity Resolution (ER)](#entity-resolution-er)
- [Community detection & the Leiden algorithm](#community-detection--the-leiden-algorithm)
- [Community reports (map-reduce summarization)](#community-reports-map-reduce-summarization)

**Part 3 — Search & Retrieval**

- [RAG (Retrieval-Augmented Generation) fundamentals](#rag-retrieval-augmented-generation-fundamentals)
- [Local search — vector retrieval + graph expansion](#local-search--vector-retrieval--graph-expansion)
- [Global search — community map-reduce](#global-search--community-map-reduce)
- [DRIFT search — combine local + global](#drift-search--combine-local--global)
- [Auto mode & query routing](#auto-mode--query-routing)
- [Plan-execute decomposition & the orchestrator](#plan-execute-decomposition--the-orchestrator)
- [Coverage check — bounded refinement loop](#coverage-check--bounded-refinement-loop)
- [Conversation-history contextualization](#conversation-history-contextualization)
- [Reranking](#reranking)

**Part 4 — Knowledge Anchor, Outputs, Models & Ops**

- [The Wikibase knowledge anchor](#the-wikibase-knowledge-anchor)
- [The continuous wiki editor](#the-continuous-wiki-editor)
- [SPARQL & WDQS (briefly)](#sparql--wdqs-briefly)
- [Multi-model / role-based model selection](#multi-model--role-based-model-selection)
- [LiteLLM gateway](#litellm-gateway)
- [Observability](#observability)
- [MCP (Model Context Protocol) surface](#mcp-model-context-protocol-surface)

---

# Part 1 — Foundations & Ingestion

A from-scratch reference for the techniques that turn a raw uploaded document into searchable vectors and a knowledge graph, and for the durable-execution machinery that runs that pipeline reliably. Every claim below is grounded in the actual code paths cited under **In our code**.

---

## 1. Durable execution & Temporal

**What it is.** A normal program lives only in process memory: if the machine crashes mid-way through a long job (download → parse → embed → call an LLM → write a graph), everything done so far is lost and you start over. *Durable execution* is a different model. You write your business logic as an ordinary function, but a server (here, **Temporal**) records every step it takes to a database. If the worker process dies, Temporal re-runs the function on another worker and *replays* the recorded history, so the function resumes exactly where it left off — already-completed steps are not re-executed, their recorded results are handed back instantly. The job survives crashes, deploys, and network blips without you writing any checkpointing code.

**How it works.** Temporal splits your code into two kinds of building blocks:

- A **Workflow** is the orchestration function. It decides *what* happens and *in what order*. It must be **deterministic**: given the same recorded history, re-running it must make the same decisions in the same order. That is the price of replay — Temporal re-executes the workflow body to rebuild in-memory state, and if the code branched on something that changed between runs (the wall clock, a random number, an env var that was edited), the replay would diverge from history and Temporal aborts with a non-determinism error.
- An **Activity** is a single unit of *real* side-effecting work (download a file, call the LLM, write to Neo4j). Activities are allowed to be non-deterministic and to fail; Temporal records only their *inputs and final result*, not their internals. The workflow calls an activity, Temporal schedules it, runs it on a worker, and persists the result to history.

Workers poll a named **task queue** for work. Each activity call carries timeouts and a **retry policy**. Two timeouts matter most:
- `start_to_close_timeout` — the wall-clock budget for *one attempt*.
- `schedule_to_close_timeout` — the *total* budget across all attempts plus the waits between them. This is the hard stop.

A tiny example, straight from our ingest workflow: `fetch_source` is called with `start_to_close_timeout=5min`, `schedule_to_close_timeout=1h`, and the `_FAST_FOREVER` retry policy (`initial_interval=1s`, `backoff_coefficient=2.0`, `maximum_interval=60s`, `maximum_attempts=0`). `maximum_attempts=0` means *retry forever* — 1s, 2s, 4s, … capped at 60s between tries — until the 1h `schedule_to_close` ceiling fires and Temporal finally fails the activity. For LLM-heavy work we use `_HEAVY_FOREVER` (start at 2min, cap at 30min). Permanent input problems (corrupt file, schema violation) are meant to be raised as `ApplicationError(non_retryable=True)` from inside the activity so they bypass the retry loop instead of burning the whole budget.

**Determinism / replay-safety in practice.** The cardinal rule is: *never read mutable settings or the environment inside a workflow body.* Instead, resolve them once when the workflow is *submitted* and pass them in as input. Our ingest route does exactly this: at submit time it snapshots the current model config — `model = cfg.effective_base`, `extraction_model = cfg.model_for("extraction")`, etc. — and packs them into `IngestParams`. The workflow then carries those frozen strings end-to-end (into `FinalizeIn`, into the analytics rows) rather than re-reading `settings` mid-run. If it re-read env on replay after an operator changed a model name, history and replay would disagree. The same pattern appears on the search side: `OrchestratorParams` resolves `coverage_check_enabled`, `max_coverage_rounds`, and `contextualize_enabled` from `AgentSettings` at submit time precisely so "the workflow never reads env at runtime (replay-safe)".

Note one subtlety: the workflow *may* read `settings` for values that are stable for the worker's lifetime — our `DocumentIngestWorkflow` references `settings.temporal.llm_task_queue` and `settings.wiki.enabled` to pick a queue / gate a branch. These are constants baked into the deployed image and the same on every replay, so they don't break determinism. The dangerous case is anything that can *change between the original run and a later replay*.

**Child workflows.** A workflow can start another workflow as a **child** and await it. We split the heavy graph half of ingest into a child, `GraphBuildWorkflow`, started by `DocumentIngestWorkflow` via `execute_child_workflow(GraphBuildWorkflow.run, kg, id=f"graph-{doc_id}", task_queue=merge_task_queue, parent_close_policy=REQUEST_CANCEL)`. The wins (documented in the child's module docstring): independent retry/`schedule_to_close` ceilings (a stuck merge can be cancelled without restarting the whole document), independent visibility in the Temporal UI (the parent `ingest-{doc_id}` finishes in seconds for the vector half while the child `graph-{doc_id}` does the slow LLM work), and per-stage metrics in the child's own history. The parent awaits the child; a failure inside the child surfaces as `ChildWorkflowError`, which the parent catches alongside `ActivityError` to downgrade `graph_status` to `"vector_only"` instead of failing the whole document.

**Why we use it / alternatives.** The alternative is a hand-rolled job system: a task table in Postgres, a cron/worker loop, manual status columns, and bespoke retry/backoff logic for every step — all of which you must keep crash-consistent yourself. Temporal gives crash-consistency, retries, timeouts, backoff, and a visibility UI for free, at the cost of the determinism discipline above and a Temporal server to operate. For a pipeline with an expensive, multi-minute LLM stage that we must never silently re-run or lose, that trade is strongly worth it.

**In our code.**
- `src/workflow/document_ingest.py` — the parent workflow, retry profiles (`_FAST_FOREVER`, `_HEAVY_FOREVER`), per-activity timeouts, and the inner/outer `try/except` that maps failures to `vector_only` / `mark_failed`.
- `src/workflow/graph_build.py` — the `GraphBuildWorkflow` child (merge + property-graph build).
- `src/workflow/worker.py` — the worker process hosting all the Worker pools.
- `src/workflow/client.py` — the process-wide Temporal client singleton (note: it mandates the `pydantic_data_converter`, since our payloads are Pydantic v2 models).
- `src/api/routes/ingest.py` (≈lines 127–159) — resolve-at-submit-time of model snapshots into `IngestParams`.

---

## 2. The claim-check pattern

**What it is.** Temporal records every workflow input and activity result into its event-history database, and each such payload is capped at **2 MB**. But our pipeline passes around big objects — a list of parsed LlamaIndex `BaseNode` chunks, KG entity/relation lists, merged entity sets — that blow past 2 MB. The *claim-check pattern* (a classic messaging pattern) solves this: instead of putting the heavy payload on the wire, you write it to a side store, get back a small *ticket* (a URI), and pass only the ticket. The next stage redeems the ticket to fetch the real data. The "check" is like a coat-check tag: small, but it points at the heavy thing.

**How it works.** Our side store is **MinIO** (S3-compatible object storage). The `StagingStore` wrapper does the two operations:
- `write_pickle(run_id, stage, obj)` — `pickle.dumps` the object and `put_object` it to MinIO under the key `{workflow_run_id}/{stage}.pkl`, returning the URI `s3://{bucket}/{run_id}/{stage}.pkl`.
- `read_pickle(uri)` — the reverse: parse the URI, `get_object`, `pickle.loads`.

So a stage activity reads its input blob, does work, writes its output blob, and returns a *contract* carrying only the new URI plus small counters. Concretely: `parse_and_chunk` pickles the node list to `{run_id}/parsed.pkl` and returns `Parsed(nodes_uri=..., chunk_count=...)`; `extract_kg` reads that blob, runs extraction, writes `{run_id}/kg.pkl`, and returns `KGExtracted(nodes_with_kg_uri=...)`. The `*_uri` fields on the contracts in `contracts.py` (`Parsed.nodes_uri`, `KGExtracted.nodes_with_kg_uri`, `Merged.merged_entities_uri`) are exactly these claim-check tickets — the file header states it outright: "Heavy state (list[BaseNode], EntityNode lists) NEVER travels in payloads — it is pickled to MinIO and referenced by URI."

Cleanup is keyed on the run prefix: at end of workflow, `finalize` (and the failure path's `mark_failed`) call `delete_prefix(run_id)` to wipe the whole `{run_id}/` folder. A janitor (`cleanup_orphans`) sweeps prefixes whose newest blob is older than a threshold — those belong to workflows that died before `finalize`/`mark_failed` could run (worker OOM, cancelled run).

Pickle is acceptable here for the three reasons the module spells out: producer and consumer share the same Python image, blobs live only for the duration of one workflow run, and the on-disk format is never read by anything outside this package.

Note the contrast on the search side: those payloads (`SerializedNode`, the messages) are deliberately tiny projections that *do* fit in the 2 MB limit, so search passes them inline rather than claim-checking them. Claim-check is used only where the data is genuinely large.

**Why we use it / alternatives.** Alternatives: (a) shrink everything to fit 2 MB — impossible for full node lists; (b) raise Temporal's payload limit — fights the platform and bloats the history DB; (c) re-derive state from scratch in each activity — wasteful (re-parse, re-extract). Claim-check keeps Temporal history small and fast while still letting stages hand off arbitrarily large intermediate state, at the cost of a MinIO dependency and a cleanup story (which we have).

**In our code.**
- `src/workflow/staging.py` — `StagingStore.write_pickle` / `read_pickle` / `delete_prefix` / `cleanup_orphans`, and `build_staging_store()` (bucket = `settings.temporal.staging_bucket`, default `kb-staging`).
- `src/workflow/contracts.py` — the `*_uri` fields: `Parsed.nodes_uri`, `KGExtracted.nodes_with_kg_uri`, `Merged.merged_entities_uri`.

---

## 3. Task-queue isolation & head-of-line blocking

**What it is.** A task queue is FIFO: workers pull tasks in roughly the order they were enqueued. **Head-of-line blocking** is the failure mode where a slow or bulky item at the front of one queue stalls everything behind it — even unrelated, fast work — because they're all waiting in the same line. *Queue isolation* is the fix: give different workloads *separate* queues (and separate worker pools with their own concurrency caps), so a burst in one lane can't monopolize the capacity another lane needs.

**How it works.** A single worker process (`python -m src.workflow.worker`) hosts several independent `Worker` pools against the same Temporal client, each polling its own task queue with its own `max_concurrent_activities` cap. For ingest, the three relevant lanes are:

- `kb-ingest` (`task_queue`, cap `TEMPORAL_ACTIVITY_CONCURRENCY` = 4) — the `DocumentIngestWorkflow` plus all the IO / embedding activities (`fetch_source`, `parse_and_chunk`, `index_vector`, `inject_canonical`, `finalize`, `mark_failed`, and `build_property_graph` for single-pool deployments).
- `kb-ingest-llm` (`llm_task_queue`, cap `TEMPORAL_LLM_ACTIVITY_CONCURRENCY` = 18) — hosts **only** `extract_kg`. The workflow pins this activity to the queue via `task_queue=settings.temporal.llm_task_queue` on its `execute_activity` call.
- `kb-ingest-merge` (`merge_task_queue`, cap `TEMPORAL_MERGE_ACTIVITY_CONCURRENCY` = 14) — hosts `GraphBuildWorkflow` plus `merge_and_resolve` + `build_property_graph`. The parent starts the child on this queue, and the child's activities carry *no* `task_queue` override, so they inherit the child's queue and ride the merge lane automatically.

The concrete bug this prevents (from `docs/QUEUES.md`): `extract_kg` and the merge stage used to share one `kb-ingest-llm` queue at concurrency 1. When many documents ingest at once, a burst of `extract_kg` tasks fills that FIFO, and a given document's merge — enqueued *behind* all the pending extracts — starves. The vector half completes fast but the graph half waits out the whole extract backlog. Splitting merge onto its own queue + pool lets extract and merge poll independent queues and interleave instead of serialising through one line.

(The same process also hosts isolated search lanes — `kb-search-small`, `kb-search-large`, `kb-graph-build`, `kb-wiki` — for the same reason: keep concurrent search sessions and offline graph builds from fighting ingest for capacity. Those are covered in `docs/QUEUES.md`.)

**Why we use it / alternatives.** Alternative: one big queue with a high concurrency cap. That maximizes raw throughput but offers zero isolation — any flood drowns the rest, and you can't size GPU vs IO pressure independently. Per-lane queues cost a few extra Worker pools and config knobs but buy predictable isolation: each workload's burst is bounded by its own cap and can't head-of-line-block a sibling.

**In our code.**
- `src/workflow/worker.py` — the `main_worker` / `llm_worker` / `merge_worker` pools and their `max_concurrent_activities` settings.
- `docs/QUEUES.md` — the authoritative queue table and the merge-queue rationale (this section aligns with it).
- Knobs: `TEMPORAL_TASK_QUEUE`/`TEMPORAL_ACTIVITY_CONCURRENCY`, `TEMPORAL_LLM_TASK_QUEUE`/`TEMPORAL_LLM_ACTIVITY_CONCURRENCY`, `TEMPORAL_MERGE_TASK_QUEUE`/`TEMPORAL_MERGE_ACTIVITY_CONCURRENCY` (config defaults in `src/config.py` `TemporalSettings`).

---

## 4. LLM concurrency pooling (LLMPool)

**What it is.** Temporal queue caps limit how many *activities of a given queue* run at once, per worker. But several different activities all hammer the *same* scarce backend — the local GPU (small-tier model) or a paid API (large-tier model) — and they live on different queues, so no single Temporal cap can bound the *total* load on that backend. The `LLMPool` is a second, finer limiter that sits *on top of* the Temporal caps: a per-process gate that bounds total concurrent LLM calls by *tier* and by *role*, regardless of which queue they came from.

**How it works.** It is a **two-level gate**, acquired in a fixed order to avoid deadlock:

1. **Per-role lane.** Each logical role — `extraction`, `judge`, `search`, `route`, `plan`, `retrieve`, `synthesis` — gets a `Lane` (a named `asyncio.Semaphore` with capacity = that role's ceiling). Default ceilings (`LLM_POOL_LANE_CAPS`): extraction 18, judge 14, search 14, plan 4, route 2, retrieve 4, synthesis 8.
2. **Per-tier global ceiling.** Every role maps to a *tier* — `small` (the local/GPU model) or `large` (the API model). The small tier has one global `Lane` of capacity `LLM_POOL_TIER_SMALL_TOTAL` (default 25); the large tier `LLM_POOL_TIER_LARGE_TOTAL` (default 8).

When a call wants an LLM for a role, `LLMPool.get(role)` returns a `BoundedLLM` wrapped with **two gates in order `[lane, tier]`** — it acquires the *lane permit first, then the tier-global*. The lane-first order keeps the scarce global occupied only around the actual call, and the consistent ordering across all roles guarantees no deadlock. So an `extraction` call must hold both an extraction-lane permit (≤18) *and* a small-tier permit (≤25) simultaneously.

The clever bit is **deliberate over-subscription**: the small-tier lane ceilings sum to far more than 25 (18+14+14+4+2+4 = 56). That's intentional — "so one workload can fill the GPU while no role can monopolize it beyond its ceiling." If only extraction is busy, it can take up to 18 small-tier slots; if judge work shows up, the tier-global (25) is the real arbiter of who gets the GPU next. The `judge_floor` (default 7) plus the sizing rule `extraction_ceiling ≤ tier_small_total − judge_floor` (18 ≤ 25 − 7) guarantees that even under a full extraction flood, ≥7 small-tier slots remain for merge/judge so it never starves.

A tiny example: 30 `extract_kg` activities become runnable at once. Temporal's `kb-ingest-llm` cap (18) lets 18 start. Each calls `pool.get("extraction")`; the extraction lane (18) admits all 18, but the small tier global (25) is also shared with any in-flight merge/judge calls — so if 8 judge calls are already running, only 17 extractions can hold a small-tier permit and the 18th waits on the tier semaphore. The pool, not Temporal, decides the true GPU occupancy.

**Why this sits on top of Temporal caps.** The docs are explicit: the Temporal per-queue caps must be **≥** the matching pool lane ceiling so the *pool binds first*. That is exactly why `kb-ingest-llm` was raised to 18 and `kb-ingest-merge` to 14 — matching the extraction/judge lane ceilings. If Temporal's cap were lower, Temporal would throttle before the pool ever got a chance to arbitrate, and the dynamic tier-sharing would be defeated. Temporal caps = coarse per-lane *isolation*; LLMPool = the real GPU/API *concurrency arbiter* shared across ingest and search in one process.

**Why we use it / alternatives.** Alternative: rely on Temporal caps alone. That can't express "total small-tier load ≤ 25 across all queues" — caps are per-queue, and you'd have to under-cap every lane and lose the ability to let one workload fill an idle GPU. Another alternative: a single global semaphore with no roles — but then a flood of one role starves the others (no `judge_floor` guarantee). The two-level lane+tier design gives both a hard total ceiling and per-role fairness. The caveat the module states: this is *per-process*, not distributed — the true cross-process GPU ceiling belongs at the LiteLLM proxy, which is out of scope for the pool.

**In our code.**
- `src/retrieval/llm_pool.py` — `Lane`, `LLMPool.get` (lane-first-then-tier ordering), the process-singleton `get_llm_pool()`. Call-sites: `parse_and_chunk` and `extract_kg` both do `get_llm_pool().get("extraction")`.
- `src/config.py` `LLMPoolSettings` — `LLM_POOL_TIER_SMALL_TOTAL` (25), `LLM_POOL_TIER_LARGE_TOTAL` (8), `LLM_POOL_JUDGE_FLOOR` (7), `LLM_POOL_LANE_CAPS` (the role→ceiling map).
- `src/config.py` `LiteLLMSettings` — `LITELLM_ROLE_TIERS` (role→tier map, defaults in `_DEFAULT_ROLE_TIERS`; only `synthesis` is `large`), `tier_for(role)` used by the pool to pick a tier.

---

## 5. Document parsing & chunking

**What it is.** An LLM and a vector index can't consume a whole 50-page PDF at once — context windows are finite and retrieval works best on focused passages. *Parsing* turns a raw file (PDF, DOCX, PPTX, TXT, MD, EML) into plain text; *chunking* (a.k.a. splitting) cuts that text into bite-sized overlapping pieces called **nodes** (or chunks). Each chunk is the unit that later gets embedded, indexed, and fed to the KG extractor.

**How it works.** Our `parse_and_chunk` activity runs a LlamaIndex `IngestionPipeline`:
1. **Read** — `SimpleDirectoryReader` loads the file into `Document` objects (default supported types: PDF, DOCX, PPTX, TXT, MD, EML).
2. **Split** — by default a `SentenceSplitter` with `chunk_size=512` and `chunk_overlap=50` (the `INGESTION_CHUNK_SIZE` / `INGESTION_CHUNK_OVERLAP` knobs). `chunk_size` is the target chunk length (in the splitter's tokens); `chunk_overlap` repeats the last ~50 units of one chunk at the start of the next. **Why overlap?** A fact that straddles a chunk boundary ("…the contract was signed by | Acme LLC on 5 March 2024…") would otherwise be cut in half and lost to both chunks; the overlap window keeps boundary-spanning context intact in at least one chunk. The splitter tries to break on sentence boundaries so chunks stay coherent. There's an opt-in `SemanticSplitterNodeParser` (`INGESTION_SEMANTIC_CHUNKING`) that places breakpoints where the embedding similarity between adjacent sentences drops below `breakpoint_percentile` (95th) — semantically-aware splitting at the cost of running the embedder during chunking.
3. **Canonical identifiers** — the `IdentifierCanonicalizationTransform` runs on each chunk (see §6).
4. **Translation** (optional, default on, `INGESTION_TRANSLATE_TO_RUSSIAN`) — fills a Russian rendering used downstream by the KG extractor without mutating the stored original-language chunk.

The output node list is pickled to staging (`{run_id}/parsed.pkl`, the claim-check of §2) and the activity returns `Parsed(nodes_uri=..., chunk_count=...)`.

A tiny example: a 512-token-target splitter on a 1,300-token document yields roughly three chunks; with 50-token overlap, chunk 2 begins by repeating chunk 1's final 50 tokens, and chunk 3 repeats chunk 2's — so ~3×512 − 2×50 ≈ 1,436 token-units of (overlapping) coverage over the 1,300.

Note: embedding generation is deliberately *not* part of this pipeline — it happens later at the vector-index insertion step (§8), because the same embedding model is also needed at retrieval time and tests mock it independently.

**Why we use it / alternatives.** Fixed-size sentence splitting is cheap, deterministic, and predictable — the default. Alternatives: no chunking (won't fit context / dilutes retrieval); pure fixed-character splitting (cuts mid-sentence); semantic splitting (better boundaries but spends embedder calls during ingest). We default to `SentenceSplitter` and keep semantic splitting opt-in.

**In our code.**
- `src/workflow/activities/parse_and_chunk.py` — the activity: read → pipeline → scrub translation scaffolding → stage.
- `src/ingestion/pipeline.py` — `build_ingestion_pipeline` (splitter choice, transform order) and `read_documents` (`SimpleDirectoryReader`).
- `src/config.py` `IngestionSettings` — `INGESTION_CHUNK_SIZE` (512), `INGESTION_CHUNK_OVERLAP` (50), `INGESTION_SEMANTIC_CHUNKING`, `INGESTION_BREAKPOINT_PERCENTILE` (95), `INGESTION_TRANSLATE_TO_RUSSIAN`.

---

## 6. Deterministic identifier canonicalization (pre-LLM)

**What it is.** Documents write the same real-world identifier in many surface forms: a phone as `+7 (495) 123-45-67` or `8 495 1234567`; a date as `05.03.2024` or `5 марта 2024`. If we let the LLM extract each verbatim, the knowledge graph ends up with multiple nodes for one entity and dedup breaks. *Canonicalization* is the act of mapping every surface form of an identifier to one **canonical** string, computed by deterministic (non-LLM) code *before* the LLM ever sees the chunk, so all variants collapse onto a single node.

**How it works.** `extract_identifiers(text)` runs a battery of detectors over the raw chunk text (24+ types across three groups: business/financial, digital identity, device/hardware). Each match yields a `NormalizedIdentifier(entity_type, canonical, original, span)`. The interesting mechanics:

- **E.164 phone normalization.** E.164 is the international standard form `+<country><national number>` with no spaces/punctuation. We use Google's `libphonenumber` port (`phonenumbers.PhoneNumberMatcher(text, "RU")`, RU as default region) and format each match as `PhoneNumberFormat.E164`. So both `+7 (495) 123-45-67` and `8 495 1234567` canonicalize to the *same* `+74951234567`.
- **Checksum-validated business IDs.** Russian **INN** (taxpayer ID, 10 or 12 digits) and **OGRN** (registration number, 13 or 15 digits) carry check digits. We don't just regex the shape — we *validate the checksum* (`_check_inn_10`/`_check_inn_12`: weighted-sum mod 11 mod 10; `_check_ogrn_13`/`_check_ogrn_15`: the 13-digit body mod 11 mod 10 must equal the last digit). A random 10-digit run that fails the checksum is rejected, which keeps order numbers and IDs out of the graph. Other types validate similarly: SNILS (mod-101), IMEI/CreditCard (Luhn), IBAN (mod-97), VIN (mod-11), bank account (RU control key against a BIC in the same text).
- **libpostal address parsing.** Postal addresses have no checksum, so we anchor on a 6-digit postcode, window ~200 chars forward to capture the street/city tokens, and normalize via **libpostal** (`parse_address` → structured `postcode/city/road/house_number/unit` → joined canonical). libpostal is heavy and optional: when the C library isn't installed, we fall back to a rule-based abbreviation-expansion normalizer (`ул.`→`ул `, drop `г.`, etc.).

When two detectors match overlapping spans (e.g. `URL` and `VKProfile` both on `https://vk.com/user`), `_resolve_overlaps` keeps the higher-priority specialised type and drops the generic one.

**Why it runs deterministically BEFORE the LLM, and how the dedup actually happens.** Two coordinated steps, both *upstream* of extraction:
1. The `IdentifierCanonicalizationTransform` (inserted into the pipeline *before* the KG extractor) appends a literal block to each chunk's text: `"Канонические идентификаторы (используй ИМЕННО ТАКУЮ форму в entity_name): - PhoneNumber: +74951234567 (в тексте: «8 495 1234567»)"`. This *instructs the LLM in-band* to use the canonical form when it names entities — so verbatim mentions in the LLM's output already match the canonical string.
2. The `inject_canonical` activity calls `inject_canonical_entities`, which upserts one `EntityNode(name=<canonical>, label=<type>)` per `(type, canonical)` pair into Neo4j *before* `extract_kg` runs. `upsert_nodes` merges by name, so the canonical node is guaranteed to exist. When the LLM later emits an entity named with that same canonical string (because the augment block told it to), it dedups onto the pre-injected node instead of creating a duplicate.

So the determinism is load-bearing: the canonical string is computed by code (reproducible, checksum-gated), seeded into the graph, *and* fed to the LLM as the required spelling — three forces all pushing every variant onto one node. Doing this with the LLM alone would be non-deterministic and would re-split entities across documents.

**Why we use it / alternatives.** Alternative: let the LLM extract and normalize identifiers itself. It's non-deterministic (same input, different spelling on different runs), can't validate checksums, and produces graph fragmentation. Alternative: post-hoc dedup after extraction — possible but lossy and expensive, and it can't influence the LLM's own naming. Pre-LLM deterministic canonicalization is cheaper, auditable, and prevents the duplicates from ever being created. Cost: a regex/parser battery to maintain per identifier type.

**In our code.**
- `src/ingestion/identifiers.py` — every detector, the E.164 phone path (`_extract_phones`), INN/OGRN checksums (`_check_inn_10/12`, `_check_ogrn_13/15`), libpostal address path (`_normalize_address` with rule-based fallback), `_resolve_overlaps`, and the `build_augment_block` that produces the in-band canonical block.
- `src/ingestion/identifier_transform.py` — `IdentifierCanonicalizationTransform` (appends the augment block to chunk text, stores `metadata["canonical_identifiers"]`) and `inject_canonical_entities` (upserts canonical `EntityNode`s into the graph).
- `src/workflow/activities/inject_canonical.py` — the `inject_canonical` activity that runs the injection before `extract_kg`.

---

## 7. Knowledge-graph extraction (LightRAG-style)

**What it is.** A vector index finds passages by similarity; a **knowledge graph** instead stores explicit facts as **triples** — `(source entity) —[relation]→ (target entity)`, e.g. `(Acme LLC) —[signed]→ (Contract №42)`. Building that graph means reading text and pulling out the entities and the relations between them. *KG extraction* is the stage that does this, here with one LLM call per chunk.

**How it works.** The `extract_kg` activity reads the parsed nodes from staging, gets the small-tier extraction LLM from the pool (`get_llm_pool().get("extraction")`), and builds a **LightRAG**-style extractor via `build_kg_extractor(llm, mode="lightrag")` → `LightRAGExtractor`. Per the factory docstring, the `lightrag` mode makes "one LLM call per chunk [that] produces entities (name + type + description) + relations (src + tgt + keywords + description) in a single structured response" — the algorithm is ported from HKUDS/LightRAG (prompts in `src/graph/lightrag_prompts.py`). Crucially, descriptions are populated *inline* in that one call — there's no separate description-enrichment pass.

The extractor's output is attached to each node as `KG_NODES_KEY` / `KG_RELATIONS_KEY` metadata. `extract_kg` then summarises what was produced (entity/relation totals, top-10 labels, 20 sample entities/relations) for the Temporal UI, pickles the enriched nodes to `{run_id}/kg.pkl` (note: a *separate* blob from `parsed.pkl`, so a retry of the downstream merge can re-read it without re-running the expensive extractor), and returns `KGExtracted`. Cross-chunk consolidation — collapsing the same entity mentioned in many chunks into one node, the dedup that §6 set up — happens later in the merge stage (`src/graph/merge.py:merge_kg_extraction`, run by `GraphBuildWorkflow`).

A tiny example: a chunk "Acme LLC signed Contract №42 with Beta Inc on 5 March 2024" yields entities like `Acme LLC` (Organization), `Beta Inc` (Organization), `CONTRACT-№42` (ContractNumber, already canonical from §6), `2024-03-05` (DocumentDate) and relations such as `(Acme LLC) —signed→ (CONTRACT-№42)` and `(Acme LLC) —counterparty→ (Beta Inc)` — all in one structured LLM response.

Other modes exist for experimentation: `simple` (plain prompt + regex parsing, kept as the regression baseline, entity types collapse to `entity`), `schema` (typed `SchemaLLMPathExtractor`, needs a function-calling model, flaky on local models), and `gliner` variants. We default to `lightrag`.

**Why we use it / alternatives.** Alternatives: `SchemaLLMPathExtractor` (rigid typed schema, but unreliable on our local small-tier model and requires function-calling); a `simple` triplet extractor (no descriptions, untyped entities); or running a separate enrichment LLM pass to add descriptions (doubles LLM cost). LightRAG's single structured call gives typed entities *with* descriptions and relations *with* keywords in one shot — fewer LLM calls, richer nodes, and it runs acceptably on the local model. Cost: it's the heaviest ingest stage (hence its own GPU-serialised lane in §3 and the `_HEAVY_FOREVER` retry profile).

**In our code.**
- `src/workflow/activities/extract_kg.py` — the activity, the `_summarise_kg` UI surfacing, and the separate `kg.pkl` staging write.
- `src/graph/index.py` `build_kg_extractor` — the mode dispatch; `mode="lightrag"` → `LightRAGExtractor` (in `src/graph/lightrag_extract.py`, prompts in `src/graph/lightrag_prompts.py`).
- Cross-chunk consolidation: `src/graph/merge.py:merge_kg_extraction` (run by `GraphBuildWorkflow`).

---

## 8. Text embeddings

**What it is.** An **embedding** is a fixed-length vector of floats (here 768 dimensions) that a model produces from a piece of text, positioning it in a high-dimensional space so that *texts with similar meaning land near each other*. "Cancel my subscription" and "how do I end my plan" end up as nearby vectors even though they share almost no words. Embeddings are what let retrieval find passages by *meaning* rather than by exact keyword match.

**How it works.** Two vectors' similarity is measured by **cosine similarity** — the cosine of the angle between them, ranging from −1 (opposite) through 0 (unrelated/orthogonal) to 1 (same direction/meaning). Intuition: ignore how *long* the vectors are, look only at which *direction* they point; texts pointing the same way are semantically close. The mechanics in our pipeline:

1. **Build the model.** `build_embedding_model()` returns a LlamaIndex `OpenAILikeEmbedding` pointed at our LiteLLM proxy: `model_name=settings.litellm.embedding_model`, `api_base=settings.litellm.base_url`, with `embed_batch_size=10`. Same OpenAI-compatible wire protocol as the chat models, just for the embeddings endpoint.
2. **Index at insertion time.** The `index_vector` activity loads the parsed nodes from staging, builds the embedding model + a Milvus vector store + index, and calls `index_nodes(index, nodes)`. *This* is where embeddings are computed — each chunk's text is sent to the embedding model and the resulting vector is stored in **Milvus** (the vector database) alongside the chunk id. (It snapshot-strips Milvus-oversized metadata like `canonical_identifiers` around the insert, then restores it so the in-memory pickle is unaffected.) Embedding is intentionally separate from the chunking pipeline (§5) because the same model is reused at query time.
3. **Retrieve.** At search time the *query* is embedded with the *same* model, and Milvus returns the chunks whose stored vectors have the highest cosine similarity to the query vector — the nearest neighbours in meaning-space. (Both halves must use the same model and dimension, or the vectors aren't comparable — hence `embedding_dim=768` is configured centrally.)

A tiny example: with the query "refund policy", the embedder maps it to a vector; Milvus ranks all chunk vectors by cosine similarity to it and returns the top-k, surfacing a chunk that says "we reimburse purchases within 30 days" even though it never uses the word "refund".

**Why we use it / alternatives.** The alternative is lexical search (BM25/keyword): fast and exact, but blind to synonyms and paraphrase — it misses "reimburse" when you searched "refund". Dense embeddings capture semantic similarity at the cost of an embedding model and a vector DB. (In practice our retrieval is hybrid — vector plus graph — but the embedding vector is the semantic-similarity backbone.) The OpenAI-compatible `OpenAILikeEmbedding` lets us swap the underlying embedding model purely via LiteLLM config, no code change.

**In our code.**
- `src/ingestion/embeddings.py` — `build_embedding_model()` (the `OpenAILikeEmbedding` factory).
- `src/workflow/activities/index_vector.py` — the `index_vector` activity: where chunks are embedded and inserted into Milvus (`index_nodes`).
- `src/config.py` `LiteLLMSettings` — `LITELLM_EMBEDDING_MODEL` (default `nomic-embed-text`), `LITELLM_EMBEDDING_DIM` (768), `LITELLM_BASE_URL`, `LITELLM_TIMEOUT_S`, `LITELLM_MAX_RETRIES`.

---

# Part 2 — Vectors, Graph & Entity Resolution

A from-scratch reference for the retrieval substrate of `kb-llamaindex`: how we find similar text by vector, how the knowledge graph is stored and indexed in Neo4j, and how we collapse the many surface forms of one real-world entity into a single canonical node — then summarise the graph into community reports.

> **Sandbox note on verification.** There is no live Neo4j / GDS in the sandbox where this was written. Every Cypher / GDS claim below is grounded by *reading* the code and tests, not by executing it against a database. Where the code itself flags an API as unverified-against-live-GDS, this doc repeats that caveat.

---

## Vector search & approximate nearest neighbour (ANN)

**What it is.** An embedding model turns a piece of text into a fixed-length list of floats — a *vector* (here 768 dimensions, `MilvusSettings.dim = 768`). Texts with similar meaning map to vectors that point in similar directions. "Search" then means: given a query vector, find the stored vectors closest to it. Closeness is a distance/similarity metric; we use **cosine similarity** (the angle between two vectors, ignoring their length).

**How it works.** The naive ("exact" / brute-force) approach compares the query against *every* stored vector, computes the similarity, and keeps the top-`k`. With `N` stored vectors of dimension `d` that is `O(N·d)` work per query — every query touches the whole collection. Concretely, at 1M chunks × 768 dims that is ~768M multiply-adds *per query*: correct, but a latency cliff.

ANN (approximate nearest neighbour) trades a little **recall** for a lot of **speed**. Instead of scanning everything, it builds an *index* — a data structure that lets a query visit only a small, cleverly-chosen subset of the stored vectors and still land on (almost) the true top-`k`. The cost: it may occasionally miss a true neighbour. We measure that loss as **recall@k** = (fraction of the true top-`k` that the approximate search actually returned). ANN is the standard answer once `N` grows past a few hundred thousand.

**Why we use it / alternatives.** Exact (FLAT) search is the correctness ground truth and is fine for small collections; ANN (HNSW, below) is what keeps query latency flat as the corpus grows. Other ANN families exist (IVF/inverted-file clustering, product-quantization/PQ for memory compression, DiskANN for on-disk graphs); HNSW is the default we picked for in-memory recall/latency.

**In our code.** Chunk vectors live in **Milvus**. The store is built in `src/retrieval/vector_index.py` (`build_vector_store` → `MilvusVectorStore(..., similarity_metric="COSINE", index_config=..., search_config=...)`). Vectors are written during ingest by the Temporal activity `src/workflow/activities/index_vector.py` (`index_vector` → `index_nodes(index, nodes)`). The recall-vs-latency trade is benchmarked in `tests/eval/scale/bench_milvus.py` (`bench_flat_vs_hnsw`, `_recall_at_k`).

---

## FLAT vs HNSW (Milvus)

**What it is.** Two index *types* for the same Milvus collection.

- **FLAT** is brute-force: it stores the raw vectors and, per query, scans all of them. Exact (recall@k = 1.0 by definition), no build cost beyond loading, but `O(N)` per query.
- **HNSW** (*Hierarchical Navigable Small World*) is a graph-based ANN index. It builds a navigable proximity graph once, then answers each query by walking that graph — touching only a tiny fraction of the vectors.

**How it works (HNSW from scratch).** Imagine the stored vectors as points. HNSW connects each point to a handful of its nearest neighbours, forming a "small-world" graph where you can greedily hop from any point toward any target in a few steps. The *hierarchical* twist: it stacks several such graphs in layers, like a skip-list.

- The **top layer** holds very few points with long-range links (a coarse map of the space).
- Each **lower layer** holds more points with shorter-range links.
- The **bottom layer** holds *every* point.

A query enters at the top, greedily walks to the closest point it can reach, then *descends* a layer and repeats — long hops first to get to the right region fast, then short hops to refine. That layered greedy descent is what turns an `O(N)` scan into roughly `O(log N)` hops.

Two build-time knobs and one query-time knob govern the recall/latency/memory trade:

- **`M`** — the graph degree (neighbours per node). Higher `M` → richer graph → better recall, but more memory and a slower build. (`hnsw_m = 16`.)
- **`efConstruction`** — how wide a candidate list the *builder* keeps while wiring each node's neighbours. Higher → better-quality graph (better recall) → slower build. (`hnsw_ef_construction = 200`.)
- **`ef`** (a.k.a. efSearch) — how wide a candidate list the *query* keeps while walking. Higher → explores more of the graph → better recall, slower query. It must be ≥ the search `top_k` or you cannot even return `k` results. (`hnsw_ef_search = 64`.)

Tiny intuition: with `ef = top_k` you do the minimum walk (fastest, lowest recall); raising `ef` is the per-query dial that buys recall back without rebuilding the index.

**Why we use it / alternatives.** FLAT is the llama-index default and is exactly what this project shipped a fix for: fine up to a few hundred k vectors, a latency cliff beyond ~1M (`MilvusSettings` comments). For the 250k-and-up target we default to **HNSW** so production collections get approximate-NN search out of the box. FLAT remains available (`MILVUS_INDEX_TYPE=FLAT`) for exact search / as the recall ground truth.

**In our code.**
- Config: `src/config.py` → `MilvusSettings`: `index_type: str = "HNSW"` (default), `hnsw_m = 16`, `hnsw_ef_construction = 200`, `hnsw_ef_search = 64`.
- Wiring: `src/retrieval/vector_index.py` → `_index_config()` emits `{"index_type": ..., "M": hnsw_m, "efConstruction": hnsw_ef_construction}` (HNSW params only when `index_type == "HNSW"`); `_search_config()` emits `{"ef": hnsw_ef_search}` for HNSW, `{}` otherwise. These feed `MilvusVectorStore(index_config=..., search_config=...)`.
- **Important caveat (in the config comment):** `index_type` only takes effect when the collection is *(re)created* — a fresh deploy or `overwrite=True` re-ingest. An existing FLAT collection keeps FLAT until rebuilt. So the HNSW default is an opt-in-by-rebuild swap, never a silent in-place mutation.
- Benchmark: `tests/eval/scale/bench_milvus.py` builds the same synthetic corpus under FLAT and HNSW, treats FLAT results as truth, and reports `hnsw_recall_at_k`, `flat_query_p50_ms`, `hnsw_query_p50_ms`, `speedup`, and both build times. It skips cleanly when no local Milvus is reachable.

---

## Neo4j as a property graph + its indexes

**What it is.** A **property graph** is a data model made of three things:

- **Nodes** — the entities. Each node can carry one or more **labels** (its types, e.g. `__Entity__`, `Person`, `Community`).
- **Relationships** — directed, typed edges between nodes (e.g. `(:__Entity__)-[:IN_COMMUNITY]->(:Community)`). The relationship *type* is part of its identity.
- **Properties** — key/value pairs on either nodes or relationships (e.g. an entity's `name`, `description`, `mention_count`, `er_embedding`).

Neo4j is a database built natively on this model; you query it with **Cypher** (the `MATCH (n)-[r]->(m) ...` pattern language). In this project the knowledge graph extracted from documents lives here: each entity is an `__Entity__` node (plus a type label like `Person`/`Org`), relations are typed edges, and the per-entity `description` property is the LightRAG-style semantic payload.

**How it works (indexes).** A bare `MATCH (e:__Entity__ {name: $name})` without an index is a full label scan — `O(N)` over every `__Entity__`. Neo4j offers several index kinds we lean on:

- **Range index** — a sorted index on a property, for equality and ordering. We add `entity_name` (`ON (e.name)`) and `entity_mention_count` (`ON (e.mention_count)`). The llama-index Neo4j store only creates a UNIQUE constraint on the node `.id`; our own Cypher matches by the *separate* `.name` property and orders by `.mention_count`, so these standalone indexes are what keep those lookups off a full scan at 250k+ nodes.
- **Full-text index** — a tokenised/analyzed text index for partial / fuzzy name lookup (powered by Lucene under the hood). `entity_name_fulltext` (`FOR (e:__Entity__) ON EACH [e.name]`) backs partial-name entity lookup in the retriever.
- **Native vector index** — Neo4j can store a `list<float>` property and index it for ANN, queried with the built-in procedure `db.index.vector.queryNodes(indexName, k, queryVector)`. We use two: `er_embedding_vec` over `__Entity__.er_vec` (entity-resolution kNN, see below) and `community_report_vec` over `Community.report_vec` (community-report retrieval). Both are created `cosine`, dimensioned by `$dim` (= `settings.milvus.dim`). This means we have vectors in *two* engines: Milvus for chunk text, Neo4j-native for the graph-side (entity and community) vectors.
- **GDS (Graph Data Science)** — a separate Neo4j plugin library of graph *algorithms* (community detection, centrality, path-finding, …). It works by *projecting* part of the graph into an in-memory representation, running an algorithm over it, and streaming/writing results back. We use it for Leiden community detection (next sections).

**Why we use it / alternatives.** A property graph (vs. a plain relational schema) makes the multi-hop, arbitrarily-typed relationships the KG extractor emits first-class and cheap to traverse. The indexes are the difference between sub-millisecond lookups and `O(N)` label scans at scale. Neo4j's *native* vector index lets graph-side similarity (entity dedup, community retrieval) live next to the graph itself instead of round-tripping to Milvus.

**In our code.**
- All index DDL is idempotent (`IF NOT EXISTS`) and **fail-open** (errors are logged and swallowed so a store/version without a given index just keeps the old behaviour). See `src/graph/index.py`: `ensure_entity_fulltext_index`, `ensure_entity_lookup_indexes` (range indexes on `name` + `mention_count`), `ensure_er_vector_index`, `ensure_community_report_vector_index`, `ensure_community_indexes`.
- Store factory: `src/graph/store.py` → `build_neo4j_graph_store()` returns a `Neo4jPropertyGraphStore`; tests use the in-memory `SimplePropertyGraphStore`. The generic Cypher entry point used throughout is `store.structured_query(cypher, param_map=...)`.

---

## Entity Resolution (ER)

**What it is.** The same real-world entity shows up under many *surface forms*. After extraction we may have separate nodes for "BCC", "Basal Cell Carcinoma", and "Базальноклеточный рак" — all the same thing. ER is the step that detects these semantically-equivalent duplicates and consolidates them into one **canonical** node, rewriting every reference to point at it. An earlier step (`merge_kg_extraction`) already collapsed entities whose *normalised names* match exactly; ER catches the duplicates that survive orthographic dedup:

- cross-language ("BCC" ≡ "Базальноклеточный рак"),
- abbreviations ("DNA" ≡ "deoxyribonucleic acid"),
- word-order / morphology ("Рак Кожи БК" ≡ "Рак БК Кожи"),
- initialisms ("Иванов И.И." ≡ "Иван Иванов"),
- cross-document (a canonical stored from doc 1 vs a new variant from doc 2).

The whole module is **conservative**: every LLM-side decision defaults to *DIFFERENT* on timeout/failure, because a false-positive merge permanently pollutes the graph (you can't easily un-merge two entities that were never the same).

The pipeline is *embedding-blocked, LLM-confirmed*. Below, each stage from scratch.

### (a) Candidate generation via cosine similarity (vectorized)

**What it is.** We can't ask the LLM about every possible pair — that's `O(N²)` LLM calls. So first we cheaply *block*: for each entity, find a short list of plausibly-equivalent neighbours by embedding similarity, and only those reach the expensive judge.

**How it works.** Each entity is embedded from `name + ": " + description` (or just `name` when it has no description). Candidates are computed **per label** (only same-typed entities can match) and the cosine of every within-label pair is taken. A pair is kept if its cosine clears a floor (`low = 0.55`, raised to `empty_description_floor = 0.70` when either side lacks a description), then trimmed to the top-`knn_k = 10` neighbours per entity. Each surviving pair is classified:

- cosine ≥ `high = 0.85` **and** same script (both ASCII or both Cyrillic) → **auto-merge** (no LLM),
- otherwise (cosine in `[low, high)`, *or* ≥ `high` but cross-script) → **borderline** → goes to the LLM judge.

The naive way to get all pairwise cosines is an `O(N²)` pure-Python double loop calling `_cosine` — the candidate-gen cost cliff at scale. We speed it up with a **vectorized matrix-vector product**: `_normalized_matrix` builds a row-normalised float64 matrix of the group's embeddings (numpy), then per row `cos_row = mat @ mat[i]` yields *every* cosine from entity `i` to the group in one BLAS call. When numpy is unavailable or the embeddings are ragged it falls back to the identical pure-Python `_cosine` path, so behaviour never silently changes (zero vectors keep cosine 0 either way). There are also name-token guards: a high Jaccard overlap of content tokens can *bypass* the cosine floor (catches the same entity described very differently across docs), and `name_token_min_overlap` can *reject* zero-overlap pairs (description-context contamination), with a cross-script exception so "Romashka" ≡ "Ромашка" still passes.

**Why we use it / alternatives.** Embedding-blocking is the standard ER trick: turn `O(N²)` LLM cost into `O(N·k)` candidate pairs. Same-label + cosine-floor + top-k is a cheap, high-recall filter; the alternative (full pairwise LLM, or no blocking) is intractable.

**In our code.** `src/graph/entity_resolution.py`: `_candidate_pairs` (the classifier), `_normalized_matrix` (the vectorized speedup), `_cosine` (fallback), `_name_tokens` / `_name_token_overlap` (guards). Knobs on `ERConfig`: `low`, `high`, `knn_k`, `empty_description_floor`, `name_token_min_overlap`, `name_overlap_floor_bypass`.

### (b) LLM-as-judge for borderline pairs + the verdict cache (`:ERVerdict`)

**What it is.** Borderline pairs (the ones embedding similarity can't decide) are handed to an LLM "adjudicator" that decides SAME / DIFFERENT / UNSURE for each. UNSURE is treated as DIFFERENT (conservative). The judge is prompted with both names, their types, and truncated descriptions, in batches of `judge_batch = 10`, and must return a strict JSON array.

**How it works.** `_llm_judge_pairs` dispatches batches concurrently (real parallelism is capped by the process-wide bounded LLM gate). `_parse_judge_response` extracts the JSON array; any missing/malformed entry → False (DIFFERENT). The system prompt teaches it the easy traps (`"Customer" ≠ "Customer #4521"`, `"Skin Cancer" ≠ "Melanoma"`, different people sharing a surname).

The **verdict cache** exists because the *same* name-pairs recur constantly — across re-ingests and within hub-heavy documents. Caching a verdict avoids re-paying for an LLM call we've already made. It caches, in Neo4j, a node `:ERVerdict {key, same}` where `key` is an **order-insensitive** identifier of the pair built from `(norm, label)` of both items (JSON-joined so a delimiter inside a name can't collide with a different pair), and `same` is the boolean verdict. Before judging, ER loads cached verdicts for the batch's keys, splits pairs into `(cached, uncached)`, judges only the uncached, then persists the fresh verdicts. The whole thing is **OPTIONAL and FAIL-SAFE**: with no `er_store` (or any Neo4j error) it degrades to pure LLM judging with byte-for-byte identical behaviour.

**Why we use it / alternatives.** The LLM is the only thing that reliably resolves cross-language / abbreviation / word-order equivalence; embeddings alone over-merge (related-but-distinct concepts embed close). The cache turns a recurring `O(pairs)` LLM cost into a one-time cost per distinct pair. Alternative (no judge): either over-merge on raw cosine or miss everything cross-script.

**In our code.** `src/graph/entity_resolution.py`: `_JUDGE_SYSTEM`, `_llm_judge_pairs`, `_parse_judge_response`; cache helpers `_verdict_key`, `_partition_cached`, `_load_verdict_cache` (`MATCH (v:ERVerdict) WHERE v.key IN $keys`), `_store_verdicts` (creates a UNIQUE constraint `er_verdict_key`, then `MERGE`es each verdict). Knobs: `ERConfig.judge_batch`, `ERConfig.verdict_cache_enabled`; config `AGENT_ER_JUDGE_BATCH_SIZE`, `AGENT_ER_VERDICT_CACHE_ENABLED`.

### (c) Union-find / connected-components clustering

**What it is.** The judge and the deterministic/auto stages produce a set of *confirmed equal* pairs (A≡B, B≡C, …). We need to turn those pairwise links into **clusters** — maximal groups where everything is transitively linked. That is exactly the connected-components problem on a graph whose edges are the confirmed pairs, and **union-find** (a.k.a. disjoint-set union) is the classic near-linear algorithm for it.

**How it works (from scratch).** Union-find maintains, for each element, a pointer to a "parent". Following parents up leads to a **root**; two elements are in the same cluster iff they share a root.

- `add(x)` — start `x` as its own root (`parent[x] = x`).
- `find(x)` — walk parent pointers up to the root. We *path-compress* along the way (re-point nodes nearer the root) so repeated `find`s get flatter and faster.
- `union(a, b)` — find both roots; if different, point one root at the other. Now `a` and `b`, and everything previously linked to either, share a root.
- `groups()` — bucket every element by its root → the clusters.

Tiny example: confirm (A,B) then (B,C). `union(A,B)` makes B the root of {A,B}. `union(B,C)` joins that root to C's root → all of A,B,C share one root → cluster `{A,B,C}`. The crucial property: we never asked the LLM "is A the same as C?" — transitivity gave it to us for free (which is also why stage (d)/verify exists, to catch *bad* transitive merges).

**Why we use it / alternatives.** Union-find is the standard, effectively `O(N·α(N))` way to do connected components incrementally as edges arrive. The alternative (BFS/DFS flood-fill per component) needs the full edge list materialised first and is clumsier to feed pair-by-pair.

**In our code.** `src/graph/entity_resolution.py`: the `_UnionFind` dataclass (`find` with path compression, `union`, `add`, `groups`). In `resolve_entities` every item is `add`ed, every confirmed pair is `union`ed, and `uf.groups()` (keeping only size > 1) yields the candidate clusters.

### (d) Hyper-hub clamp

**What it is.** Occasionally clustering snowballs: a generic or noisy entity links to dozens of others and the connected component balloons. Auto-merging a huge cluster is almost always wrong (one bad edge dragged in unrelated entities). The hyper-hub clamp refuses to auto-merge clusters above a size threshold.

**How it works.** Components with `len(cluster) >= hyper_hub_threshold` (default **12**) are split off into `review_clusters` and **not merged**. Their new members keep their original names, but each is flagged `properties["er_review_needed"] = True` for manual review. Smaller clusters proceed normally.

**Why we use it / alternatives.** It's a precision guardrail consistent with the module's "never false-merge" stance: a 12-member "cluster" is far more likely to be a contamination chain than 12 truly-identical surface forms. The alternative — trusting it — risks collapsing many distinct entities into one polluted node.

**In our code.** `src/graph/entity_resolution.py`: the `review_clusters` / `final_clusters` split in `resolve_entities`, gated by `ERConfig.hyper_hub_threshold`; review members get `er_review_needed` set. (Separately, clusters of `>= verify_cluster_size = 3` get a single LLM *consolidation* call, `_verify_cluster`, to split bad transitive merges before canonical selection.)

### (e) Canonical selection

**What it is.** Once a cluster is final, one member must survive as the canonical name; the rest become aliases that redirect to it.

**How it works.** `_pick_canonical` ranks members by a tuple key (highest wins), in priority order:

1. **`source == "stored"` always wins** — an entity already in Neo4j from a prior ingest beats a brand-new one. This prevents orphans: if a higher-mention *new* entity were chosen, the canonical upsert would create a fresh node and leave the old stored node dangling.
2. higher `mention_count`,
3. longer name (more specific),
4. Cyrillic surface preferred when `language` is Russian,
5. alphabetical (deterministic tiebreak).

The chosen canonical's `EntityNode` is rebuilt by `_consolidate_cluster`: it sums mention counts, unions source chunks / file paths, collects the others as `aliases`, picks the majority label, and consolidates descriptions via `_maybe_summarize_descriptions`. It also stamps `er_canonical_name` and `er_embedding` (JSON) so the *next* ingest's incremental ER can match against it; under native-kNN mode it additionally stamps `er_vec` (native list). Then `_apply_name_map` rewrites every chunk-level reference and merged relation to the canonical name, drops self-loops, and re-aggregates colliding relations; `_cleanup_stored_losers` repoints a non-canonical *stored* node's edges onto the canonical (via `apoc.merge.relationship`) and detach-deletes the loser — safe-by-inaction if APOC/the query fails (leaves the duplicate intact rather than dropping edges).

**Why we use it / alternatives.** The stored-wins rule is the key correctness choice — it keeps the graph's node identity stable across ingests. The rest is a deterministic, language-aware "most informative name" heuristic.

**In our code.** `src/graph/entity_resolution.py`: `_pick_canonical`, `_consolidate_cluster`, `_apply_name_map`, `_cleanup_stored_losers`. Knob: `ERConfig.language`.

### (f) Candidate source: incremental window vs native-vector kNN

This is the **scale** story of ER, and the most important knob for a large graph.

**The window (default).** To match a new entity against entities stored by *previous* ingests, ER loads a bounded slice of stored canonicals into memory and runs the same Python candidate-gen over them. `_load_existing_canonicals` reads `MATCH (n:__Entity__) WHERE n.er_canonical_name IS NOT NULL ... ORDER BY mention_count DESC LIMIT $limit` with `incremental_window = 5000`. The `ORDER BY mention_count DESC` matters: it guarantees the window always contains the *most-mentioned (hub)* canonicals rather than an arbitrary Neo4j slice.

**The recall cliff.** Memory and candidate-gen cost both grow with the window, so it's bounded at 5000. But once the graph has *more* than 5000 canonicals, any canonical outside the window is **invisible** to a new mention — it simply cannot match, so the new mention silently fragments into a duplicate. On a synthetic 200k-canonical graph the mention_count window reaches only **~2 %** of the true nearest canonicals (see `tests/eval/scale/bench_er_native.py`, `bench_native_vs_window` → `window_reachable`). Ordering by mention_count makes the *hub* entities reliable, but the long tail beyond 5000 is lost.

**The fix: native-vector kNN (opt-in).** Instead of loading a window and brute-forcing, query Neo4j's native vector index *per new entity* for its `k` nearest stored canonicals across the **whole graph** — no window ceiling. `_load_candidates_native` calls `db.index.vector.queryNodes('er_embedding_vec', $k, $vec)` (the index over `__Entity__.er_vec`) for each new entity and unions the deduplicated stored matches. On the same 200k graph, native kNN recovers **~96 %** of true nearest canonicals at ~6 ms/query. It's best-effort: returns `[]` (→ within-batch ER only) if the store is missing or the index isn't built yet.

**Backfill + flag ordering (must do in this order).** The native path can only find canonicals that have `er_vec` populated. Existing entities only have the legacy `er_embedding` JSON string. So:

1. Run `scripts/backfill_er_vector.py --no-dry-run` first — it parses each existing entity's `er_embedding` JSON into a native `er_vec` list (via `apoc.periodic.iterate` + `apoc.convert.fromJsonList`, idempotent, only touches entities still lacking `er_vec`) and then builds the index (`ensure_er_vector_index`).
2. *Only then* flip the flag `ER_USE_NATIVE_VECTOR_KNN=true`.

Flip it before the backfill and the index is empty / missing, so kNN returns nothing and new entities never match stored ones. The default is **off** precisely to force this ordering.

**Why we use it / alternatives.** The window is simple, dependency-free, and fine for small graphs; native kNN is the only thing that keeps cross-document dedup correct past ~5000 canonicals. (Raising `incremental_window` is the cheap stopgap — memory ≈ window × dim × 4 bytes, so 5k×768 ≈ 15 MB, 25k ≈ 75 MB — but it doesn't remove the ceiling, only raises it.)

**In our code.**
- `src/graph/entity_resolution.py`: `_load_existing_canonicals` (window), `_load_candidates_native` (kNN), the branch in `resolve_entities` keyed on `cfg.use_native_vector_knn`. Knobs: `ERConfig.incremental_window`, `use_native_vector_knn`, `vector_knn_k = 20`.
- `src/graph/index.py`: `ER_VECTOR_INDEX_CYPHER` / `ensure_er_vector_index` (the `er_embedding_vec` index over `er_vec`, cosine, `$dim`).
- `scripts/backfill_er_vector.py`: the one-shot `er_embedding` → `er_vec` backfill + index build (dry-run by default).
- Config: `AGENT_ER_ENABLED`, `AGENT_ER_USE_NATIVE_VECTOR_KNN` (default `False`), `AGENT_ER_VECTOR_KNN_K` (default 20), `AGENT_ER_JUDGE_BATCH_SIZE`, `AGENT_ER_VERDICT_CACHE_ENABLED` in `src/config.py` → `AgentSettings`.
- Bench: `tests/eval/scale/bench_er_native.py` (native recall vs window-reachable vs latency).

---

## Community detection & the Leiden algorithm

**What it is.** A graph **community** is a group of nodes more densely connected to each other than to the rest of the graph — a "cluster" in the network sense (a tightly-related set of entities in our KG). Community *detection* automatically finds those groups. We run it offline over the whole `__Entity__` sub-graph.

**How it works (from scratch).**

- **Modularity** is the standard quality score for a partition of a graph into communities. Intuitively it measures: *how many more edges fall inside communities than you'd expect if the same nodes were wired up at random* (preserving each node's degree). High modularity = communities that are genuinely denser-than-chance internally. Detection = find the partition that (greedily) maximises modularity.

- **Louvain** is the classic fast modularity optimiser. It works in two repeating phases: (1) *local move* — start every node in its own community, then repeatedly move each node into the neighbouring community that most increases modularity, until no move helps; (2) *aggregate* — collapse each community into a single super-node and repeat phase 1 on the smaller graph. Iterating builds a hierarchy of ever-coarser communities. It's fast and widely used — but it has a known flaw: **it can produce internally-disconnected communities.** Because a node moves based on modularity gain alone, Louvain can leave a community whose members aren't actually connected to each other through that community — a "community" that's really two unrelated pieces.

- **Leiden** is the fix (Traag et al.). It adds a **refinement phase** between the local-move and aggregate steps: within each community, it re-partitions into well-connected sub-communities before aggregating, and only aggregates those refined pieces. This *guarantees* every community it outputs is **internally connected** (no disconnected blobs), and in practice it's also faster and finds higher-quality partitions than Louvain. That well-connectedness guarantee is the reason we use Leiden, not Louvain.

**Hierarchical communities.** Like Louvain, Leiden produces a *dendrogram* — a multi-level nesting where coarse communities split into finer sub-communities. GDS exposes this with `includeIntermediateCommunities: true`, yielding per node a list of community ids from finest → coarsest (its last element is the final `communityId`). We materialise this as multiple `:Community` levels: **level 0 = coarsest**, finer levels carry higher numbers, and `(:Community {level:k-1})-[:PARENT_OF]->(:Community {level:k})` wires the dendrogram coarse→fine. A structural invariant (Leiden nesting): a level-(k-1) community is the union of its level-k children, so a parent is never smaller than a child — which is why the `min_size` floor can never drop a parent while keeping its child (no orphaned `PARENT_OF`).

**Why we use it / alternatives.** We use **GDS Leiden** specifically for the well-connected-communities guarantee and the built-in hierarchical dendrogram; Louvain is the alternative we deliberately don't use (disconnected-community flaw). The graph is projected *undirected* (`undirectedRelationshipTypes: ['*']`) because Leiden requires it and edge direction is meaningless for community detection on a KG. A fixed `randomSeed: 19` makes runs reproducible.

**In our code.**
- `src/graph/communities.py`: the GDS Cypher constants (`_project_cypher` — Cypher projection of the `__Entity__` sub-graph, handling arbitrary relationship types; `_leiden_stream_cypher` — `gds.leiden.stream(..., {randomSeed: 19, includeIntermediateCommunities: true})`; `_drop_cypher`). Grouping: `_coarsest_from_rows` (single-level, `detect_communities`) and `_group_by_levels` (full dendrogram → `CommunityRef`s, `detect_hierarchy`). The whole module is **fail-safe**: a `None` store or any GDS/Cypher error logs and returns `[]` so the Temporal activity never raises. Per-call projection names (`_new_graph_name`) isolate concurrent rebuilds; level-scoped (`_PRUNE_LEVEL_CYPHER`) / full (`_PRUNE_ALL_CYPHER`) prunes clear stale `:Community` nodes before a rewrite.
- Activity wrapper: `src/workflow/search/activities/community.py` → `detect_communities_activity` (picks `detect_hierarchy` when `max_levels > 1`, else `detect_communities`).
- Config: `AgentSettings.community_max_levels` (`AGENT_COMMUNITY_MAX_LEVELS`, default 1, capped 1–10), `TemporalSettings.community_min_size` (default 3), `community_summary_parallelism`.
- **Verification caveat:** as the module's own docstring states, the GDS calls are written per the Neo4j GDS 2.x API but are **UNVERIFIED against a live GDS install** — there is no Neo4j/GDS in this sandbox, so the Cypher above is verified by reading code/tests, not execution.

---

## Community reports (map-reduce summarization)

**What it is.** A raw community is just a set of entity names — not directly useful to a reader or to a "global" question ("what are the main themes of this corpus?"). A **community report** turns each community into a structured, human-readable summary: `{title, summary, findings:[{statement, importance}]}`. The full set of reports across all levels is the GraphRAG **"global" substrate** — the coarse, queryable map of the whole corpus that global/drift search ranks and reduces over, instead of touching raw chunks.

**How it works (map-reduce, bottom-up).** Summarisation is a **map-reduce** over the community hierarchy:

- **Map** — one LLM call *per community* produces its structured report. To keep cost down, each call uses the small-tier LLM, runs with bounded parallelism, and reads only that community's local context (not the whole graph).
- **Bottom-up over levels** — the build processes the **finest level first**, coarsest (level 0) last. A level-0 (coarse) community's report is composed from its *child* reports (`_CHILD_REPORTS_CYPHER`, reading `(parent)-[:PARENT_OF]->(child)` where the child already has a report) rather than re-reading every leaf entity — cheaper than re-summarising the whole subtree, and it's why finest-first ordering is required (parents need children's reports to exist). A level-0 community with no children (or before any exist) falls back to member context (`_MEMBER_CONTEXT_CYPHER`: member names, descriptions, and inter-member relation types).
- **Incremental carry-over by members-hash** — `members_hash` is an order-insensitive content hash of a community's member names. Before a rebuild prunes the old communities, the prior reports are read; any community whose `(level, members_hash)` is unchanged **carries its old report over** (marked `needs_report=False`) instead of being re-summarised — and keeps its original `summarized_at` so staleness logic reflects content freshness, not rebuild time. So a rebuild only pays for communities that actually changed.
- **`report_vec` embedding** — `title + "\n" + summary` is embedded (same model as ER/ingest) and stored as a native `report_vec` list on the `:Community` node, indexed by `community_report_vec`. This powers *semantic* community selection at query time (`community_dynamic_selection = "semantic"` does kNN over `report_vec`; `"descent"` walks the hierarchy; default `"lexical"` is word-overlap). Embedding is fail-open: on failure the report still persists, just without a vector (search degrades to lexical).

The report itself is parsed tolerantly (`_parse_report`): strips a ```json fence, grabs the outermost `{...}`, and on any failure falls back to a shape that still carries the raw text as `summary` — so the activity never raises and there's always *something* to persist.

**Why we use it / alternatives.** This is the GraphRAG idea: pre-compute a hierarchy of community summaries offline so "global" questions can map-reduce over a few hundred compact reports instead of scanning the entire chunk corpus at query time. Bottom-up child-report composition and members-hash carry-over keep the (otherwise expensive) rebuild affordable. The alternative — summarising on the query path, or re-summarising everything every build — is far slower and costlier.

**In our code.**
- Activities: `src/workflow/search/activities/community.py` → `summarize_community_activity` (gather context → small-LLM `_REPORT_PROMPT` → `_parse_report` → `_embed_report` → idempotent `_WRITE_REPORT_CYPHER` MERGE of report/title/summary/report_vec). Context builders `_gather_context`, `_build_member_context`, `_build_child_context`.
- Carry-over: `src/graph/communities.py` → `members_hash`, `_read_old_reports`, and the carry block in `detect_hierarchy` (`needs_report=False` for unchanged communities).
- Orchestration: `src/workflow/search/community_wf.py` → `CommunityBuildWorkflow` (detect → fan-out summarize). `build_summarize_specs` skips `needs_report=False` communities; `group_specs_by_level` orders **finest-first** so a coarse parent's child reports are persisted before it runs; the fan-out is bounded by `community_summary_parallelism`.
- Index: `src/graph/index.py` → `community_report_vec` / `ensure_community_report_vector_index` (ensured once in `detect_communities_activity`).
- Config: `AgentSettings.community_dynamic_selection` (`lexical` | `semantic` | `descent`), `community_max_levels`; `TemporalSettings.community_summary_parallelism`, `community_min_size`.
- The whole build is **DECOUPLED / OFFLINE** — it runs on the dedicated `kb-graph-build` queue (admin endpoint / schedule), never on the query hot path.

---

# Part 3 — Search & Retrieval

A from-scratch reference for the retrieval techniques that turn a user question into a grounded answer in `kb-llamaindex`. Each concept is explained from zero, then tied to the exact code that implements it. For the end-to-end narrative and diagrams, see [`docs/SEARCH.md`](../SEARCH.md) (deep reference) and [`docs/SEARCH-FLOW.md`](../SEARCH-FLOW.md) (flow + diagrams) — this section deliberately does **not** duplicate those.

> All config knobs below live on `AgentSettings` in `src/config.py` with the `AGENT_` env prefix (e.g. `graph_walk_enabled` → `AGENT_GRAPH_WALK_ENABLED`). They are resolved at **submit time** in `src/api/routes/search_v2.py` (`_local_params` / `_global_params`) and baked into the workflow input, so a config change can't desync a running (replaying) workflow.

---

## RAG (Retrieval-Augmented Generation) fundamentals

**What it is.** A bare large language model answers from its frozen training weights alone. It cannot know your private corpus, it cannot cite a source, and when it doesn't know something it tends to *confabulate* a plausible-sounding answer. Retrieval-Augmented Generation (RAG) fixes this by putting a search step **in front of** generation: first fetch the passages most relevant to the question, then ask the model to answer **using only those passages**. The model becomes a reader/summarizer over fresh, attributable evidence rather than an oracle.

**How it works.** The classic loop has two phases.

- *Offline (ingest):* documents are split into **chunks** (passages small enough to retrieve and to fit a context window), each chunk is turned into an **embedding** (a vector capturing its meaning), and the vectors are stored in a vector index. (See the ingest section of these concepts for how chunking/embedding work here.)
- *Online (query):* the question is embedded the same way, the index returns the nearest chunks (semantic nearest-neighbour search), and those chunks are concatenated into a prompt that instructs the model to **synthesize** an answer grounded in them — ideally with citations and explicit "I don't know" when the evidence is thin.

So the pipeline is **chunk → embed → retrieve → synthesize**. Retrieve-then-generate beats a bare LLM because the answer is grounded in current, inspectable text, hallucination drops, and you can show *where* each claim came from.

**Why we use it / alternatives.** The alternatives are (a) a bigger/fine-tuned model — expensive, still frozen, still un-citable; or (b) stuffing the whole corpus into the prompt — impossible at our scale and ruinously slow. RAG is the standard way to ground an LLM on a private, changing knowledge base. Our system goes beyond plain vector RAG by adding a **knowledge graph** (entities + relations) and **community summaries** on top of chunks, so it can answer connection questions and corpus-level themes that flat chunk retrieval misses (the next concepts).

**In our code.** The retrieval primitives are the *atomic tools* in `src/retrieval/atomic_tools.py`: `vector_search` (dense chunk retrieval) and the graph tools. The synthesis step is the `synthesize_answer` activity, run once at the end of every mode on the heavyweight ("large") LLM tier. The whole retrieve→synthesize loop is orchestrated by Temporal workflows under `src/workflow/search/`. Nothing here is "LLM picks the next tool in a loop" — the older ReAct/Self-RAG agent was removed; the flow is a fixed plan-execute-synthesize pipeline.

---

## Local search — vector retrieval + graph expansion

**What it is.** "Local" search answers a **specific, factual** question (who is X, where does Y work, what is Z's phone number) by gathering concrete chunk evidence. It blends two retrieval modalities: **dense vector** retrieval over chunks (semantic similarity) and **graph expansion** (pull the entity the question is about, plus its neighbourhood in the knowledge graph). Graph expansion matters because facts about an entity are often scattered across documents; the graph stitches them together by *who/what is connected to whom*.

**How it works.** For one (sub-)question the deterministic pipeline runs three tools in order — `vector_search`, then `graph_search`, then `find_entity_by_name` — and merges all their chunk results, deduped by `chunk_id`.

- *Entity linking onto ER-canonical entities.* The graph is built on `:__Entity__` nodes that entity-resolution (ER) has already canonicalised — duplicates merged, identifiers normalised (phone in E.164, INN, email, …). `graph_search` does **similarity** matching against those canonical entities and returns them plus their neighbours up to `path_depth` triplet-hops; `find_entity_by_name` does a **full-text name** match (partial-name tolerant: "Иванов" → "Иванов Иван Иванович"). Both resolve the question's surface text onto the *same canonical entity* the rest of the graph hangs off, so expansion lands on real, deduplicated nodes rather than near-duplicate aliases.
- *Dual walk-seed.* A one-hop `graph_search` neighbourhood is often too shallow for "how is X connected to …" questions, so after the fixed pipeline runs we optionally launch a bounded multi-hop `graph_walk`. The interesting part is **where the walk starts from**. Seeding only from the top `graph_search` hit misses entities that were found by name match but not by similarity. So with `dual_seed` on, the walk is seeded from the **union of BOTH** the top `graph_search` entity **and** the top `find_entity_by_name` entity (deduped, graph_search first). Each seed contributes its own neighbourhood. This is `_walk_seeds` in the retrieve activity. The walk is **fail-open per seed**: a missing/garbled seed or a store error just skips that one walk and returns the vector + graph_search results unchanged — it never sinks the activity.

**Why we use it / alternatives.** Pure vector RAG would find passages that *mention* X but miss the relational structure ("X's manager's company"). Pure graph traversal would miss free-text passages that never became graph entities. Running both and merging gives recall from vectors and connectivity from the graph. The dual seed specifically guards against the failure mode where the entity the user named is only reachable by exact name, not by embedding similarity.

**In our code.** `src/workflow/search/activities/retrieve.py` — `retrieve_subquestion` runs `_PIPELINE = ("vector_search", "graph_search", "find_entity_by_name")`, then `_walk_seeds(...)` builds the `graph_walk` start set. The atomic tools live in `src/retrieval/atomic_tools.py` (`graph_search` maps `depth`→ the retriever's `path_depth`; `graph_walk` is hard-capped on nodes/edges/hops). The graph retriever is built with `similarity_top_k = settings.agent.graph_similarity_top_k` in `src/workflow/_search_deps.py`. Config knobs (all `AGENT_`):

| knob | default | effect |
|---|---|---|
| `graph_walk_enabled` | `True` | turn the post-pipeline multi-hop walk on/off |
| `graph_walk_hops` | `2` (1–3) | requested hop count for the walk (clamped to the retriever's `GRAPH_WALK_MAX_HOPS`) |
| `graph_walk_dual_seed` | `True` | seed the walk from BOTH graph_search and find_entity_by_name (vs graph_search-only) |
| `graph_search_path_depth` | `1` (1–3) | neighbour expansion depth for `graph_search` |
| `graph_similarity_top_k` | `20` (1–100) | how many entities the graph retriever's similarity search returns |

> **Accuracy note on per-call depth/hops.** The HTTP `SearchRequest` (`src/models/search.py`) carries only `query`, `history`, and `top_k` — it has **no** `depth`/`hops` field, so over HTTP these are governed solely by the `AGENT_*` config above. Per-call depth/hops overrides exist **only on the MCP tools** in `src/mcp/tools_server.py`: `graph_search(depth=…)`, `find_neighbours(hops=…)`, and `graph_walk(hops=…)`. Don't confuse the two surfaces.

---

## Global search — community map-reduce

**What it is.** Some questions aren't about one entity at all — "what are the main themes across the corpus", "summarise the overall trends", "how many … across all documents". Retrieving a handful of chunks can't answer these; you'd need to read *everything*. **Global search** (the GraphRAG pattern) answers them by pre-computing, offline, a summary for each **community** (a densely-connected cluster of entities found by Leiden clustering), and then doing **map-reduce** over those summaries at query time.

**How it works.** *Map-reduce* here means:

- **Map** — for each selected community, ask a cheap (small-tier) LLM: "given ONLY this community's summary, what does it say about the question?" Off-topic communities self-report `НЕТ` and are dropped (score 0). This fans out one independent LLM call per community, bounded by a concurrency limit.
- **Reduce** — concatenate the surviving partial answers and run **one** large-tier `synthesize_answer` over them to produce the final answer.

The corpus can have far more communities than we want to map over, so the workflow first **selects** which communities to map. There are **three dynamic selection strategies**, and all of them **fail open to lexical**:

1. **lexical** — read the stored summaries and rank them by plain word-overlap with the query (`rank_summaries`). Cheap, deterministic, LLM-free. This is the default and the fallback.
2. **semantic** — kNN over each community's stored *report vector* (`community_report_vec` index): embed the query, return the nearest community reports first (`select_communities_semantic`).
3. **descent** — coarse→fine hierarchy walk (`select_communities_descent`): start at the coarsest level-0 communities, keep the ones whose report vector is most cosine-similar to the query, descend into their `PARENT_OF` children, repeat — spending the budget on the most relevant *leaf* communities.

Both vector strategies (semantic, descent) return `[]` on **any** error or an empty result, and the activity then falls straight back to the lexical path — so a missing vector index or a flaky embedder degrades gracefully instead of failing the search.

**Why we use it / alternatives.** The alternative for a "themes across everything" question is to retrieve top-k chunks and hope they're representative — they won't be, because top-k is biased toward whatever phrasing matches, not toward corpus-wide coverage. Map-reduce over community summaries gives genuine breadth: every relevant community gets a vote. The selection strategies trade cost vs. precision — lexical is free but blunt; semantic is sharper; descent exploits the community hierarchy to avoid mapping irrelevant branches at all.

**In our code.** Selection + map activities: `src/workflow/search/activities/global_search.py` (`map_communities` switches on `selection`; `rank_summaries`, `select_communities_semantic`, `select_communities_descent`; `map_community_partial` is the per-community map step). The map-reduce orchestration: `src/workflow/search/global_wf.py` (`GlobalSearchWorkflow` — map fan-out with a semaphore, `partials_to_sources` filtering, then `build_reduce_call` → one large-tier synthesis). Communities themselves are built offline by `src/workflow/search/community_wf.py` (`CommunityBuildWorkflow`: Leiden detect → finest-first summarize), which runs on a dedicated `kb-graph-build` queue, never on the query hot path. Config (all `AGENT_`):

| knob | default | effect |
|---|---|---|
| `community_dynamic_selection` | `"lexical"` | which selection strategy: `lexical` / `semantic` / `descent` |
| `global_max_communities` | `20` (1–200) | cap on how many community summaries enter the MAP step |
| `global_map_parallelism` | `4` (1–32) | bound on concurrent per-community MAP LLM calls |

---

## DRIFT search — combine local + global

**What it is.** A complex/mixed question may need *both* concrete facts *and* a broad overview ("compare these companies and their role in the wider network"). **DRIFT** runs local search first for concrete chunk evidence, then expands it with corpus-level community context from global search. Local evidence leads; the community partials broaden it; one synthesis at the end fuses them.

**How it works.** It's a **bounded one-shot**, not an open-ended loop: exactly one local pass plus one global pass.

1. (Optional) contextualise the follow-up against conversation history **once** here, then pass the rewritten standalone query to both children with history cleared, so neither child re-contextualises.
2. Run `SearchOrchestratorWorkflow` (the full local plan-execute flow) as a child → concrete sources.
3. Run `GlobalSearchWorkflow` as a child with `drift_mode=True`, handing it the local sources as the **drift seed**. In drift mode the global workflow merges the local sources **ahead of** the community partials in the reduce context (local evidence first), and labels the outcome `"drift"`.

The key resilience feature is `_drift_local_fallback`: if the **global pass fails** (child-workflow error, timeout, activity failure), drift doesn't error — it returns the already-computed local answer, keeping the `"drift"` mode label so callers/metrics still see the request *was* drift, just degraded.

**Why we use it / alternatives.** Without drift, you'd have to pick local *or* global up front and lose half the answer. Drift gets both in a single bounded pass. The one-shot design (vs. an iterative agent that keeps drilling) keeps latency and cost predictable, and the local fallback means the global half is strictly *additive* — it can only improve the answer, never break it.

**In our code.** `src/workflow/search/router_wf.py` — `DriftSearchWorkflow.run` (contextualise-once → local child → global child with `drift_mode`); `_drift_local_fallback` for the degrade path; `merge_doc_ids` unions the local+global document ids. The drift-mode merge ("local ahead of partials") lives in `GlobalSearchWorkflow.run` in `src/workflow/search/global_wf.py`.

---

## Auto mode & query routing

**What it is.** Callers don't always know which mode a question needs. **Auto** mode asks a small, cheap LLM to **classify** the question into `local` / `global` / `drift`, then dispatches to the matching flow — so the right strategy is picked per question without the client choosing.

**How it works.** The `route_query` activity sends the question to the small-tier router model with a strict prompt: reply with exactly one word — LOCAL, GLOBAL, or DRIFT. `AutoSearchWorkflow` then maps the label to a child workflow via the pure `dispatch_for_route` helper and runs it.

There is a **double fail-safe to local** (the cheapest, always-grounded mode):

1. In `classify_route` (the pure parser): an empty, garbled, or unrecognised reply → `route="local"`. It also tolerates wrapping prose by recognising the first known label that appears ("Route: GLOBAL." still parses as global).
2. In `route_query` itself: any LLM error (proxy down, timeout) is caught → `route="local"`.

And `dispatch_for_route` maps any unexpected label to the local workflow as a third belt-and-suspenders. So a flaky router can never break search — it just degrades to local.

**Why we use it / alternatives.** The alternative is forcing the client to choose a mode (brittle — clients guess wrong) or always running the most expensive mode (drift) regardless (wasteful). A cheap small-tier classifier is a good cost/quality trade, and because every failure path collapses to local, the worst case is "we ran the safe cheap mode."

**In our code.** Classifier activity: `src/workflow/search/activities/route.py` (`route_query` + pure `classify_route`). Dispatch workflow: `src/workflow/search/router_wf.py` (`AutoSearchWorkflow.run`, `dispatch_for_route` with the local default). Auto is wired into the HTTP `auto` endpoint in `src/api/routes/search_v2.py`.

---

## Plan-execute decomposition & the orchestrator

**What it is.** A complex question ("who are X's co-founders and where are they based now") is hard to answer with a single retrieval. **Plan-execute** decomposition breaks it into atomic **sub-queries** up front, retrieves for each independently, merges the evidence, and synthesizes **once**. There is no "LLM decides the next step" loop — the plan is fixed before any retrieval runs.

> An older "selfrag/ReAct" agent (LLM picks the next tool in a loop) was **removed**. It is not part of the current system; the only LLM calls in the local flow are the up-front planner and the final synthesizer.

**How it works.** `SearchOrchestratorWorkflow`:

1. (Optional) contextualise the query against conversation history.
2. **plan** — `plan_subquestions` (small planner model) splits the question into ≤ `max_subqueries` atomic sub-questions; an atomic question yields just `[query]`.
3. **execute** — fan out one `SubQueryRetrievalWorkflow` child per sub-question, **in parallel** via `asyncio.gather`. Each child runs the deterministic vector+graph `retrieve_subquestion` (see Local search) — no agent, no tool-selection LLM.
4. **merge** — union all children's sources, dedup by `chunk_id`.
5. (coverage gate — next concept)
6. **rerank** then **synthesize once** over the merged pool on the large tier.

**Why we use it / alternatives.** A single retrieval over a multi-part question retrieves for the "average" of the parts and serves none of them well. An open-ended ReAct agent *can* decompose adaptively but is slow, non-deterministic, hard to test, and prone to loops — which is why it was removed here. Fixed plan-execute keeps the decomposition benefit (each sub-question retrieved well, in parallel) while staying deterministic and replay-safe under Temporal.

**In our code.** `src/workflow/search/orchestrator.py` (`SearchOrchestratorWorkflow.run`: plan → parallel children → `merge_subquery_sources` → rerank → one `synthesize_answer`). The per-sub-question retrieval child: `src/workflow/search/subquery_wf.py` (`SubQueryRetrievalWorkflow`, deterministic, dedup by chunk_id). Config: `AGENT_MAX_SUBQUERIES` (`max_subqueries`, default `5`, range 1–20).

---

## Coverage check — bounded refinement loop

**What it is.** After merging the evidence, the system asks itself "does this actually cover the whole question, or is something missing?" If a concrete gap is named, it runs **one more** retrieval round for that gap and folds the results in. It's a small, bounded self-correction loop — not an open-ended "keep going until perfect."

**How it works.** After the merge step, while the round budget is left:

1. `coverage_check` (small tier) reads the question + a bounded evidence blob (`build_evidence`, capped at ~12k chars) and returns `complete` plus a `missing` gap phrase.
2. The pure `should_run_coverage_round` decides: if `complete` is false **and** a non-empty gap is named **and** rounds remain → return the gap phrase; otherwise → `None` (go straight to synthesis).
3. On a named gap: run **one** extra `SubQueryRetrievalWorkflow` for that gap phrase, re-merge its sources into the pool (dedup by `chunk_id`), decrement the budget, loop.

The loop is **bounded** by `max_coverage_rounds` and **fail-open** at every step: any error in the check *or* the extra retrieval breaks out of the loop and proceeds to synthesis — it never blocks the answer. The feature is gated by `coverage_check_enabled`, which (like all knobs) is **resolved at submit time** and carried in the workflow input, so the decision is stable across replays.

**Why we use it / alternatives.** Plan-execute decomposition is done before retrieval, so it can miss a gap that only becomes obvious *after* seeing what was retrieved. The coverage check is a cheap second look that catches those. The alternatives are no self-correction (misses gaps) or an unbounded refinement agent (expensive, can loop forever) — the bounded one-round-by-default loop is the middle ground.

**In our code.** Pure helpers: `src/workflow/search/_coverage.py` (`build_evidence`, `should_run_coverage_round`, `COVERAGE_EVIDENCE_MAX_CHARS = 12_000`). The loop + fail-open call sites are the "coverage gate" block in `src/workflow/search/orchestrator.py`. The judge activity is `coverage_check`. Config (`AGENT_`): `coverage_check_enabled` (default `True`), `max_coverage_rounds` (default `1`, range 0–3).

---

## Conversation-history contextualization

**What it is.** Follow-up questions are full of references — "what about *his* company?", "and *there*?". Retrieval can't resolve those, because the index doesn't know what "his" means. **Contextualization** rewrites the follow-up into a **self-contained** question using the recent conversation turns ("what about Ivanov's company?") *before* any retrieval runs.

**How it works.** When the request carries `history` and the feature is on, the workflow runs `contextualize_query` (small tier) **once** at the start: it bounds the history to the most recent turns (by turn count and char budget, keeping the latest turns), prompts the model to "rewrite the LAST question as self-contained, expanding pronouns and references, keep the language, return ONLY the rewritten question", and replaces the query everywhere downstream via `model_copy`. It is **fail-open**: empty history, no usable turns, or any LLM error → the original query unchanged.

It is **opt-in/gated** (`contextualize_enabled`, sourced from `AGENT_CONVERSATION_HISTORY_ENABLED`) and **resolved at submit time** (the flag and the history list are baked into the workflow input in `_local_params`/`_global_params`), so it's **replay-safe** — a replaying workflow makes the same decision it made the first time. Drift contextualises once in the parent and clears history on its children so it isn't redone.

**Why we use it / alternatives.** Without it, multi-turn chat retrieval silently degrades on every follow-up because pronouns retrieve nothing useful. The alternative — stuffing raw history into the retrieval query — pollutes the embedding with off-topic earlier turns; a clean rewrite into one standalone question retrieves far better. Fail-open means it can only help: a bad rewrite falls back to the raw query.

**In our code.** `src/workflow/search/activities/contextualize.py` (`contextualize_query`, `_bound_history`, `_build_prompt`; uses the small `route`-tier LLM). Called at step 0 of `SearchOrchestratorWorkflow`, `GlobalSearchWorkflow`, and once in `DriftSearchWorkflow`. Config (`AGENT_`): `conversation_history_enabled` (default `True`), `history_max_turns` (default `6`, range 0–40), `history_max_chars` (default `4000`).

---

## Reranking

**What it is.** Retrievers (vector, graph) are *recall-oriented* — they cast a wide net and return many candidates in their own per-modality score order, which doesn't reflect true relevance to *this* question. A **reranker** is a second-stage model that reads each candidate chunk **together with** the query and re-scores it for relevance, so the best few float to the top before synthesis sees them. We use a **cross-encoder** (the query and chunk go through the model *jointly*), which is more accurate than the bi-encoder embeddings used for first-stage retrieval — but too expensive to run over the whole corpus, hence "second stage only."

**How it works.** The orchestrator's merged pool mixes graph-derived and vector chunks in raw union order. Before synthesis, `rerank_sources` co-ranks them in **one** cross-encoder pass (`BAAI/bge-reranker-v2-m3` via `SentenceTransformerRerank`), deduping by `chunk_id` first so each unique chunk is scored once, and returns the top-N (`rerank_top_n`). This is **unified** rerank: graph and vector chunks compete in the *same* ranking rather than being interleaved by their incomparable native scores. It is **capped fail-open**: if the reranker errors, the orchestrator doesn't fail — it falls back to the merged pool but still **caps** it to `rerank_top_n` (`cap_synth_sources`), so a flaky reranker can't blow past the synthesis prompt size / timeout.

**Why we use it / alternatives.** First-stage vector similarity is fast but coarse, and graph and vector scores aren't comparable, so feeding their raw union to synthesis wastes context on mediocre chunks. A cross-encoder rerank is the standard precision boost. The alternative is no rerank (cheaper, worse top-N) or reranking everything (too slow) — second-stage rerank of the merged candidate set is the usual sweet spot.

**In our code.** Activity: `src/workflow/search/activities/rerank.py` (`rerank_sources`, `prepare_rerank_pool`). Model factory: `src/retrieval/reranker.py` (`build_reranker`, default `settings.hf.rerank_model = BAAI/bge-reranker-v2-m3`, offline-cache aware). The reranker is lazy-built and process-cached in `src/workflow/_search_deps.py` (`get_reranker`). Fail-open cap: `cap_synth_sources` in `src/workflow/search/orchestrator.py`.

> **BM25 hybrid is NOT wired.** `src/retrieval/hybrid.py` (`build_bm25_retriever` / `build_hybrid_retriever`, BM25 + dense RRF fusion) exists as an A/B *experiment candidate* only — the production retriever is **dense-only** (built in `_search_deps`), and the file's own docstring says it is "NOT wired into the active search path." Despite the `vector_search` docstring saying "Hybrid (BM25 + dense)", the live retriever is dense vector retrieval; treat the hybrid module as off-path until benchmarked and adopted.

---

# Part 4 — Knowledge Anchor, Outputs, Models & Ops

This section explains the platform layer around the RAG pipeline — the canonical knowledge anchor (Wikibase), the human-readable outputs (the continuous wiki editor and SPARQL access), how we pick and record models per role, the LLM gateway, observability, and the MCP tool surface — building each idea up from zero and then grounding it in the actual code.

---

## The Wikibase knowledge anchor

**What it is (from scratch).** Wikibase is the open-source software that runs Wikidata. It is built on MediaWiki (the wiki engine behind Wikipedia) plus an extension that adds *structured data*. The data model has three core object types:

- **Items** — the "things" (a person, a company, a concept). Each Item has a stable machine ID called a **QID** (`Q42`, `Q14`, …), one or more human labels/descriptions/aliases, and a list of *statements*.
- **Properties** — the *kinds of facts* you can assert. Each Property has a **PID** (`P31`, `P569`, …) and a fixed *datatype* (e.g. `wikibase-item` for "points at another Item", `external-id` for an identifier string, `string`, `quantity`). On Wikidata, `P31` is famously "instance of".
- **Statements** — a fact about an Item, shaped as `(Item, Property, value)`. Example: `Q42 — instance-of → Q5 (human)`. The value's shape is governed by the Property's datatype.

So Wikibase is essentially a typed, queryable graph with stable IDs, an editing UI, a REST/Action API, and (via WDQS) a SPARQL endpoint.

**How it works.** We run a *self-hosted* Wikibase (the `wikibase/wikibase-bundle` image, MySQL-backed) and *project* our Neo4j knowledge graph into it after each successful ingest. The projection rules are:

- **Owner entities → Items.** Anything that is not an identifier type (Person, Organization, Concept, Metric, …) becomes a standalone Item.
- **Identifier entities → external-id statements.** Phone, email, INN, OGRN, etc. do *not* get their own Item. Instead they are folded onto their owner Item as `external-id` statements (one Property per identifier type). This keeps the graph clean: an INN is an attribute of a company, not a node to navigate to.
- **Owner↔owner relations → object-property statements.** A relation label between two owner entities becomes a `wikibase-item` statement linking the two Items.
- **Common claims.** Every owner also gets `instance_of` (its base class QID), `er_canonical_name` (string), and `mention_count` (quantity), when those Properties exist in the bootstrap cache.
- **Lazy property creation.** When a relation label has no matching PID yet, we `create_property` on the fly (datatype `wikibase-item`), cache the new PID in Neo4j, and reuse it next time.
- **QID writeback for create-vs-update.** Before pushing an owner, we look up the `wikibase_qid` property on its `:__Entity__` node in Neo4j. If present, we `update_item` (reusing the QID); if absent, we `create_item` and *stamp the new QID back onto the Neo4j node*. This makes re-ingest idempotent: the second pass updates rather than duplicating.

The push is **best-effort**: per-owner and per-relation errors are logged and skipped, and the whole activity never fails the workflow — a Wikibase outage cannot block ingest from completing.

**Why we use it / alternatives.** Neo4j is the RAG team's working store; it is fast for the retriever but it is not a product consumers outside the team should be handed direct access to. Wikibase gives the *rest of the org* a canonical, schema-rich, stable-ID source of truth they can query over standard interfaces (Wikibase REST/Action API + WDQS SPARQL) without Neo4j credentials or Cypher knowledge. The alternative — exposing Neo4j directly, or hand-rolling a separate API — would couple external consumers to our internal store and its churn. Projecting into Wikibase decouples them and reuses the mature Wikidata tooling (UI, history, SPARQL) for free.

**In our code.**
- `src/workflow/activities/push_wikibase.py` — the Temporal activity. Honours `WIKIBASE_ENABLED` (returns `status="skipped"` when off), loads the `:WikibaseBaseClass` and `:WikibaseProperty` caches from Neo4j, logs in via `AsyncWikibase.from_settings`, then calls `push_entities`. It also flags the silent no-op case (entities in, 0 items created/updated → `status="failed"`).
- `src/storage/wikibase.py` — `AsyncWikibase` (async wrapper over the synchronous `wikibaseintegrator` SDK via `asyncio.to_thread`) and `push_entities` (the projection orchestrator: partition owners vs identifiers, index relations, upsert owners, write owner↔owner statements, lazy-create properties). Identifier labels come from `IdentifierType` at import time (`_IDENTIFIER_LABELS`). QID lookup/writeback: `_lookup_qid_for_entity` / `_persist_qid_for_entity`. Observed surface forms are also written as Item *aliases* (`set_aliases`) to feed canonical entity linking.
- `scripts/setup_wikibase.py` — one-time idempotent bootstrap: creates the 10 base-class Items and 27 Properties (3 common + 24 identifier `external-id`), provisions the runtime bot account via `createAndPromote`, and persists the QID/PID cache into Neo4j so the ingest hot path never re-looks-up. Flags: `--dry-run`, `--refresh-cache`.
- Config knobs (`src/config.py`, `WikibaseSettings`, env prefix `WIKIBASE_`): `WIKIBASE_ENABLED` (default `False` — opt-in), `WIKIBASE_BASE_URL` (default `http://localhost:8181`), `WIKIBASE_BOT_USER` / `WIKIBASE_BOT_PASSWORD`, `WIKIBASE_LANGUAGE` (default `ru`), `WIKIBASE_TIMEOUT_S`.
- Cross-link: `docs/runbook/wikibase.md` (bring-up, bootstrap, SPARQL examples, teardown).

---

## The continuous wiki editor

**What it is (from scratch).** A wiki editor that turns the graph into human-readable, per-entity MediaWiki articles. Every entity gets its own page; the page is (re)generated from the graph rather than written by hand. If the entity has a `wikibase_qid`, the page is also linked to its Wikibase Item via a *sitelink*. This is a separate concern from the Wikibase projection above: Wikibase holds *structured* data for machines/queries, the wiki editor produces *prose* for humans to read.

**How it works.**

- **Hybrid trigger.** Ingest does not synchronously regenerate articles. Instead, right after writing to the graph, a best-effort hook *marks the touched entities dirty* (`wiki_dirty = true`). Separately, a Temporal-scheduled sweep periodically *drains* the dirty queue in batches. This decouples ingest latency from article generation and lets many small ingests coalesce into one sweep.
- **Per-entity unit of work.** Each sweep selects up to `sweep_batch` dirty entities (oldest-dirty first), and runs one activity per entity.
- **BOT-SECTION ownership.** Only the text between the markers `<!-- KB-BOT:START -->` and `<!-- KB-BOT:END -->` is owned by the bot. Everything outside the markers is human-owned and preserved verbatim. On first write (no markers), the bot section is prepended and any existing human text is kept below.
- **Anti-drift design.** The LLM prompt is grounded *only* in current graph facts (relations) plus citation snippets — **prior article prose is never fed back to the model**. This is the key guarantee: hallucinations from one run cannot accumulate over successive regenerations, because each regeneration starts from the graph, not from the last article.
- **Hash-skip change-detection.** Before doing any LLM work, the activity computes a stable hash over the entity's facts (name/label/description + relations) *and* its source-document id set. If the hash matches the last-written `wiki_hash`, the article is unchanged → the activity clears the dirty flag and returns `SKIPPED` with no MediaWiki call. Folding the doc-id set into the hash means a *new source document* (which adds a download link) also regenerates the article even if no 1-hop relation changed.
- **"Источники" section.** After the LLM prose, a *deterministic* (non-LLM) `== Источники ==` section is appended with download links to the original source files (`{docs_base_url}/documents/{doc_id}`), one per distinct source document. Omitted entirely when there are no docs or no base URL.

**Why we use it / alternatives.** A graph is great for machines but unreadable for a human browsing "what do we know about company X". Generating articles gives a familiar wiki UX. The alternatives — letting humans write articles by hand (stale, unscalable) or letting an LLM freely rewrite the whole page each time (drift / hallucination accumulation / clobbering human edits) — are exactly what the bot-section + anti-drift + hash-skip design avoids: the machine owns a bounded, regenerated-from-facts section; humans keep the rest; unchanged entities cost nothing.

**In our code.**
- `src/workflow/wiki/wiki_sweep.py` — `WikiSweepWorkflow` plus the two activities: `select_dirty_entities` (drains the queue) and `write_entity_article` (reads subgraph + docs, hash-skips, renders the bot section, splices it into the page, upserts via MediaWiki, ensures the sitelink, persists the page title + hash, clears dirty).
- `src/workflow/wiki/article.py` — `splice_bot_section` (marker-bounded replace/prepend), `render_bot_section` (the LLM render grounded only in `ctx` + citations), the prompt (note the explicit "Use ONLY the facts … Do NOT invent anything"), and `_fmt_sources` (the deterministic Источники section).
- `src/graph/wiki_context.py` — `read_entity_subgraph` (1-hop subgraph, relations capped + ranked by neighbour `mention_count`), `read_citations`, `read_source_docs`, and `subgraph_hash` (the change-detection hash over facts + doc ids; deliberately excludes QID/page-title/citation text).
- `src/graph/wiki_dirty.py` — `mark_dirty` / `select_dirty` / `clear_dirty` (the `wiki_dirty` / `wiki_hash` / `wiki_synced_at` bookkeeping on `:__Entity__` nodes). The ingest-side hook lives in `src/workflow/activities/mark_dirty.py`.
- Config knobs (`src/config.py`, `WikiSettings`, env prefix `WIKI_`): `WIKI_ENABLED` (default `False` — opt-in), `WIKI_TASK_QUEUE` (`kb-wiki`), `WIKI_ACTIVITY_CONCURRENCY` (4), `WIKI_SWEEP_BATCH` (50), `WIKI_SWEEP_INTERVAL_MINUTES` (15), `WIKI_CITATIONS_TOP_K` (8), `WIKI_MAX_RELATIONS` (30), `WIKI_DOCS_BASE_URL` (`http://localhost:8000/api/v1`), `WIKI_MEDIAWIKI_API_URL` (empty → derived from `wikibase.base_url`), `WIKI_SITE_GLOBAL_ID` (`kbwiki`).
- Cross-link: `docs/runbook/wiki-editor.md` (enable steps, schedule registration, data flow).

---

## SPARQL & WDQS (briefly)

**What it is (from scratch).** SPARQL is the standard query language for RDF graph data — think "SQL for triples". You write graph patterns (`?item wdt:P31 wd:Q5 .`) and the engine returns every binding that matches, including multi-hop traversals across the whole dataset. **WDQS** (Wikidata Query Service) is the query frontend that exposes a Wikibase instance over SPARQL; in our stack it is backed by Blazegraph.

**How it works / caveat.** WDQS does not query Wikibase's MySQL directly. It maintains its *own* copy, updated from the Wikibase change stream in batches. So it is **eventually consistent**: freshly-pushed Items usually appear within seconds, but under heavy ingest the lag can stretch to a minute or two. The MediaWiki REST/Action API (`wbgetentities`) is always authoritative — treat WDQS as the convenient-but-lagging analytics view, not the source of truth. WDQS is optional for runtime; we use it for instance-wide graph-style queries rather than per-Item lookups.

**Why we use it / alternatives.** SPARQL is the right tool when a consumer wants traversal across the whole instance ("all Organizations linked to topic X") rather than fetching one known Item. The alternative — paging the REST API and stitching results client-side — is fine for single-Item reads but poor for graph queries. Knowing the eventual-consistency caveat avoids the classic "I just pushed it, why is SPARQL empty" trap (answer: re-check via REST).

**In our code.** No application code calls WDQS directly; it is operator-facing. Exposed at `http://localhost:8989` (compose service `wdqs`). See `docs/runbook/wikibase.md` §6 ("Querying via SPARQL (wdqs)") for the endpoint, example queries, and the sync-lag caveat.

---

## Multi-model / role-based model selection

**What it is (from scratch).** Different jobs in the pipeline have different cost/quality needs. High-volume internal work (entity extraction, judging, search-side normalisation) wants a cheap, fast, local model; the single user-facing answer synthesis wants the best model available. "Role-based model selection" means we name *logical roles* (`extraction`, `judge`, `search`, `route`, `plan`, `retrieve`, `synthesis`) and map each to one of just two *physical tiers* the operator actually manages — `small` and `large`.

**How it works.**

- Two physical model names: `LITELLM_MODEL_SMALL` and `LITELLM_MODEL_LARGE`. Operators manage exactly these two.
- A declarative role→tier map (`_DEFAULT_ROLE_TIERS`): everything defaults to `small` *except* `synthesis`, which is `large`.
- `LITELLM_ROLE_TIERS` lets an operator escalate a single role without re-declaring the rest — provided overrides are *merged* onto the defaults (e.g. `{"plan":"large"}` only changes `plan`).
- Resolution is `model_for(role)`: `role → tier_for(role) → model_large if "large" else model_small`.
- **Model snapshots at submit time.** When a document is submitted for ingest, the API records the *exact* model resolved for each role *right then* (`cfg.model_for("extraction")`, `"judge"`, `"search"`) and carries those strings through the workflow. So even if config changes later, the run is tagged with the models it actually used. The `finalize` activity passes these per-role models into the metrics extractor, and they land in `ingest_metrics.model` per activity row — giving accurate per-activity model attribution for version-compare dashboards.

**Why we use it / alternatives.** A single global model forces a bad trade-off: either pay large-model cost on every extraction call, or accept small-model quality on the final answer. The tier indirection keeps the *operator's* surface tiny (two model names) while still letting any one role be escalated. Recording the snapshot at submit time (rather than reading live config at metrics-write time) is what makes "version A vs version B" comparisons honest.

**In our code.**
- `src/config.py` — `LITELLM_MODEL_SMALL` / `LITELLM_MODEL_LARGE`, `_DEFAULT_ROLE_TIERS`, the `LITELLM_ROLE_TIERS` merge validator, and `LiteLLMSettings.model_for(role)` / `tier_for(role)`.
- `src/api/routes/ingest.py` — captures the per-role snapshots at submit (`extraction_model` / `judge_model` / `search_model` via `cfg.model_for(...)`) and the `version_tag`.
- `src/workflow/activities/finalize.py` — builds `models_per_role` from the snapshotted fields and feeds it to the timings extractor.
- `src/storage/ingest_metrics.py` — the `MetricRow` schema and writer; `model` is one of the recorded columns (with `version_tag`, `env`).
- Cross-link: `docs/MODELS.md`.

---

## LiteLLM gateway

**What it is (from scratch).** An *LLM gateway* (a.k.a. proxy/front-door) is a single service that sits in front of one or more model backends and exposes them all behind one OpenAI-compatible HTTP API. Clients always speak the same protocol (`/v1/chat/completions`, `/v1/embeddings`) and just pass a model name; the gateway routes to the actual backend (a local server, a hosted API, etc.). **LiteLLM** is the gateway we run.

**How it works.** Everything in the app — LlamaIndex LLM calls and embeddings — points at one base URL (`LITELLM_BASE_URL`, default `http://localhost:4000`) with an API key. LiteLLM, configured by `docker/litellm_config.yaml`, maps the model names we send (the small/large tier names) to real upstreams. Because both `llama-index-llms-openai-like` and `llama-index-embeddings-openai-like` speak the OpenAI wire format, no client code needs to know which backend is live.

**Why we use it / alternatives.** Routing every call through one OpenAI-compatible endpoint means swapping a backend (local → hosted, or one model → another) is a *config-only* change with zero application edits; it also centralises auth, the place to add rate-limiting/observability, and keeps the codebase provider-agnostic. The alternative — wiring each client to a specific provider SDK — scatters provider knowledge across the code and makes model swaps a code change.

**In our code.**
- `docker-compose.yml` — the `litellm` service (`ghcr.io/berriai/litellm:main-stable`, port `4000`, config mounted from `docker/litellm_config.yaml`, master key from `LITELLM_API_KEY`, plus a liveness healthcheck).
- `src/config.py` — `LiteLLMSettings.base_url` (`LITELLM_BASE_URL`) and `api_key` (`LITELLM_API_KEY`); model/embedding names resolve here too.

---

## Observability

**What it is (from scratch).** Observability is being able to answer "what happened, and why" after the fact. Our stack has three complementary layers:

1. **Temporal Web UI** — Temporal records every workflow's full event history (each activity scheduled/started/completed/failed, inputs, retries). The Web UI lets you inspect any run, see exactly where it is/failed, and — because Temporal is deterministic-replay based — *replay* a workflow's history to debug it.
2. **Prometheus + Grafana** — the Temporal Python worker exposes a built-in Prometheus metrics endpoint; Prometheus scrapes it on an interval, and Grafana dashboards visualise the aggregates. This is the *live, aggregated* view (activity latencies, success/fail counts, labelled by activity type and task queue).
3. **Per-activity `ingest_metrics` (Postgres)** — the *frozen, per-run* view. At the end of each workflow, `finalize` reads its own Temporal history, derives per-activity durations, and writes one row per `(activity, attempt)` into Postgres, tagged with `version_tag`, `model`, and `env`.

**How it works.** The two metric paths are deliberately redundant. Prometheus answers "how is the system doing right now / over the last hour" (live, aggregated, ephemeral retention). Postgres `ingest_metrics` answers "exactly how did *this specific run* behave, and how does version A compare to version B" (durable, per-run, model-tagged). The Postgres write is best-effort — a Temporal/Postgres hiccup logs a warning but never fails ingest — and de-duped via an `ON CONFLICT DO NOTHING` on `(workflow_run_id, activity_name, attempt)`, so Temporal history replays don't double-count.

**Why we use it / alternatives.** Workflow histories give causal, replayable detail but are awkward to aggregate; Prometheus aggregates beautifully but forgets specifics and lacks our business tags (version, model); a per-run Postgres table gives exactly the durable, queryable, version/model-tagged drill-down that A/B model comparisons need. Each covers the others' blind spot.

**In our code.**
- `src/storage/ingest_metrics.py` — `MetricRow` schema + `AsyncIngestMetrics.insert_metrics` (bulk insert with the `ON CONFLICT DO NOTHING` de-dup). Columns include `duration_ms`, `started_at`/`completed_at`, `version_tag`, `model`, `env`.
- `src/workflow/activities/finalize.py` — `_persist_ingest_metrics` fetches parent + child workflow histories and parses per-activity timings (best-effort).
- `docker-compose.yml` — `prometheus` (`prom/prometheus`, scrape config from `infra/prometheus/prometheus.yml`) and `grafana` (`grafana/grafana`, auto-provisioned datasources + dashboards from `infra/grafana/provisioning`).
- Cross-link: `docs/runbook/analytics.md` (the two paths, bring-up, the three Grafana dashboards: Ingest Overview / Version compare / Run drill-down).

---

## MCP (Model Context Protocol) surface

**What it is (from scratch).** MCP (Model Context Protocol) is a standard protocol for exposing *tools* (and resources/prompts) to LLM agents. An MCP *server* advertises a set of callable tools with typed schemas; an MCP *client* (an LLM host like Claude Desktop, Cursor, OpenWebUI, or our Hermes Agent) discovers those tools and lets the model call them during its own reasoning loop. It standardises the "give the model tools" plumbing across hosts, the way LSP standardised editor↔language-server plumbing.

**How it works.** We expose our raw retrieval primitives as atomic MCP tools — no Temporal workflow in between. The client's LLM drives its *own* ReAct-style loop and composes these primitives itself. The exposed tools include `vector_search`, `graph_search`, `graph_walk`, `find_entity_by_id`, `find_entity_by_name`, `find_neighbours`, `get_chunks_by_doc_id`, and `read_full_document`. Each tool returns a JSON-serialisable dict, carries an 1800s (30 min) execution timeout so a slow graph walk can't hang a client, and routes every LLM sub-call (e.g. the synonym-normalisation step inside `graph_search`) through the per-process `LLMPool`, which keeps each role bounded and serialises concurrent clients behind it. (`filter_by_metadata` is deliberately *not* exposed — it operates on an in-process accumulator that stateless tool-call clients don't maintain.)

**Why we use it / alternatives.** Exposing atomic tools over a standard protocol lets *any* MCP-capable agent build its own search strategy against our knowledge base without us shipping a bespoke client per host or hard-coding one orchestration. This complements the sibling "already-orchestrated answer" server (MCP-1, `kb_search`): MCP-2 hands over primitives for clients that want to drive the loop; MCP-1 returns a finished answer for clients that don't. The alternative — a single fixed endpoint — denies agents the flexibility to compose retrieval themselves.

**In our code.**
- `src/mcp/tools_server.py` — the FastMCP server `kb-llamaindex-tools`: the 8 `@mcp.tool` definitions (each wrapping a function from `src/retrieval/atomic_tools.py`), lazy dependency bootstrap (`_init`), the 1800s per-tool timeout, SSE auth (`build_sse_auth`), and the `--transport stdio|sse` entrypoint. GPU/LLM protection via the per-process `LLMPool` (`src/retrieval/llm_pool.py`).
- Cross-link: `docs/runbook/mcp.md` (two-server overview; note its banner that MCP-1 was re-pointed at `SearchOrchestratorWorkflow` post-R7b — the MCP-2 atomic-tools section is current).
