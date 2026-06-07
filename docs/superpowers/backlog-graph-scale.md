# Graph-scale & GraphRAG-parity backlog

Deferred items from the 250k-entity scaling review (2026-06-06/07) and the
GraphRAG/LightRAG feature comparison. **In scope NOW (separate specs/plans):**
cheap wins 8/13/12, conversation history (#4), and the communities track
(hierarchical communities + dynamic selection, #1+#2). Everything below is
parked with enough context to pick up later. Effort: S/M/L.

## From the GraphRAG / LightRAG comparison

### #3 — Claims / covariates extraction (HIGH domain value) — L
GraphRAG-style typed claims (subject–predicate–object + status/time) attached
to entities, instead of burying such facts in free-text `description`. Fits our
B2B/support domain directly ("party A owes amount Z under contract N", "order
#4521 refunded", "contract terminated on DATE"). Gives factual, cited local
answers and is the load-bearing structure for the deferred **Project B**
(document type / authority / fact conflicts). **Code already scouted** (ready
for spec): claim step slots into `LightRAGExtractor._aextract` (a 3rd metadata
key `KG_CLAIMS_KEY` alongside KG_NODES/RELATIONS, parsed per chunk), merges in
`merge.py`, stored on entities, surfaced in `graph_search` observation +
synthesis context (`retriever.py:204-209`, `synthesize_answer.py`). Cost: extra
LLM work at ingest. Pull off the backlog together with Project B.

### #5 — DRIFT iterative follow-up questions — M–L
Upgrade drift from one-shot local+global to GraphRAG-style primer (global →
draft answer + follow-up questions) → iterative local refinement. We already
have a weak analog (coverage-check → 1 extra round), so marginal gain over that;
+latency/+LLM rounds. Do after the communities track lands.

### #6 — Relationship-centric global (cheap alternative) — M
LightRAG's global = ranked relationship chains, no community build. Useful as a
second global path that needs no Leiden/summaries (sidesteps community
staleness). Duplicates our community path; consider only if community rebuild
cost stays painful after the communities track.

### #7 — Multimodal: tables/images into the graph — L (ingest track)
RAG-Anything/MinerU/Docling extraction of tables/images/formulas. High domain
value (contracts/invoices are tables — amounts, line items, requisites we
currently lose). Big ingest track, not search. Separate initiative.

### Tier-3 (low value for us)
- **Prompt auto-tuning** (GraphRAG) — we already hand-tuned multilingual prompts; auto-tune is for cold-start on arbitrary domains. Skip.
- **Question generation** (suggest next questions) — UX nicety, not retrieval quality. Skip.
- **Dual-level keyword extraction** (LightRAG low/high keywords) — overlaps our plan-execute decomposition + auto-router. Marginal. Skip/maybe.
- **Document deletion → KG regen** (LightRAG) — operational/GDPR; needs entity/chunk ref-counting. Defer.

## From the ER / scaling deep-dive (not yet done)

### Superseded
- **Incremental community summarisation via `members_hash` carry-over** (old plan item 11) — **SUPERSEDED** by the communities track: hierarchical detection + dynamic selection reshapes how/when summaries (reports) are (re)generated. Drop the standalone members_hash plan; fold change-detection into the communities spec.

### Still open
- **generic-singleton consolidation** — M. Short single-token names (mention_count=1, no description) skip ER and get neither `er_canonical_name` nor `er_vec`, so they never consolidate across ingests → accumulate as duplicate singletons. Mark + match cautiously (needs an eval set to avoid false merges). Risk: medium (ER recall behaviour change). NOTE: whole-graph batch re-resolution now EXISTS (`scripts/reresolve_graph.py`, runbook `reresolve-graph.md`) — it sweeps every eligible entity, but generic singletons still lack `er_vec`, so they only match under `--candidate-source window`; folding them into native-kNN remains the open part.
- **verdict-cache TTL** — S. `:ERVerdict` grows unbounded; lookups are indexed so it is storage-only. Add a maintenance prune (older-than-N-days), NOT inline in the ingest hot path.
- **`_embed_entities` streaming batches** — S. One embed batch per ingest risks rate-limit/timeout on very large documents. Stream in chunks of 100–200 with bounded concurrency.
- **cluster verify/consolidate token blow-up** — M. Large clusters (8–11) inflate the verify LLM prompt; clusters ≥12 stay unmerged (hyper-hub clamp). Split large clusters / iterative verify.
- **entity-linking loses index with `query.filters`** — S–M. llama-index's native entity vector path is bypassed when filters are present (`if not query.filters`). A latency mine if entity-type filtering is ever added on search. Avoid filters at that step or use a filter-capable index.
- **Milvus collection partitioning** — M. Single index over all chunks; partitions would help only if queries can be segmented (e.g. by doc_type). HNSW already ~5ms at 250k, so low priority.
- **recall tuning** `graph_similarity_top_k` 20→50, `path_depth` 1→2 — S + eval. Needs the recall@k labelled set (harness stub exists) to validate; safe now that the hub-walk cliff is measured mild.

## Documentation

### Fully document the search paths — S–M (docs only)
Write end-to-end documentation of **every** search path, not just the high-level
overview in `docs/SEARCH-FLOW.md`. Each mode traced from `POST /search` →
router/auto-mode → activities → stores → synthesis, with the exact decision
points and fallbacks:
- **local** (vector + graph_search dual walk-seed, ER-canonical entity linking),
- **global** (community map-reduce; lexical | semantic-kNN | descent selection,
  fail-open to lexical),
- **drift** (local+global one-shot + `_drift_local_fallback`),
- **auto / router** mode-selection logic and the **coverage-check → extra round**
  loop (SearchOrchestratorWorkflow / SubQueryRetrievalWorkflow),
- per-call knobs (`graph_search.depth`, `find_neighbours.hops`, `top_k`,
  `path_depth`) and where each is resolved (submit-time vs activity),
- conversation-history contextualization path,
- what degrades vs hard-fails on each store outage.
Include per-path sequence diagrams (Mermaid + D2) like `docs/INGEST.md`. Goal: a
reader can follow any query to the exact code without reading the source first.

## Config hygiene

### Prune dead AgentSettings fields (NOT remove the class) — S
`AgentSettings` is **not** legacy — it's read in ~28 sites across `src/` (ER,
search, communities, conversation history, coverage, graph-walk knobs incl.
`er_use_native_vector_knn`, `community_max_levels`, `conversation_history_enabled`).
Do **not** remove the class. But two fields are genuinely dead (0 read sites
outside `config.py`):
- `llm_max_concurrent` — superseded by the per-process `LLMPool` (post LLM-pool
  consolidation); the pool now owns LLM concurrency.
- `observation_max_chars` — no readers.
Drop those two (and audit `top_k` / `er_enabled` / `canonical_linker_enabled`,
which ARE read but gate dormant/opt-in features). Optional: the name
`AgentSettings` is a vestige of the earlier "agent" framing — a rename to
`SearchSettings`/`RetrievalSettings` could be folded in, but that touches many
call sites (`settings.agent.*`) so it's a separate, mechanical change.

## Notes
- P1.2 hub-walk degree-cap: measured MILD (≤2× across hops 2–3) → not worth a fix unless a restored hub→hub→hub graph shows otherwise.
- All "search feature" gaps are deltas vs systems we already resemble (extractor ported from LightRAG; local/global/drift naming from GraphRAG).
