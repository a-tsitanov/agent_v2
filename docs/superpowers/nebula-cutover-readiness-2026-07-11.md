# NebulaGraph cutover-readiness assessment — 2026-07-11

Honest audit of what MUST work before flipping `GRAPH_BACKEND=nebula` as the default (and migrating data + decommissioning Neo4j), vs. what degrades acceptably. Basis for the "finish line" before the (deferred-until-full-migration) push.

## ✅ DONE — works under nebula, live-verified on the cluster

The **core ingest → community → search cycle** runs end-to-end under `GRAPH_BACKEND=nebula`:
- **Ingest write:** `upsert_nodes`/`upsert_relations` (batched multi-VALUES), `er_canonical_name` + first-seen `created_at`/`first_doc_id` (read-back preserve, first-write-wins), `RELATED.weight`.
- **Entity resolution:** `ERVerdict` cache + edge-redirect merge (safety-preserving) + candidate kNN via Milvus.
- **Community lifecycle:** BUILD + SUMMARIZE + READ; `detect_communities` (leidenalg) runs FULLY under nebula (edge-export → Leiden → materialise via the seam).
- **Search read:** GraphRetriever (Phase 2), community map-summaries, vectors in Milvus.
- Schema/readiness: `ensure_schema` with write-readiness probes (Entity/Community tags, RELATED-weight edge, new columns).

## 🔴 MUST-FIX before a nebula-default flip

### 1. Data migration (neo4j → nebula) — NO TOOLING EXISTS (biggest blocker)
There is no graph backfill or dual-write. You cannot flip to an empty nebula. Options:
- **(a) Full-graph backfill script:** read ALL neo4j `__Entity__` (with props: name/description/mention_count/created_at/label/er_canonical_name/first_doc_id) + ALL `RELATED` (with props incl weight) → `upsert_nodes`/`upsert_relations` to nebula. Needs a full-node exporter (today's `graph_edge_export` streams only names+edge-weights, not full node props — insufficient). **A buildable slice.**
- **(b) Re-ingest all documents under nebula:** clean (rebuilds the graph via the now-working nebula ingest) but expensive (re-runs LLM extraction).
- **At billion-scale:** either path needs the deferred **bulk-import** (nebula-importer / SST offline load) — per-statement/batched online writes won't load billions in time.

### 2. Wiki-editor subsystem — the ONLY code that CRASHES (not degrades) under nebula
Raw Cypher with no fail-soft wrapper, on a real Temporal-workflow crash path:
- `graph/wiki_dirty.py` + `wiki_sweep.py::select_dirty_entities` — no try/except at any layer → propagates out of `WikiSweepWorkflow.run` (whole-workflow crash).
- `graph/wiki_context.py` + `wiki_sweep.py::write_entity_article` (+ 2 inline raw-Cypher calls) — every article write fails deterministically under nebula (workflow survives via per-entity catch, but produces 0 articles after burning retries).
- `api/routes/admin.py POST /admin/wiki/rebuild?all=true` — unwrapped `structured_query` → 500.
**Fix choice:** (a) translate wiki graph ops to nGQL (a slice), OR (b) add fail-open wrappers so wiki DEGRADES like everything else (cheap, if degraded-wiki-under-nebula is acceptable), OR (c) gate the wiki subsystem OFF under `GRAPH_BACKEND=nebula`.

### 3. Operational provisioning (ops, not code)
- Space is `replica_factor=1` (single replica, no HA) — prod needs `replica_factor=3` + multi-storaged (fixed at space creation → provision a NEW prod space correctly before the data load).
- Backup/restore, monitoring, capacity for the real dataset.

## 🟡 DEGRADES cleanly — deferrable (business decision, not a crash)
All fail-open (try/except → `[]`/`StageResult(error)`/no-op); under a nebula flip these features return EMPTY until their nGQL ports land, but the app keeps running:
- **Analytics** — entire `analytics/primitives/*` catalog (all funnel through the fail-soft `store_query.run_rows` → `[]`), `analytics/materialize` + `materialize_activities` (activity-level catch), `graph/analysis` (admin GDS endpoints), `graph/alerts`.
- **Monitoring** — `workflow/monitor/activities` (never raises across Temporal).
- **Index DDL** — `graph/index.py` `ensure_*_index` (silently no-op under nebula; nebula builds its own indexes in `ensure_schema`).
- **Wikibase push** — `push_wikibase` + `storage/wikibase` (doubly-wrapped, explicitly best-effort/non-blocking).
- **doc↔community search enrichment** — `documents.py` (fail-open; never blocks the answer).
Decision needed: is "empty analytics/monitoring until ported" acceptable during/after cutover, or must they be ported first?

## 🟡 Functional-completeness gaps (deferred; not crashes)
- **REL first-seen** — `RELATED.created_at`/`first_doc_id` + `upsert_relations` preserve-rework (entity first-seen done; edges deferred).
- **descent** community-select — needs Milvus-backed report-vector retrieval (report_vec is in Milvus, not on the node).
- **doc↔community** graph read — chunks aren't nebula nodes (Milvus); needs a different design.
- **distributed centralities** (GraphScope) — stub/manual-gate; store-agnostic, relieves GDS-OOM on neo4j too.
- **billion-scale direct-read** — replace the GO-through-query-layer full extract with a connector/dump.
- **write_retry.py** is Neo4j-specific (`Neo.TransientError.*`) — nebula writes get no equivalent transient-retry (minor).

## Recommended cutover sequence
1. **Wiki CRASH set** — decide fix vs. gate-off-under-nebula (smallest concrete pre-cutover code item).
2. **Data-migration slice** — full-node exporter + neo4j→nebula backfill script (moderate scale) OR commit to re-ingest.
3. **Decide the DEGRADE set** — accept empty analytics/monitor temporarily, or port the high-value ones (analytics is the stated dominant workload — likely port before relying on it).
4. **Prod nebula provisioning** (replica_factor=3, HA, backup) + load the migrated data.
5. **Flip `GRAPH_BACKEND` default → nebula**, validate the core cycle live, then decommission neo4j.
6. Only THEN: push (per the no-push-until-full-migration policy).

**Bottom line:** the core (ingest+community+search) is done and verified. The gating items are **data migration (no tooling)** and the **wiki crash set**; everything else degrades safely and is a prioritisation call. The dominant-workload **analytics** currently returns empty under nebula — porting it (or the GraphScope centralities) is the main functional decision beyond the two hard blockers.
