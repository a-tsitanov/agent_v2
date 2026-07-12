# Nebula analytics Tier-A port (nGQL) — remaining primitive families

**Status:** proposed (autonomous, delegated — user chose "port analytics to nGQL"; `connections` shipped as slice 1) 2026-07-11. NebulaGraph migration. Branch `feat/nebula-analytics-tier-a` off `main` (base `530ea96`, which is `ed62455` + the rules doc).

## Goal

Under `GRAPH_BACKEND=nebula`, port the remaining Tier-A analytics primitive families so they return real rows instead of failing-open to `[]`. Follows the `AnalyticsGraphOps` seam pattern established by `connections` (Protocol + Neo4j-verbatim impl + Nebula-nGQL impl + `build_*` dispatch), one **sibling seam per primitive family file**. Neo4j path byte-for-byte unchanged.

The nGQL is not guesswork: `docs/superpowers/nebula-analytics-ngql-rules-2026-07-11.md` records the cluster-proven translation rules and the exact schema. nebula 3.8's `MATCH` subset supports aggregation + variable-length paths, so these port near-verbatim (unlike `connections`, which needed GO/FETCH point-lookups).

## Background (grounded)

Every primitive calls `run_rows(store, cypher, params)` (`analytics/store_query.py`, fail-soft `try/except → []`, `await asyncio.to_thread`). Under nebula the raw Cypher raises → `[]`. The `connections` slice replaced those calls with `build_analytics_graph_ops(store).<method>(...)` wrapped in `asyncio.to_thread`. This slice does the same for the remaining families.

### Viability classification (from live schema inspection)

**Portable now** (columns/edges exist):
- `aggregations.py` (entity/edge counts, degree distributions, rel-type distribution, graph summary)
- `quality.py` (duplicate edges, orphan/dangling entities, missing/expected identifiers)
- `domain.py` (issue/resolution stats, communication patterns)
- `events.py` (new-since entities/edges, entity event timeline)
- `events_llm.py` (event core/actors, participation, trending)
- `dynamics.py` — 3 of 4 (relationship growth/velocity, edge churn); `topic_momentum` is Chunk-dependent → `[]`
- `rollups.py` (amount rollups by label-target neighbor)
- `signals.py` — 3 of 5 (entities-missing-identifiers, org neighborhood, ownership cycle via var-len); `top_risk` + `risk_clusters` need `Entity.risk_score` → `[]`
- `communities.py` (list by level, entity communities — Community tag + IN_COMMUNITY exist)

**Blocked → `[]` (documented)** — the primitive's data is materialized by a Tier-B compute stage that has not been ported to nebula. These already return `[]` today (fail-soft); this slice makes that explicit + documented rather than "portable":
- `signals.top_risk`, `signals.risk_clusters` — need `Entity.risk_score`
- `centrality.top_by_metric` — need centrality-metric columns
- `centrality.entity_resolution_candidates` — need `LIKELY_LINK` edge
- `cooccurrence`, `dynamics.topic_momentum` — need `Chunk` tag / MENTIONS-from-Chunk
- `alerts.py` — TBD during its task (composes over other primitives; verify it needs no new graph read)

This slice does NOT build the Tier-B compute stages or add schema columns — that is separate, deferred work. Porting the query layer for the blocked primitives without the compute would be dead code.

## Global Constraints

- **Default neo4j path byte-for-byte unchanged.** neo4j issues the SAME Cypher + params. Nebula only under `GRAPH_BACKEND=nebula`.
- **All nGQL follows `docs/superpowers/nebula-analytics-ngql-rules-2026-07-11.md` verbatim** — the 10 proven rules (label scan, tag-qualified props, `label ==`/`NOT IN`, `type(r)`→`r.rel_type`, **ORDER BY aliased column only**, MATCH aggregation, two-MATCH patterns, var-len `all()`, OPTIONAL MATCH, inline `_q`/`entity_vid` no param_map).
- **Row-shape parity:** each nebula method returns rows with the SAME keys the neo4j Cypher `RETURN`s. The primitive's downstream mapping stays unchanged.
- **`asyncio.to_thread`:** every seam call from a primitive is `await asyncio.to_thread(ops.method, ...)` (preserves `run_rows`'s off-event-loop behaviour; the seam methods are sync + fail-soft). This was an Important fix on `connections` — do not regress it.
- **Fail-soft:** each seam method wraps its query so a backend/schema error → `[]` (same `try/except → []` contract as `run_rows`), matching `connections`' `_rows()` / `_nebula_fail_soft`.
- **Blocked primitives:** nebula impl returns `[]` with a one-line comment naming the missing column/edge + the Tier-B stage that would fill it. Do NOT add schema columns or compute.
- Local commits only (**no push until FULL migration**). Never stage `docs/bruno/collection.bru`. Unit tests DB-free (fake store asserting the emitted nGQL/Cypher + params). Each ported file gets a live-verify on the running `kb` space (seed → assert non-empty sane rows same keys → `DELETE VERTEX ... WITH EDGE`).

## Design

### Seam per family (sibling to `AnalyticsGraphOps`)
Each primitive file gets a `<Family>GraphOps` seam in `src/graph/<family>_graph_ops.py`:
- `Neo4j<Family>GraphOps(store)` — each method runs the current Cypher verbatim (constants move here), fail-soft `_rows()`.
- `Nebula<Family>GraphOps(store)` — nGQL per the rules doc; blocked methods → `[]` + comment.
- `build_<family>_graph_ops(store)`: `settings.graph.backend == "nebula"` → Nebula else Neo4j.
This mirrors `src/graph/analytics_graph_ops.py` exactly. Keeping one seam file per primitive family (rather than one 40-method Protocol) keeps files focused and reviewable.

### Integration (each primitive file)
Replace each `run_rows(store, <cypher>, params)` with
`await asyncio.to_thread(build_<family>_graph_ops(store).<method>, ...args)`.
Move the Cypher constants into the Neo4j impl. Downstream row-mapping unchanged. Keep the `PrimitiveResult(cypher=..., params=..., rows=...)` shape (the `cypher` field becomes the seam-method label, as in `connections`).

### Tasks (one per family file — reviewable independently)
1. `aggregations.py` + `AggregationsGraphOps` (flagship: pure MATCH+agg)
2. `quality.py` + `QualityGraphOps` (two-MATCH duplicate edges, orphan checks)
3. `domain.py` + `DomainGraphOps`
4. `events.py` + `EventsGraphOps` (created_at scans)
5. `events_llm.py` + `EventsLlmGraphOps`
6. `dynamics.py` + `DynamicsGraphOps` (topic_momentum → [])
7. `rollups.py` + `RollupsGraphOps`
8. `signals.py` + `SignalsGraphOps` (3 viable; top_risk/risk_clusters → [])
9. `communities.py` + `CommunitiesGraphOps`
10. `centrality.py` + `CentralityGraphOps` (mostly → [] + document) & `alerts.py` (verify/port)

Order runs simplest-reuse-first (aggregations proves the MATCH pattern) → blocked-heavy last.

## REVISED classification (post file-by-file inspection, 2026-07-11)

Reading each primitive against the live nebula schema sharpened the plan. **Guiding principle:** a family where EVERY primitive is blocked already returns `[]` under nebula via `run_rows`'s fail-soft — building a seam that also returns `[]` is dead code. So **build a seam only for families with ≥1 portable primitive; document+skip fully-blocked families.**

**BUILD seam (≥1 portable):**
- `aggregations` ✅ DONE (7 portable)
- `quality` ✅ DONE (4 portable)
- `domain` — 2 portable (issue_resolution_stats via 2 queries + Python two-level agg; communication_stats via MATCH rel_type IN + undirected dedup)
- `events` — `new_events` PARTIAL (new **entities** portable via `e.created_at`; new **edges** → `[]` — RELATED has no `created_at`/`first_doc_id`, the deferred REL-first-seen gap); `entity_new_connections` → `[]` (same REL-first-seen)
- `rollups` — 1 portable (numeric_rollup: MATCH `label=='Amount'` neighbor, Python parse/agg in the primitive)
- `signals` — 3 portable (recommended_merges = name-group; review_queue = shell-org via query+Python; circular_ownership = var-len OWNS cycle); `risk_score`+`investigate_next` → `[]` (no `risk_score`/`risk_band`/`completeness_score` columns)
- `communities` — 2 portable (community_overview on Community tag; entity_communities via IN_COMMUNITY); `personalized_pagerank` → `[]` (GDS compute, Tier-B, degrades via analysis fail-open)

**SKIP + document (all primitives blocked → already `[]` via run_rows fail-soft):**
- `events_llm` — event columns (`event_type`/`event_ts_raw`/`event_start_epoch`/…/`polarity`) absent from the nebula Entity schema (E2 event-timeframe ingest feature not ported to nebula)
- `dynamics` — `valid_from`/`valid_to` are ISO-**strings** in neo4j (Cypher does `substring(r.valid_from,0,7)` + string range compares) but **int64** in nebula: a representation divergence, not a translation; plus RELATED has no `created_at` (whats_changed) and topic_trend/entity_activity are Chunk-dependent. Needs a dedicated temporal-dynamics slice once REL temporal semantics settle under nebula.
- `centrality` — `pagerank`/`betweenness`/`eigenvector` columns + `LIKELY_LINK` edge absent (Tier-B centrality materialize)
- `alerts` — no `:Alert` tag in nebula + Arc-2 monitor not writing alerts under nebula

This revision means **5 more seams** (domain, events, rollups, signals, communities), not 10, plus 4 documented-skipped families — a more honest scope: the skipped families genuinely need separate tracks (schema columns, ingest features, or distributed compute), not query translation.

## Out of scope (deferred)
- Tier-B compute stages under nebula (risk-scoring, centrality/materialize, communities distributed) — separate track; until then the blocked primitives stay `[]`.
- Adding `risk_score` / centrality-metric columns or `LIKELY_LINK`/`Chunk` to the nebula schema.
- `cooccurrence` / `topic_momentum` under nebula (Chunk-dependent).

## Interfaces produced
- `src/graph/<family>_graph_ops.py` per family: `<Family>GraphOps` Protocol + Neo4j/Nebula impls + `build_<family>_graph_ops`.
- Each `src/analytics/primitives/<family>.py` routes through its seam via `asyncio.to_thread`.
