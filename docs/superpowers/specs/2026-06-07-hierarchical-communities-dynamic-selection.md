# Hierarchical communities + dynamic community selection — design

**Status:** spec for review (supersedes backlog items 10 & 11).
**Tracks:** GraphRAG-parity #2 (hierarchical communities + reports) and #1 (dynamic community selection).

## Problem (measured)

Global/drift search today is weak and expensive at scale:

- **Flat communities.** `detect_communities` runs a single `gds.leiden.stream` and materialises one level (`level=0`) of `:Community` nodes (`communities.py:85-90`, `CommunityRef.level` defaults 0). No multi-resolution view: a 250k-entity graph becomes ~10–20k tiny communities and nothing coarser, so "big picture" questions have no coarse summary to read.
- **Short summaries, not reports.** Each community gets a 3–5 sentence `c.summary` (`community.py:_WRITE_SUMMARY_CYPHER`). No structured findings / importance ranking.
- **Lexical, O(N) selection.** `global_search.rank_summaries` reads **all** summaries and ranks by Python word-overlap (`global_search.py:_READ_SUMMARIES_CYPHER` + the lexical loop), capped at `global_max_communities=20`. At 10–20k communities this is slow and semantically blind ("GPU" ≠ "видеокарта"). GraphRAG's dynamic selection cut global token cost ~77% by replacing this.
- **Full re-summarisation every build.** `CommunityBuildWorkflow` summarises every detected community each run; communities go stale immediately after ingest (old backlog item 11).

## Goals

1. Build a **community hierarchy** (multiple Leiden levels) from a single GDS run.
2. Generate **structured community reports** (title + summary + ranked findings), bottom-up, **incrementally** (skip unchanged communities).
3. **Dynamic selection** for global/drift: pick relevant communities by semantic relevance + hierarchy descent instead of flat lexical scan.
4. Fully **opt-in / fail-open / backward-compatible**: with the feature off, today's flat level-0 + lexical path is byte-for-byte unchanged.

Non-goals: changing local search; changing extraction; replacing Leiden with another algorithm.

## Approach

### A. Hierarchy from one Leiden run (`#2`)

GDS 2.x `gds.leiden.stream(g, {includeIntermediateCommunities: true})` yields `intermediateCommunityIds` — a list per node, one community id per dendrogram iteration (finest → coarsest). One run gives every level; no multiple-resolution sweeps needed (confirmed: Neo4j 5-enterprise + GDS plugin in `docker-compose.yml`).

- **Level numbering:** level 0 = finest (most communities, current behaviour), increasing = coarser. We cap at `community_max_levels` (e.g. 3) by taking the first K distinct dendrogram columns; the coarsest cap is the last column.
- **Materialise** `:Community {id, level, member_count, members_hash}` per (level, community) and a `(:Community)-[:PARENT_OF]->(:Community)` edge from level L+1 (coarser) to level L (finer), derived from the dendrogram (a finer community's nodes all share one coarser id). Level-0 communities keep `(:__Entity__)-[:IN_COMMUNITY]->(:Community {level:0})` exactly as today; higher levels link to their child communities, not to entities.
- **Decision:** keep `detect_communities(level=...)` callable for a single level (back-compat); add `detect_hierarchy(max_levels)` that does the intermediate-communities run and the parent wiring. When `community_max_levels == 1`, `detect_hierarchy` degrades to today's single-level behaviour.

### B. Structured reports, bottom-up + incremental (`#2`)

- **Report shape:** `{title, summary, findings: [{statement, importance}]}` stored as `c.report` (JSON string), with `c.title` + `c.summary` kept as plain columns (summary stays the lexical-ranking fallback text and the embedding source).
- **Bottom-up generation:** level-0 reports from member entities + inter-member relations (today's `_MEMBER_CONTEXT_CYPHER`); level L+1 reports synthesised from **child reports** (cheaper than re-reading all members, and the GraphRAG approach). One small-tier LLM call per community.
- **Incremental (folds in old item 11):** before the build prunes the prior level, read `{(level, members_hash) → report}`. A community whose `members_hash` (sha256 of sorted member ids/names) is unchanged **carries over** its report — no LLM call. Only changed/new communities are (re)summarised. Expected: a rebuild after a small ingest re-reports O(changed), not O(all).
- **Report embeddings:** embed each report's title+summary; store as a native `c.report_vec` list + a `community_report_vec` vector index (same pattern as the ER `er_vec` index we shipped). This powers semantic selection (C).

### C. Dynamic selection for global/drift (`#1`)

Replace the flat lexical scan in `global_search`:

- **v1 (semantic flat):** embed the query; kNN over `community_report_vec` at the working level to pick the top-`global_max_communities` reports. Drop-in replacement for `rank_summaries`; immediately removes the O(N) Python word-overlap and the "GPU"≠"видеокарта" miss. Low risk.
- **v2 (hierarchy descent):** start at the coarsest level, rate each community's relevance (cheap-LLM yes/no or a similarity threshold), keep relevant, descend via `PARENT_OF` into their children, repeat to level 0 or a node budget. Only relevant leaf reports enter the existing MAP→REDUCE. This is the −77%-token GraphRAG behaviour; built on top of v1's index.
- **Decision:** ship v1 first (semantic ranking), then v2 (descent) as a follow-up flag. Both behind `community_dynamic_selection` (off → today's lexical path).

### Data flow

```
BUILD (offline, kb-graph-build queue):
  Leiden(includeIntermediateCommunities) ──► per-level communities + PARENT_OF
        │
        ├─ read old {(level,hash)→report}      (incremental)
        ├─ reports bottom-up, skip unchanged   (small LLM / carry-over)
        └─ embed reports ──► community_report_vec index

QUERY (global / drift):
  embed(query) ──► dynamic selection (v1 kNN | v2 descent) over report index
        └─► selected reports ──► MAP (per-community partial) ──► REDUCE (synthesis)
```

### Components touched

| File | Change |
|---|---|
| `src/graph/communities.py` | `detect_hierarchy` (intermediate communities, per-level materialise, PARENT_OF, members_hash); read-old-reports; carry-over MERGE |
| `src/workflow/search/activities/community.py` | report-shaped prompt (title/summary/findings) replacing summary; bottom-up from child reports; write `report`/`title`/`report_vec` |
| `src/workflow/search/community_wf.py` | drive hierarchy build level-by-level; only summarise `needs_report` communities |
| `src/workflow/search/activities/global_search.py` | dynamic selection (v1 kNN over report index; v2 descent) behind flag |
| `src/graph/index.py` | `ensure_community_report_vector_index` (+ the level/doc_id indexes from the 8/13/12 plan) |
| `src/workflow/contracts.py` | `CommunityRef` gains `level`, `parent_id`, `members_hash`, `needs_report`; report dataclasses |
| `src/config.py` | `community_max_levels`, `community_dynamic_selection`, report knobs |

### Error handling / back-compat

- No `includeIntermediateCommunities` support / GDS error → fall back to single-level detection (today's path).
- `community_max_levels == 1` and `community_dynamic_selection == False` → behaviour identical to today (default until verified).
- No report index / embed failure → dynamic selection falls back to lexical `rank_summaries`.
- Report carry-over failure → re-summarise (never lose a community's report).

### Testing

- Unit: dendrogram → per-level community mapping + PARENT_OF wiring (synthetic `intermediateCommunityIds`); `members_hash` order-insensitive/stable; report carry-over skips unchanged; v1 selection picks nearest report (mock embeddings); v2 descent keeps-relevant/prunes (mock ratings); all fallbacks fail-open.
- Live smoke (local Neo4j + GDS): build hierarchy on the 156-entity dev graph, assert ≥2 levels + PARENT_OF edges + report_vec populated; rebuild with no change → reports carried over (LLM calls ≈ 0); a global query returns selected reports. Isolated label cleanup, never touch prod.
- Extend `tests/eval/scale/` with a community-count/level probe so hierarchy size is measurable on a synthetic graph.

## Open questions (decide before / during plan)

1. **Levels:** auto-take all dendrogram columns, or cap at `community_max_levels` (default 3)? (Recommend cap — bounded reports/cost.)
2. **Selection v1 vs v2 in first cut:** ship v1 (semantic kNN) only, v2 (descent) next? (Recommend yes — v1 is the cheap, high-value win.)
3. **Higher-level reports:** synthesise from child reports (cheap) vs from all members (faithful but costly)? (Recommend child reports.)
4. **Keep flat lexical global** as a permanent fallback mode, or remove once v1 is verified? (Recommend keep as fallback.)
