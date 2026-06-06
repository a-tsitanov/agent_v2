# Continuous Wiki Article Editor — Design

**Date:** 2026-06-06
**Status:** Draft (pending review)
**Goal:** A continuously-running editor that turns Neo4j graph entities into well-written, interlinked MediaWiki articles — regenerating each entity's bot-managed section from the graph (grounded, cited, drift-free) while preserving human edits.

---

## 1. Motivation

The graph (Neo4j) holds rich, deduplicated entities + relations, but it is not human-readable prose. We want a living encyclopedia: each entity gets a MediaWiki article that summarizes what the graph knows about it, links to related entities, and cites sources — kept current as ingestion changes the graph, without burning LLM budget on unchanged entities and without a model slowly corrupting facts over time.

This is **Project A** of two (the other, Project B, adds document type + authority weighting; independent, deferred). The existing `push_wikibase` activity already projects entities into Wikibase as structured Items + statements and stores the resulting QID back on each Neo4j entity (`wikibase_qid`). This project adds a **generative prose layer** (MediaWiki pages) on top, linked to those Items.

---

## 2. Locked decisions (from brainstorming)

| # | Decision |
|---|----------|
| D1 | **Output = a real MediaWiki page** (wikitext), linked to the entity's Wikibase Item via sitelink. Needs a MediaWiki Action API client (new). |
| D2 | **Article unit = per entity** (one `__Entity__` ↔ one page, 1:1). |
| D3 | **Trigger = hybrid**: ingest marks touched entities *dirty*; a Temporal Schedule sweeps dirty entities periodically. |
| D4 | **Update = full rewrite of a bot-managed section from the graph** (between markers), preserving the human section. **No drift**: the prior prose is NEVER fed to the LLM — generation is grounded only in the graph + cited source snippets. |
| D5 | **Approach A1**: Temporal-native (new `kb-wiki` queue + `WikiSweepWorkflow`); reuse LLMPool synthesis lane, Neo4j store, the R6 community-build queue+Schedule pattern. |
| D6 | **Scope**: search/authority weighting is Project B, out of scope here. Co-editing (humans + bot) is supported via the bot-section markers. |

---

## 3. Architecture & data flow

```
DocumentIngestWorkflow (after graph build, if WIKI_ENABLED)
  └─ mark_entities_dirty(names)  →  Neo4j: e.wiki_dirty=true, e.wiki_dirty_at=now
       names = this doc's merged entities + both endpoints of each written relation

Temporal Schedule (every WIKI_SWEEP_INTERVAL_MINUTES)  →  WikiSweepWorkflow
  ├─ select_dirty_entities(limit=WIKI_SWEEP_BATCH)  →  list[name]
  └─ fan out  write_entity_article(name)  (bounded by Temporal cap + synthesis lane):
       1. ctx = read_entity_subgraph(name)        # neighbors, relations+desc, qid, page_title
       2. cites = top-K (:Chunk)-[:MENTIONS]->(e) # source snippets
       3. h = subgraph_hash(ctx)
       4. if h == e.wiki_hash:  clear dirty; return SKIPPED   # no LLM, no edit
       5. bot_md = render_bot_section(ctx, cites, llm=pool.get("synthesis"))
       6. page  = get_page(title); new = splice_bot_section(page, bot_md)
       7. if new != page:  edit page (CSRF, bot=1)            # else no-op
       8. ensure_sitelink(e.wikibase_qid, title)              # if qid present
       9. Neo4j: e.wiki_dirty=false, e.wiki_hash=h, e.wiki_synced_at=now
```

### New components (one responsibility each)

- **`src/storage/mediawiki.py`** — `AsyncMediaWiki`: `login` (Action API, bot creds), `get_page(title)`, `upsert_page(title, wikitext, summary)` (CSRF token, `action=edit`, `bot=1`, editconflict retry), `ensure_sitelink(qid, title)`. Sync MW client wrapped in `asyncio.to_thread`, mirroring `AsyncWikibase`.
- **`src/graph/wiki_dirty.py`** — pure Cypher helpers: `mark_dirty(store, names)`, `select_dirty(store, limit)`, `clear_dirty(store, name, hash)`.
- **`src/graph/wiki_context.py`** — `read_entity_subgraph(store, name) -> EntityContext`, `read_citations(store, name, k)`, `subgraph_hash(ctx) -> str`.
- **`src/workflow/wiki/article.py`** — `render_bot_section(ctx, citations, llm) -> str`, `splice_bot_section(existing_wikitext, bot_md) -> str`.
- **`src/workflow/wiki/wiki_sweep.py`** — `WikiSweepWorkflow` + activities `select_dirty_entities`, `write_entity_article`.
- **`src/workflow/activities/mark_dirty.py`** — `mark_entities_dirty` activity (MAIN queue; called from ingest).
- **Hook** in `src/workflow/document_ingest.py` — after graph build, best-effort `mark_entities_dirty` when `WIKI_ENABLED`.
- **Admin route** `POST /admin/wiki/rebuild` (`?all=true` to mark every entity dirty).
- **`scripts/setup_wiki_schedule.py`** — idempotently create/update the Temporal Schedule.

Reused: `LLMPool` (`get("synthesis")`), `build_neo4j_graph_store`, the R6 community-build queue+Schedule pattern. No staging blobs.

### `EntityContext` shape
`name, label, description, wikibase_qid, page_title, relations: list[(rel_label, direction, neighbor_name, neighbor_label, rel_description)]`. Citations are fetched separately by `read_citations` (a `list[(text, doc_id)]`) and passed to `render_bot_section` alongside the context — they are NOT part of the hash (only graph facts are).

---

## 4. Dirty tracking & change detection

Neo4j `__Entity__` properties (alongside `wikibase_qid`): `wiki_dirty: bool`, `wiki_dirty_at: datetime`, `wiki_hash: str`, `wiki_synced_at: datetime`, `wiki_page_title: str`.

- **Mark dirty** (post-graph-build, from `Merged`): the doc's merged entity names **plus both endpoints of every written relation** — a new edge changes the neighbor's 1-hop subgraph too. 2-hop is not marked (its own 1-hop is unchanged).
- **`subgraph_hash(ctx)`** = sha256 over a canonical serialization: `name | label | description | sorted[(rel_label, direction, neighbor_name, rel_description)…]`. Fixed ordering → stable. Matching `wiki_hash` → SKIP (clear dirty, no LLM, no edit). This is the LLM-cost saver.
  - `description` is included (it's LLM-merged text; if it churns, a rewrite is correct since content changed). A structure-only hash is a documented toggle if churn proves noisy.
- **`select_dirty(store, limit)`**: `MATCH (e:__Entity__) WHERE e.wiki_dirty=true RETURN e.name ORDER BY e.wiki_dirty_at LIMIT $limit` — bounded fan-out per sweep.

---

## 5. Generation, citations, splice (anti-drift)

- **`render_bot_section(ctx, citations, llm)`** — `llm = pool.get("synthesis")` (large tier). Prompt: write a factual section about `<name>` using ONLY the supplied facts + source snippets; cite each statement `[doc_id]`; output MediaWiki markup; invent nothing. **The prior page prose is NOT an input** → no drift accumulation; citations make it verifiable. Output structure: lead sentence, facts as prose, a "Связи / Related" subsection with `[[wiki links]]` to neighbor pages (this builds the interlinked wiki), a references list. Entity names stay in the source language.
- **`splice_bot_section(existing_wikitext, bot_md)`** — markers `<!-- KB-BOT:START -->` … `<!-- KB-BOT:END -->`:
  - markers present → replace only the content between them (human content outside untouched);
  - markers absent (new or human-only page) → insert the bot section (with markers) at the top, human content below;
  - idempotent: splicing twice equals once.
- **Upsert** (`AsyncMediaWiki`): `get_page(title)` → splice → if `new == current` skip the edit (no spurious revision; second guard over hash-skip) → else `action=edit` with CSRF token, `bot=1`, summary "KB bot: updated from graph". Then `ensure_sitelink(wikibase_qid, title)` if a QID exists.

---

## 6. Queue, Schedule, config, admin

- **Queue/worker**: new `kb-wiki` queue; a worker pool in `worker.py` hosts `WikiSweepWorkflow` + `select_dirty_entities` + `write_entity_article`. `mark_entities_dirty` lives in `MAIN_ACTIVITIES`. Generation rides the synthesis lane; the `kb-wiki` Temporal cap is modest.
- **`WikiSettings` (env prefix `WIKI_`)**: `enabled=False`; `task_queue="kb-wiki"`; `activity_concurrency=4`; `sweep_batch=50`; `sweep_interval_minutes=15`; `citations_top_k=5`; `mediawiki_api_url=""` (default derived from `wikibase.base_url` + `/api.php`). MediaWiki login reuses `wikibase.bot_user` / `wikibase.bot_password`.
- **Temporal Schedule**: when `WIKI_ENABLED`, `scripts/setup_wiki_schedule.py` idempotently creates a Schedule starting `WikiSweepWorkflow` every `sweep_interval_minutes` (R6 pattern).
- **`WikiSweepWorkflow`**: `select_dirty_entities(limit)` → fan out `write_entity_article` (best-effort: one failure leaves that entity dirty + logs, never fails the sweep) → return counts `{written, skipped_unchanged, failed}`.
- **Admin** `POST /admin/wiki/rebuild` (`?all=true` marks every entity dirty for a full rebuild), mirroring `/admin/communities/rebuild`.
- **Ingest hook**: `mark_entities_dirty` after graph build, only if `WIKI_ENABLED`, best-effort (never fails ingest — graph is already committed).

---

## 7. Error handling, idempotency, testing

### Idempotency / errors
- `write_entity_article` is idempotent (hash-skip or deterministic splice) → safe to retry.
- MediaWiki failures (expired login, network, editconflict): retry in-activity with a capped Temporal retry policy; on exhaustion leave the entity dirty + log → next sweep retries. The sweep never fails wholesale.
- Edit conflict: `get_page` is fetched immediately before `edit` (small window); on `editconflict`, re-fetch + re-splice + retry once.
- `mark_entities_dirty` and the ingest hook are best-effort (never fail ingest).
- No `wikibase_qid` (Wikibase push off/pending): the page is still created by name; sitelink is skipped. The wiki works without the structured push.
- Title collisions (two entities, same canonical name): names are post-ER canonical (ER dedups); residual collisions are a noted future disambiguation (append QID to title). Default title = name.
- `WIKI_ENABLED=false`: hook is a no-op, Schedule not created, sweep returns skipped.

### Testing (pure units are the focus — repo workflow tests cover only pure helpers)
1. `subgraph_hash`: deterministic; changes when a relation/description changes; stable under input reordering.
2. `splice_bot_section`: inserts markers on a marker-less page (human content preserved); replaces only between markers; idempotent (twice == once).
3. `render_bot_section` (mock LLM): the prompt includes facts + citations and does NOT include any prior prose (anti-drift); output contains `[[wiki links]]` to neighbors and `[doc_id]` citations.
4. dirty / subgraph Cypher helpers: against a mock `structured_query` — correct Cypher + parsing.
5. `AsyncMediaWiki` (mock HTTP): login→token→edit sequence; editconflict retry; no-op when text unchanged.
6. `WikiSweepWorkflow` fan-out logic with mocked activities: best-effort on a failing entity; correct counts.

### Rollout
`WIKI_ENABLED=false` by default. In dev: enable → ingest → entities marked dirty → `POST /admin/wiki/rebuild` → verify a page appears with bot section, `[doc_id]` citations, `[[wiki links]]`; a manual human edit outside the markers survives a re-run. Live e2e later (as done for the LLM pool).

---

## 8. Out of scope (YAGNI)

Per-community / topic articles (D2 chose per-entity; could reuse R6 later); incremental LLM merge (drift risk — rejected per D4); document authority weighting (Project B); title disambiguation beyond canonical name; non-MediaWiki output targets.
