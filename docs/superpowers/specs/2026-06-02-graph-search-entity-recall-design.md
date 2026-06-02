# Graph-search entity recall — design

**Date:** 2026-06-02
**Status:** approved (design), pending implementation plan

## Goal

Fix poor graph-search recall on large graphs: a query naming an entity
(e.g. "Иванов") must reliably surface that entity even when the stored
name is longer/partial ("Иванов Иван Иванович"), **transparently in the
main `/search/*` path**. Also lift general semantic recall.

## Root cause (confirmed)

- The wired graph retrievers are LlamaIndex `LLMSynonymRetriever` (LLM
  keywords → **exact** match on entity `name`/`id`) + `VectorContextRetriever`
  (embedding similarity). The synonym path misses partial names; the
  vector path ranks a bare surname outside `similarity_top_k` (default 10)
  on a large graph.
- There is **no full-text index** on `:__Entity__(name)`. Neo4j
  `:__Entity__` nodes carry only `id`/`name`/`label`/`description`
  (aliases live in Wikibase, not on the graph node).

## Approach (A + B)

**A — full-text name lookup** (the indexed, scalable fit for partial-name
recall) and **B — raise vector recall** (general recall). Exact-token
full-text by default. Out of scope (future, optional): fuzzy `~`,
alias-signal from Wikibase, hybrid+rerank, LLM query-time resolution.

## Components

### 1. Full-text index `entity_name_fulltext`

Idempotent DDL:
```cypher
CREATE FULLTEXT INDEX entity_name_fulltext IF NOT EXISTS
FOR (e:__Entity__) ON EACH [e.name]
```
A helper `ensure_entity_fulltext_index(store)` (in `src/graph/index.py`)
runs it. Called from BOTH:
- `build_property_graph` activity (covers new/ongoing ingest), and
- the `GraphRetriever` bootstrap / `_search_deps.get_graph_retriever`
  (covers **existing** large graphs with no new ingest — runs once,
  idempotent).

Neo4j's default `standard` analyzer lowercases + tokenises on whitespace,
so "Иванов Иван Иванович" → tokens `[иванов, иван, иванович]`; a query
token "иванов" matches.

### 2. `GraphRetriever.afind_entities_by_name(query, *, limit)`

New method on `src/graph/retriever.py`:
```cypher
CALL db.index.fulltext.queryNodes('entity_name_fulltext', $lucene)
YIELD node, score
WHERE node:__Entity__
RETURN node.name AS name,
       [l IN labels(node) WHERE l <> '__Entity__' AND l <> '__Node__'] AS labels,
       coalesce(node.description, '') AS description
ORDER BY score DESC
LIMIT $limit
```
- `$lucene` built by a **pure** helper `build_fulltext_query(query) -> str`:
  tokenise the query, escape Lucene special chars
  (`+ - && || ! ( ) { } [ ] ^ " ~ * ? : \ /`), join with ` OR `. Exact
  tokens (no `~`). Empty/blank query → returns `""` and the method
  short-circuits to empty.
- Returns `RoundGraphData` (entities only) — same shape as `aretrieve`.
- **Fail-open**: missing index / no store / any Cypher error → empty
  `RoundGraphData` (degrades to current behaviour). `limit` defaults to
  the retriever's `similarity_top_k`.

### 3. `find_entity_by_name` atomic tool + retrieve-path wiring

- New async function in `src/retrieval/atomic_tools.py`
  `find_entity_by_name(graph_retriever, query, *, ...)` mirroring
  `graph_search` (returns a `ToolResult` with `sources` + JSON
  `observation`); calls `graph_retriever.afind_entities_by_name`. Empty
  for a `None` retriever. Registered in the tool dispatch table.
- `src/workflow/search/activities/retrieve.py`: add it to the pipeline —
  `_PIPELINE = ("vector_search", "graph_search", "find_entity_by_name")`
  and `ALLOWED_TOOLS`. Its entities merge into the graph results
  (dedupe by name). The existing R3b auto-seed of `graph_walk` may seed
  from the fulltext-found top entity when `graph_search` returned none,
  so the matched person's neighbourhood/chunks are pulled in.

### 4. Configurable vector recall (B)

- New `AgentSettings.graph_similarity_top_k: int = 20` (env
  `AGENT_GRAPH_SIMILARITY_TOP_K`).
- `src/workflow/_search_deps.py:get_graph_retriever` passes it:
  `GraphRetriever(pg, similarity_top_k=settings.agent.graph_similarity_top_k)`.
  (Default 10→20 lifts the VectorContextRetriever candidate count.)

## Error handling

Everything fail-open: no index / no APOC / store down / blank query →
empty result; the rest of the search proceeds. Index creation is
idempotent (`IF NOT EXISTS`), safe to call repeatedly and concurrently.

## Testing

- Pure `build_fulltext_query`: tokenisation, Lucene special-char escaping,
  `OR` join, blank → `""`.
- `afind_entities_by_name` with a stub store: rows → entities; store
  `None` / `structured_query` raises → empty.
- `find_entity_by_name` atomic tool: `None` retriever → empty; stub
  retriever → `ToolResult` with sources/observation.
- `ensure_entity_fulltext_index`: asserts the idempotent `IF NOT EXISTS`
  Cypher constant; called with a stub store.
- retrieve-path: `find_entity_by_name` is in `_PIPELINE` and its entities
  merge (stub retriever).
- `get_graph_retriever` uses `settings.agent.graph_similarity_top_k`.

## Operational

After deploy: the index is created on the next ingest AND on first
graph-retriever build (existing graphs covered, no manual step needed).
A worker/API restart picks up the new config + pipeline.

## Out of scope (future)

Fuzzy `~` matching, Wikibase alias-signal mirrored to the graph, full
hybrid + rerank of entity candidates, LLM query-time entity resolution.
