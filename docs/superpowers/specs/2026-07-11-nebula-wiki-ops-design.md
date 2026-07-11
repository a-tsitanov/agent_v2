# Nebula wiki-editor graph ops — the cutover CRASH-set port

**Status:** proposed (autonomous, delegated — user chose "перенести") 2026-07-11. NebulaGraph migration. Ports the ONE subsystem whose raw Cypher CRASHES (not fail-opens) under `GRAPH_BACKEND=nebula` — the pre-cutover hard blocker #2. Branch `feat/nebula-wiki-ops` off `main` (base `be6b5bb`).

## Goal

Under `GRAPH_BACKEND=nebula`, the wiki-editor graph ops run via nGQL so `WikiSweepWorkflow` and the admin rebuild endpoint no longer crash: dirty-flag bookkeeping (mark/select/clear), the article-context subgraph read, the sweep's inline hash-check/title-write, and admin mark-all-dirty. Chunk-dependent reads (citations, source-docs) return EMPTY under nebula (chunks aren't nebula graph nodes — same limit as doc↔community; article gets no citations under nebula but does NOT crash). Neo4j path byte-for-byte unchanged.

## Background (grounded)

Raw Cypher + non-empty `param_map` → RAISES under nebula. The crash-set (from the cutover audit):
- `graph/wiki_dirty.py`: `_MARK` (`UNWIND $names MATCH (e {name}) SET e.wiki_dirty=true, e.wiki_dirty_at=datetime()`), `_SELECT` (`MATCH (e) WHERE e.wiki_dirty=true RETURN e.name ORDER BY e.wiki_dirty_at LIMIT $limit`), `_CLEAR` (`MATCH (e {name}) SET e.wiki_dirty=false, e.wiki_hash=$hash, e.wiki_synced_at=datetime()`). No try/except → `WikiSweepWorkflow.select_dirty_entities` crashes the workflow.
- `graph/wiki_context.py`: `_SUBGRAPH_CYPHER` (entity + its `RELATED` neighbours + `wikibase_qid` + `wiki_page_title`), `_CITATIONS_CYPHER` + `_SOURCE_DOCS_CYPHER` (both `MATCH (c:Chunk)-[:MENTIONS]->(e)` — **Chunk-dependent**). No try/except → `write_entity_article` fails per-entity.
- `wiki_sweep.py` inline: hash-check `MATCH (e {name:$n}) RETURN coalesce(e.wiki_hash,'')`, title-write `MATCH (e {name:$n}) SET e.wiki_page_title=$t`.
- `api/routes/admin.py POST /admin/wiki/rebuild?all=true`: `MATCH (e:__Entity__) SET e.wiki_dirty=true, e.wiki_dirty_at=datetime()` (mark ALL) — unwrapped → 500.

Nebula Entity TAG lacks ALL wiki columns. `datetime()` → `int(time.time()*1000)` under nebula (ordering by it works). Writes must PRESERVE other Entity columns → `UPDATE VERTEX` (not INSERT, which resets — cluster-verified).

## Global Constraints

- **Default neo4j path byte-for-byte unchanged.** neo4j issues the SAME Cypher + params. Nebula only under `GRAPH_BACKEND=nebula`.
- Local commits only (**no push until FULL migration**). Never stage `docs/bruno/collection.bru`. Unit tests DB-free. Nebula inline nGQL (no param_map); `_q`/`entity_vid`; `UPDATE VERTEX` for writes (preserve other cols).
- Chunk-dependent reads (citations, source-docs) are OUT OF SCOPE under nebula (return `[]`) — same deferral as doc↔community.

## Design

### 1. Schema (`nebula_schema.py`)
`Entity` TAG gains 6 wiki columns: `wiki_dirty bool DEFAULT false`, `wiki_dirty_at int DEFAULT 0`, `wiki_hash string DEFAULT ''`, `wiki_synced_at int DEFAULT 0`, `wiki_page_title string DEFAULT ''`, `wikibase_qid string DEFAULT ''` (CREATE for fresh + best-effort `ALTER TAG` for existing + extend the Entity probe INSERT to include them). NEW index `CREATE TAG INDEX \`entity_wiki_dirty_idx\` ON \`Entity\`(wiki_dirty)` (backs the select-dirty LOOKUP).

### 2. Seam: `WikiGraphOps` (`src/graph/wiki_graph_ops.py`, new)
```python
class WikiGraphOps(Protocol):
    def mark_dirty(self, names: list[str]) -> None: ...
    def select_dirty(self, limit: int) -> list[str]: ...
    def clear_dirty(self, name: str, digest: str) -> None: ...
    def mark_all_dirty(self) -> None: ...
    def read_subgraph(self, name: str, max_relations: int) -> list[dict]: ...  # rows shaped as _SUBGRAPH_CYPHER
    def read_citations(self, name: str, k: int) -> list[dict]: ...            # nebula -> []
    def read_source_docs(self, name: str) -> list[str]: ...                   # nebula -> []
    def read_wiki_hash(self, name: str) -> str: ...                           # sweep hash-check
    def write_page_title(self, name: str, title: str) -> None: ...           # sweep title-write
```
- **`Neo4jWikiGraphOps(store)`** — the existing Cypher verbatim (`_MARK`/`_SELECT`/`_CLEAR`/`_SUBGRAPH_CYPHER`/`_CITATIONS_CYPHER`/`_SOURCE_DOCS_CYPHER` + the two inline sweep Cypher + the admin mark-all Cypher, moved here). Byte-for-byte.
- **`NebulaWikiGraphOps(store)`** — nGQL:
  - `mark_dirty(names)`: per-name `UPDATE VERTEX ON \`Entity\` "<vid>" SET wiki_dirty = true, wiki_dirty_at = <now_ms>;`.
  - `select_dirty(limit)`: `LOOKUP ON \`Entity\` WHERE \`Entity\`.wiki_dirty == true YIELD id(vertex) AS vid, \`Entity\`.name AS name, \`Entity\`.wiki_dirty_at AS at | ORDER BY $-.at ASC | LIMIT <limit>;` → return names. (Index-backed.)
  - `clear_dirty(name, digest)`: `UPDATE VERTEX ON \`Entity\` "<vid>" SET wiki_dirty = false, wiki_hash = <_q digest>, wiki_synced_at = <now_ms>;`.
  - `mark_all_dirty()`: `LOOKUP ON \`Entity\` WHERE \`Entity\`.wiki_dirty != true YIELD id(vertex) AS vid` (or all) → per-vid `UPDATE VERTEX ... SET wiki_dirty=true, wiki_dirty_at=<now>`. (Expensive — admin-only, rare; note the per-vertex cost.)
  - `read_subgraph(name, max_relations)`: `GO FROM "<vid>" OVER \`RELATED\` BIDIRECT YIELD ...` for neighbours + `FETCH PROP ON \`Entity\` "<vid>"` for the entity's own props (name/label/description/wikibase_qid/wiki_page_title); assemble the same row shape `_SUBGRAPH_CYPHER` returns (`relations` list of `{rl,dir,nn,nl,rd}`, capped at max_relations, ordered by neighbour mention_count desc). `rd` (relation description) = `''` under nebula (RELATED has no description column — degraded).
  - `read_citations` / `read_source_docs`: return `[]` (Chunk-dependent, deferred). Log once at debug.
  - `read_wiki_hash(name)`: `FETCH PROP ON \`Entity\` "<vid>" YIELD \`Entity\`.wiki_hash AS h;` → the hash or `''`.
  - `write_page_title(name, title)`: `UPDATE VERTEX ON \`Entity\` "<vid>" SET wiki_page_title = <_q title>;`.
- `build_wiki_graph_ops(store)`: `settings.graph.backend == "nebula"` → Nebula; else Neo4j.

### 3. Integration
- `wiki_dirty.py`: `mark_dirty`/`select_dirty`/`clear_dirty` route through `build_wiki_graph_ops(store)`. Move the 3 constants into the neo4j impl.
- `wiki_context.py`: `read_entity_subgraph`/`read_citations`/`read_source_docs` route through the seam (map the seam's `read_subgraph` rows into `EntityContext` as today). `read_entity_subgraph` still raises `ValueError("entity not found")` when the seam returns no entity row (preserve).
- `wiki_sweep.py`: the two inline `structured_query` calls → `ops.read_wiki_hash(name)` / `ops.write_page_title(name, title)`.
- `admin.py POST /admin/wiki/rebuild?all=true`: → `build_wiki_graph_ops(store).mark_all_dirty()`.
- **These sites currently have NO try/except (that's the crash) — porting them to the seam makes them WORK under nebula; the neo4j path is unchanged.** (Optionally the seam calls could also gain fail-open wrappers, but the port itself removes the crash — keep the change to routing + the neo4j-byte-for-byte guarantee.)

### 4. Tests (DB-free) + manual gate
- Schema: the 6 columns + the wiki_dirty index in SCHEMA_DDL; ALTER + probe.
- Neo4j impl: fake store asserts each moved constant + params (byte-for-byte).
- Nebula impl: mark/clear = `UPDATE VERTEX ... SET`; select = LOOKUP wiki_dirty==true + ORDER + LIMIT → names; mark_all = LOOKUP + per-vid UPDATE; read_subgraph = GO + FETCH assembled into the row shape; citations/source_docs → []; read_wiki_hash = FETCH; write_page_title = UPDATE.
- Dispatch + integration (wiki_dirty/wiki_context/wiki_sweep/admin route through a fake ops; neo4j default unchanged).
- Manual gate: on the cluster, mark_dirty → select_dirty (returns the marked, ordered) → clear_dirty (drops it); read_subgraph on an entity with neighbours; write_page_title + read_wiki_hash round-trip.

## Out of scope (deferred)
- `read_citations`/`read_source_docs` under nebula (Chunk-dependent → `[]`); `RELATED.description` (relation `rd` → `''`); `wikibase_qid` WRITE (wikibase.py is fail-open/degrade, separate).

## Interfaces produced
- `src/graph/nebula_schema.py`: 6 Entity wiki columns + `entity_wiki_dirty_idx`.
- `src/graph/wiki_graph_ops.py`: `WikiGraphOps` + Neo4j/Nebula impls + `build_wiki_graph_ops`.
- `src/graph/wiki_dirty.py`, `src/graph/wiki_context.py`, `src/workflow/wiki/wiki_sweep.py`, `src/api/routes/admin.py`: route through the seam.
