# Wiki article quality + source-download links — design

**Date:** 2026-06-07
**Status:** approved (scope + decisions locked via interactive Q&A)
**Area:** continuous wiki editor (Project A) — `src/workflow/wiki/*`, `src/graph/wiki_context.py`, `src/config.py`

## Goal

Improve per-entity wiki article quality and add a deterministic **"Источники"**
section that links to the original uploaded documents (download from MinIO via
the existing `GET /api/v1/documents/{doc_id}` endpoint).

## Motivation

Today `render_bot_section` is fed: entity `description`, **all** 1-hop relations
(no cap/ranking), and 5 citation chunks ordered by `doc_id`. Three concrete gaps:

1. **Hub flooding** — `_SUBGRAPH_CYPHER` pulls every 1-hop edge. An entity with
   hundreds of relations blows up the prompt and dilutes the article. Bigger
   quality lever than graph depth.
2. **Citation clustering** — `ORDER BY c.doc_id, text LIMIT 5` can return 5
   chunks of the **same** document, starving the article of source diversity.
3. **No source provenance** — the article never points a reader at the original
   files the facts came from.

## Decisions (locked)

- **Link target:** stable API URL `{WIKI_DOCS_BASE_URL}/documents/{doc_id}` as-is.
  The endpoint is behind `require_api_key`; access for browser readers is an
  infra concern (authenticated gateway / reverse-proxy), NOT solved here. Chosen
  over presigned MinIO URLs (expire → dead links + interact badly with hash-skip)
  and over a signed-token endpoint variant (more code/secret surface).
- **Scope:** all three parts (relation cap/rank + citation dedup + sources).
- **Link text:** bare `doc_id` (UUID). No Postgres lookup → wiki worker keeps its
  Neo4j-only dependency set. Human-readable filenames are a future improvement.

## Design

### Part 1 — relation ranking + cap (hub fix)

`_SUBGRAPH_CYPHER` orders 1-hop relations by neighbour `mention_count` (desc) and
caps to `WIKI_MAX_RELATIONS` (new, default 30). The hash and the prompt both see
the capped, ranked set — so a hub produces a bounded, salient relation list.

Cartesian-safe shape (relations and chunks are aggregated in **separate**
queries — never both `OPTIONAL MATCH` in one `WITH`, which would multiply rows):

```cypher
MATCH (e:__Entity__ {name: $name})
OPTIONAL MATCH (e)-[r]-(m:__Entity__)
WITH e, r, m, coalesce(m.mention_count, 0) AS mc
ORDER BY mc DESC
WITH e, collect(CASE WHEN m IS NULL THEN NULL ELSE {
       rl: type(r),
       dir: CASE WHEN startNode(r) = e THEN 'out' ELSE 'in' END,
       nn: m.name,
       nl: head([l IN labels(m) WHERE l <> '__Entity__' AND l <> '__Node__']),
       rd: coalesce(r.description, '')
     } END) AS rels
RETURN e.name AS name,
  head([l IN labels(e) WHERE l <> '__Entity__' AND l <> '__Node__']) AS label,
  coalesce(e.description, '') AS description,
  coalesce(e.wikibase_qid, '') AS qid,
  coalesce(e.wiki_page_title, '') AS page_title,
  [x IN rels WHERE x IS NOT NULL][0..$max_rel] AS relations
```

`read_entity_subgraph(store, name, max_relations)` passes `max_rel`.

### Part 2 — citation dedup (one best chunk per document)

`read_citations` collapses to one chunk per `doc_id` before `LIMIT $k`, so the K
slots span K distinct sources. Default `citations_top_k` raised 5 → 8.

```cypher
MATCH (c:Chunk)-[:MENTIONS]->(e:__Entity__ {name: $name})
WITH c.doc_id AS doc_id, collect(c)[0] AS c
RETURN coalesce(c.text, '') AS text, doc_id
ORDER BY doc_id LIMIT $k
```

### Part 3 — "Источники" section + hash fix

New helper:

```cypher
-- read_source_docs(store, name)
MATCH (c:Chunk)-[:MENTIONS]->(e:__Entity__ {name: $name})
RETURN DISTINCT c.doc_id AS doc_id ORDER BY doc_id
```

Rendered **deterministically** (NOT via the LLM) and appended inside the bot
markers, after the LLM prose:

```mediawiki
== Источники ==
* [{base}/documents/{doc_id} {doc_id}] — скачать исходник
```

where `{base}` = `WIKI_DOCS_BASE_URL` (new, default `http://localhost:8000/api/v1`).
Empty doc-set → section omitted entirely.

**Hash fix (critical):** `subgraph_hash` MUST fold the sorted `doc_id` set in, or
a new document that mentions the entity without adding a 1-hop relation will be
hash-skipped and the new link never appears. Signature becomes
`subgraph_hash(ctx, source_doc_ids)`; the sorted ids join the hashed payload.
This is consistent with the dirty mechanism (a new doc mentioning the entity
already flags it dirty via `merge_and_resolve` → `mark_entities_dirty`).

### Call-site wiring (`write_entity_article`)

```
ctx   = read_entity_subgraph(store, name, settings.wiki.max_relations)
docs  = read_source_docs(store, name)
h     = subgraph_hash(ctx, docs)
... hash-skip unchanged ...
cites = read_citations(store, name, settings.wiki.citations_top_k)
bot   = await render_bot_section(ctx, cites, llm,
                                 source_doc_ids=docs,
                                 docs_base_url=settings.wiki.docs_base_url)
... splice / upsert / sitelink / persist unchanged ...
```

## Config additions (`WikiSettings`, env prefix `WIKI_`)

| Field | Default | Env |
|---|---|---|
| `max_relations` | `30` | `WIKI_MAX_RELATIONS` |
| `docs_base_url` | `http://localhost:8000/api/v1` | `WIKI_DOCS_BASE_URL` |
| `citations_top_k` | **8** (was 5) | `WIKI_CITATIONS_TOP_K` |

## Non-goals / future

- Human-readable filenames (needs Postgres or a filename stamped on `Chunk`).
- 2-hop context / signed-token unauthenticated download / presigned URLs.
- Citation ranking by vector relevance (kept deterministic for now).

## Testing

Pure-helper unit tests (no Temporal/Neo4j), matching the repo convention:

- `subgraph_hash` changes when `source_doc_ids` changes; stable under reorder;
  unchanged by qid/page_title.
- `_fmt_sources`: correct wikitext, base-url join, empty → "" (section omitted).
- relation cap: `read_entity_subgraph` returns ≤ `max_relations` (stubbed store
  returning ranked rows — assert slice/limit respected).
- citation dedup: stubbed rows with duplicate doc_ids → one per doc.
- `splice_bot_section` unaffected (sources live inside the bot block).

Live/stub Cypher behaviour (cap ordering, dedup) covered by a graph-store stub
in `tests/` as elsewhere in the wiki suite.

## Backout

All additive + flag-tunable. Set `WIKI_MAX_RELATIONS` very high to disable
capping; the "Источники" section is inert when `docs_base_url` is unreachable
(links just 404 until the gateway is wired). No schema migration.
