# nGQL translation rules for the analytics port (live-proven, nebula 3.8)

Cluster-verified 2026-07-11 against the running `kb` space (graphd 9669). These
are the concrete rules for translating each analytics primitive's neo4j Cypher
into nebula `MATCH`. nebula 3.8 supports the openCypher `MATCH` subset **including
aggregation and variable-length paths**, so the aggregation/scan primitives port
near-verbatim — unlike `connections` (VID point-lookups) which needed GO/FETCH.

## Schema facts (the port must respect these)

Entity TAG columns (DESCRIBE TAG `Entity`):
`name, description, mention_count, created_at, label, er_canonical_name,
first_doc_id, wiki_dirty, wiki_dirty_at, wiki_hash, wiki_synced_at,
wiki_page_title, wikibase_qid`.

RELATED edge columns: `rel_type, polarity, valid_from, valid_to, weight`.
Other tags: `Community(id, level, member_count, members_hash, updated, report,
title, summary, summarized_at)`, `ERVerdict(er_key, same, updated)`.
Other edges: `IN_COMMUNITY, MENTIONS, PARENT_OF`.

**ABSENT columns/edges the neo4j analytics reads** — these primitives return `[]`
under nebula until the corresponding Tier-B compute stage runs and materializes
them (documented, honest — they return `[]` today anyway because the raw Cypher
raises → fail-soft):
- `Entity.risk_score` → `signals.top_risk`, `signals.risk_clusters` → `[]`
- `Entity.<centrality metric>` (pagerank/betweenness/degree column) →
  `centrality.top_by_metric` → `[]`
- `LIKELY_LINK` edge → `centrality.entity_resolution_candidates` → `[]`
- `Chunk` tag / MENTIONS-from-Chunk → `cooccurrence`, `dynamics.topic_momentum` → `[]`

## Translation rules (each live-verified)

1. **Label / node scan:** `MATCH (e:__Entity__)` → `` MATCH (e:`Entity`) ``.
2. **Property access:** `e.prop` → `` e.`Entity`.prop `` (tag-qualified). Edge:
   `r.weight` stays `r.weight` (edge props are not tag-qualified in RETURN, but
   `r.rel_type` etc. work directly).
3. **Multi-label filter:** `WHERE $type IN labels(e)` / `(e:__Entity__:Issue)` →
   `` WHERE e.`Entity`.label == $type `` / `` WHERE e.`Entity`.label == 'Issue' ``.
   `NONE(l IN labels(e) WHERE l IN $id_types)` →
   `` WHERE e.`Entity`.label NOT IN $id_types `` (label is single-valued here).
4. **Relation semantic type:** `type(r)` returns `'RELATED'` (the edge tag), NOT
   the semantic type. Use `` r.rel_type `` wherever the Cypher used `type(r)`.
   Relation-type filter `-[r:OWNS]-` → `` -[r:`RELATED`]- ... WHERE r.rel_type == 'OWNS' ``
   (or `r.rel_type IN [...]` for `-[:CONTACT|RESPONDED_TO]-`).
5. **ORDER BY (the one real gotcha):** nebula rejects `ORDER BY <expr>` — "Only
   column name can be used as sort item". ALWAYS alias the sort key into RETURN
   and order by the alias: `RETURN e.`Entity`.created_at AS ts ... ORDER BY ts DESC`.
6. **Aggregation:** `RETURN col, count(*) AS n` and `RETURN count(e) AS n` work in
   MATCH directly. (In `LOOKUP`, aggregation is only allowed in a separate pipe
   stage `| YIELD count(*)`, never in LOOKUP's own YIELD — but prefer MATCH.)
7. **Two-pattern MATCH** (e.g. `MATCH (a)-[r1]->(b), (a)-[r2]->(b)`): write as two
   MATCH clauses — `` MATCH (a:`Entity`)-[r1:`RELATED`]->(b:`Entity`) MATCH (a)-[r2:`RELATED`]->(b) WHERE r1.rel_type < r2.rel_type ``.
8. **Variable-length + per-edge filter:** `(a)-[:OWNS*2..6]->(a)` →
   `` MATCH p=(a:`Entity`)-[e:`RELATED`*2..6]->(a) WHERE all(rel IN e WHERE rel.rel_type == 'OWNS') ``.
   `nodes(p)`, `[n IN nodes(p) | n.`Entity`.name]` list comprehension work.
9. **OPTIONAL MATCH** works: `MATCH (e:`Entity`) WHERE ... OPTIONAL MATCH (e)-[r:`RELATED`]-(:`Entity`) RETURN ..., count(r) AS deg`.
10. **Params:** the seam uses INLINE nGQL (no `param_map`) — interpolate with
    `_q(...)` for strings and `entity_vid(name)` for VIDs, exactly as `connections`.

## Verified example shapes (copy as starting points)

```
# entity_counts_by_type
MATCH (e:`Entity`) RETURN e.`Entity`.label AS type, count(*) AS n ORDER BY n DESC
# rel_type distribution  (neo4j type(r) -> r.rel_type)
MATCH (:`Entity`)-[r:`RELATED`]->(:`Entity`) RETURN r.rel_type AS rel, count(*) AS n ORDER BY n DESC
# top by degree
MATCH (e:`Entity`)-[r:`RELATED`]-(:`Entity`) RETURN e.`Entity`.name AS name, count(r) AS degree ORDER BY degree DESC LIMIT 10
# new-since (note aliased ORDER BY)
MATCH (e:`Entity`) WHERE e.`Entity`.created_at >= <since> RETURN e.`Entity`.name AS name, e.`Entity`.created_at AS ts ORDER BY ts DESC
# duplicate edges (two MATCH)
MATCH (a:`Entity`)-[r1:`RELATED`]->(b:`Entity`) MATCH (a)-[r2:`RELATED`]->(b) WHERE r1.rel_type < r2.rel_type RETURN a.`Entity`.name AS a, b.`Entity`.name AS b, count(*) AS dupes
# ownership cycle (var-len + all())
MATCH p=(a:`Entity`)-[e:`RELATED`*2..6]->(a) WHERE all(rel IN e WHERE rel.rel_type == 'OWNS') RETURN [n IN nodes(p) | n.`Entity`.name] AS cycle LIMIT <n>
```

## Method: every ported primitive gets a live-verify

Seed a tiny graph (INSERT VERTEX `Entity` real columns + INSERT EDGE `RELATED`),
run the nebula method, assert non-empty sane rows with the SAME keys the neo4j
Cypher RETURNs, then `DELETE VERTEX ... WITH EDGE`. The scratchpad spikes
(`agg_spike*.py`) are the template.
