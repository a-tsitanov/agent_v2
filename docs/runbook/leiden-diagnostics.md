# Leiden community detection — diagnostics

When `POST /admin/communities/rebuild` reports **0 communities** on a graph
that clearly has entities, the cause is almost always a **sparse / disconnected
`__Entity__` sub-graph** (singletons dropped by `community_min_size`), not a
Leiden performance limit (50k nodes is seconds for GDS).

Since `detect_communities` is fail-soft, the worker log now distinguishes the
three cases — check it first:

- `GDS Leiden detection FAILED: …` (ERROR) → a real GDS/Cypher fault (GDS not
  installed, projection syntax, OOM). Fix the infra.
- `Leiden returned 0 rows — projected 0 entities / 0 relationships` → empty /
  disconnected projection. See the queries below.
- `detected 0 communities … from N rows — projected P entities / R relationships`
  with R small relative to P → all-singletons; the graph has entities but too
  few edges to cluster.

## Run these in Neo4j Browser

```cypher
// 1. Are there edges between entities at all? (the #1 cause of "no communities")
MATCH (:__Entity__)-[r]->(:__Entity__)
RETURN count(r) AS rels, count(DISTINCT type(r)) AS rel_types;

// 2. Connectivity — how many singletons vs real components
CALL gds.graph.project('diag', '__Entity__', '*', {}, {undirectedRelationshipTypes: ['*']});
CALL gds.wcc.stats('diag') YIELD componentCount, componentDistribution
RETURN componentCount, componentDistribution;

// 3. Run Leiden directly to see the real community count + any error
CALL gds.leiden.stats('diag', {randomSeed: 19, relationshipWeightProperty: 'weight'})
YIELD communityCount, ranLevels, modularity
RETURN communityCount, ranLevels, modularity;

CALL gds.graph.drop('diag');
```

## Interpreting

- `rels == 0` → extraction produced entities but no relations (or merge dropped
  them). Fix is upstream (extraction/merge), not Leiden.
- `componentDistribution.p99 == 1` (mostly size-1 components) → disconnected
  graph; lower `community_min_size` won't help much — the graph needs denser
  relations.
- `communityCount > 0` here but the rebuild still wrote 0 → the `min_size` floor
  is dropping them; lower `COMMUNITY_MIN_SIZE` or accept smaller communities.

## Notes

- Leiden now runs **weighted** on `r.weight` (distinct co-occurrence count from
  the merge layer); denser ties dominate the partition. Legacy edges without a
  weight fall back to `1.0` via `coalesce`.
- `randomSeed: 19` makes runs deterministic.
- Hierarchy depth is controlled by `AGENT_COMMUNITY_MAX_LEVELS` (1 = single
  level, today's default).
