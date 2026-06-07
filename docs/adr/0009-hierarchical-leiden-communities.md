# ADR-0009: Hierarchical Leiden communities + structured reports (GraphRAG-style global)

- Status: Accepted
- Date: 2026-06-07

## Context

Local retrieval (chunk + entity neighbourhood) answers specific questions but
cannot answer corpus-spanning "global" questions ("what are the main themes
across all documents?"). GraphRAG's answer is to detect communities in the
entity graph and summarise each into a report that a map-reduce global search
can consume. Community detection over GDS Leiden and per-community
summarization are heavy and must never touch the query hot path.

## Decision

Run an **offline** `CommunityBuildWorkflow` on the dedicated `kb-graph-build`
queue (admin endpoint / optional schedule, never a search). `detect_hierarchy`
projects the `__Entity__` subgraph (undirected — Leiden requires it), runs
`gds.leiden.stream` with `includeIntermediateCommunities`, and materialises the
full dendrogram: level 0 carries `(:__Entity__)-[:IN_COMMUNITY]->(:Community)`
links; finer levels are wired `(:Community {level:k-1})-[:PARENT_OF]->(...)`.
`summarize_community_activity` produces a **structured report**
`{title, summary, findings:[{statement, importance}]}` via the small-tier LLM,
embeds `title+summary` into a native `report_vec`, and persists it
idempotently. Reports are built bottom-up (level>0 reads child reports) and
carried over unchanged when `(level, members_hash)` matches a prior build.

## Consequences

- Enables GraphRAG global search (ADR-0010) without burdening queries;
  idempotent/incremental rebuilds keep summaries fresh and skip unchanged work.
- Everything is fail-safe (a `None` store or any GDS/Cypher error → `[]`,
  logged, never raised) so a partial rebuild degrades gracefully.
- Commits us to a Neo4j GDS install. The exact GDS 2.x calls are **unverified
  against a live GDS install in this sandbox** (no Neo4j/GDS available) — see
  the module docstring / R6 report.

## Alternatives considered

- **No community layer (local-only RAG)** — cannot answer global/thematic
  questions.
- **Compute communities on the query path** — far too slow; the offline,
  decoupled queue is the whole point.

## References

- `src/graph/communities.py` (`detect_hierarchy`, `detect_communities`),
  `src/workflow/search/activities/community.py`,
  `src/workflow/search/community_wf.py`; `docs/QUEUES.md` ("kb-graph-build")
- CONCEPTS.md → "GraphRAG communities and reports"
