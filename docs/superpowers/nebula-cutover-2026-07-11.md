# NebulaGraph cutover — neo4j decommissioned

**2026-07-11.** GRAPH_BACKEND default flipped neo4j → **nebula**; neo4j removed from all compose topologies. This is the migration cutover.

## What changed (code — this commit)

- `src/config.py`: `GraphSettings.backend` default `neo4j` → **`nebula`**. NebulaGraph is now the sole graph backend at runtime.
- `docker-compose.yml` / `docker-compose.prod.yml` / `docker-compose.scale.yml`: the **neo4j service, its volumes (`neo4j_data`/`neo4j_logs`), env refs (`NEO4J_URI`/`NEO4J_USER`/`NEO4J_PASSWORD`), and `depends_on: neo4j`** are all removed. neo4j no longer starts.
- `tests/conftest.py`: an autouse fixture pins the **test-harness** default to `neo4j`. The DB-free unit tests validate the RETAINED Neo4j seam impls byte-for-byte through fake stores; pinning keeps that coverage valid. Nebula-path tests override with their own `monkeypatch(..., "nebula")`. Prod default is nebula; the harness default is a test-only choice.

## What is DEPLOY (you run this), not code

- **Re-ingest.** Cutover uses a **fresh nebula graph via re-ingest** (data migration was waived — no neo4j→nebula backfill). Bring up the nebula cluster (metad/storaged/graphd; register the storage host once via `scripts/nebula_bootstrap.py`), then run a full ingest so nebula holds the graph. Until then nebula is empty and analytics/search return empty (fail-soft, no crash).
- **Production neo4j teardown.** Stop/remove the prod neo4j instance + its data volume after the re-ingest validates nebula end-to-end.

## Rollback (retained on purpose)

The **Neo4j seam implementation code is retained** (every `Neo4j*GraphOps`, `Neo4jPropertyGraphStore`, the moved Cypher constants) — inert, never hit while `GRAPH_BACKEND=nebula`. Rollback = set `GRAPH_BACKEND=neo4j` and redeploy a neo4j service (the compose block is in git history). This is deliberate: **do not rip out the neo4j code until a production re-ingest has validated nebula end-to-end.** Once validated, a cleanup PR can remove the neo4j branches + this test fixture.

## Nebula functional coverage at cutover (all live-verified, merged to local main)

Ingest (write + batch + ER-dedup + entity/REL first-seen), community (BUILD + SUMMARIZE + READ + descent + doc↔community-approx), search-read, wiki-editor, and the FULL analytics/monitor layer: aggregations, quality, domain, events, rollups, signals, communities, dynamics, events_llm (incl. trending), centrality + risk (in-worker igraph), alerts (Arc-2). Documented degrades under nebula (return `[]` / approximation, never crash): `centrality.link_prediction` (no in-worker nodeSimilarity), the monitor `burst` sub-check (watched-only burst cypher unported), `cooccurrence` + `topic_trend` (chunks aren't nebula nodes), doc↔community is a member-first_doc_id approximation.

## Known nGQL rules that shaped the port (reference)

`docs/superpowers/nebula-analytics-ngql-rules-2026-07-11.md` + this session's additions: edge-property WHERE needs a bound/name-anchored endpoint or it IndexNotFounds (else client-filter); `double DEFAULT 0.0` (float literal — `0` → `Invalid param!`); async `ALTER ... DROP` GC races an in-place re-ADD (recreate the space instead); ORDER BY an aliased column only.
