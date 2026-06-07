# ADR-0002: Claim-check staging via MinIO for heavy workflow state

- Status: Accepted
- Date: 2026-06-07

## Context

Ingest activities produce large in-memory state — parsed LlamaIndex nodes, KG
entity/relation lists — that the next activity needs. Temporal passes activity
inputs/outputs through its payload converter and persists them in workflow
history; shipping multi-megabyte blobs through history is slow and hits size
limits.

## Decision

Apply the **claim-check pattern**: an activity pickles its heavy output to
MinIO under `s3://{staging_bucket}/{workflow_run_id}/{stage}.pkl` and passes
only the `s3://` URI to the next activity (`StagingStore.write_pickle` /
`read_pickle`). Pickle is acceptable because producer and consumer share the
same Python image and blobs live only for one workflow run. `finalize` and
`mark_failed` call `delete_prefix(workflow_run_id)`; a `cleanup_orphans` sweep
(`list_orphan_runs`, default 24h) reclaims prefixes from runs that died before
either ran.

## Consequences

- Workflow history stays small (just URIs); arbitrarily large stage state is
  supported.
- Adds a MinIO dependency on the hot path and a cleanup obligation; orphaned
  blobs are bounded by the age-threshold sweep, not eliminated.
- Pickle ties the staging format to one shared image and Python version — it is
  never read by anything outside the package.

## Alternatives considered

- **Pass blobs through Temporal payloads directly** — exceeds payload/history
  size limits and bloats history storage.
- **A typed external store (Postgres/Neo4j) for intermediate state** — heavier
  schema work for short-lived, single-run scratch data; pickle-to-object-store
  is simpler for ephemeral blobs.

## References

- `src/workflow/staging.py`; consumers e.g.
  `src/workflow/activities/inject_canonical.py`, `push_wikibase.py`,
  `finalize.py`; `scripts/cleanup_staging.py`
- CONCEPTS.md → "Claim-check staging"
