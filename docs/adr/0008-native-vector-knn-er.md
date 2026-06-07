# ADR-0008: Opt-in native-vector kNN ER over the 5000-row window

- Status: Accepted
- Date: 2026-06-07

## Context

Incremental cross-document ER (ADR-0007) compares each new entity against
already-stored canonicals. The default loads a bounded window
(`incremental_window`, default 5000) of canonicals ordered by `mention_count
DESC` and brute-forces candidates in Python. Past that many canonicals the
window covers only a fraction of the true nearest neighbours — on a synthetic
200k graph the mention-count window reaches ~2% of true nearest canonicals, so
new mentions of a frequent entity can silently fragment into duplicates.

## Decision

Add an **opt-in native Neo4j vector-index kNN** path
(`ERConfig.use_native_vector_knn`, env `ER_USE_NATIVE_VECTOR_KNN`). When on, ER
stores each canonical's embedding as a native `er_vec` list property and queries
an `er_embedding_vec` index per new entity for its k nearest stored canonicals
across the **whole** graph — no window ceiling (measured ~96% recall at
~6 ms/query). It is **default off**, enabled only after running the backfill:
`scripts/backfill_er_vector.py` parses each existing entity's legacy
`er_embedding` JSON into `er_vec` and builds the index. The ordering is
**backfill-then-flag** — the kNN path can only find canonicals that already
have `er_vec`.

## Consequences

- Removes the window ceiling and the duplicate-fragmentation failure at scale,
  at native-index speed.
- Strict operational ordering: flipping the flag before backfilling yields an
  empty/partial index (it fails open to within-batch ER, never crashes). Adds
  an `er_vec` property + index to maintain.
- The default path is unchanged, so existing deployments are unaffected until
  they opt in.

## Alternatives considered

- **Raise `incremental_window`** — only postpones the ceiling and grows memory
  (≈ window × dim × 4 bytes) and candidate-gen cost linearly.
- **Flip the flag without backfill** — the index would miss un-migrated
  canonicals; rejected, hence the explicit backfill-then-flag order.

## References

- `src/graph/entity_resolution.py` (`use_native_vector_knn`,
  `_load_candidates_native`), `scripts/backfill_er_vector.py`,
  `src/graph/index.py` (`ensure_er_vector_index`)
- `docs/runbook/er-native-vector-knn.md`; CONCEPTS.md → "Native-vector ER at scale"
