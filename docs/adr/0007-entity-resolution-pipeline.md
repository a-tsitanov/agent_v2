# ADR-0007: Entity Resolution = candidate-gen + LLM-judge + verdict cache + union-find

- Status: Accepted
- Date: 2026-06-07

## Context

Orthographic dedup (normalise-by-name) leaves semantically equivalent
duplicates: cross-language (`BCC` ≡ `Базальноклеточный Рак`), abbreviations,
word-order/morphology variants, initialisms (`Иванов И.И.` ≡ `Иван Иванов`),
and cross-document variants. Naively LLM-judging every pair is O(N²) calls and
non-deterministic; naively auto-merging on embedding similarity over-merges
distinct entities that co-occur in similar contexts.

## Decision

A multi-stage **embedding-blocked, LLM-confirmed** pipeline:
1. exclude deterministic-identifier labels (ADR-0005);
2. embed entities (batched); deterministic pre-pass (initialism + deep-normal);
3. **vectorized candidate generation** — same-label top-K cosine neighbours
   above a floor, with a name-token-overlap bypass/guard against
   description-context contamination;
4. **auto-merge** only when cosine ≥ HIGH AND same script (cross-script always
   goes to the judge);
5. **LLM-judge** borderline pairs (batched, JSON SAME/DIFFERENT/UNSURE),
   defaulting to DIFFERENT on any failure/timeout;
6. a **persistent verdict cache** (`:ERVerdict`, order-insensitive key) skips
   re-judging recurring pairs (optional, fail-safe);
7. **union-find** → clusters, with cluster verification (consolidation call) to
   undo transitive over-merge and a hyper-hub clamp that flags huge clusters
   `er_review_needed` instead of merging.

## Consequences

- Conservative-by-default: every LLM failure routes to DIFFERENT, so ER never
  pollutes the graph with false-positive merges; cross-script always verified.
- The verdict cache cuts repeated LLM cost across re-ingests/hub-heavy docs;
  vectorized cosines remove the pure-Python candidate-gen cliff.
- Commits us to embeddings + an LLM judge in the merge lane and to tuning many
  thresholds (`ERConfig`); incremental cross-document ER reads stored canonicals.

## Alternatives considered

- **Pure embedding auto-merge** — over-merges distinct co-occurring entities.
- **Judge every pair every run** — O(N²) LLM cost and non-determinism; the
  pre-pass + blocking + cache cut this drastically.

## References

- `src/graph/entity_resolution.py` (`resolve_entities`, `ERConfig`,
  `_candidate_pairs`, `_llm_judge_pairs`, `:ERVerdict` helpers),
  `src/workflow/activities/merge_and_resolve.py`
- CONCEPTS.md → "Entity resolution"
