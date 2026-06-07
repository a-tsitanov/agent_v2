# ADR-0010: Dynamic community selection (lexical / semantic / descent, fail-open)

- Status: Accepted
- Date: 2026-06-07

## Context

Global search map-reduces over community reports (ADR-0009). Mapping over
**every** community wastes small-tier LLM calls on irrelevant ones, but a fixed
"largest-first" cut ignores query relevance. We want to spend the map budget on
the communities most relevant to the question, and we cannot let a missing
vector index or embed failure break search.

## Decision

`map_communities` selects which summaries to map over via a strategy switch:
- **lexical** — read stored summaries, rank by query word-overlap (deterministic,
  LLM-free); the default and the universal fallback;
- **semantic** — kNN over the native `report_vec` index (`community_report_vec`);
- **descent** — GraphRAG dynamic selection: start at the coarsest level and
  greedily descend `PARENT_OF` toward the finest query-relevant communities
  (cosine of query vs `report_vec`), capped at a budget.

Both vector strategies **fall open to lexical** on an empty result or any
error. Each mapped community gets a small-tier partial answer
(`map_community_partial`) that self-reports `НЕТ` (score 0) when off-topic so
REDUCE drops it; REDUCE reuses the large-tier `synthesize_answer` (ADR-0011's R5
pattern).

## Consequences

- The map budget concentrates on relevant communities; descent gives
  coarse→fine pruning at hierarchy scale.
- Robust: missing index / embed failure silently degrades to lexical, never
  failing the search. Off-topic communities are filtered at map time, before
  the expensive reduce.
- Commits us to keeping `report_vec` and its index in sync for the vector
  strategies to add value.

## Alternatives considered

- **Map over all communities** — wasteful and noisy at scale.
- **Static largest-first cut** — ignores query relevance; the lexical/semantic/
  descent strategies all rank by the query instead.

## References

- `src/workflow/search/activities/global_search.py` (`map_communities`,
  `select_communities_semantic`, `select_communities_descent`,
  `rank_summaries`), `src/workflow/search/global_wf.py`
- `docs/SEARCH.md`; CONCEPTS.md → "Dynamic community selection"
