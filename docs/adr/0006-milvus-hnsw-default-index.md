# ADR-0006: Milvus HNSW as the default chunk index

- Status: Accepted
- Date: 2026-06-07

## Context

llama-index's `MilvusVectorStore` defaults the chunk collection to
`index_type="FLAT"` — exhaustive, exact brute-force search. FLAT is fine up to
a few hundred thousand vectors but hits a latency cliff as the corpus grows
toward 1M+, and the system targets 250k-scale knowledge bases.

## Decision

Default the chunk collection's vector index to **HNSW** approximate-NN
(`MilvusSettings.index_type="HNSW"`, `M=16`, `efConstruction=200`, `ef=64`).
`index_type` takes effect only when the collection is (re)created — a fresh
deploy or `overwrite=True` re-ingest — so an existing FLAT collection keeps FLAT
until rebuilt. This is an **opt-in-by-rebuild** swap, never a silent in-place
mutation; `MILVUS_INDEX_TYPE=FLAT` keeps exact search. A benchmark
(`bench_flat_vs_hnsw`) measures the FLAT→HNSW latency speedup and HNSW recall
against FLAT (the exact ground truth) at any target vector count.

## Consequences

- Approximate search keeps query latency low at 250k+ vectors; tunable
  recall/latency via `ef` (must be ≥ search `top_k`).
- Recall is no longer exact — accepted as the cost of scale, and quantified by
  the benchmark rather than assumed.
- Existing FLAT collections require a rebuild to gain HNSW; operators can pin
  FLAT for exactness.

## Alternatives considered

- **Keep FLAT (the upstream default)** — exact but a latency cliff past ~1M
  vectors; unworkable at target scale.
- **IVF-family indexes** — viable, but HNSW gives strong recall/latency without
  cluster-count tuning; chosen as the default with tunable build/search width.

## References

- `src/config.py` (`MilvusSettings`), `tests/eval/scale/bench_milvus.py`,
  `src/retrieval/vector_index.py`
- `docs/SEARCH.md`; CONCEPTS.md → "Vector index at scale: FLAT vs HNSW"
