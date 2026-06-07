# Synthetic scale-bench harness

Validate the 250k-entity scaling work **without touching or exporting
from the production database**. Every graph / vector / entity set is
generated from a seed, so the *shape* that drives each cliff (entity
count, label skew, duplicate rate, embedding spread, node degree) is
reproduced locally and swept.

This exists because prod can't be read and data can't leave it — so we
bracket the real graph by shape instead of measuring it directly.

## What each benchmark brackets

| Command | Cliff | Needs | What it proves |
|---|---|---|---|
| `er-cost` | **P0.2** ER candidate-gen O(N²) | nothing | how fast `_candidate_pairs` (the real function) blows up with entities-per-label |
| `er-recall` | **P0.1** dedup window/floor | nothing | of planted near-duplicates, how many are surfaced as candidates (ceiling on dedup quality) |
| `bench_er_native` | **P0.1 structural** native vs window | local Neo4j | native Neo4j vector kNN recall + latency vs what the 5000-window can even reach (justifies dropping the window) |
| `milvus` | **P1.1** FLAT→HNSW | local Milvus | FLAT (exhaustive, the shipped-fix default) vs HNSW latency + HNSW recall vs FLAT |
| `walk` | **P1.2** hub traversal | local Neo4j | `(e)-[*1..hops]-` walk latency from a normal node vs a planted hub |

`milvus` / `walk` print a `skipped` line (never fail) when the local dev
stack isn't up.

> **Never** point `--milvus-uri` / `--neo4j-uri` at production. These
> write throwaway data (`:_ScaleBench` label / a temp collection) into
> whatever they connect to.

## Run

```bash
# infra-free (run anywhere, now)
uv run python -m tests.eval.scale.run_scale_bench er-cost   --sizes 200,400,800
uv run python -m tests.eval.scale.run_scale_bench er-recall --n 600 --dup-rate 0.12 --knn-ks 5,10,20

# needs a LOCAL dev stack (docker compose up -d milvus neo4j) — never prod
uv run python -m tests.eval.scale.run_scale_bench milvus --n 200000 --dim 768
uv run python -m tests.eval.scale.run_scale_bench walk   --hub-degrees 500,1000,5000 --hops 2

# everything available (infra ones skip if down)
uv run python -m tests.eval.scale.run_scale_bench all
```

## Interpreting

- **er-cost** `growth_vs_linear` ≈ 2 ⇒ quadratic O(N²); ≈ 1 ⇒ linear.
  Extrapolate the curve to your real per-label entity count (the ER
  window caps the *stored* side, so use the window size as N).
- **milvus** `speedup` and `hnsw_recall_at_k` quantify the shipped P1.1
  fix at your target vector count; FLAT is the recall ground truth.
- **walk** `slowdown_vs_normal` shows the hub cliff — pick a degree cap
  where it becomes unacceptable.

## Measured numbers (Apple Silicon dev box, live local stack)

### P0.2 — ER candidate-gen, single label, real `_candidate_pairs`

| entities-in-label | original (pure-Python) | after numpy cosine | after numpy + token-cache |
|---|---|---|---|
| 800 | 16.1 s | 1.6 s | **0.136 s** |

The original was clean O(N²) (×4 time per ×2 entities) dominated by a
pure-Python `_cosine` that re-normalised per pair, plus per-pair name
re-tokenisation. Vectorising the cosine (one BLAS matrix-vector per row)
gave ~10×; caching name tokens per item gave the rest → **~118×** total,
**identical candidate set** (locked by
`test_candidate_pairs_numpy_path_matches_pure_python`). Extrapolated to a
~2250-same-label window: ~127 s → ~1.1 s/ingest.

Dedup candidate-recall for close duplicates is 100 % at knn_k 5/10/20 —
the candidate floor isn't the dedup bottleneck; the window (P0.1, fixed)
and the O(N²) cost (P0.2, now fixed) were.

### P1.1 — Milvus FLAT vs HNSW (clustered vectors, top-k=10, ef=64)

| n vectors | FLAT p50 | HNSW p50 | speedup | recall@10 |
|---|---|---|---|---|
| 50k | 16 ms | 2.1 ms | 7.6× | **1.00** |
| 250k | 26.6 ms | 5.2 ms | 5.1× | **1.00** |

ef-sweep at 50k (64/128/256) held recall 1.00 throughout. FLAT latency
grows with N (16→27 ms); HNSW stays single-digit ms. The shipped HNSW
default is safe at **perfect recall** on structured data, and the win is
conservative here — at 250k *entities* the real *chunk* collection is
~0.6–1M vectors, where FLAT is even slower. (NB: recall is only
meaningful on *clustered* vectors — uniform-random vectors in 768-d are
near-equidistant and give a misleading ~0.3; `gen_vectors` scales cluster
noise by `1/sqrt(dim)` so the structure survives.)

### P1.2 — graph_walk hub cliff (live Neo4j, 20k nodes, hops=2, cap=50)

| start node | degree | walk p50 | slowdown vs normal |
|---|---|---|---|
| normal | ~4 | 11.0 ms | 1.0× |
| hub | 500 | 9.0 ms | 0.8× |
| hub | 1000 | 10.7 ms | 1.0× |
| hub | 5000 | 15.6 ms | **1.4×** |

At hops=**3** the worst case (degree 5000, cap 50) is ×1.9 / 19 ms; with
cap 200 it is not slower at all (the hub fills the cap from its abundant
1-hop neighbours before any deep traversal).

The `LIMIT` inside the subquery makes Neo4j stop early, so across
hops 2–3 and caps 50–200 the hub cliff never exceeded ~2× / ~20 ms —
**not** the orders-of-magnitude blow-up first feared → P1.2 deprioritised.
(Caveat: synthetic hubs link to random low-degree nodes; a real
hub→hub→hub chain on a much larger graph could differ — re-measure on a
restored graph before fully closing.)

### P0.1 structural — native Neo4j vector kNN vs the 5000-window

Loading clustered entities into a native Neo4j vector index, querying
`db.index.vector.queryNodes` for the nearest stored canonical per
incoming entity:

| n_stored | native kNN recall | 5000-window reachable | native p50 |
|---|---|---|---|
| 50k | 0.985 | **0.08** | 7.6 ms |
| 200k | 0.96 | **0.02** | 6.0 ms |

"window reachable" = fraction of true nearest-canonical matches that the
mention_count-ordered 5000-window can even see — i.e. the **hard ceiling
on incremental-ER dedup today**. At 200k it is **2 %**: the window is
structurally blind to 98 % of potential matches even after the
`ORDER BY mention_count` fix. Native vector kNN recovers ~96–98 % at
6–8 ms with no window. → strong justification for migrating
`er_embedding` to a native vector property + index and replacing the
window load with a per-entity kNN.
