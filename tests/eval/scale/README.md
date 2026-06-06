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

## First measured numbers (Apple Silicon dev box)

ER candidate-gen, single label, real `_candidate_pairs` (pure-Python
cosine, recomputes norms per pair):

| entities-in-label | seconds |
|---|---|
| 200 | 1.0 |
| 400 | 4.1 |
| 800 | 16.1 |

Clean O(N²) (×4 time per ×2 entities). Extrapolated to the ER window
(~2250 same-label entities at a 5000 window) ⇒ ~120 s/ingest on
candidate-gen alone — the P0.2 case for vector-kNN / blocking.

Dedup candidate-recall for *close* duplicates (jitter 0.03) is 100 % at
knn_k 5/10/20 — i.e. the candidate floor isn't the dedup bottleneck; the
window (P0.1, fixed) and the O(N²) cost (P0.2) are.
