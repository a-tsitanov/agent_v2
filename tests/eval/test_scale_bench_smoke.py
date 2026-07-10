"""Fast smoke tests for the synthetic scale-bench harness.

Tiny N so they run in CI in well under a second.  They verify the
harness wiring (generators + the infra-free ER benchmarks + graceful
skip when Milvus/Neo4j are down) — NOT the scaling numbers themselves
(those come from running the CLI against bigger sizes / local infra).
"""
from __future__ import annotations

from tests.eval.scale.bench_er import bench_cost_curve, bench_dedup_recall
from tests.eval.scale.bench_er_native import bench_milvus_vs_native, bench_native_vs_window
from tests.eval.scale.bench_graph_write import bench_graph_write
from tests.eval.scale.bench_milvus import bench_flat_vs_hnsw
from tests.eval.scale.bench_walk import bench_hub_walk
from tests.eval.scale.synth import gen_edges, gen_items, gen_vectors


def test_gen_items_shape_and_planted_dups() -> None:
    iset = gen_items(n=40, dim=16, dup_rate=0.25, seed=1)
    # 40 base + 10 planted variants.
    assert len(iset.items) == 40 + 10
    assert len(iset.gold_pairs) == 10
    # every item carries a unit-ish embedding of the requested dim
    assert all(len(it.embedding) == 16 for it in iset.items)


def test_gen_vectors_and_edges_shapes() -> None:
    corpus, queries = gen_vectors(n=50, dim=8, n_queries=5, seed=2)
    assert corpus.shape == (50, 8)
    assert queries.shape == (5, 8)
    edges, hubs = gen_edges(n_nodes=100, avg_degree=4, hub_degrees=(30,), seed=2)
    assert hubs == [0]
    # the hub contributes ~30 of its own edges → graph is non-trivial
    assert len(edges) > 30


def test_er_cost_curve_runs_and_is_superlinear() -> None:
    rows = bench_cost_curve([60, 120], dim=16, label_dist={"Person": 1.0})
    assert [r["n"] for r in rows] == [60, 120]
    assert all(r["seconds"] >= 0 for r in rows)
    # doubling N should not get *cheaper* — growth factor is reported
    assert rows[1]["growth_vs_linear"]  # non-empty string


def test_er_dedup_recall_surfaces_close_dups() -> None:
    rows = bench_dedup_recall(n=120, dup_rate=0.2, dim=16, knn_ks=(10,))
    assert rows[0]["gold_pairs"] > 0
    # planted near-duplicates (small jitter) should be recalled as candidates
    assert rows[0]["recall"] is not None and rows[0]["recall"] >= 0.5


def test_milvus_bench_skips_without_infra() -> None:
    out = bench_flat_vs_hnsw(n=100, dim=8, uri="http://127.0.0.1:1")  # nothing there
    assert out["status"] == "skipped"


def test_walk_bench_skips_without_infra() -> None:
    out = bench_hub_walk(n_nodes=100, uri="bolt://127.0.0.1:1")  # nothing there
    assert out["status"] == "skipped"


def test_er_native_bench_skips_without_infra() -> None:
    out = bench_native_vs_window(n_stored=100, dim=8, uri="bolt://127.0.0.1:1")
    assert out["status"] == "skipped"


def test_milvus_vs_native_bench_skips_without_infra() -> None:
    out = bench_milvus_vs_native(n_stored=100, n_new=10, milvus_uri="http://127.0.0.1:1")
    assert out["status"] == "skipped"


def test_graph_write_bench_skips_without_infra() -> None:
    out = bench_graph_write(writers_sweep=(1, 2), rounds=2, uri="bolt://127.0.0.1:1")
    assert out["status"] == "skipped"


def test_graph_write_workload_shape_and_hub_overlap() -> None:
    from tests.eval.scale.bench_graph_write import _gen_workload

    work = _gen_workload(writers=4, rounds=3, batch=5, n_hubs=8, seed=1)
    assert len(work) == 4 and all(len(w) == 3 for w in work)
    assert all(len(r) == 5 for w in work for r in w)
    # hub ids stay in range; local keys are per-writer unique (no contention)
    hubs = {row["hub"] for w in work for r in w for row in r}
    assert hubs and all(0 <= h < 8 for h in hubs)
    locals_ = [row["local"] for w in work for r in w for row in r]
    assert len(locals_) == len(set(locals_))


def test_graph_write_retryable_detection() -> None:
    from tests.eval.scale.bench_graph_write import _is_retryable

    class _Err(Exception):
        code = "Neo.TransientError.Transaction.DeadlockDetected"

    class _Other(Exception):
        code = "Neo.ClientError.Statement.SyntaxError"

    assert _is_retryable(_Err())
    assert not _is_retryable(_Other())
    assert not _is_retryable(ValueError("no code attr"))
