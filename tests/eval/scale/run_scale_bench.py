"""CLI for the synthetic scale-bench harness.

Runs without production data.  Each sub-benchmark brackets one scaling
cliff from the 250k-entity assessment:

  er-cost     P0.2  ER candidate-gen O(N²) cost curve            (no infra)
  er-recall   P0.1  duplicate-candidate recall vs knn_k          (no infra)
  milvus      P1.1  Milvus FLAT vs HNSW latency + recall         (local Milvus)
  walk        P1.2  graph_walk hub-degree cliff                  (local Neo4j)
  graph-write A.A0  concurrent MERGE hub-node write contention   (local Neo4j)

Examples::

    uv run python -m tests.eval.scale.run_scale_bench er-cost --sizes 200,400,800
    uv run python -m tests.eval.scale.run_scale_bench milvus --n 200000 --dim 768
    uv run python -m tests.eval.scale.run_scale_bench walk --hub-degrees 500,1000,5000
    uv run python -m tests.eval.scale.run_scale_bench graph-write --writers 1,2,4,8
    uv run python -m tests.eval.scale.run_scale_bench graph-write --writers 4,8 --with-retry
    uv run python -m tests.eval.scale.run_scale_bench all          # everything available

infra-bound benches print a ``skipped`` line (never fail) when the local
dev Milvus / Neo4j isn't up.  NEVER point --milvus-uri / --neo4j-uri at
production.
"""
from __future__ import annotations

import argparse
import json


def _print_rows(title: str, rows) -> None:
    print(f"\n### {title}")
    if isinstance(rows, dict):
        rows = [rows]
    for r in rows:
        print("  " + json.dumps(r, ensure_ascii=False))


def _cmd_er_cost(args) -> None:
    from tests.eval.scale.bench_er import bench_cost_curve

    sizes = [int(x) for x in args.sizes.split(",")]
    dist = {"Person": 1.0} if args.single_label else None
    _print_rows(
        "ER candidate-gen cost (P0.2 — pure-Python O(N²))",
        bench_cost_curve(sizes, dim=args.dim, label_dist=dist),
    )


def _cmd_er_recall(args) -> None:
    from tests.eval.scale.bench_er import bench_dedup_recall

    ks = tuple(int(x) for x in args.knn_ks.split(","))
    _print_rows(
        "ER duplicate-candidate recall (P0.1)",
        bench_dedup_recall(
            n=args.n, dup_rate=args.dup_rate, dim=args.dim, knn_ks=ks,
        ),
    )


def _cmd_milvus(args) -> None:
    from tests.eval.scale.bench_milvus import bench_flat_vs_hnsw

    _print_rows(
        "Milvus FLAT vs HNSW (P1.1)",
        bench_flat_vs_hnsw(
            n=args.n, dim=args.dim, n_queries=args.queries,
            top_k=args.top_k, uri=args.milvus_uri,
        ),
    )


def _cmd_walk(args) -> None:
    from tests.eval.scale.bench_walk import bench_hub_walk

    degs = tuple(int(x) for x in args.hub_degrees.split(","))
    out = bench_hub_walk(
        n_nodes=args.nodes, hub_degrees=degs, hops=args.hops,
        uri=args.neo4j_uri, user=args.neo4j_user, password=args.neo4j_password,
    )
    if out.get("status") == "ok":
        _print_rows("graph_walk hub cliff (P1.2)", out["rows"])
    else:
        _print_rows("graph_walk hub cliff (P1.2)", out)


def _cmd_graph_write(args) -> None:
    from tests.eval.scale.bench_graph_write import bench_graph_write

    writers = tuple(int(x) for x in args.writers.split(","))
    out = bench_graph_write(
        writers_sweep=writers, rounds=args.rounds, batch=args.batch,
        n_hubs=args.n_hubs, with_retry=args.with_retry,
        uri=args.neo4j_uri, user=args.neo4j_user, password=args.neo4j_password,
    )
    if out.get("status") == "ok":
        _print_rows("Neo4j concurrent-write contention (A.A0)", out["rows"])
    else:
        _print_rows("Neo4j concurrent-write contention (A.A0)", out)


def _cmd_all(args) -> None:
    from tests.eval.scale.bench_er import bench_dedup_recall

    _cmd_er_cost(args)
    # er-recall must use a SMALL set (its own knob), never --n which here
    # sizes the Milvus corpus (e.g. 200k) and would hang pure-Python ER.
    ks = tuple(int(x) for x in args.knn_ks.split(","))
    _print_rows(
        "ER duplicate-candidate recall (P0.1)",
        bench_dedup_recall(n=args.recall_n, dup_rate=args.dup_rate, dim=args.dim, knn_ks=ks),
    )
    _cmd_milvus(args)
    _cmd_walk(args)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Synthetic scale-bench harness")
    sub = p.add_subparsers(dest="cmd", required=True)

    def _common(sp):
        sp.add_argument("--dim", type=int, default=768)

    sp = sub.add_parser("er-cost"); _common(sp)
    sp.add_argument("--sizes", default="200,400,800")
    sp.add_argument("--single-label", action="store_true", default=True)
    sp.set_defaults(func=_cmd_er_cost)

    sp = sub.add_parser("er-recall"); _common(sp)
    sp.add_argument("--n", type=int, default=600)
    sp.add_argument("--dup-rate", type=float, default=0.12)
    sp.add_argument("--knn-ks", default="5,10,20")
    sp.set_defaults(func=_cmd_er_recall)

    sp = sub.add_parser("milvus"); _common(sp)
    sp.add_argument("--n", type=int, default=200000)
    sp.add_argument("--queries", type=int, default=100)
    sp.add_argument("--top-k", type=int, default=10)
    sp.add_argument("--milvus-uri", default="http://localhost:19530")
    sp.set_defaults(func=_cmd_milvus)

    sp = sub.add_parser("walk"); _common(sp)
    sp.add_argument("--nodes", type=int, default=20000)
    sp.add_argument("--hub-degrees", default="500,1000,5000")
    sp.add_argument("--hops", type=int, default=2)
    sp.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    sp.add_argument("--neo4j-user", default="neo4j")
    sp.add_argument("--neo4j-password", default="changeme")
    sp.set_defaults(func=_cmd_walk)

    sp = sub.add_parser("graph-write"); _common(sp)
    sp.add_argument("--writers", default="1,2,4,8",
                    help="comma-separated concurrency levels to sweep")
    sp.add_argument("--rounds", type=int, default=20)
    sp.add_argument("--batch", type=int, default=25)
    sp.add_argument("--n-hubs", type=int, default=16,
                    help="shared hub-node count; smaller = more contention")
    sp.add_argument("--with-retry", action="store_true", default=False,
                    help="enable the deadlock-retry wrapper (A3) — measure vs baseline")
    sp.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    sp.add_argument("--neo4j-user", default="neo4j")
    sp.add_argument("--neo4j-password", default="changeme")
    sp.set_defaults(func=_cmd_graph_write)

    sp = sub.add_parser("all"); _common(sp)
    sp.add_argument("--sizes", default="200,400,800")
    sp.add_argument("--single-label", action="store_true", default=True)
    sp.add_argument("--n", type=int, default=200000)
    sp.add_argument("--queries", type=int, default=100)
    sp.add_argument("--top-k", type=int, default=10)
    sp.add_argument("--dup-rate", type=float, default=0.12)
    sp.add_argument("--knn-ks", default="5,10,20")
    sp.add_argument("--recall-n", type=int, default=600,
                    help="entity count for the er-recall set (kept small)")
    sp.add_argument("--milvus-uri", default="http://localhost:19530")
    sp.add_argument("--nodes", type=int, default=20000)
    sp.add_argument("--hub-degrees", default="500,1000,5000")
    sp.add_argument("--hops", type=int, default=2)
    sp.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    sp.add_argument("--neo4j-user", default="neo4j")
    sp.add_argument("--neo4j-password", default="changeme")
    # er-recall in `all` reuses --n for the recall set size too; keep modest.
    sp.set_defaults(func=_cmd_all)

    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
