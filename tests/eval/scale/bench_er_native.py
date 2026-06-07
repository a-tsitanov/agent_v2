"""Feasibility benchmark: native Neo4j vector kNN for ER candidate
generation vs the current 5000-entity window (P0.1 / P0.2 structural).

The shipped incremental ER loads at most ``window`` stored canonicals
(ORDER BY mention_count) and brute-forces cosines in Python.  At 250k
canonicals that window is ~2 % of the graph — entities outside it can
never match, so duplicates fragment.

This bench loads ``n_stored`` synthetic entities into a LOCAL Neo4j with
a native vector index and asks, for a batch of incoming entities planted
near real stored ones:

* **native kNN recall** — does ``db.index.vector.queryNodes`` recover the
  true nearest canonical (no window)?
* **window recall** — what fraction of those true matches the
  mention_count-ordered ``window`` slice can even see (the dedup ceiling
  the current code is capped at)?
* **native kNN latency** — per-query, to compare against load-window +
  brute-force.

Skips cleanly if no Neo4j.  Writes throwaway ``:_ScaleBenchER`` and
deletes it — never touches ``:__Entity__``.  Dev Neo4j only, never prod.
"""
from __future__ import annotations

import time

import numpy as np

from tests.eval.scale.synth import gen_vectors

_LABEL = "_ScaleBenchER"
_INDEX = "scalebench_er_vec"


def _driver(uri, user, password, timeout: float = 4.0):
    try:
        from neo4j import GraphDatabase

        d = GraphDatabase.driver(uri, auth=(user, password))
        d.verify_connectivity()
        return d
    except Exception:
        return None


def _load(session, corpus: np.ndarray, mention_counts: np.ndarray, dim: int) -> None:
    session.run(f"MATCH (n:{_LABEL}) DETACH DELETE n")
    session.run(
        f"CREATE VECTOR INDEX {_INDEX} IF NOT EXISTS FOR (n:{_LABEL}) ON n.vec "
        "OPTIONS {indexConfig: {`vector.dimensions`: $d, "
        "`vector.similarity_function`: 'cosine'}}",
        d=dim,
    )
    rows = [
        {"nid": i, "vec": corpus[i].tolist(), "mc": int(mention_counts[i])}
        for i in range(corpus.shape[0])
    ]
    for j in range(0, len(rows), 1000):
        session.run(
            f"UNWIND $rows AS r CREATE (n:{_LABEL}) "
            "SET n.nid = r.nid, n.vec = r.vec, n.mention_count = r.mc",
            rows=rows[j:j + 1000],
        )
    # Wait for the index to come online.
    for _ in range(60):
        rec = session.run(
            "SHOW INDEXES YIELD name, state WHERE name=$n RETURN state",
            n=_INDEX,
        ).single()
        if rec and rec["state"] == "ONLINE":
            break
        time.sleep(0.5)


def bench_native_vs_window(
    *,
    n_stored: int = 50000,
    dim: int = 768,
    n_new: int = 200,
    knn_k: int = 10,
    window: int = 5000,
    uri: str = "bolt://localhost:7687",
    user: str = "neo4j",
    password: str = "changeme",
) -> dict:
    driver = _driver(uri, user, password)
    if driver is None:
        return {"status": "skipped", "reason": f"no Neo4j at {uri}"}

    corpus, queries = gen_vectors(n=n_stored, dim=dim, n_queries=n_new, seed=5)
    rng = np.random.default_rng(5)
    mention_counts = rng.integers(1, 50, size=n_stored)

    # Ground truth: true nearest stored id per query (brute force, full set).
    truth = [int(np.argmax(corpus @ q)) for q in queries]

    # Window the current code would see: top `window` by mention_count.
    window_ids = set(np.argsort(-mention_counts)[:window].tolist())
    window_reachable = sum(1 for t in truth if t in window_ids) / len(truth)

    try:
        with driver.session() as s:
            _load(s, corpus, mention_counts, dim)
            lat: list[float] = []
            native_hit = 0
            for q, t in zip(queries, truth):
                t0 = time.perf_counter()
                res = s.run(
                    f"CALL db.index.vector.queryNodes('{_INDEX}', $k, $q) "
                    "YIELD node RETURN node.nid AS nid",
                    k=knn_k, q=q.tolist(),
                ).data()
                lat.append((time.perf_counter() - t0) * 1000.0)
                if t in {r["nid"] for r in res}:
                    native_hit += 1
            s.run(f"MATCH (n:{_LABEL}) DETACH DELETE n")
            s.run(f"DROP INDEX {_INDEX} IF EXISTS")
    finally:
        driver.close()

    return {
        "status": "ok",
        "n_stored": n_stored, "n_new": n_new, "knn_k": knn_k, "window": window,
        "native_recall": round(native_hit / len(truth), 3),
        "window_reachable": round(window_reachable, 3),
        "native_knn_p50_ms": round(float(np.percentile(lat, 50)), 3),
        "native_knn_p95_ms": round(float(np.percentile(lat, 95)), 3),
    }
