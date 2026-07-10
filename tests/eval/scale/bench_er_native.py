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

Task 4 adds a Milvus comparison arm (``bench_milvus_vs_native``, plus the
``bench_native_vs_milvus`` convenience wrapper that runs both) — the same
synthetic corpus/query set run through ``MilvusEntityVectorStore.knn``
instead of the Neo4j native vector index.  This is the read path
NebulaGraph uses in production (see
``src.graph.entity_vector_store.build_entity_vector_store``), so its
recall@k + p95 latency can be read side-by-side with the native numbers
above.  Skips cleanly (``status: skipped``) when no local Milvus is
reachable; writes to a throwaway UUID-suffixed collection, dropped after
the run — never touches the real ``entity_er_vec`` collection.  Dev
Milvus only, never prod.
"""
from __future__ import annotations

import time
import uuid

import numpy as np

from src.config import settings
from tests.eval.scale.synth import gen_vectors

_LABEL = "_ScaleBenchER"
_INDEX = "scalebench_er_vec"
_MILVUS_COLLECTION_PREFIX = "scalebench_er_vec"
_MILVUS_DEFAULT_URI = "http://localhost:19530"


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


def _milvus_client(uri: str, timeout: float = 4.0):
    """Return a connected MilvusClient or None if unreachable (mirrors
    ``bench_milvus.py``'s skip convention)."""
    try:
        from pymilvus import MilvusClient

        c = MilvusClient(uri=uri, timeout=timeout)
        c.list_collections()  # forces a real round-trip
        return c
    except Exception:
        return None


def bench_milvus_vs_native(
    *,
    n_stored: int = 50000,
    dim: int | None = None,
    n_new: int = 200,
    knn_k: int = 10,
    milvus_uri: str = _MILVUS_DEFAULT_URI,
) -> dict:
    """Milvus arm of the native-vs-Milvus parity comparison (Task 4).

    Builds the SAME synthetic corpus/query set as ``bench_native_vs_window``
    (identical seed=5) but resolves candidates through
    ``MilvusEntityVectorStore.knn`` against a throwaway collection instead
    of the Neo4j native vector index — the ER candidate-kNN read path
    NebulaGraph uses in production
    (``src.graph.entity_vector_store.build_entity_vector_store``).

    ``dim`` defaults to ``settings.milvus.dim`` because
    ``MilvusEntityVectorStore`` builds its collection schema from that
    config value at ``_ensure()`` time (not a per-call argument) — pass an
    explicit ``dim`` only if it matches your configured ``MILVUS_DIM``,
    otherwise inserts fail on a dimension mismatch.

    Skips cleanly (``status: skipped``) when no local Milvus is reachable.
    Never touches the real ``entity_er_vec`` collection — writes to a
    throwaway UUID-suffixed collection and drops it when done.
    """
    client = _milvus_client(milvus_uri)
    if client is None:
        return {"status": "skipped", "reason": f"no Milvus at {milvus_uri}"}

    from src.graph.entity_vector_store_milvus import MilvusEntityVectorStore

    resolved_dim = dim if dim is not None else settings.milvus.dim
    corpus, queries = gen_vectors(n=n_stored, dim=resolved_dim, n_queries=n_new, seed=5)
    rng = np.random.default_rng(5)
    mention_counts = rng.integers(1, 50, size=n_stored)

    # Same ground truth as the native arm: true nearest stored id per query.
    truth = [int(np.argmax(corpus @ q)) for q in queries]

    collection = f"{_MILVUS_COLLECTION_PREFIX}_{uuid.uuid4().hex[:8]}"
    store = MilvusEntityVectorStore(client=client, collection=collection)
    try:
        entities = [{
            "name": f"bench-{i:07d}",
            "label": "Other",
            "embedding": corpus[i].tolist(),
            "mention_count": int(mention_counts[i]),
            "description": "",
        } for i in range(n_stored)]
        # Batched upsert: keeps each gRPC message well under Milvus's 64 MB
        # cap (mirrors bench_milvus.py's _INSERT_BATCH convention).
        for j in range(0, len(entities), 2000):
            store.upsert(entities[j:j + 2000])
        name_to_id = {e["name"]: i for i, e in enumerate(entities)}

        lat: list[float] = []
        milvus_hit = 0
        for q, t in zip(queries, truth):
            t0 = time.perf_counter()
            hits = store.knn(q.tolist(), knn_k)
            lat.append((time.perf_counter() - t0) * 1000.0)
            hit_ids = {name_to_id.get(h["name"]) for h in hits}
            if t in hit_ids:
                milvus_hit += 1
    finally:
        try:
            client.drop_collection(collection_name=collection)
        except Exception:
            pass
        client.close()

    return {
        "status": "ok",
        "n_stored": n_stored, "n_new": n_new, "knn_k": knn_k, "dim": resolved_dim,
        "milvus_recall": round(milvus_hit / len(truth), 3),
        "milvus_knn_p50_ms": round(float(np.percentile(lat, 50)), 3),
        "milvus_knn_p95_ms": round(float(np.percentile(lat, 95)), 3),
    }


def bench_native_vs_milvus(
    *,
    n_stored: int = 50000,
    dim: int | None = None,
    n_new: int = 200,
    knn_k: int = 10,
    window: int = 5000,
    neo4j_uri: str = "bolt://localhost:7687",
    neo4j_user: str = "neo4j",
    neo4j_password: str = "changeme",
    milvus_uri: str = _MILVUS_DEFAULT_URI,
) -> dict:
    """Run both arms on the SAME dimensionality and return them
    side-by-side: native Neo4j-index kNN (``bench_native_vs_window``) vs
    Milvus kNN (``bench_milvus_vs_native``) — the parity comparison this
    task delivers.  ``dim`` defaults to ``settings.milvus.dim`` so the
    Milvus arm's collection schema matches without extra config; the same
    value is used for the native arm so recall/p95 are comparable.

    Either arm's own ``status: skipped`` is returned untouched when its
    infra isn't reachable — this never raises.
    """
    resolved_dim = dim if dim is not None else settings.milvus.dim
    native = bench_native_vs_window(
        n_stored=n_stored, dim=resolved_dim, n_new=n_new, knn_k=knn_k,
        window=window, uri=neo4j_uri, user=neo4j_user, password=neo4j_password,
    )
    milvus = bench_milvus_vs_native(
        n_stored=n_stored, dim=resolved_dim, n_new=n_new, knn_k=knn_k,
        milvus_uri=milvus_uri,
    )
    return {"native": native, "milvus": milvus}
