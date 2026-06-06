"""Milvus FLAT-vs-HNSW latency + recall benchmark (P1.1).

Runs against a LOCAL Milvus (docker dev stack — NOT production) using
synthetic random vectors: the index doesn't care whether vectors are
real, only how many and what dimension, so this faithfully measures the
FLAT→HNSW win at any target vector count.

FLAT is exhaustive (the llama-index default this project shipped a fix
for) and therefore also the recall ground truth; HNSW recall is measured
against it.

Skips cleanly (returns a ``skipped`` row) when no Milvus is reachable —
mirrors the repo's live-infra test convention.  Bring up the dev stack
(``docker compose up -d milvus``) to get numbers; never point this at
prod.
"""
from __future__ import annotations

import time
import uuid

import numpy as np

from tests.eval.scale.synth import gen_vectors

_DEFAULT_URI = "http://localhost:19530"


def _client(uri: str, timeout: float = 4.0):
    """Return a connected MilvusClient or None if unreachable."""
    try:
        from pymilvus import MilvusClient

        c = MilvusClient(uri=uri, timeout=timeout)
        c.list_collections()  # forces a real round-trip
        return c
    except Exception:
        return None


def _build_and_query(
    client, *, corpus: np.ndarray, queries: np.ndarray, top_k: int,
    index_type: str, metric: str, index_params: dict | None,
    search_params: dict | None,
) -> tuple[list[list[int]], float, float]:
    """Create a collection with the given index, insert corpus, run the
    query set.  Returns ``(result_ids, build_seconds, query_p50_ms)``."""
    from pymilvus import DataType

    name = f"scalebench_{index_type.lower()}_{uuid.uuid4().hex[:8]}"
    dim = corpus.shape[1]

    schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=dim)
    client.create_collection(collection_name=name, schema=schema)

    rows = [{"id": i, "vec": corpus[i].tolist()} for i in range(corpus.shape[0])]
    client.insert(collection_name=name, data=rows)

    ip = client.prepare_index_params()
    ip.add_index(
        field_name="vec", index_type=index_type, metric_type=metric,
        params=index_params or {},
    )
    t0 = time.perf_counter()
    client.create_index(collection_name=name, index_params=ip)
    client.load_collection(collection_name=name)
    build_s = time.perf_counter() - t0

    lat: list[float] = []
    ids: list[list[int]] = []
    for q in queries:
        t1 = time.perf_counter()
        res = client.search(
            collection_name=name, data=[q.tolist()], limit=top_k,
            search_params={"metric_type": metric, "params": search_params or {}},
            output_fields=["id"],
        )
        lat.append((time.perf_counter() - t1) * 1000.0)
        ids.append([int(h["id"]) for h in res[0]])

    client.drop_collection(collection_name=name)
    p50 = float(np.percentile(lat, 50)) if lat else 0.0
    return ids, build_s, p50


def _recall_at_k(approx: list[list[int]], truth: list[list[int]]) -> float:
    hits = total = 0
    for a, t in zip(approx, truth):
        ts = set(t)
        hits += len(ts & set(a))
        total += len(ts)
    return hits / total if total else 0.0


def bench_flat_vs_hnsw(
    *,
    n: int,
    dim: int = 768,
    n_queries: int = 100,
    top_k: int = 10,
    hnsw_m: int = 16,
    hnsw_ef_construction: int = 200,
    hnsw_ef_search: int = 64,
    metric: str = "COSINE",
    uri: str = _DEFAULT_URI,
) -> dict:
    """Compare FLAT (exhaustive, = current default) vs HNSW at ``n``
    vectors.  Returns a result dict; ``{"status": "skipped", ...}`` when
    no local Milvus is reachable."""
    client = _client(uri)
    if client is None:
        return {"status": "skipped", "reason": f"no Milvus at {uri}", "n": n}

    corpus, queries = gen_vectors(n=n, dim=dim, n_queries=n_queries)

    flat_ids, flat_build, flat_p50 = _build_and_query(
        client, corpus=corpus, queries=queries, top_k=top_k,
        index_type="FLAT", metric=metric, index_params={}, search_params={},
    )
    hnsw_ids, hnsw_build, hnsw_p50 = _build_and_query(
        client, corpus=corpus, queries=queries, top_k=top_k,
        index_type="HNSW", metric=metric,
        index_params={"M": hnsw_m, "efConstruction": hnsw_ef_construction},
        search_params={"ef": hnsw_ef_search},
    )
    client.close()
    return {
        "status": "ok",
        "n": n, "dim": dim, "top_k": top_k,
        "flat_query_p50_ms": round(flat_p50, 3),
        "hnsw_query_p50_ms": round(hnsw_p50, 3),
        "speedup": round(flat_p50 / hnsw_p50, 2) if hnsw_p50 else None,
        "hnsw_recall_at_k": round(_recall_at_k(hnsw_ids, flat_ids), 4),
        "flat_build_s": round(flat_build, 3),
        "hnsw_build_s": round(hnsw_build, 3),
    }
