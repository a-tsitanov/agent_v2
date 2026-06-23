"""Neo4j concurrent-write contention benchmark (Track A baseline).

Reproduces the merge/graph-phase freeze: many documents in flight
(``INGEST_ADMISSION_MAX_INFLIGHT`` > 1) all ``MERGE`` into the SAME
canonical ``__Entity__`` hub nodes from different transactions, taking
node write-locks that serialise — and, with the ``ON MATCH SET`` on a
shared node plus a relation onto a growing hub, deadlock outright
(``Neo.TransientError.Transaction.DeadlockDetected``).  This is the
900%-CPU trigger from ``perf_freeze_merge_graph``; the bench makes the
cost of raising K visible BEFORE any tuning lands, so A1-A3 can be
measured against this baseline.

Mirrors ``bench_walk``: connects to a LOCAL dev Neo4j (never prod),
writes into an isolated ``:_ScaleBenchWrite`` label, cleans up after,
and returns ``status='skipped'`` (never fails) when no Neo4j is up.

Each "writer" is a thread (the neo4j sync driver releases the GIL on
I/O, and the driver is thread-safe — one session per thread).  Sweeping
``writers`` from 1 upward shows the contention cliff: with a single
writer there are no cross-transaction locks; at K>1 throughput should
flatten or collapse and deadlocks appear.  Pass ``with_retry=True`` to
measure the A3 deadlock-retry wrapper against the no-retry baseline.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np

_LABEL = "_ScaleBenchWrite"
# Neo4j status codes that mean "transient write contention — safe to
# retry the whole transaction" (deadlock + lock-client races).
_RETRYABLE_CODES: frozenset[str] = frozenset({
    "Neo.TransientError.Transaction.DeadlockDetected",
    "Neo.TransientError.Transaction.LockClientStopped",
})


def _driver(uri: str, user: str, password: str):
    d = None
    try:
        from neo4j import GraphDatabase

        d = GraphDatabase.driver(uri, auth=(user, password))
        d.verify_connectivity()
        return d
    except Exception:
        if d is not None:
            d.close()  # don't leak a half-open driver on the skip path
        return None


def _is_retryable(exc: Exception) -> bool:
    code = getattr(exc, "code", "") or ""
    return code in _RETRYABLE_CODES


def _gen_workload(
    *,
    writers: int,
    rounds: int,
    batch: int,
    n_hubs: int,
    seed: int,
) -> list[list[list[dict]]]:
    """Per-writer list of rounds; each round is a batch of
    ``{"hub": int, "local": str}`` rows.

    Every row MERGEs one of ``n_hubs`` SHARED hub keys (the contention
    surface — all writers fight over the same small set) and one
    writer-local leaf key (no contention), plus a relation leaf→hub so
    the hub grows into a supernode.  A small hub set + many writers =
    maximal lock overlap, which is the real shape (shared phones / orgs).
    """
    rng = np.random.default_rng(seed)
    work: list[list[list[dict]]] = []
    for w in range(writers):
        wrounds: list[list[dict]] = []
        for r in range(rounds):
            rows = [
                {
                    "hub": int(rng.integers(0, n_hubs)),
                    "local": f"w{w}-r{r}-{i}",
                }
                for i in range(batch)
            ]
            wrounds.append(rows)
        work.append(wrounds)
    return work


# MERGE shared hub (with a write on MATCH → forces a node write-lock
# even when the node already exists), MERGE writer-local leaf, MERGE the
# leaf→hub relation (grows the hub into a supernode).  This is the
# inject_canonical / upsert_nodes contention pattern in miniature.
_WRITE_CYPHER = (
    f"UNWIND $rows AS row "
    f"MERGE (h:{_LABEL} {{key: 'hub-' + toString(row.hub)}}) "
    f"  ON CREATE SET h.mention_count = 1 "
    f"  ON MATCH SET h.mention_count = h.mention_count + 1 "
    f"MERGE (e:{_LABEL} {{key: row.local}}) "
    f"MERGE (e)-[:_REL]->(h)"
)


def _run_writer(
    driver,
    rounds: list[list[dict]],
    *,
    with_retry: bool,
    max_retries: int,
) -> dict:
    """Execute one writer's rounds in its own session.  Returns timing +
    deadlock/retry counters for this writer."""
    latencies: list[float] = []
    deadlocks = 0
    retried = 0
    failed = 0
    with driver.session() as s:
        for rows in rounds:
            t0 = time.perf_counter()
            attempt = 0
            while True:
                try:
                    s.run(_WRITE_CYPHER, rows=rows).consume()
                    break
                except Exception as exc:
                    if _is_retryable(exc):
                        deadlocks += 1
                        if with_retry and attempt < max_retries:
                            attempt += 1
                            retried += 1
                            # Bounded backoff; jitter by attempt only
                            # (no wall-clock RNG) to keep it deterministic.
                            time.sleep(0.02 * attempt)
                            continue
                    failed += 1
                    break
            latencies.append((time.perf_counter() - t0) * 1000.0)
    return {
        "latencies_ms": latencies,
        "deadlocks": deadlocks,
        "retried": retried,
        "failed": failed,
    }


def _pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    return float(np.percentile(values, q))


def bench_graph_write(
    *,
    writers_sweep: tuple[int, ...] = (1, 2, 4, 8),
    rounds: int = 20,
    batch: int = 25,
    n_hubs: int = 16,
    with_retry: bool = False,
    max_retries: int = 5,
    uri: str = "bolt://localhost:7687",
    user: str = "neo4j",
    password: str = "changeme",
    seed: int = 7,
) -> dict:
    """For each concurrency level in ``writers_sweep``, run ``writers``
    threads each MERGE-ing ``rounds`` batches into a shared hub set and
    report throughput + deadlocks.

    Returns a result dict; ``status='skipped'`` when no Neo4j is
    reachable.  ``n_hubs`` is deliberately small so writers contend over
    the same nodes — raise it to dilute contention, lower it to intensify.
    """
    driver = _driver(uri, user, password)
    if driver is None:
        return {"status": "skipped", "reason": f"no Neo4j at {uri}"}

    rows_out: list[dict] = []
    try:
        for writers in writers_sweep:
            with driver.session() as s:
                s.run(f"MATCH (n:{_LABEL}) DETACH DELETE n").consume()
            work = _gen_workload(
                writers=writers, rounds=rounds, batch=batch,
                n_hubs=n_hubs, seed=seed,
            )
            t0 = time.perf_counter()
            results: list[dict] = []
            with ThreadPoolExecutor(max_workers=writers) as pool:
                futs = [
                    pool.submit(
                        _run_writer, driver, work[w],
                        with_retry=with_retry, max_retries=max_retries,
                    )
                    for w in range(writers)
                ]
                for f in as_completed(futs):
                    results.append(f.result())
            wall = time.perf_counter() - t0

            all_lat = [x for r in results for x in r["latencies_ms"]]
            entities = writers * rounds * batch  # leaf MERGEs attempted
            deadlocks = sum(r["deadlocks"] for r in results)
            failed = sum(r["failed"] for r in results)
            retried = sum(r["retried"] for r in results)
            rows_out.append({
                "writers": writers,
                "wall_s": round(wall, 3),
                "entities_per_s": round(entities / wall, 1) if wall else None,
                "round_p50_ms": round(_pct(all_lat, 50), 1),
                "round_p95_ms": round(_pct(all_lat, 95), 1),
                "deadlocks": deadlocks,
                "retried": retried,
                "failed": failed,
            })
        with driver.session() as s:
            s.run(f"MATCH (n:{_LABEL}) DETACH DELETE n").consume()
    finally:
        driver.close()

    return {
        "status": "ok",
        "rounds": rounds,
        "batch": batch,
        "n_hubs": n_hubs,
        "with_retry": with_retry,
        "rows": rows_out,
    }
