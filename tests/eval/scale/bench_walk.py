"""graph_walk hub-degree cliff benchmark (P1.2).

Loads a synthetic degree-skewed graph into a LOCAL Neo4j (dev stack —
NOT production) and times the variable-length walk
``(e)-[r*1..hops]-(m)`` from a low-degree node vs a planted hub.  The
walk enumerates paths before the LIMIT applies, so on a hub the cost
grows ~degree^hops — this benchmark makes that cliff visible and lets
you find the degree threshold worth capping.

Skips cleanly when no Neo4j is reachable.  Writes into an isolated label
(``:_ScaleBench``) and deletes it afterwards, so it never touches real
``:__Entity__`` data — but still: point it at a dev Neo4j, never prod.
"""
from __future__ import annotations

import time

from tests.eval.scale.synth import gen_edges

_LABEL = "_ScaleBench"


def _driver(uri: str, user: str, password: str, timeout: float = 4.0):
    try:
        from neo4j import GraphDatabase

        d = GraphDatabase.driver(uri, auth=(user, password))
        d.verify_connectivity()
        return d
    except Exception:
        return None


def _load_graph(session, edges: list[tuple[int, int]]) -> None:
    session.run(f"MATCH (n:{_LABEL}) DETACH DELETE n")
    # Batch insert via UNWIND.
    batch = [{"a": a, "b": b} for a, b in edges]
    for i in range(0, len(batch), 5000):
        session.run(
            f"UNWIND $rows AS r "
            f"MERGE (x:{_LABEL} {{nid: r.a}}) "
            f"MERGE (y:{_LABEL} {{nid: r.b}}) "
            f"MERGE (x)-[:_REL]-(y)",
            rows=batch[i:i + 5000],
        )


def _time_walk(session, nid: int, hops: int, node_cap: int, reps: int) -> float:
    cypher = (
        f"MATCH (e:{_LABEL} {{nid: $nid}}) "
        f"CALL {{ WITH e MATCH p = (e)-[r*1..{hops}]-(m:{_LABEL}) "
        f"RETURN m LIMIT {node_cap} }} "
        f"RETURN count(DISTINCT m) AS c"
    )
    times: list[float] = []
    for _ in range(reps):
        t0 = time.perf_counter()
        session.run(cypher, nid=nid).consume()
        times.append((time.perf_counter() - t0) * 1000.0)
    times.sort()
    return times[len(times) // 2]  # median ms


def bench_hub_walk(
    *,
    n_nodes: int = 20000,
    avg_degree: float = 4.0,
    hub_degrees: tuple[int, ...] = (500, 1000, 5000),
    hops: int = 2,
    node_cap: int = 50,
    reps: int = 5,
    uri: str = "bolt://localhost:7687",
    user: str = "neo4j",
    password: str = "changeme",
) -> dict:
    """Load a synthetic graph and time the walk from a normal node vs
    each planted hub.  Returns a result dict; ``status='skipped'`` when
    no Neo4j is reachable."""
    driver = _driver(uri, user, password)
    if driver is None:
        return {"status": "skipped", "reason": f"no Neo4j at {uri}"}

    edges, hubs = gen_edges(
        n_nodes=n_nodes, avg_degree=avg_degree, hub_degrees=hub_degrees,
    )
    rows: list[dict] = []
    try:
        with driver.session() as s:
            _load_graph(s, edges)
            # A normal (non-hub) node as baseline.
            normal_nid = n_nodes - 1
            base = _time_walk(s, normal_nid, hops, node_cap, reps)
            rows.append({"node": "normal", "degree": "~avg", "walk_p50_ms": round(base, 2)})
            for hub_nid, deg in zip(hubs, hub_degrees):
                ms = _time_walk(s, hub_nid, hops, node_cap, reps)
                rows.append({
                    "node": f"hub#{hub_nid}", "degree": deg,
                    "walk_p50_ms": round(ms, 2),
                    "slowdown_vs_normal": round(ms / base, 1) if base else None,
                })
            s.run(f"MATCH (n:{_LABEL}) DETACH DELETE n")
    finally:
        driver.close()
    return {"status": "ok", "hops": hops, "node_cap": node_cap, "rows": rows}
