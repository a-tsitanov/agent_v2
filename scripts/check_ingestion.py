"""Diagnostic — show what landed in each backend after ingestion.

Pings every storage backend in turn:
  * Postgres — count + status breakdown of `documents` rows.
  * Milvus   — collection stats.
  * Neo4j    — entity/relationship counts (when reachable).

Usage::

    python -m scripts.check_ingestion
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncio

import psycopg

from src.config import settings

_SEP = "─" * 70


async def check_postgres() -> None:
    print(_SEP)
    print("Postgres — documents")
    print(_SEP)
    try:
        async with await psycopg.AsyncConnection.connect(
            settings.postgres.dsn, connect_timeout=5,
        ) as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT status, COUNT(*) FROM documents GROUP BY status"
            )
            rows = await cur.fetchall()
        if not rows:
            print("  (no rows yet)\n")
            return
        for status, n in rows:
            print(f"  {status:12s} {n}")
    except Exception as exc:
        print(f"  unreachable: {exc}\n")


def check_milvus() -> None:
    from pymilvus import MilvusClient

    print(_SEP)
    print("Milvus")
    print(_SEP)
    cfg = settings.milvus
    try:
        client = MilvusClient(uri=cfg.uri, timeout=cfg.timeout_s)
        try:
            collections = client.list_collections()
            print(f"  collections: {collections}")
            if cfg.collection in collections:
                info = client.get_collection_stats(cfg.collection)
                print(f"  {cfg.collection}: {info}")
        finally:
            client.close()
    except Exception as exc:
        print(f"  unreachable: {exc}")


def check_neo4j() -> None:
    print(_SEP)
    print("Neo4j")
    print(_SEP)
    try:
        from neo4j import GraphDatabase

        cfg = settings.neo4j
        driver = GraphDatabase.driver(
            cfg.uri,
            auth=(cfg.user, cfg.password.get_secret_value()),
        )
        try:
            with driver.session(database=cfg.database) as session:
                node_count = session.run(
                    "MATCH (n) RETURN COUNT(n) AS c"
                ).single()["c"]
                rel_count = session.run(
                    "MATCH ()-[r]->() RETURN COUNT(r) AS c"
                ).single()["c"]
                print(f"  nodes: {node_count}")
                print(f"  relations: {rel_count}")
        finally:
            driver.close()
    except Exception as exc:
        print(f"  unreachable: {exc}")


def check_events() -> None:
    from neo4j import GraphDatabase

    print(_SEP)
    print("Neo4j — E2 events / time-frames")
    print(_SEP)
    nj = settings.neo4j
    try:
        auth = (nj.user, nj.password.get_secret_value())
        with GraphDatabase.driver(nj.uri, auth=auth) as driver, driver.session(database=nj.database) as s:
            row = s.run(
                "MATCH (e:__Entity__:EventOrAction) RETURN count(e) AS total, "
                "count(e.event_ts_raw) AS ts_present, count(e.event_start_epoch) AS ts_resolved"
            ).single()
            print(f"  events {row['total']}  ts_present {row['ts_present']}  ts_resolved {row['ts_resolved']}")
            for r in s.run(
                "MATCH (e:__Entity__:EventOrAction) "
                "WHERE e.event_ts_raw IS NOT NULL AND e.event_start_epoch IS NULL "
                "RETURN e.event_ts_raw AS raw, count(*) AS n ORDER BY n DESC LIMIT 15"
            ):
                print(f"    unresolved ×{r['n']}: {r['raw']!r}")
    except Exception as exc:
        print(f"  unreachable: {exc}")


async def main() -> None:
    await check_postgres()
    check_milvus()
    check_neo4j()
    check_events()


if __name__ == "__main__":
    asyncio.run(main())
