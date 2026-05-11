"""DESTRUCTIVE — wipe all data stores.

Drops:
  * Postgres   — TRUNCATE `documents` (job-status table).
  * Milvus     — drop the configured collection.
  * Neo4j      — MATCH (n) DETACH DELETE n (all nodes + relations).
  * Filesystem — clear API upload dir + ingestion cache (if local).

Then runs `setup_db.py` so the next ingest finds clean schemas.

The API / worker do not need to be stopped — they'll see empty
stores on the next request.  But any in-flight ingest will fail
mid-flight (its row in `documents` disappears).

Usage::

    uv run python -m scripts.wipe_db                 # interactive confirm
    uv run python -m scripts.wipe_db --yes           # no prompt (CI / scripts)
    uv run python -m scripts.wipe_db --keep-files    # skip uploads/cache
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger  # noqa: E402

from src.config import settings  # noqa: E402
from src.utils.logging import configure_logging  # noqa: E402


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--yes", "-y", action="store_true",
                   help="skip the interactive confirmation prompt.")
    p.add_argument("--keep-files", action="store_true",
                   help="don't touch upload dir / ingestion cache.")
    p.add_argument("--no-setup", action="store_true",
                   help="skip running setup_db after the wipe.")
    return p.parse_args()


# ── Postgres ─────────────────────────────────────────────────────────


def wipe_postgres() -> None:
    import psycopg

    pg = settings.postgres
    logger.info("postgres wipe  host={h}:{p}  db={d}",
                h=pg.host, p=pg.port, d=pg.db)
    with psycopg.connect(
        pg.dsn, connect_timeout=pg.connect_timeout_s, autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("TRUNCATE TABLE documents")
                logger.info("postgres  TRUNCATE documents  ok")
            except psycopg.errors.UndefinedTable:
                logger.info("postgres  documents table absent — nothing to wipe")


# ── Milvus ───────────────────────────────────────────────────────────


def wipe_milvus() -> None:
    from pymilvus import MilvusClient

    mv = settings.milvus
    client = MilvusClient(uri=mv.uri, timeout=mv.timeout_s)
    try:
        existing = client.list_collections()
        if mv.collection in existing:
            client.drop_collection(mv.collection)
            logger.info("milvus  dropped collection={c}", c=mv.collection)
        else:
            logger.info("milvus  collection={c} absent — nothing to wipe",
                        c=mv.collection)
    finally:
        client.close()


# ── Neo4j ────────────────────────────────────────────────────────────


def wipe_neo4j() -> None:
    from neo4j import GraphDatabase

    nj = settings.neo4j
    logger.info("neo4j wipe  uri={u}  db={d}", u=nj.uri, d=nj.database)
    auth = (nj.user, nj.password.get_secret_value())
    with GraphDatabase.driver(nj.uri, auth=auth) as driver:
        with driver.session(database=nj.database) as s:
            # CALL APOC if available is faster on big graphs;
            # default Cypher works on stock Neo4j too.
            res = s.run("MATCH (n) DETACH DELETE n RETURN count(*) AS n")
            row = res.single()
            n = row["n"] if row else 0
            logger.info("neo4j  detach-deleted nodes (sweep total: {n})", n=n)
            # Drop any persisted indexes / constraints LightRAG /
            # PropertyGraphIndex created so the next build is clean.
            try:
                idx_res = s.run("SHOW INDEXES YIELD name")
                names = [r["name"] for r in idx_res]
                for name in names:
                    # Skip system / lookup indexes Neo4j ships with.
                    if name.startswith("__org_neo4j") or name == "index_343aff4e":
                        continue
                    try:
                        s.run(f"DROP INDEX `{name}`")
                        logger.info("neo4j  dropped index {n}", n=name)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("neo4j  drop index {n} failed: {e}",
                                       n=name, e=exc)
            except Exception as exc:  # noqa: BLE001
                logger.warning("neo4j  list indexes failed: {e}", e=exc)


# ── files ────────────────────────────────────────────────────────────


def wipe_files() -> None:
    targets = [
        Path(settings.api.upload_dir),
        Path(settings.ingestion.cache_dir),
    ]
    for t in targets:
        if t.exists():
            shutil.rmtree(t, ignore_errors=True)
            logger.info("files  removed {p}", p=t)
        else:
            logger.info("files  {p} absent — nothing to wipe", p=t)


def confirm() -> bool:
    print(
        "\n  Will WIPE:\n"
        f"    * Postgres `documents` table @ {settings.postgres.host}:{settings.postgres.port}/{settings.postgres.db}\n"
        f"    * Milvus collection `{settings.milvus.collection}` @ {settings.milvus.uri}\n"
        f"    * Neo4j graph @ {settings.neo4j.uri} db={settings.neo4j.database}\n"
        f"    * Files: {settings.api.upload_dir}, {settings.ingestion.cache_dir}\n"
    )
    ans = input("  Type 'yes' to continue: ").strip().lower()
    return ans == "yes"


def main() -> int:
    args = _parse_args()
    configure_logging(level=settings.api.log_level, json_output=False)

    if not args.yes and not confirm():
        print("aborted.")
        return 1

    wipe_postgres()
    wipe_milvus()
    wipe_neo4j()
    if not args.keep_files:
        wipe_files()

    if not args.no_setup:
        # Recreate Postgres schema + verify Milvus connectivity.
        from scripts.setup_db import main as setup_main
        logger.info("running setup_db to recreate schemas")
        setup_main()

    logger.info("wipe complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
