"""Operator-side smoke for :mod:`src.storage.wikibase`.

Pushes a tiny fake corpus (1 Person + 1 PhoneNumber + the relation
between them) into the local Wikibase via :func:`push_entities`,
then prints the resulting QID for visual verification in the
Wikibase UI (http://localhost:8181/wiki/Item:<QID>).

Loads the base-class + property caches from Neo4j (populated by
``scripts/setup_wikibase.py``) so the smoke exercises the real
lookup path.

Usage::

    uv run python -m scripts.smoke_wikibase_push

Requires:
  * docker compose up wikibase neo4j
  * uv run python -m scripts.setup_wikibase  (one-time)
  * ``WIKIBASE_BOT_USER`` / ``WIKIBASE_BOT_PASSWORD`` set in .env
    OR ``WIKIBASE_ADMIN_USER`` / ``WIKIBASE_ADMIN_PASS`` so we can
    fall back to admin creds (the bootstrap also did this).
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from llama_index.core.graph_stores.types import EntityNode, Relation
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv()

from src.config import WikibaseSettings, settings  # noqa: E402
from src.graph.store import build_neo4j_graph_store  # noqa: E402
from src.storage.wikibase import AsyncWikibase, push_entities  # noqa: E402
from src.utils.logging import configure_logging  # noqa: E402


def _load_cache_from_neo4j() -> tuple[dict[str, str], dict[str, str]]:
    """Pull cached base-class QIDs + property PIDs from Neo4j."""
    gs = build_neo4j_graph_store()
    base = gs.structured_query(
        "MATCH (b:WikibaseBaseClass) RETURN b.label AS label, b.qid AS qid",
    )
    base_qids = {r["label"]: r["qid"] for r in base if r.get("qid")}
    props = gs.structured_query(
        "MATCH (p:WikibaseProperty) RETURN p.label AS label, p.pid AS pid",
    )
    property_pids = {r["label"]: r["pid"] for r in props if r.get("pid")}
    return base_qids, property_pids


async def main() -> int:
    configure_logging(level="info", json_output=False)

    # Allow admin-creds fallback like setup_wikibase.py does.
    admin_user = os.environ.get("WIKIBASE_ADMIN_USER")
    admin_pass = os.environ.get("WIKIBASE_ADMIN_PASS")
    if admin_user and admin_pass:
        cfg = WikibaseSettings(
            base_url=settings.wikibase.base_url,
            bot_user=admin_user,
            bot_password=admin_pass,
            language=settings.wikibase.language,
        )
    else:
        cfg = settings.wikibase

    logger.info("smoke  base_url={u}  user={n}", u=cfg.base_url, n=cfg.bot_user)

    base_qids, property_pids = _load_cache_from_neo4j()
    logger.info(
        "smoke  cache  base_qids={b}  property_pids={p}",
        b=len(base_qids), p=len(property_pids),
    )

    wb = await AsyncWikibase.from_settings(cfg)

    person = EntityNode(
        name="Анна Морозова (smoke)",
        label="Person",
        properties={"description": "Smoke-test entity", "mention_count": 1},
    )
    phone = EntityNode(name="+74951234567", label="PhoneNumber")
    rel = Relation(
        label="has_phone", source_id=person.id, target_id=phone.id,
    )

    gs = build_neo4j_graph_store()
    counts = await push_entities(
        entities=[person, phone],
        relations=[rel],
        neo4j_store=gs,
        wb_client=wb,
        base_class_qids=base_qids,
        property_pids=property_pids,
    )

    logger.info("smoke done  counts={c}", c=counts)
    # The QID we just wrote is stamped onto the Neo4j node.
    rows = gs.structured_query(
        "MATCH (n:__Entity__ {name: $name}) "
        "RETURN n.wikibase_qid AS qid",
        param_map={"name": person.name},
    )
    if rows:
        qid = rows[0].get("qid")
        logger.info(
            "smoke  view item at  http://localhost:8181/wiki/Item:{q}",
            q=qid,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
