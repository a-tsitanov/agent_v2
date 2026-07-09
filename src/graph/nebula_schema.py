"""NebulaGraph schema for the KB graph (nGQL DDL).

Mirrors the Neo4j model: `:__Entity__` -> tag `Entity`; typed rels ->
same-named edge types.  Vectors (`er_vec`/`report_vec`) are intentionally
absent — they live in Milvus after Phase 3.  All statements are
IF NOT EXISTS so `ensure_schema` is safe to re-run on every boot (matches
the fail-open `ensure_*` DDL helpers in `src/graph/index.py`).
"""

from __future__ import annotations

from typing import Any

SPACE_NAME = "kb"

SCHEMA_DDL: list[str] = [
    # int64 VID via a stable hash of the entity name (set at write time).
    f"CREATE SPACE IF NOT EXISTS `{SPACE_NAME}` "
    "(partition_num=100, replica_factor=1, vid_type=INT64);",
    "CREATE TAG IF NOT EXISTS `Entity` ("
    "name string, description string, mention_count int DEFAULT 0, "
    "created_at int DEFAULT 0, label string DEFAULT '');",
    "CREATE EDGE IF NOT EXISTS `RELATED` ("
    "polarity string DEFAULT '', valid_from int DEFAULT 0, valid_to int DEFAULT 0);",
    "CREATE EDGE IF NOT EXISTS `MENTIONS` (doc_id string DEFAULT '');",
    "CREATE EDGE IF NOT EXISTS `IN_COMMUNITY` (level int DEFAULT 0);",
    "CREATE EDGE IF NOT EXISTS `PARENT_OF` ();",
    # tag/edge indexes needed for full-scan + lookups (Nebula requires an
    # index to LOOKUP by property; traversals from a known VID do not).
    "CREATE TAG INDEX IF NOT EXISTS `entity_name_idx` ON `Entity`(name(256));",
]


def ensure_schema(session: Any) -> None:
    """Execute SCHEMA_DDL on an open nebula3 session (fail-open, logged)."""
    from loguru import logger

    for stmt in SCHEMA_DDL:
        resp = session.execute(stmt)
        if not resp.is_succeeded():
            logger.warning("nebula ensure_schema: {s} -> {e}", s=stmt[:60], e=resp.error_msg())
