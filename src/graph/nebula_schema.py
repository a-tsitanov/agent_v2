"""NebulaGraph schema for the KB graph (nGQL DDL).

Mirrors the Neo4j model: `:__Entity__` -> tag `Entity`; typed rels ->
same-named edge types.  Vectors (`er_vec`/`report_vec`) are intentionally
absent — they live in Milvus after Phase 3.  All statements are
IF NOT EXISTS so `ensure_schema` is safe to re-run on every boot (matches
the fail-open `ensure_*` DDL helpers in `src/graph/index.py`).

nGQL scoping note: tag/edge/index DDL is scoped to the currently selected
graph space — a session must issue `USE <space>` before any `CREATE TAG` /
`CREATE EDGE` / `CREATE TAG INDEX`. `CREATE SPACE` is also asynchronous: the
space is not immediately usable, so the `USE` that follows it must retry
across the propagation window. `ensure_schema` below handles both.
"""

from __future__ import annotations

import time
from typing import Any

SPACE_NAME = "kb"

# 128-bit VID: the 32-hex-char blake2b digest of the entity name
# (`entity_vid` in nebula_store.py), set at write time. FIXED_STRING(32)
# fits it exactly. Chosen over INT64 because a 64-bit hash has
# non-negligible birthday-collision probability at the billions-of-entities
# target, and a VID collision silently merges two distinct entities.
# vid_type is fixed at space creation, so this must be right before any load.
SPACE_DDL = (
    f"CREATE SPACE IF NOT EXISTS `{SPACE_NAME}` "
    "(partition_num=100, replica_factor=1, vid_type=FIXED_STRING(32));"
)

# In-space DDL only — requires `USE `{SPACE_NAME}`;` to have already
# succeeded on the session. Do NOT add CREATE SPACE or USE here.
SCHEMA_DDL: list[str] = [
    # `label string DEFAULT ''` is intentional: the Phase-1 write adapter
    # uses it to store the entity type (mirrors Neo4j's node label).
    "CREATE TAG IF NOT EXISTS `Entity` ("
    "name string, description string, mention_count int DEFAULT 0, "
    "created_at int DEFAULT 0, label string DEFAULT '');",
    "CREATE EDGE IF NOT EXISTS `RELATED` ("
    "rel_type string DEFAULT '', polarity string DEFAULT '', "
    "valid_from int DEFAULT 0, valid_to int DEFAULT 0);",
    "CREATE EDGE IF NOT EXISTS `MENTIONS` (doc_id string DEFAULT '');",
    "CREATE EDGE IF NOT EXISTS `IN_COMMUNITY` (level int DEFAULT 0);",
    "CREATE EDGE IF NOT EXISTS `PARENT_OF` ();",
    # tag/edge indexes needed for full-scan + lookups (Nebula requires an
    # index to LOOKUP by property; traversals from a known VID do not).
    "CREATE TAG INDEX IF NOT EXISTS `entity_name_idx` ON `Entity`(name(256));",
]


def _execute_with_retry(session: Any, stmt: str, *, attempts: int, delay_s: float) -> bool:
    """Execute `stmt`, retrying up to `attempts` times on failure.

    Returns True on the first `resp.is_succeeded()`, False if all attempts
    are exhausted (a warning is logged in that case). Never raises.
    """
    from loguru import logger

    for attempt in range(1, attempts + 1):
        resp = session.execute(stmt)
        if resp.is_succeeded():
            return True
        if attempt < attempts:
            time.sleep(delay_s)
    logger.warning(
        "nebula ensure_schema: {s} -> {e} (exhausted {n} attempt(s))",
        s=stmt[:60],
        e=resp.error_msg(),
        n=attempts,
    )
    return False


def ensure_schema(session: Any, *, use_attempts: int = 30, use_delay_s: float = 1.0) -> None:
    """Execute SPACE_DDL then SCHEMA_DDL on an open nebula3 session.

    Fail-open overall (logs warnings, never raises) to match the project's
    `ensure_*` DDL convention. Space creation is async, so the `USE` that
    selects it is retried across the propagation window; nothing downstream
    can succeed without a selected space, so `ensure_schema` returns early
    if `USE` never succeeds.
    """
    from loguru import logger

    # 1. Create the space (fail-open: log and continue regardless — it may
    # already exist, or errors here are surfaced again by the USE retry).
    _execute_with_retry(session, SPACE_DDL, attempts=1, delay_s=0)

    # 2. Select the space, retrying to cover the async creation window.
    if not _execute_with_retry(
        session, f"USE `{SPACE_NAME}`;", attempts=use_attempts, delay_s=use_delay_s
    ):
        logger.warning(
            "nebula ensure_schema: could not USE `{s}`; skipping tag/edge/index DDL",
            s=SPACE_NAME,
        )
        return

    # 3. Only now run in-space DDL (tag -> index propagation can also lag
    # briefly, hence the small per-statement retry).
    for stmt in SCHEMA_DDL:
        _execute_with_retry(session, stmt, attempts=3, delay_s=1.0)

    # 4. Storage-side schema propagation lags meta by ~one heartbeat: a
    # `CREATE TAG`/`CREATE EDGE` — and even `DESCRIBE TAG` — succeeds BEFORE
    # an `INSERT` against that tag works ("No schema found"). So `DESCRIBE`
    # is NOT a reliable readiness signal; the only one is a real write
    # landing. Probe with a sentinel `Entity` vertex until it succeeds, then
    # remove it, so the first caller write doesn't race propagation.
    probe = "__kb_schema_probe__"
    probe_insert = (
        "INSERT VERTEX `Entity` (name, description, mention_count, created_at, label) "
        f'VALUES "{probe}":("", "", 0, 0, "");'
    )
    for attempt in range(1, use_attempts + 1):
        if session.execute(probe_insert).is_succeeded():
            session.execute(f'DELETE VERTEX "{probe}";')
            return
        if attempt < use_attempts:
            time.sleep(use_delay_s)
    logger.warning(
        "nebula ensure_schema: `Entity` tag not write-ready after {n} attempt(s)",
        n=use_attempts,
    )
