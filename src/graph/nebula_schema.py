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
    # wiki_dirty/wiki_dirty_at/wiki_hash/wiki_synced_at/wiki_page_title/
    # wikibase_qid back the wiki-editor graph ops (nebula-wiki-ops design,
    # Design.1): dirty-flag bookkeeping (mark/select/clear) + article
    # metadata written by the sweep.
    "CREATE TAG IF NOT EXISTS `Entity` ("
    "name string, description string, mention_count int DEFAULT 0, "
    "created_at int DEFAULT 0, label string DEFAULT '', "
    "er_canonical_name string DEFAULT '', first_doc_id string DEFAULT '', "
    "wiki_dirty bool DEFAULT false, wiki_dirty_at int DEFAULT 0, "
    "wiki_hash string DEFAULT '', wiki_synced_at int DEFAULT 0, "
    "wiki_page_title string DEFAULT '', wikibase_qid string DEFAULT '');",
    "CREATE EDGE IF NOT EXISTS `RELATED` ("
    "rel_type string DEFAULT '', polarity string DEFAULT '', "
    "valid_from int DEFAULT 0, valid_to int DEFAULT 0, weight double DEFAULT 1.0);",
    "CREATE EDGE IF NOT EXISTS `MENTIONS` (doc_id string DEFAULT '');",
    "CREATE EDGE IF NOT EXISTS `IN_COMMUNITY` (level int DEFAULT 0);",
    "CREATE EDGE IF NOT EXISTS `PARENT_OF` ();",
    # Community vertices materialised by the BUILD stage of community
    # detection (src/graph/community_writeback.py). Report columns
    # (report/title/summary/summarized_at) are declared now so the SUMMARIZE
    # slice adds only write logic, not a schema migration. `report_vec` is
    # intentionally absent — it lives in Milvus (Phase 3).
    "CREATE TAG IF NOT EXISTS `Community` ("
    "id string, level int DEFAULT 0, member_count int DEFAULT 0, "
    "members_hash string DEFAULT '', updated int DEFAULT 0, "
    "report string DEFAULT '', title string DEFAULT '', "
    "summary string DEFAULT '', summarized_at int DEFAULT 0);",
    # Backs prune_level / prune_all / read_old_reports LOOKUPs (Nebula
    # requires an index to LOOKUP by property).
    "CREATE TAG INDEX IF NOT EXISTS `community_level_idx` ON `Community`(level);",
    # tag/edge indexes needed for full-scan + lookups (Nebula requires an
    # index to LOOKUP by property; traversals from a known VID do not).
    "CREATE TAG INDEX IF NOT EXISTS `entity_name_idx` ON `Entity`(name(256));",
    # Entity-resolution decision cache: backs ER's pairwise same/different
    # verdict lookup so it can skip re-judging a pair (mirrors the neo4j
    # `:ERVerdict {key, same}` node). VID = `verdict_vid(key)`.
    "CREATE TAG IF NOT EXISTS `ERVerdict` ("
    "er_key string, same bool DEFAULT false, updated int DEFAULT 0);",
    "CREATE TAG INDEX IF NOT EXISTS `er_verdict_key_idx` ON `ERVerdict`(er_key(256));",
    # Backs the wiki-sweep's select-dirty LOOKUP (WikiSweepWorkflow /
    # NebulaWikiGraphOps.select_dirty) — Nebula requires an index to LOOKUP
    # by a non-VID property.
    "CREATE TAG INDEX IF NOT EXISTS `entity_wiki_dirty_idx` ON `Entity`(wiki_dirty);",
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


def _probe_tag_write_ready(
    session: Any, tag: str, insert_stmt: str, probe_vid: str, *,
    attempts: int, delay_s: float,
) -> bool:
    """Probe until an INSERT against `tag` lands, then delete the sentinel.

    Storaged schema propagation lags meta by ~1 heartbeat, so CREATE/DESCRIBE
    succeed before an INSERT works ("Schema not exist"/"No schema found"). A
    real write landing is the only reliable readiness signal. Returns True
    once ready; logs a warning and returns False if it never lands within
    `attempts`. Never raises.
    """
    from loguru import logger

    for attempt in range(1, attempts + 1):
        if session.execute(insert_stmt).is_succeeded():
            session.execute(f'DELETE VERTEX "{probe_vid}";')
            return True
        if attempt < attempts:
            time.sleep(delay_s)
    logger.warning(
        "nebula ensure_schema: `{t}` tag not write-ready after {n} attempt(s)",
        t=tag, n=attempts,
    )
    return False


def _probe_edge_write_ready(
    session: Any, edge: str, insert_stmt: str, cleanup_stmts: list[str], *,
    attempts: int, delay_s: float,
) -> bool:
    """Probe until an INSERT against `edge` lands, then run `cleanup_stmts`.

    An `ALTER EDGE ... ADD` (existing space) and a fresh `CREATE EDGE` (new
    space) both propagate to storaged with the same ~1-heartbeat lag as a tag,
    so the first edge-write touching a just-added column can hit
    "Unknown column"/"No schema found" until it propagates. The write path
    (`upsert_relations`) is fail-open, so a raced batch is silently DROPPED —
    wait for readiness here. Never raises.
    """
    from loguru import logger

    for attempt in range(1, attempts + 1):
        if session.execute(insert_stmt).is_succeeded():
            for stmt in cleanup_stmts:
                session.execute(stmt)
            return True
        if attempt < attempts:
            time.sleep(delay_s)
    logger.warning(
        "nebula ensure_schema: `{e}` edge not write-ready after {n} attempt(s)",
        e=edge, n=attempts,
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

    # 3b. Schema-evolution for EXISTING spaces created before RELATED had a
    # `weight` column (SCHEMA_DDL's CREATE EDGE IF NOT EXISTS is a no-op on
    # them). Best-effort/fail-open: if the column already exists, the ALTER
    # fails harmlessly — _execute_with_retry logs a warning and returns
    # False, it never raises.
    _execute_with_retry(
        session, "ALTER EDGE `RELATED` ADD (weight double DEFAULT 1.0);",
        attempts=1, delay_s=0,
    )

    # 3c. Same schema-evolution story for EXISTING spaces created before
    # `Entity` had `er_canonical_name` (entity-resolution canonical stamp).
    # Best-effort/fail-open, same as the RELATED.weight ALTER above.
    _execute_with_retry(
        session, "ALTER TAG `Entity` ADD (er_canonical_name string DEFAULT '');",
        attempts=1, delay_s=0,
    )

    # 3d. Same schema-evolution story for EXISTING spaces created before
    # `Entity` had `first_doc_id` (first-seen provenance). Best-effort/
    # fail-open, same pattern as the RELATED.weight / er_canonical_name
    # ALTERs above.
    _execute_with_retry(
        session, "ALTER TAG `Entity` ADD (first_doc_id string DEFAULT '');",
        attempts=1, delay_s=0,
    )

    # 3e. Same schema-evolution story for EXISTING spaces created before
    # `Entity` had the 6 wiki-editor columns (nebula-wiki-ops design,
    # Design.1: dirty-flag bookkeeping + article metadata). Best-effort/
    # fail-open, same pattern as the ALTERs above. Nebula's `ALTER TAG ...
    # ADD (...)` accepts a comma-separated multi-column list in one
    # statement, so all 6 are added together here.
    _execute_with_retry(
        session,
        "ALTER TAG `Entity` ADD (wiki_dirty bool DEFAULT false, "
        "wiki_dirty_at int DEFAULT 0, wiki_hash string DEFAULT '', "
        "wiki_synced_at int DEFAULT 0, wiki_page_title string DEFAULT '', "
        "wikibase_qid string DEFAULT '');",
        attempts=1, delay_s=0,
    )

    # 4. Storage-side schema propagation lags meta by ~one heartbeat: a
    # `CREATE TAG`/`CREATE EDGE` — and even `DESCRIBE TAG` — succeeds BEFORE
    # an `INSERT` against that tag works ("No schema found"). So `DESCRIBE`
    # is NOT a reliable readiness signal; the only one is a real write
    # landing. Probe EACH write-target tag with a sentinel vertex until it
    # lands, then remove it, so the first caller write doesn't race
    # propagation. Both `Entity` (ingest) and `Community` (community BUILD)
    # are probed — they share the lag, so probing only one leaves the other's
    # first post-DDL write able to hit "Schema not exist".
    probe = "__kb_schema_probe__"
    _probe_tag_write_ready(
        session, "Entity",
        "INSERT VERTEX `Entity` (name, description, mention_count, created_at, label, "
        "er_canonical_name, first_doc_id, wiki_dirty, wiki_dirty_at, wiki_hash, "
        "wiki_synced_at, wiki_page_title, wikibase_qid) "
        f'VALUES "{probe}":("", "", 0, 0, "", "", "", false, 0, "", 0, "", "");',
        probe, attempts=use_attempts, delay_s=use_delay_s,
    )
    _probe_tag_write_ready(
        session, "Community",
        "INSERT VERTEX `Community` (id, level, member_count, members_hash, updated, "
        "report, title, summary, summarized_at) "
        f'VALUES "{probe}":("", 0, 0, "", 0, "", "", "", 0);',
        probe, attempts=use_attempts, delay_s=use_delay_s,
    )

    # The `RELATED` weight column reaches storaged with the same lag (via the
    # fresh CREATE EDGE on a new space, or the ALTER EDGE above on an existing
    # one). Probe a weighted edge-write between two sentinel vertices until it
    # lands, so the first ingest batch's weighted edges aren't silently dropped
    # by the fail-open write path.
    probe_b = "__kb_schema_probe_b__"
    session.execute(
        "INSERT VERTEX `Entity` (name, description, mention_count, created_at, label) VALUES "
        f'"{probe}":("", "", 0, 0, ""), "{probe_b}":("", "", 0, 0, "");'
    )
    _probe_edge_write_ready(
        session, "RELATED",
        "INSERT EDGE `RELATED` (rel_type, polarity, valid_from, valid_to, weight) "
        f'VALUES "{probe}" -> "{probe_b}":("", "", 0, 0, 1.0);',
        [f'DELETE VERTEX "{probe}" WITH EDGE;', f'DELETE VERTEX "{probe_b}" WITH EDGE;'],
        attempts=use_attempts, delay_s=use_delay_s,
    )
