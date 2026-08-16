"""The `er_verdict` DDL, asserted as text like the other setup_db tests.

The table is where the ER verdict cache moved to after it had grown to
1 395 491 vertices inside the Nebula space — 89.5% of every vertex there
— and pushed full scans past the graph's memory ceiling.
"""

from __future__ import annotations

from scripts.setup_db import _ER_VERDICT_DDL


def test_table_is_created_idempotently():
    assert "CREATE TABLE IF NOT EXISTS er_verdict" in _ER_VERDICT_DDL


def test_key_is_the_primary_key():
    """The PRIMARY KEY *is* the lookup index — `er_key = ANY(...)` is the
    only access pattern.  This is the whole reason the cache belongs in a
    relational table: the graph had to carry a separate index for it."""
    assert "er_key   TEXT PRIMARY KEY" in _ER_VERDICT_DDL


def test_verdict_is_not_nullable():
    """`same` is the cached answer.  A NULL would be a third state that
    neither `load_verdicts` nor its callers have any meaning for."""
    assert "same     BOOLEAN     NOT NULL" in _ER_VERDICT_DDL


def test_no_secondary_index_is_created():
    """Deliberate: nothing queries by age, and an index on `updated` would
    only serve pruning-by-age — which evicts exactly the wrong rows, since
    stable name pairs recur indefinitely and are the most valuable to
    keep."""
    assert "CREATE INDEX" not in _ER_VERDICT_DDL


def test_ddl_matches_the_one_the_worker_creates_on_a_fresh_database():
    """`PostgresERVerdictCache.ensure_verdict_schema` carries its own copy
    so a worker meeting an empty database is not blocked on setup_db
    having been run.  Two copies of a CREATE TABLE that drift apart is how
    a schema quietly diverges — pin them equal."""
    from src.graph.er_graph_ops import _ER_VERDICT_TABLE_DDL

    assert _ER_VERDICT_TABLE_DDL.strip() == _ER_VERDICT_DDL.strip()
