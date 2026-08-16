"""The stats DDL is asserted as text, not by hitting a live Postgres —
the point is that the constraints which make the schema correct are
actually present, and that they are GENERATED from
`src.stats.align.VALUE_KINDS` / `GRANULARITIES` rather than duplicated
as literal text, so the two can never quietly drift apart."""

from __future__ import annotations

from scripts.setup_db import (
    _PG_TRGM_DDL,
    _STAT_INDEXES_DDL,
    _STAT_INDICATOR_CONSTRAINTS_DDL,
    _STAT_INDICATOR_DDL,
    _STAT_OBSERVATION_DDL,
)
from src.stats.align import GRANULARITIES, VALUE_KINDS


def test_tables_are_created_idempotently():
    assert "CREATE TABLE IF NOT EXISTS stat_indicator" in _STAT_INDICATOR_DDL
    assert "CREATE TABLE IF NOT EXISTS stat_observation" in _STAT_OBSERVATION_DDL


def test_indicator_is_unique_per_source_and_code():
    assert "UNIQUE (source, code)" in _STAT_INDICATOR_DDL


def test_indicator_constrains_value_kind_and_granularity():
    """Not pinned as a literal string: the expected clause is built here
    from the same `VALUE_KINDS`/`GRANULARITIES` the DDL is supposed to be
    generated from.  A test that hard-codes the current values proves
    nothing about drift — this one fails the moment the generated DDL
    text stops matching what the constants actually say.
    """
    value_kind_clause = ", ".join(f"'{v}'" for v in sorted(VALUE_KINDS))
    granularity_clause = ", ".join(f"'{v}'" for v in sorted(GRANULARITIES))
    assert f"value_kind IN ({value_kind_clause})" in _STAT_INDICATOR_DDL
    assert f"granularity IN ({granularity_clause})" in _STAT_INDICATOR_DDL


def test_dims_is_not_null_with_empty_default():
    """A nullable `dims` would silently break the UNIQUE constraint,
    because NULL never compares equal to NULL — duplicate undimensioned
    observations would slip in."""
    assert "dims           JSONB   NOT NULL DEFAULT '{}'::jsonb" in _STAT_OBSERVATION_DDL


def test_observation_unique_key_includes_revision():
    assert (
        "UNIQUE (indicator_id, period_start, dims, revision)"
        in _STAT_OBSERVATION_DDL
    )


def test_trigram_extension_and_indexes_present():
    assert "CREATE EXTENSION IF NOT EXISTS pg_trgm" in _PG_TRGM_DDL
    assert "gin_trgm_ops" in _STAT_INDEXES_DDL
    assert "USING GIN (dims)" in _STAT_INDEXES_DDL


# ── constraint drift on an already-existing table ──────────────────────
#
# `stat_indicator` already exists in the live database, so
# `CREATE TABLE IF NOT EXISTS` above is a no-op there.  Adding a value to
# `VALUE_KINDS` would otherwise be accepted by Python and rejected by
# Postgres forever, with no code path that ever fixes it.  This idempotent
# ALTER (same DROP-then-ADD shape as `documents_status_check` elsewhere in
# this file) is what brings an existing table's constraints back in line
# on every `setup_db` run.


def test_constraints_ddl_is_generated_from_the_same_constants():
    value_kind_clause = ", ".join(f"'{v}'" for v in sorted(VALUE_KINDS))
    granularity_clause = ", ".join(f"'{v}'" for v in sorted(GRANULARITIES))
    assert f"value_kind IN ({value_kind_clause})" in _STAT_INDICATOR_CONSTRAINTS_DDL
    assert f"granularity IN ({granularity_clause})" in _STAT_INDICATOR_CONSTRAINTS_DDL


def test_constraints_ddl_drops_before_it_adds_so_reruns_never_conflict():
    """DROP CONSTRAINT IF EXISTS before ADD CONSTRAINT is what makes
    re-running this safe: DROP never fails on a second run, and by the
    time ADD executes the name has already been cleared."""
    for name in ("stat_indicator_value_kind_check", "stat_indicator_granularity_check"):
        assert f"DROP CONSTRAINT IF EXISTS {name}" in _STAT_INDICATOR_CONSTRAINTS_DDL
        assert f"ADD CONSTRAINT {name}" in _STAT_INDICATOR_CONSTRAINTS_DDL
        drop_at = _STAT_INDICATOR_CONSTRAINTS_DDL.index(f"DROP CONSTRAINT IF EXISTS {name}")
        add_at = _STAT_INDICATOR_CONSTRAINTS_DDL.index(f"ADD CONSTRAINT {name}")
        assert drop_at < add_at, f"{name}: DROP must precede ADD"


def test_setup_postgres_runs_the_constraints_ddl_idempotently(monkeypatch):
    """`setup_db` must exit 0 both times it is run against an
    already-created table.  Exercised with a fake connection here per
    task constraints (no live database in this task) — this proves the
    ALTER is sent, and sent identically, on a second run rather than
    erroring or being skipped."""
    import scripts.setup_db as setup_db

    executed: list[str] = []

    class _FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, *a, **kw):
            executed.append(sql)

    class _FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def cursor(self):
            return _FakeCursor()

    monkeypatch.setattr(setup_db.psycopg, "connect", lambda *a, **kw: _FakeConn())

    setup_db.setup_postgres()  # first run: exit 0
    setup_db.setup_postgres()  # second run against the "already-created" table: exit 0

    assert executed.count(_STAT_INDICATOR_CONSTRAINTS_DDL) == 2
