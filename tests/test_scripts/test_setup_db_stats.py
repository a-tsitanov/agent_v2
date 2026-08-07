"""The stats DDL is asserted as text, not by hitting a live Postgres —
the point is that the constraints which make the schema correct are
actually present."""

from __future__ import annotations

from scripts.setup_db import (
    _PG_TRGM_DDL,
    _STAT_INDEXES_DDL,
    _STAT_INDICATOR_DDL,
    _STAT_OBSERVATION_DDL,
)


def test_tables_are_created_idempotently():
    assert "CREATE TABLE IF NOT EXISTS stat_indicator" in _STAT_INDICATOR_DDL
    assert "CREATE TABLE IF NOT EXISTS stat_observation" in _STAT_OBSERVATION_DDL


def test_indicator_is_unique_per_source_and_code():
    assert "UNIQUE (source, code)" in _STAT_INDICATOR_DDL


def test_indicator_constrains_value_kind_and_granularity():
    assert "value_kind IN ('share','level','rate','index')" in _STAT_INDICATOR_DDL
    assert (
        "granularity IN ('day','week','month','quarter','year')"
        in _STAT_INDICATOR_DDL
    )


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
