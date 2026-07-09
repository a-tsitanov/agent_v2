# tests/test_graph/test_nebula_schema.py
from __future__ import annotations

from src.graph.nebula_schema import SCHEMA_DDL, SPACE_NAME


def test_space_and_core_schema_present():
    joined = "\n".join(SCHEMA_DDL)
    assert f"CREATE SPACE IF NOT EXISTS `{SPACE_NAME}`" in joined
    assert "CREATE TAG IF NOT EXISTS `Entity`" in joined
    for edge in ("RELATED", "MENTIONS", "IN_COMMUNITY", "PARENT_OF"):
        assert f"CREATE EDGE IF NOT EXISTS `{edge}`" in joined


def test_ddl_is_idempotent():
    # every statement must be IF NOT EXISTS so ensure_schema can re-run
    for stmt in SCHEMA_DDL:
        assert "IF NOT EXISTS" in stmt, stmt
