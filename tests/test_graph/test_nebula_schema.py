# tests/test_graph/test_nebula_schema.py
from __future__ import annotations

from src.graph.nebula_schema import SCHEMA_DDL, SPACE_DDL, SPACE_NAME, ensure_schema


def test_space_and_core_schema_present():
    assert f"CREATE SPACE IF NOT EXISTS `{SPACE_NAME}`" in SPACE_DDL

    joined = "\n".join(SCHEMA_DDL)
    assert "CREATE TAG IF NOT EXISTS `Entity`" in joined
    for edge in ("RELATED", "MENTIONS", "IN_COMMUNITY", "PARENT_OF"):
        assert f"CREATE EDGE IF NOT EXISTS `{edge}`" in joined
    assert "rel_type string" in "\n".join(SCHEMA_DDL)  # RELATED carries original type


def test_related_edge_ddl_has_weight_column():
    # Weighted Leiden parity with neo4j (merge.py writes properties["weight"]):
    # fresh spaces must get a `weight` column on `RELATED`.
    related_ddl = next(
        s for s in SCHEMA_DDL if s.startswith("CREATE EDGE IF NOT EXISTS `RELATED`")
    )
    assert "weight double" in related_ddl


def test_ddl_is_idempotent():
    # every statement must be IF NOT EXISTS so ensure_schema can re-run.
    # There is no longer a non-idempotent `USE` in either list.
    assert "IF NOT EXISTS" in SPACE_DDL
    for stmt in SCHEMA_DDL:
        assert "IF NOT EXISTS" in stmt, stmt


class _FakeResp:
    """Always-succeeds response — no retry/sleep should ever fire."""

    def is_succeeded(self) -> bool:
        return True

    def error_msg(self) -> str:
        return ""


class _FakeSession:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, stmt: str) -> _FakeResp:
        self.statements.append(stmt)
        return _FakeResp()


def test_ensure_schema_selects_space_before_creates(monkeypatch):
    # Regression test for the Critical bug: ensure_schema must USE the
    # space before issuing any tag/edge/index DDL. Fail the test loudly
    # (rather than just slowly) if a retry/sleep path is ever exercised.
    def _no_sleep(*_args, **_kwargs):
        raise AssertionError("ensure_schema should not sleep on an always-succeeding session")

    monkeypatch.setattr("src.graph.nebula_schema.time.sleep", _no_sleep)

    fake = _FakeSession()
    ensure_schema(fake)

    assert fake.statements, "ensure_schema executed no statements"

    create_space_idx = next(
        i for i, s in enumerate(fake.statements) if s.startswith("CREATE SPACE")
    )
    use_idx = next(
        i for i, s in enumerate(fake.statements) if s == f"USE `{SPACE_NAME}`;"
    )
    schema_idxs = [
        i
        for i, s in enumerate(fake.statements)
        if s.startswith("CREATE TAG") or s.startswith("CREATE EDGE")
    ]

    assert schema_idxs, "no tag/edge DDL was executed"
    assert create_space_idx < use_idx
    assert all(use_idx < i for i in schema_idxs), (
        "USE must be issued before any CREATE TAG/EDGE/INDEX statement"
    )


def test_ensure_schema_probes_both_entity_and_community():
    # Both write-target tags share the storaged propagation lag (CREATE/DESCRIBE
    # succeed ~1 heartbeat before INSERT works). ensure_schema must probe-write
    # a sentinel for BOTH `Entity` (ingest) and `Community` (community BUILD) so
    # the first post-DDL write to EITHER doesn't race "Schema not exist".
    fake = _FakeSession()
    ensure_schema(fake)
    joined = "\n".join(fake.statements)
    assert "INSERT VERTEX `Entity` " in joined, "Entity write-readiness probe missing"
    assert "INSERT VERTEX `Community` " in joined, "Community write-readiness probe missing"
    # each successful probe removes its sentinel vertex
    assert fake.statements.count('DELETE VERTEX "__kb_schema_probe__";') == 2


def test_schema_has_community_tag_with_report_columns():
    tag = next((s for s in SCHEMA_DDL if "CREATE TAG IF NOT EXISTS `Community`" in s), None)
    assert tag is not None, "Community TAG missing from SCHEMA_DDL"
    # Structural columns written by the BUILD stage + report columns declared
    # now so the SUMMARIZE slice adds only writes, not a schema migration.
    for col in ("id string", "level int", "member_count int", "members_hash string",
                "updated int", "report string", "title string", "summary string",
                "summarized_at int"):
        assert col in tag, f"missing column: {col}"
    # report_vec lives in Milvus (Phase 3) — never on the vertex.
    assert "report_vec" not in tag


def test_schema_has_community_level_index():
    assert any(
        "CREATE TAG INDEX IF NOT EXISTS `community_level_idx` ON `Community`(level)" in s
        for s in SCHEMA_DDL
    ), "community_level_idx missing (needed for prune/lookup by level)"


def test_ensure_schema_alters_related_edge_to_add_weight():
    # Best-effort schema-evolution step for EXISTING spaces (created before
    # RELATED had a weight column). Fail-open: if the column already exists,
    # the ALTER errors harmlessly (see _FakeResp always-succeeds here — the
    # real fail-open behaviour is covered by _execute_with_retry's own
    # tests/usage elsewhere).
    fake = _FakeSession()
    ensure_schema(fake)
    assert "ALTER EDGE `RELATED` ADD (weight double DEFAULT 1.0);" in fake.statements
