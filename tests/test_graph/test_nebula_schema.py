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
