"""Stage-1 unit tests for ``scripts/setup_db.py``.

Live-DB integration is verified manually by running
``python -m scripts.setup_db`` against a docker compose stack.  These
tests cover the module-level invariants:
  * ``_DOCUMENTS_DDL`` mentions every column the ingestion pipeline
    will need (status / department / summary / etc).
  * Both setup functions are callable (signature + module load).
"""

from __future__ import annotations

import importlib

setup_db = importlib.import_module("scripts.setup_db")


def test_documents_ddl_contains_required_columns() -> None:
    ddl = setup_db._DOCUMENTS_DDL
    for column in (
        "id",
        "path",
        "department",
        "doc_type",
        "status",
        "error",
        "summary",
        "created_at",
        "updated_at",
    ):
        assert column in ddl, f"column {column!r} missing from DDL"


def test_documents_ddl_uses_create_if_not_exists() -> None:
    """Idempotency contract: re-running the script against an existing
    DB must not error."""
    ddl = setup_db._DOCUMENTS_DDL.upper()
    assert "CREATE TABLE IF NOT EXISTS DOCUMENTS" in ddl
    # status check constraint covers the four-value FSM
    for state in ("pending", "processing", "completed", "failed"):
        assert state in setup_db._DOCUMENTS_DDL


def test_setup_functions_are_callable() -> None:
    assert callable(setup_db.setup_postgres)
    assert callable(setup_db.setup_milvus)
    assert callable(setup_db.main)
