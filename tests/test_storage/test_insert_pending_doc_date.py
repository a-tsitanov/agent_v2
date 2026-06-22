from __future__ import annotations

import inspect

from src.storage.postgres import AsyncPostgres


def test_insert_pending_accepts_doc_date():
    sig = inspect.signature(AsyncPostgres.insert_pending)
    assert "doc_date" in sig.parameters


def test_insert_pending_sql_includes_doc_date():
    src = inspect.getsource(AsyncPostgres.insert_pending)
    assert "doc_date" in src  # column threaded into the INSERT


def test_documents_ddl_has_doc_date_column():
    import scripts.setup_db as sd
    assert "doc_date" in sd._DOCUMENTS_DDL
    assert "documents_doc_date_idx" in sd._DOCUMENTS_DDL
