"""Tests for the push_wikibase activity.  Three behaviours:
  * cache disabled -> status="skipped", no Wikibase / Neo4j traffic.
  * happy path -> push_entities called, counts surfaced in result.
  * any exception -> status="failed", does not raise out.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.workflow.contracts import (
    Ctx, KGExtracted, Merged, Parsed,
)


def _fake_merged():
    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None,
              workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl",
                    chunk_count=1)
    kg = KGExtracted(parsed=parsed,
                     nodes_with_kg_uri="s3://kb-staging/r/kg.pkl")
    return Merged(kg=kg, merged_entities_uri="s3://kb-staging/r/merged.pkl")


@pytest.mark.asyncio
async def test_disabled_returns_skipped(monkeypatch):
    from src.config import settings
    from src.workflow.activities.push_wikibase import push_wikibase

    monkeypatch.setattr(settings.wikibase, "enabled", False, raising=False)

    # Patch heavy IO so the early-return path doesn't even reach them.
    with patch(
        "src.workflow.activities.push_wikibase.build_staging_store"
    ) as ms, patch(
        "src.workflow.activities.push_wikibase.build_neo4j_graph_store"
    ) as mg, patch(
        "src.workflow.activities.push_wikibase.activity"
    ):
        out = await push_wikibase(_fake_merged())

    assert out.status == "skipped"
    ms.assert_not_called()
    mg.assert_not_called()


@pytest.mark.asyncio
async def test_happy_path_runs_push_entities(monkeypatch):
    from src.config import settings
    from src.workflow.activities.push_wikibase import push_wikibase

    monkeypatch.setattr(settings.wikibase, "enabled", True, raising=False)

    staging = MagicMock()
    staging.read_pickle.return_value = ([], [], [])
    gs = MagicMock()
    gs.structured_query.side_effect = [
        [{"label": "Person", "qid": "Q1"}],     # base classes
        [{"label": "PhoneNumber", "pid": "P4"}],  # properties
    ]

    fake_counts = {
        "created_items": 2, "updated_items": 1,
        "external_id_statements": 3, "relation_statements": 4,
        "new_properties_created": 1,
    }

    wb_client = MagicMock()
    with patch(
        "src.workflow.activities.push_wikibase.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.push_wikibase.build_neo4j_graph_store",
        return_value=gs,
    ), patch(
        "src.workflow.activities.push_wikibase.AsyncWikibase",
    ) as wb_factory, patch(
        "src.workflow.activities.push_wikibase.push_entities",
        new=AsyncMock(return_value=fake_counts),
    ) as push_fn, patch(
        "src.workflow.activities.push_wikibase.activity"
    ):
        wb_factory.from_settings.return_value = wb_client
        out = await push_wikibase(_fake_merged())

    assert out.status == "ok"
    assert out.created_items == 2
    assert out.updated_items == 1
    assert out.external_id_statements == 3
    assert out.relation_statements == 4
    assert out.new_properties_created == 1
    push_fn.assert_awaited_once()


@pytest.mark.asyncio
async def test_exception_returns_failed_not_raise(monkeypatch):
    from src.config import settings
    from src.workflow.activities.push_wikibase import push_wikibase

    monkeypatch.setattr(settings.wikibase, "enabled", True, raising=False)

    with patch(
        "src.workflow.activities.push_wikibase.build_staging_store",
        side_effect=RuntimeError("MinIO down"),
    ), patch(
        "src.workflow.activities.push_wikibase.activity"
    ):
        out = await push_wikibase(_fake_merged())

    assert out.status == "failed"
    # No raise -- workflow goes on.
