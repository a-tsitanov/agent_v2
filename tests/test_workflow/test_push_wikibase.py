"""Tests for the push_wikibase activity.  Three behaviours:
  * cache disabled -> status="skipped", no Wikibase / Neo4j traffic.
  * happy path -> push_entities called, counts surfaced in result.
  * any exception -> status="failed", does not raise out.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.workflow.contracts import (
    Ctx,
    KGExtracted,
    Merged,
    Parsed,
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
        "src.workflow.activities.push_wikibase.build_graph_store"
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
    # One owner entity in the staging blob so the zero-counters
    # guard isn't tripped (which would otherwise mask happy-path).
    staging.read_pickle.return_value = (
        [MagicMock(name="ent-1")], [], [],
    )
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
        "src.workflow.activities.push_wikibase.build_graph_store",
        return_value=gs,
    ), patch(
        "src.workflow.activities.push_wikibase.AsyncWikibase",
    ) as wb_factory, patch(
        "src.workflow.activities.push_wikibase.push_entities",
        new=AsyncMock(return_value=fake_counts),
    ) as push_fn, patch(
        "src.workflow.activities.push_wikibase.activity"
    ):
        # `from_settings` is async — must await — so the test mock has
        # to be an AsyncMock to support `await` in the activity.
        wb_factory.from_settings = AsyncMock(return_value=wb_client)
        out = await push_wikibase(_fake_merged())

    assert out.status == "ok"
    assert out.created_items == 2
    assert out.updated_items == 1
    assert out.external_id_statements == 3
    assert out.relation_statements == 4
    assert out.new_properties_created == 1
    push_fn.assert_awaited_once()


@pytest.mark.asyncio
async def test_zero_counters_marked_failed_not_ok(monkeypatch):
    """If push_entities returns counters where created+updated == 0
    yet we DID receive entities to push, treat as failed.  Catches
    the silent-no-op class of bugs (e.g. wb_client was a coroutine
    object so every per-owner create_item raised, push_entities's
    inner try/except swallowed each, top-level returned all zeros).
    """
    from src.config import settings
    from src.workflow.activities.push_wikibase import push_wikibase

    monkeypatch.setattr(settings.wikibase, "enabled", True, raising=False)

    staging = MagicMock()
    # 3 entities went in...
    staging.read_pickle.return_value = (
        [MagicMock(name=f"ent-{i}") for i in range(3)], [], [],
    )
    gs = MagicMock()
    gs.structured_query.side_effect = [
        [{"label": "Person", "qid": "Q1"}],
        [{"label": "PhoneNumber", "pid": "P4"}],
    ]
    # ...but nothing landed in Wikibase.
    zero_counts = {
        "created_items": 0, "updated_items": 0,
        "external_id_statements": 0, "relation_statements": 0,
        "new_properties_created": 0,
    }

    wb_client = MagicMock()
    with patch(
        "src.workflow.activities.push_wikibase.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.push_wikibase.build_graph_store",
        return_value=gs,
    ), patch(
        "src.workflow.activities.push_wikibase.AsyncWikibase",
    ) as wb_factory, patch(
        "src.workflow.activities.push_wikibase.push_entities",
        new=AsyncMock(return_value=zero_counts),
    ), patch(
        "src.workflow.activities.push_wikibase.activity"
    ):
        wb_factory.from_settings = AsyncMock(return_value=wb_client)
        out = await push_wikibase(_fake_merged())

    assert out.status == "failed", (
        "had work to do but produced no items — must NOT report ok"
    )
    # Counters still surfaced for diagnostics.
    assert out.created_items == 0
    assert out.updated_items == 0


@pytest.mark.asyncio
async def test_empty_input_returns_ok(monkeypatch):
    """An ingest with no entities to push (empty merged blob) IS a
    valid no-op — status="ok" with all zeros is honest here."""
    from src.config import settings
    from src.workflow.activities.push_wikibase import push_wikibase

    monkeypatch.setattr(settings.wikibase, "enabled", True, raising=False)

    staging = MagicMock()
    staging.read_pickle.return_value = ([], [], [])  # genuinely empty
    gs = MagicMock()
    gs.structured_query.side_effect = [
        [{"label": "Person", "qid": "Q1"}],
        [{"label": "PhoneNumber", "pid": "P4"}],
    ]
    zero_counts = {
        "created_items": 0, "updated_items": 0,
        "external_id_statements": 0, "relation_statements": 0,
        "new_properties_created": 0,
    }

    wb_client = MagicMock()
    with patch(
        "src.workflow.activities.push_wikibase.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.push_wikibase.build_graph_store",
        return_value=gs,
    ), patch(
        "src.workflow.activities.push_wikibase.AsyncWikibase",
    ) as wb_factory, patch(
        "src.workflow.activities.push_wikibase.push_entities",
        new=AsyncMock(return_value=zero_counts),
    ), patch(
        "src.workflow.activities.push_wikibase.activity"
    ):
        wb_factory.from_settings = AsyncMock(return_value=wb_client)
        out = await push_wikibase(_fake_merged())

    assert out.status == "ok"


@pytest.mark.asyncio
async def test_heartbeats_pulse_during_push_entities(monkeypatch):
    """push_entities makes one sequential Wikibase REST round-trip per
    owner/relation and emits no heartbeats of its own; a real batch can
    easily outrun the 2-min heartbeat_timeout.  The activity must pulse
    on a timer *while push_entities runs* so a slow-but-progressing push
    is never mistaken for a dead worker and retried.

    Regression for the heartbeat-gap retry storm that made push_wikibase
    succeed only after ~26 attempts.
    """
    import sys

    from temporalio.testing import ActivityEnvironment

    import src.workflow.activities.push_wikibase  # noqa: F401  (ensure import)
    from src.config import settings

    # The activities package __init__ rebinds the name ``push_wikibase`` to
    # the *function*, shadowing the submodule attribute — so reach the real
    # module object via sys.modules to patch its globals.
    mod = sys.modules["src.workflow.activities.push_wikibase"]

    monkeypatch.setattr(settings.wikibase, "enabled", True, raising=False)
    # Shrink the pulse interval so the test runs in fractions of a second.
    monkeypatch.setattr(mod, "_HEARTBEAT_INTERVAL_S", 0.05, raising=False)

    staging = MagicMock()
    staging.read_pickle.return_value = ([MagicMock(name="ent-1")], [], [])
    gs = MagicMock()
    gs.structured_query.side_effect = [
        [{"label": "Person", "qid": "Q1"}],
        [{"label": "PhoneNumber", "pid": "P4"}],
    ]
    fake_counts = {
        "created_items": 1, "updated_items": 0,
        "external_id_statements": 0, "relation_statements": 0,
        "new_properties_created": 0,
    }

    async def slow_push(*_a, **_k):
        await asyncio.sleep(0.3)  # outlasts several 0.05s pulse intervals
        return fake_counts

    wb_client = MagicMock()
    beats: list[tuple] = []
    with patch.object(mod, "build_staging_store", return_value=staging), \
         patch.object(mod, "build_graph_store", return_value=gs), \
         patch.object(mod, "AsyncWikibase") as wb_factory, \
         patch.object(mod, "push_entities", new=slow_push):
        wb_factory.from_settings = AsyncMock(return_value=wb_client)
        env = ActivityEnvironment()
        env.on_heartbeat = lambda *args: beats.append(args)
        out = await env.run(mod.push_wikibase, _fake_merged())

    assert out.status == "ok"
    pushing_beats = [
        b for b in beats if b and isinstance(b[0], dict)
        and b[0].get("stage") == "pushing"
    ]
    assert len(pushing_beats) >= 3, (
        "no timer heartbeats fired during push_entities — a slow push "
        "will hit heartbeat_timeout and be retried"
    )


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
