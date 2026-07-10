from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

from src.workflow.activities.inject_canonical import inject_canonical
from src.workflow.contracts import Ctx, Parsed


@pytest.mark.asyncio
async def test_calls_inject_with_loaded_nodes():
    n = MagicMock(node_id="a")
    staging = MagicMock()
    staging.read_pickle.return_value = [n]

    graph_store = MagicMock()
    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=1)

    with patch(
        "src.workflow.activities.inject_canonical.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.inject_canonical.build_graph_store",
        return_value=graph_store,
    ), patch(
        "src.workflow.activities.inject_canonical.inject_canonical_entities",
    ) as mock_inject, patch(
        "src.workflow.activities.inject_canonical.activity"
    ):
        out = await inject_canonical(parsed)

    mock_inject.assert_called_once_with(graph_store, [n])
    assert out.count == 1


@pytest.mark.asyncio
async def test_store_built_off_the_event_loop():
    """Regression: build_neo4j_graph_store() does blocking driver I/O +
    takes a process-global lock; calling it ON the event loop froze the
    heartbeat_every pulser under contention → timeout_type_heartbeat. It
    must run in a worker thread (asyncio.to_thread), not on the loop."""
    loop_thread = threading.get_ident()
    seen: dict[str, int] = {}

    def _build():
        seen["thread"] = threading.get_ident()
        return MagicMock()

    staging = MagicMock()
    staging.read_pickle.return_value = [MagicMock(node_id="a")]
    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=1)

    with patch(
        "src.workflow.activities.inject_canonical.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.inject_canonical.build_graph_store",
        _build,
    ), patch(
        "src.workflow.activities.inject_canonical.inject_canonical_entities",
    ), patch(
        "src.workflow.activities.inject_canonical.activity"
    ):
        await inject_canonical(parsed)

    assert seen["thread"] != loop_thread  # built off-loop, loop free to pulse
