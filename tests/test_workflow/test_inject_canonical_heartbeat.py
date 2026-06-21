"""5b guard: inject_canonical must heartbeat DURING its blocking Neo4j
upsert, not only at the init/loaded/injected checkpoints.

The upsert (`inject_canonical_entities` via asyncio.to_thread) can be slow
under hub-node lock contention. Without a continuous pulse there's no
liveness signal mid-upsert, so a stuck Neo4j connection ties up the
admission slot until start_to_close (1h). Wrapping it in `heartbeat_every`
closes that gap (mirrors extract_kg / merge_and_resolve).
"""

from __future__ import annotations

import importlib
import time
import types

import pytest
from temporalio.testing import ActivityEnvironment

from src.workflow.contracts import Ctx, Parsed

# import the MODULE (the `inject_canonical` name in the activities package is
# the re-exported function, which shadows the submodule) so monkeypatch targets
# the module globals the activity reads.
ic = importlib.import_module("src.workflow.activities.inject_canonical")


@pytest.mark.asyncio
async def test_pulses_during_neo4j_upsert(monkeypatch):
    # tiny interval so a short test upsert yields several pulses
    monkeypatch.setattr(ic, "_HEARTBEAT_INTERVAL_S", 0.05)
    monkeypatch.setattr(
        ic, "build_staging_store",
        lambda: types.SimpleNamespace(read_pickle=lambda _uri: [1, 2, 3]),
    )
    monkeypatch.setattr(ic, "build_neo4j_graph_store", lambda: object())

    def slow_upsert(_store, _nodes):
        time.sleep(0.3)  # blocking, runs in a thread — must be heartbeat-covered

    monkeypatch.setattr(ic, "inject_canonical_entities", slow_upsert)

    beats: list[tuple] = []
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *a: beats.append(a)

    parsed = Parsed(
        ctx=Ctx(doc_id="d1", local_path="/x", cleanup_dir=None,
                workflow_run_id="r1"),
        nodes_uri="uri", chunk_count=3,
    )
    result = await env.run(ic.inject_canonical, parsed)

    assert result.count == 3
    # 3 checkpoint beats + ~6 pulses during the 0.3s upsert at 0.05s interval.
    # Pre-fix (no heartbeat_every wrapping the upsert) only the 3 checkpoints
    # fire → this fails.
    assert len(beats) >= 6
