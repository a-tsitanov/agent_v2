"""Guard: build_property_graph must heartbeat DURING its blocking Neo4j
writes (index build + entity/relation upsert), not only at the discrete
init/loaded/.../indexes_ensured checkpoints.

The upserts (`graph_store.upsert_nodes/upsert_relations` via
`write_with_retry` in asyncio.to_thread) get slow under hub-node lock
contention — at 37k nodes / 60k edges a single upsert can outrun the
5m heartbeat_timeout. Without a continuous pulse Temporal mistakes the
progressing-but-slow activity for a dead one, fires timeout_type_heartbeat,
and _FAST_RETRY re-runs the whole activity (up to 50×) — a write-amplifying
retry storm that pegs Neo4j CPU. Wrapping the blocking region in
`heartbeat_every` closes the gap (mirrors inject_canonical / merge_and_resolve).
"""

from __future__ import annotations

import importlib
import time
import types

import pytest
from temporalio.testing import ActivityEnvironment

from src.workflow.contracts import Ctx, KGExtracted, Merged, Parsed

# import the MODULE (the `build_property_graph` name in the activities package
# is the re-exported function, which shadows the submodule) so monkeypatch
# targets the module globals the activity reads.
bpg = importlib.import_module("src.workflow.activities.build_property_graph")
gi = importlib.import_module("src.graph.index")


@pytest.mark.asyncio
async def test_pulses_during_neo4j_upsert(monkeypatch):
    # tiny interval so the short test upserts yield many pulses.
    # raising=False: the constant only exists once the fix lands, so the
    # RED run sets it harmlessly and fails on the beat count instead of erroring.
    monkeypatch.setattr(bpg, "_HEARTBEAT_INTERVAL_S", 0.02, raising=False)

    entities = [1]
    relations = [1]
    nodes = [types.SimpleNamespace()]  # no .metadata → scrub is a no-op
    monkeypatch.setattr(
        bpg, "build_staging_store",
        lambda: types.SimpleNamespace(
            read_pickle=lambda _uri: (entities, relations, nodes)
        ),
    )
    # store needs upsert_* attrs: they're referenced as args to write_with_retry
    # (which is patched), so the attribute access must resolve even though the
    # methods themselves are never invoked.
    graph_store = types.SimpleNamespace(
        upsert_nodes=lambda *_a: None, upsert_relations=lambda *_a: None,
    )
    monkeypatch.setattr(bpg, "build_neo4j_graph_store", lambda: graph_store)
    monkeypatch.setattr(bpg, "build_embedding_model", lambda: object())
    monkeypatch.setattr(bpg, "build_property_graph_index", lambda **_kw: None)
    # index DDL is imported from src.graph.index at call time → patch there.
    monkeypatch.setattr(gi, "ensure_entity_fulltext_index", lambda _s: None)
    monkeypatch.setattr(gi, "ensure_entity_lookup_indexes", lambda _s: None)
    monkeypatch.setattr(gi, "ensure_chunk_date_indexes", lambda _s: None)

    def slow_write(_fn, *_args):
        time.sleep(0.25)  # blocking, runs in a thread — must be heartbeat-covered

    monkeypatch.setattr(bpg, "write_with_retry", slow_write)

    beats: list[tuple] = []
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *a: beats.append(a)

    ctx = Ctx(doc_id="d1", local_path="/x", cleanup_dir=None, workflow_run_id="r1")
    parsed = Parsed(ctx=ctx, nodes_uri="uri", chunk_count=1)
    kg = KGExtracted(parsed=parsed, nodes_with_kg_uri="kg")
    merged = Merged(kg=kg, merged_entities_uri="merged")

    result = await env.run(bpg.build_property_graph, merged)

    assert result.entities == 1
    assert result.relations == 1
    # 7 discrete checkpoint beats fire regardless of the fix. The two 0.25s
    # blocking upserts at a 0.02s interval add ~25 pulses. Pre-fix (no
    # heartbeat_every around the writes) only the 7 checkpoints fire → fails.
    assert len(beats) >= 12
