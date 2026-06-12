"""`index_vector` loads nodes from staging, scrubs Milvus-oversized
metadata, inserts to Milvus, returns the node IDs.  Original
metadata is restored on the in-memory nodes for downstream graph
activities (they receive the same blob via staging again — this test
just verifies the snapshot/restore around insert)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from llama_index.core.schema import TextNode

from src.workflow.activities.index_vector import index_vector
from src.workflow.contracts import Ctx, Parsed


@pytest.mark.asyncio
async def test_indexes_and_returns_ids():
    n1 = TextNode(id_="a", text="hello")
    n1.metadata = {"canonical_identifiers": ["x"], "translated_text": "RU"}
    n2 = TextNode(id_="b", text="world")
    n2.metadata = {"canonical_identifiers": ["y"]}

    staging = MagicMock()
    staging.read_pickle.return_value = [n1, n2]

    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=2)

    with patch(
        "src.workflow.activities.index_vector.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.index_vector.build_vector_store",
    ), patch(
        "src.workflow.activities.index_vector.build_vector_index",
    ), patch(
        "src.workflow.activities.index_vector.build_embedding_model",
    ), patch(
        "src.workflow.activities.index_vector.index_nodes",
    ) as mock_index_nodes, patch(
        "src.workflow.activities.index_vector.activity"
    ) as mock_activity:
        mock_activity.heartbeat = MagicMock()
        out = await index_vector(parsed)

    assert out.node_ids == ["a", "b"]
    assert out.count == 2
    mock_index_nodes.assert_called_once()


@pytest.mark.asyncio
async def test_restores_metadata_after_insert():
    n = TextNode(id_="a", text="hello")
    n.metadata = {"canonical_identifiers": ["x"], "translated_text": "RU", "k": 1}

    staging = MagicMock()
    staging.read_pickle.return_value = [n]

    captured: dict = {}

    def _capture(idx, nodes):
        # During insert, the oversize keys should be stripped.
        captured["inside"] = dict(nodes[0].metadata)

    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=1)

    with patch(
        "src.workflow.activities.index_vector.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.index_vector.build_vector_store",
    ), patch(
        "src.workflow.activities.index_vector.build_vector_index",
    ), patch(
        "src.workflow.activities.index_vector.build_embedding_model",
    ), patch(
        "src.workflow.activities.index_vector.index_nodes",
        side_effect=_capture,
    ), patch(
        "src.workflow.activities.index_vector.activity"
    ) as mock_activity:
        mock_activity.heartbeat = MagicMock()
        await index_vector(parsed)

    # Inside insert: oversize keys removed
    assert "canonical_identifiers" not in captured["inside"]
    assert "translated_text" not in captured["inside"]
    assert captured["inside"]["k"] == 1
    # After insert: everything restored on the in-memory node
    assert "canonical_identifiers" in n.metadata
    assert "translated_text" in n.metadata
