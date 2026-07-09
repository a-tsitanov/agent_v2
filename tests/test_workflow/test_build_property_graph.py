from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.identifier_transform import _AUGMENT_METADATA_KEY
from src.workflow.activities.build_property_graph import (
    _strip_neo4j_unsafe_metadata,
    build_property_graph,
)
from src.workflow.contracts import Ctx, KGExtracted, Merged, Parsed


@pytest.mark.asyncio
async def test_strips_metadata_builds_pg_upserts_entities():
    n = MagicMock(node_id="a")
    n.metadata = {"safe": "str", "bad": {"nested": "x"}}
    entities = [MagicMock()]
    relations = [MagicMock()]

    staging = MagicMock()
    staging.read_pickle.return_value = (entities, relations, [n])

    graph_store = MagicMock()

    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=1)
    kg = KGExtracted(parsed=parsed, nodes_with_kg_uri="s3://kb-staging/r/kg.pkl")
    merged = Merged(kg=kg, merged_entities_uri="s3://kb-staging/r/merged.pkl")

    with patch(
        "src.workflow.activities.build_property_graph.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.build_property_graph.build_graph_store",
        return_value=graph_store,
    ), patch(
        "src.workflow.activities.build_property_graph.build_embedding_model",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.build_property_graph.build_property_graph_index",
    ) as mock_build, patch(
        "src.workflow.activities.build_property_graph.activity"
    ):
        out = await build_property_graph(merged)

    mock_build.assert_called_once()
    graph_store.upsert_nodes.assert_called_once_with(entities)
    graph_store.upsert_relations.assert_called_once_with(relations)
    # nested metadata stripped
    assert "bad" not in n.metadata
    assert n.metadata.get("safe") == "str"
    assert out.entities == 1
    assert out.relations == 1


def test_strip_neo4j_unsafe_removes_augment_key() -> None:
    """``canonical_identifiers_augment`` must be removed from Chunk node
    metadata before Neo4j upsert — consistent with fix #5's goal of keeping
    the augment block out of all stores."""
    node = MagicMock()
    node.metadata = {
        "safe_key": "value",
        _AUGMENT_METADATA_KEY: "Канонические идентификаторы: +79001234567 (PHONE)",
        "canonical_identifiers": [{"canonical": "+79001234567"}],
    }
    _strip_neo4j_unsafe_metadata([node])
    assert _AUGMENT_METADATA_KEY not in node.metadata
    assert "canonical_identifiers" not in node.metadata
    assert node.metadata.get("safe_key") == "value"
