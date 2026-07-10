from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.workflow.activities.merge_and_resolve import merge_and_resolve
from src.workflow.contracts import Ctx, KGExtracted, Parsed


def _make_entity(name: str):
    e = MagicMock()
    e.name = name
    return e


def _make_relation(source_id: str, target_id: str):
    r = MagicMock()
    r.source_id = source_id
    r.target_id = target_id
    return r


@pytest.mark.asyncio
async def test_merge_consolidate_resolve_chain():
    nodes = [MagicMock(node_id="a")]
    staging = MagicMock()
    staging.read_pickle.return_value = nodes
    staging.write_pickle.return_value = "s3://kb-staging/r/merged.pkl"

    merged_entities = [_make_entity("E1")]
    merged_relations = [_make_relation("E1", "E2")]

    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=1)
    kg = KGExtracted(parsed=parsed, nodes_with_kg_uri="s3://kb-staging/r/kg.pkl")

    fake_settings = MagicMock()
    fake_settings.agent.er_enabled = False

    with patch(
        "src.workflow.activities.merge_and_resolve.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.merge_and_resolve.get_llm_pool",
        return_value=MagicMock(**{"get.return_value": MagicMock()}),
    ), patch(
        "src.workflow.activities.merge_and_resolve.merge_kg_extraction",
        new=AsyncMock(return_value=(merged_entities, merged_relations)),
    ), patch(
        "src.workflow.activities.merge_and_resolve.consolidate_phone_entities",
        return_value=(merged_entities, merged_relations, {}),
    ), patch(
        "src.workflow.activities.merge_and_resolve.settings",
        fake_settings,
    ), patch(
        "src.workflow.activities.merge_and_resolve.activity"
    ):
        out = await merge_and_resolve(kg)

    assert out.merged_entities_uri == "s3://kb-staging/r/merged.pkl"
    # write_pickle called with (run_id, "merged", (entities, relations, nodes))
    args = staging.write_pickle.call_args.args
    assert args[0] == "r"
    assert args[1] == "merged"


@pytest.mark.asyncio
async def test_runs_er_when_enabled():
    nodes = [MagicMock(node_id="a")]
    staging = MagicMock()
    staging.read_pickle.return_value = nodes
    staging.write_pickle.return_value = "s3://kb-staging/r/merged.pkl"

    merged_entities = [_make_entity("E1")]
    merged_relations = [_make_relation("E1", "E2")]

    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=1)
    kg = KGExtracted(parsed=parsed, nodes_with_kg_uri="s3://kb-staging/r/kg.pkl")

    fake_settings = MagicMock()
    fake_settings.agent.er_enabled = True
    fake_settings.agent.er_judge_batch_size = 8

    er_mock = AsyncMock(return_value=(merged_entities, merged_relations, {}))

    with patch(
        "src.workflow.activities.merge_and_resolve.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.merge_and_resolve.get_llm_pool",
        return_value=MagicMock(**{"get.return_value": MagicMock()}),
    ), patch(
        "src.workflow.activities.merge_and_resolve.build_embedding_model",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.merge_and_resolve.build_graph_store",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.merge_and_resolve.merge_kg_extraction",
        new=AsyncMock(return_value=(merged_entities, merged_relations)),
    ), patch(
        "src.workflow.activities.merge_and_resolve.consolidate_phone_entities",
        return_value=(merged_entities, merged_relations, {}),
    ), patch(
        "src.workflow.activities.merge_and_resolve.resolve_entities",
        new=er_mock,
    ), patch(
        "src.workflow.activities.merge_and_resolve.settings",
        fake_settings,
    ), patch(
        "src.workflow.activities.merge_and_resolve.activity"
    ):
        await merge_and_resolve(kg)

    er_mock.assert_awaited_once()


def test_merge_and_resolve_wired_to_pool():
    import src.workflow.activities.merge_and_resolve as mr
    src = __import__("inspect").getsource(mr)
    assert "get_llm_pool" in src
    assert "build_judge_llm" not in src


@pytest.mark.asyncio
async def test_entity_names_and_relation_endpoints_populated():
    """Task 8b: Merged must surface entity_names and relation_endpoints."""
    nodes = [MagicMock(node_id="a")]
    staging = MagicMock()
    staging.read_pickle.return_value = nodes
    staging.write_pickle.return_value = "s3://kb-staging/r/merged.pkl"

    merged_entities = [_make_entity("Alpha"), _make_entity("Beta")]
    merged_relations = [
        _make_relation("Alpha", "Beta"),
        _make_relation("Beta", "Gamma"),
    ]

    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=1)
    kg = KGExtracted(parsed=parsed, nodes_with_kg_uri="s3://kb-staging/r/kg.pkl")

    fake_settings = MagicMock()
    fake_settings.agent.er_enabled = False

    with patch(
        "src.workflow.activities.merge_and_resolve.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.merge_and_resolve.get_llm_pool",
        return_value=MagicMock(**{"get.return_value": MagicMock()}),
    ), patch(
        "src.workflow.activities.merge_and_resolve.merge_kg_extraction",
        new=AsyncMock(return_value=(merged_entities, merged_relations)),
    ), patch(
        "src.workflow.activities.merge_and_resolve.consolidate_phone_entities",
        return_value=(merged_entities, merged_relations, {}),
    ), patch(
        "src.workflow.activities.merge_and_resolve.settings",
        fake_settings,
    ), patch(
        "src.workflow.activities.merge_and_resolve.activity"
    ):
        out = await merge_and_resolve(kg)

    assert out.entity_names == ["Alpha", "Beta"]
    assert set(out.relation_endpoints) == {"Alpha", "Beta", "Gamma"}
    # No duplicates in relation_endpoints
    assert len(out.relation_endpoints) == len(set(out.relation_endpoints))
