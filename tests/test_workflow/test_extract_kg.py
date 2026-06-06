from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.workflow.activities.extract_kg import extract_kg
from src.workflow.contracts import Ctx, Parsed


@pytest.mark.asyncio
async def test_runs_extractor_and_writes_kg_blob():
    n = MagicMock(node_id="a")
    staging = MagicMock()
    staging.read_pickle.return_value = [n]
    staging.write_pickle.return_value = "s3://kb-staging/r/kg.pkl"

    extractor = MagicMock()
    extractor.acall = AsyncMock(return_value=[n])

    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=1)

    mock_pool = MagicMock()
    mock_pool.get.return_value = MagicMock()

    with patch(
        "src.workflow.activities.extract_kg.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.extract_kg.build_kg_extractor",
        return_value=extractor,
    ), patch(
        "src.workflow.activities.extract_kg.get_llm_pool",
        return_value=mock_pool,
    ), patch(
        "src.workflow.activities.extract_kg.activity"
    ) as mock_activity:
        mock_activity.heartbeat = MagicMock()
        out = await extract_kg(parsed)

    extractor.acall.assert_awaited_once_with([n])
    staging.write_pickle.assert_called_once_with("r", "kg", [n])
    assert out.nodes_with_kg_uri == "s3://kb-staging/r/kg.pkl"
    assert out.parsed == parsed


def test_extract_kg_wired_to_pool():
    """extract_kg must obtain its LLM from the shared pool, not a raw
    ungated build_extraction_llm()."""
    import src.workflow.activities.extract_kg as ek
    src = __import__("inspect").getsource(ek)
    assert "get_llm_pool" in src
    assert "build_extraction_llm" not in src
