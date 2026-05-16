"""The legacy `process_document` taskiq task is now a thin shim that
starts the Temporal workflow and waits for it.  Verifies the call
shape; no Temporal infra needed."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.ingestion.tasks import process_document


@pytest.mark.asyncio
async def test_process_document_starts_workflow():
    handle = MagicMock()
    handle.result = AsyncMock(return_value=MagicMock(doc_id="d"))
    client = MagicMock()
    client.start_workflow = AsyncMock(return_value=handle)

    doc_id = str(uuid.uuid4())
    with patch(
        "src.ingestion.tasks.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        await process_document(doc_id, "s3://kb-uploads/x/y.pdf")

    client.start_workflow.assert_awaited_once()
    call = client.start_workflow.call_args
    # workflow id ingest-<doc_id> + task_queue from settings
    assert call.kwargs.get("id") == f"ingest-{doc_id}"
    handle.result.assert_awaited_once()
