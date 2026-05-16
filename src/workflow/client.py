"""Process-wide Temporal client singleton.

FastAPI handlers and CLI entry points call `get_temporal_client()`;
the connection is opened once and reused across requests.

The pydantic data converter is required: the project's payloads
(`IngestParams`, `Ctx`, `Parsed`, ...) are Pydantic v2 models and
the default JSON converter does not know how to serialise them,
which would cause workflow execution to hang.
"""

from __future__ import annotations

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter

from src.config import settings

_client_singleton: Client | None = None


async def get_temporal_client() -> Client:
    global _client_singleton
    if _client_singleton is None:
        _client_singleton = await Client.connect(
            settings.temporal.target,
            namespace=settings.temporal.namespace,
            data_converter=pydantic_data_converter,
        )
    return _client_singleton
