from __future__ import annotations

from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from src.workflow.client import get_temporal_client


@pytest.mark.asyncio
async def test_get_temporal_client_uses_settings():
    fake_client = MagicMock()
    fake_settings = MagicMock()
    fake_settings.temporal.target = "host:7233"
    fake_settings.temporal.namespace = "default"

    import src.workflow.client as mod
    mod._client_singleton = None

    with patch(
        "src.workflow.client.Client.connect", new=AsyncMock(return_value=fake_client),
    ) as mock_connect, patch(
        "src.workflow.client.settings", fake_settings,
    ):
        client = await get_temporal_client()

    mock_connect.assert_awaited_once_with(
        "host:7233", namespace="default", data_converter=ANY,
    )
    assert client is fake_client


@pytest.mark.asyncio
async def test_get_temporal_client_caches_instance():
    fake_client = MagicMock()
    fake_settings = MagicMock()
    fake_settings.temporal.target = "h:1"
    fake_settings.temporal.namespace = "n"

    import src.workflow.client as mod
    mod._client_singleton = None

    with patch(
        "src.workflow.client.Client.connect", new=AsyncMock(return_value=fake_client),
    ) as mock_connect, patch(
        "src.workflow.client.settings", fake_settings,
    ):
        a = await get_temporal_client()
        b = await get_temporal_client()

    assert a is b
    mock_connect.assert_awaited_once()
