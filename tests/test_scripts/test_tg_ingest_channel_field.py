"""post_ingest puts the channel slug into the multipart form data."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

from scripts.tg_ingest import post_ingest


def test_post_ingest_sends_channel() -> None:
    http = MagicMock()
    resp = MagicMock()
    resp.status_code = 202
    http.post = AsyncMock(return_value=resp)

    asyncio.run(
        post_ingest(
            http, "http://api", "k", "tg_acme_9.txt", "body", "2026-07-23",
            None, group="news", channel="acme",
        )
    )

    _, kwargs = http.post.call_args
    assert kwargs["data"]["channel"] == "acme"
    assert kwargs["data"]["group"] == "news"
