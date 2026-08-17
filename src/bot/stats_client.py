"""Ingest-volume adapters for `/channels` and `/volume`.

Both hit routes that already exist on the API — `GET /api/v1/stats/messages`
and `GET /api/v1/stats/timeline`, verified live on 2026-08-17. The bot
therefore still talks to one HTTP surface and needs no MCP client, even
though the same numbers are also exposed as MCP tools.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import httpx

ChannelsFn = Callable[[], Awaitable[list[dict[str, Any]]]]
TimelineFn = Callable[..., Awaitable[list[dict[str, Any]]]]


def make_channels(
    *, api_base: str, api_key: str, timeout_s: float = 30.0,
) -> ChannelsFn:
    """Per-channel ingest totals → the `rows` list."""
    url = f"{api_base.rstrip('/')}/api/v1/stats/messages"

    async def channels() -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=timeout_s) as http:
            resp = await http.get(
                url, headers={"X-API-Key": api_key}, params={"group_by": "channel"},
            )
            resp.raise_for_status()
            return (resp.json() or {}).get("rows") or []

    return channels


def make_timeline(
    *, api_base: str, api_key: str, timeout_s: float = 30.0,
) -> TimelineFn:
    """Daily counts → the `buckets` list.

    ``date_field=doc_date`` — the post's own date, not when we happened to
    ingest it. The ingest date would show the corpus being back-filled
    rather than when anything was published.
    """
    url = f"{api_base.rstrip('/')}/api/v1/stats/timeline"

    async def timeline(
        *, channel: str | None = None, since: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"date_field": "doc_date"}
        # Sent only when given: an empty `channel=` is a filter matching
        # nothing, not an absent filter.
        if channel:
            params["channel"] = channel
        if since:
            params["since"] = since
        async with httpx.AsyncClient(timeout=timeout_s) as http:
            resp = await http.get(url, headers={"X-API-Key": api_key}, params=params)
            resp.raise_for_status()
            return (resp.json() or {}).get("buckets") or []

    return timeline


__all__ = ["make_channels", "make_timeline"]
