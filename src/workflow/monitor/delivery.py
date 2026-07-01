"""Arc 2 push — fail-soft generic webhook delivery for :Alert records."""

from __future__ import annotations

import httpx
from loguru import logger


async def post_alert(url: str, payload: dict, *, timeout_s: float = 5.0) -> bool:
    """POST one alert payload as JSON; True on 2xx, False on any error. Never raises."""
    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            resp = await client.post(url, json=payload)
        return 200 <= resp.status_code < 300
    except Exception as exc:
        logger.warning("post_alert failed url={u}: {e}", u=url, e=exc)
        return False
