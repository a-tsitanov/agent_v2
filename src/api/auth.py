"""X-API-Key auth dependency.

Same simple shared-key model enterprise-kb uses — comma-separated
list of valid keys in ``API_KEYS`` env var.  Health endpoint stays
public; everything under ``/api/v1`` requires the header.
"""

from __future__ import annotations

from fastapi import Header, HTTPException, status

from src.config import settings


async def require_api_key(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    if x_api_key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
        )
    if x_api_key not in settings.api.keys_list:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid X-API-Key",
        )
    return x_api_key
