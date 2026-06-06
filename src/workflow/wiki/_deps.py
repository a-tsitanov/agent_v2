"""Process-singleton MediaWiki client for wiki activities."""
from __future__ import annotations

import asyncio

from src.config import settings
from src.storage.mediawiki import AsyncMediaWiki

_lock = asyncio.Lock()
_mw: AsyncMediaWiki | None = None


def _api_url() -> str:
    cfg = settings.wiki
    if cfg.mediawiki_api_url:
        return cfg.mediawiki_api_url
    return settings.wikibase.base_url.rstrip("/") + "/w/api.php"


async def get_mediawiki() -> AsyncMediaWiki:
    global _mw
    async with _lock:
        if _mw is None:
            _mw = await AsyncMediaWiki.login(
                _api_url(), settings.wikibase.bot_user,
                settings.wikibase.bot_password.get_secret_value())
    return _mw


def reset_for_tests() -> None:
    global _mw, _lock
    _mw = None
    _lock = asyncio.Lock()
