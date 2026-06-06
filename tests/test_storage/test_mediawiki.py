import pytest
from unittest.mock import AsyncMock, MagicMock
from src.storage.mediawiki import AsyncMediaWiki


def _client_returning(payloads):
    """httpx-like async client whose .get/.post return queued JSON payloads."""
    it = iter(payloads)

    async def _call(*a, **kw):
        resp = MagicMock()
        resp.json.return_value = next(it)
        resp.raise_for_status = MagicMock()
        return resp
    c = MagicMock()
    c.get = AsyncMock(side_effect=_call)
    c.post = AsyncMock(side_effect=_call)
    return c


@pytest.mark.asyncio
async def test_get_page_returns_wikitext():
    c = _client_returning([
        {"query": {"pages": {"1": {"revisions": [{"slots": {"main": {"content": "WT"}}}]}}}},
    ])
    mw = AsyncMediaWiki(client=c, api_url="http://x/w/api.php")
    assert await mw.get_page("Title") == "WT"


@pytest.mark.asyncio
async def test_get_missing_page_returns_empty():
    c = _client_returning([{"query": {"pages": {"-1": {"missing": ""}}}}])
    mw = AsyncMediaWiki(client=c, api_url="http://x/w/api.php")
    assert await mw.get_page("Nope") == ""


@pytest.mark.asyncio
async def test_get_page_reads_legacy_formatversion1_star_slot():
    # Default MediaWiki (formatversion=1) returns slot content under "*",
    # not "content". get_page must read both (verified live against a real
    # wikibase-docker instance).
    c = _client_returning([
        {"query": {"pages": {"7": {"revisions": [{"slots": {"main": {"*": "WT1"}}}]}}}},
    ])
    mw = AsyncMediaWiki(client=c, api_url="http://x/w/api.php")
    assert await mw.get_page("Title") == "WT1"


@pytest.mark.asyncio
async def test_upsert_page_fetches_token_then_edits():
    c = _client_returning([
        {"query": {"tokens": {"csrftoken": "T+\\"}}},     # csrf token
        {"edit": {"result": "Success"}},                  # edit
    ])
    mw = AsyncMediaWiki(client=c, api_url="http://x/w/api.php")
    ok = await mw.upsert_page("Title", "NEW", summary="s")
    assert ok is True
    # the edit POST carried the token + text
    _args, kwargs = c.post.call_args
    data = kwargs.get("data") or {}
    assert data.get("token") == "T+\\" and data.get("text") == "NEW"


@pytest.mark.asyncio
async def test_ensure_sitelink_uses_configured_site_id():
    c = _client_returning([
        {"query": {"tokens": {"csrftoken": "T"}}},   # csrf
        {"entity": {"id": "Q5"}},                      # wbsetsitelink ok
    ])
    mw = AsyncMediaWiki(client=c, api_url="http://x/w/api.php", site_global_id="myWiki")
    await mw.ensure_sitelink("Q5", "Some Title")
    _args, kwargs = c.post.call_args
    data = kwargs.get("data") or {}
    assert data.get("linksite") == "myWiki" and data.get("linktitle") == "Some Title"
