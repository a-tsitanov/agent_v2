"""Minimal async MediaWiki Action API client (login + read/edit page +
sitelink). Used by the wiki editor to write entity article pages.

Cookie-session auth via httpx.AsyncClient; CSRF token fetched per edit."""
from __future__ import annotations

from typing import Any

import httpx
from loguru import logger


class AsyncMediaWiki:
    def __init__(self, client: httpx.AsyncClient, api_url: str) -> None:
        self._c = client
        self._api = api_url

    @classmethod
    async def login(cls, api_url: str, user: str, password: str) -> "AsyncMediaWiki":
        client = httpx.AsyncClient(timeout=30.0)
        # 1) login token
        r = await client.get(api_url, params={
            "action": "query", "meta": "tokens", "type": "login", "format": "json"})
        r.raise_for_status()
        ltoken = r.json()["query"]["tokens"]["logintoken"]
        # 2) login
        r = await client.post(api_url, data={
            "action": "login", "lgname": user, "lgpassword": password,
            "lgtoken": ltoken, "format": "json"})
        r.raise_for_status()
        if r.json().get("login", {}).get("result") != "Success":
            raise RuntimeError(f"mediawiki login failed: {r.json()}")
        return cls(client=client, api_url=api_url)

    async def _csrf(self) -> str:
        r = await self._c.get(self._api, params={
            "action": "query", "meta": "tokens", "format": "json"})
        r.raise_for_status()
        return r.json()["query"]["tokens"]["csrftoken"]

    async def get_page(self, title: str) -> str:
        r = await self._c.get(self._api, params={
            "action": "query", "prop": "revisions", "titles": title,
            "rvslots": "main", "rvprop": "content", "format": "json"})
        r.raise_for_status()
        pages = r.json().get("query", {}).get("pages", {})
        for _pid, page in pages.items():
            if "missing" in page:
                return ""
            revs = page.get("revisions") or []
            if revs:
                return revs[0]["slots"]["main"]["content"]
        return ""

    async def upsert_page(self, title: str, wikitext: str, summary: str) -> bool:
        token = await self._csrf()
        r = await self._c.post(self._api, data={
            "action": "edit", "title": title, "text": wikitext,
            "summary": summary, "bot": "1", "token": token, "format": "json"})
        r.raise_for_status()
        result = r.json().get("edit", {}).get("result")
        if result != "Success":
            logger.warning("mediawiki edit non-success title={t} resp={r}",
                           t=title, r=r.json())
            return False
        return True

    async def ensure_sitelink(self, qid: str, title: str) -> bool:
        """Link a Wikibase Item to its MediaWiki article page. Best-effort."""
        if not qid:
            return False
        token = await self._csrf()
        r = await self._c.post(self._api, data={
            "action": "wbsetsitelink", "id": qid,
            "linksite": "kbwiki", "linktitle": title,
            "token": token, "format": "json"})
        r.raise_for_status()
        return "error" not in r.json()

    async def aclose(self) -> None:
        await self._c.aclose()
