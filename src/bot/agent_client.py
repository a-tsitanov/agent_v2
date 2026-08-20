"""Ask the openclaw agent one question over its OpenAI-compatible endpoint.

openclaw ships `POST /v1/chat/completions` (enabled via
gateway.http.endpoints.chatCompletions). Unlike our /ask — one kb_search
call — the agent picks and CHAINS MCP tools. So the bot's value-add here
is orchestration, and the client is just a plain OpenAI call.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import httpx

# The gateway rejects any other model id with a message naming this one.
AGENT_MODEL = "openclaw"


def make_agent(
    *, base_url: str, token: str, timeout_s: float = 300.0,
) -> Callable[..., Awaitable[str]]:
    """Build `agent(question) -> answer`. Raises on non-2xx / transport
    error so the caller's fail-soft path records it against the request."""
    url = f"{base_url.rstrip('/')}/v1/chat/completions"

    async def agent(question: str, *, session: str = "") -> str:
        async with httpx.AsyncClient(timeout=timeout_s) as http:
            resp = await http.post(
                url,
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "model": AGENT_MODEL,
                    "messages": [{"role": "user", "content": question}],
                    # openclaw maps the OpenAI `user` field to its sessionKey
                    # (verified 2026-08-20: a second call with the same `user`
                    # recalled the first). One key per chat = per-chat agent
                    # memory, so follow-ups carry — the thing the bare call
                    # dropped.
                    **({"user": session} if session else {}),
                },
            )
            resp.raise_for_status()
            body = resp.json() or {}
        choices = body.get("choices") or [{}]
        return (choices[0].get("message") or {}).get("content") or ""

    return agent


__all__ = ["AGENT_MODEL", "make_agent"]
