"""Bounded-concurrency wrapper for the project LLM.

The default LlamaIndex ``LLM`` interface has no built-in concurrency
limit.  When multiple ReAct sessions hit the agent endpoints
simultaneously (or several MCP clients call ``graph_search`` whose
``LLMSynonymRetriever`` does a normalisation LLM call), they all rush
the same GPU / proxy.  We don't want that.

``BoundedLLM`` is a thin composition wrapper that gates every async
chat method through one ``asyncio.Semaphore``.  Cap is set per-process
via ``settings.agent.llm_max_concurrent``.  Sync methods (``chat``,
``complete``) are forwarded without gating — production code paths
should use async variants; sync paths are mostly for diag scripts.

Why composition not subclass:
  * ``OpenAILike`` / other LlamaIndex LLM concrete types pull in
    pydantic v2 metadata that's tricky to subclass cleanly.
  * Wrapping by composition keeps the public surface identical via
    ``__getattr__`` fallback for methods we haven't explicitly bound.

The wrapper is created once at DI bootstrap time (``src/di/providers.py``)
and injected wherever an ``LLM`` is needed downstream, so LLM call-sites
(e.g. the search synthesizer and ``GraphRetriever``'s
``LLMSynonymRetriever``) get rate-limited transparently.
"""

from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from typing import Any, AsyncIterator

from llama_index.core.llms import LLM


class BoundedLLM:
    """Wraps an LLM with a process-wide asyncio.Semaphore, or through an
    ordered list of async context-manager gates (the ``gates=`` path)."""

    def __init__(
        self,
        inner: LLM,
        *,
        max_concurrent: int | None = None,
        gates: list[AbstractAsyncContextManager[Any]] | None = None,
    ) -> None:
        if gates is None and max_concurrent is None:
            raise ValueError("supply either max_concurrent= or gates=")
        if gates is None:
            if max_concurrent < 1:
                raise ValueError(
                    f"max_concurrent must be >= 1, got {max_concurrent}",
                )
            gates = [asyncio.Semaphore(max_concurrent)]
            self._max_concurrent: int | None = max_concurrent
        else:
            if not gates:
                raise ValueError("gates must be a non-empty list")
            self._max_concurrent = None
        self._inner = inner
        self._gates = gates
        # Back-compat: keep a `_sem` alias to the first gate for any
        # introspection that referenced it.  Note: gate[0] may be a
        # non-Semaphore (e.g. a pool Lane), so callers must not assume
        # `.locked()` / `.acquire()` are available.
        self._sem = gates[0]

    @asynccontextmanager
    async def _acquire_all(self):
        async with AsyncExitStack() as stack:
            for g in self._gates:
                await stack.enter_async_context(g)
            yield

    # ── async hot path methods (gated) ───────────────────────────────

    async def achat(self, *a, **kw):
        async with self._acquire_all():
            return await self._inner.achat(*a, **kw)

    async def acomplete(self, *a, **kw):
        async with self._acquire_all():
            return await self._inner.acomplete(*a, **kw)

    async def achat_with_tools(self, *a, **kw):
        async with self._acquire_all():
            return await self._inner.achat_with_tools(*a, **kw)

    async def astructured_predict(self, *a, **kw):
        async with self._acquire_all():
            return await self._inner.astructured_predict(*a, **kw)

    async def astream_chat(self, *a, **kw) -> AsyncIterator[Any]:
        # streaming variant: hold the gates for the entire stream.
        # one concurrent stream per slot — fine.
        async with self._acquire_all():
            async for chunk in self._inner.astream_chat(*a, **kw):
                yield chunk

    async def astream_complete(self, *a, **kw) -> AsyncIterator[Any]:
        async with self._acquire_all():
            async for chunk in self._inner.astream_complete(*a, **kw):
                yield chunk

    async def apredict(self, *a, **kw):
        async with self._acquire_all():
            return await self._inner.apredict(*a, **kw)

    async def aget_tool_calls_from_response(self, *a, **kw):
        # not actually a remote call — but forward for completeness.
        return await self._inner.aget_tool_calls_from_response(*a, **kw)

    def get_tool_calls_from_response(self, *a, **kw):
        return self._inner.get_tool_calls_from_response(*a, **kw)

    # ── pass-through for everything else (metadata, sync, …) ─────────

    def __getattr__(self, name: str) -> Any:
        # Note: only called when normal attribute lookup fails on the
        # wrapper.  Lets things like ``llm.metadata``, ``llm.callback_manager``,
        # ``llm.model`` etc. resolve to the inner LLM transparently.
        return getattr(self._inner, name)

    # ── introspection helpers ────────────────────────────────────────

    @property
    def max_concurrent(self) -> int | None:
        return self._max_concurrent

    @property
    def inner(self) -> LLM:
        return self._inner

    def __repr__(self) -> str:
        return (
            f"BoundedLLM(inner={self._inner.__class__.__name__}, "
            f"max_concurrent={self._max_concurrent})"
        )


def wrap_if_needed(llm: LLM, *, max_concurrent: int) -> LLM:
    """Return an already-wrapped LLM untouched; wrap a raw one.

    Lets DI bootstrap call this idempotently — useful when the same
    container fixture is reused in tests where we sometimes inject a
    pre-wrapped mock.
    """
    if isinstance(llm, BoundedLLM):
        return llm
    return BoundedLLM(llm, max_concurrent=max_concurrent)  # type: ignore[return-value]
