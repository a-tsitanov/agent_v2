"""Per-process LLM concurrency pool - single home for LLM gating (K+N).

ONE ``asyncio.Semaphore`` of size N (``LLM_POOL_N``) gates EVERY LLM call
across all roles.  Paired with admission K (in-flight documents) this is the
whole concurrency contract: "at most K documents in flight, at most N
concurrent LLM calls".

Role still selects the physical *model* (``build_llm(role)`` ->
``LiteLLMSettings.model_for``); it no longer affects gating.

Per-process, NOT distributed: the true cross-process GPU ceiling belongs at
the LiteLLM proxy (out of scope).
"""

from __future__ import annotations

import asyncio
from typing import Any

from llama_index.core.llms import LLM
from loguru import logger

from src.config import LLMRole
from src.retrieval.llm import build_llm
from src.retrieval.llm_semaphore import BoundedLLM


class Lane:
    """A named counting async gate: bounded concurrency + an in_use counter.

    Usable directly as ``async with lane:`` - that's how ``BoundedLLM``
    acquires it.  ``in_use`` is our own counter (never ``Semaphore._value``).
    """

    def __init__(self, name: str, cap: int) -> None:
        if cap < 1:
            raise ValueError(f"lane {name!r} cap must be >= 1, got {cap}")
        self.name = name
        self.cap = cap
        self._sem = asyncio.Semaphore(cap)
        self.in_use = 0

    @property
    def available(self) -> int:
        return self.cap - self.in_use

    async def __aenter__(self) -> Lane:
        if self._sem.locked():
            logger.warning(
                "LLMPool saturated (N={cap}, in_use={in_use}) - caller waiting",
                cap=self.cap, in_use=self.in_use,
            )
        await self._sem.acquire()
        self.in_use += 1
        return self

    async def __aexit__(self, *exc: Any) -> None:
        self._sem.release()
        self.in_use -= 1


class LLMPool:
    """Registry of role -> gated LLM, all sharing ONE global N semaphore."""

    def __init__(self, settings: Any) -> None:
        self._settings = settings
        self._n = settings.llm_pool.n
        self._lane = Lane("pool", self._n)
        self._llms: dict[str, LLM] = {}

    def get(self, role: LLMRole) -> LLM:
        if role not in self._llms:
            self._llms[role] = BoundedLLM(  # type: ignore[assignment]
                build_llm(role), gates=[self._lane],
            )
        return self._llms[role]

    def stats(self) -> dict[str, Any]:
        return {
            "mode": "kn",
            "n": self._n,
            "in_use": self._lane.in_use,
            "available": self._lane.available,
        }


_pool: LLMPool | None = None


def get_llm_pool() -> LLMPool:
    """Process-singleton accessor (lazy)."""
    global _pool
    if _pool is None:
        from src.config import settings
        _pool = LLMPool(settings)
    return _pool


def reset_for_tests() -> None:
    """Test hook - drop the singleton so the next get_llm_pool rebuilds."""
    global _pool
    _pool = None
