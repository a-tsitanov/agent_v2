"""Per-process LLM concurrency pool — single home for LLM gating.

Two modes (selected by ``LLM_POOL_GLOBAL_N``):

* **Hierarchical** (default, ``global_n == 0``) — one ``LLMPool`` owns a
  per-tier global semaphore (``small`` = GPU capacity, ``large`` = OpenAI
  budget) and a per-role *lane* ceiling that intentionally over-subscribes
  the tier total.  Each small-tier call acquires its lane permit first,
  then the tier-global (lane-first keeps the scarce global occupied only
  around the actual call).  Lets one workload fill the GPU while reserving
  headroom (``judge_floor``) so a flood of one role can't starve another.

* **Global / simple** (``global_n > 0``) — ONE semaphore of size ``N``
  gates EVERY role and tier; ``lane_caps`` and the per-tier totals are
  ignored.  The single operator knob becomes "at most N concurrent LLM
  calls", paired with admission ``K`` (in-flight documents).  Trades the
  per-role headroom guarantee for one number.

All call-sites for a role share ONE wrapped LLM, so the scattered
``BoundedLLM`` instances collapse into one gate per role.

This is a *per-process* control, NOT distributed: the true cross-process
GPU ceiling belongs at the LiteLLM proxy (out of scope).
"""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger
from llama_index.core.llms import LLM

from src.config import LLMRole
from src.retrieval.llm import build_llm
from src.retrieval.llm_semaphore import BoundedLLM


class Lane:
    """A named counting async gate: bounded concurrency + an in_use counter.

    Usable directly as ``async with lane:`` — that's how ``BoundedLLM``
    acquires it.  ``in_use`` is our own counter (never ``Semaphore._value``).
    """

    def __init__(self, name: str, tier: str, cap: int) -> None:
        if cap < 1:
            raise ValueError(f"lane {name!r} cap must be >= 1, got {cap}")
        self.name = name
        self.tier = tier
        self.cap = cap
        self._sem = asyncio.Semaphore(cap)
        self.in_use = 0

    @property
    def available(self) -> int:
        return self.cap - self.in_use

    async def __aenter__(self) -> "Lane":
        if self._sem.locked():
            logger.warning(
                "LLMPool lane {name!r} saturated (cap={cap}, in_use={in_use}) "
                "— caller waiting",
                name=self.name, cap=self.cap, in_use=self.in_use,
            )
        await self._sem.acquire()
        self.in_use += 1
        return self

    async def __aexit__(self, *exc: Any) -> None:
        self._sem.release()
        self.in_use -= 1


class LLMPool:
    """Registry of role -> gated LLM, with shared per-tier globals."""

    # settings is the project Settings; typed Any to avoid a circular
    # import (src.config would import back through the retrieval stack).
    def __init__(self, settings: Any) -> None:
        self._settings = settings
        cfg = settings.llm_pool
        # Global/simple mode when LLM_POOL_GLOBAL_N > 0.  The isinstance
        # guard keeps MagicMock-based tests (auto-attr ⇒ not an int) in the
        # default hierarchical mode without having to set global_n.
        gn = getattr(cfg, "global_n", 0)
        self._global_n = gn if isinstance(gn, int) and gn > 0 else 0
        if self._global_n > 0:
            self._global_lane: Lane | None = Lane("global", "global", self._global_n)
            self._tiers: dict[str, Lane] = {}
        else:
            self._global_lane = None
            self._tiers = {
                "small": Lane("tier:small", "small", cfg.tier_small_total),
                "large": Lane("tier:large", "large", cfg.tier_large_total),
            }
        self._lanes: dict[str, Lane] = {}
        self._llms: dict[str, LLM] = {}

    def get(self, role: LLMRole) -> LLM:
        if role not in self._llms:
            if self._global_lane is not None:
                # Global mode: every role shares the one semaphore.
                self._lanes.setdefault("global", self._global_lane)
                gates = [self._global_lane]
                logger.debug(
                    "LLMPool[global] register role={role!r} N={n}",
                    role=role, n=self._global_n,
                )
            else:
                tier = self._settings.litellm.tier_for(role)
                cap = self._settings.llm_pool.lane_caps[role]
                lane = Lane(role, tier, cap)
                self._lanes[role] = lane
                logger.debug(
                    "LLMPool register role={role!r} tier={tier} lane_cap={cap}",
                    role=role, tier=tier, cap=cap,
                )
                # lane first, then tier-global (consistent order => no deadlock).
                gates = [lane, self._tiers[tier]]
            self._llms[role] = BoundedLLM(  # type: ignore[assignment]
                build_llm(role), gates=gates,
            )
        return self._llms[role]

    def stats(self) -> dict[str, Any]:
        return {
            "lanes": {
                name: {
                    "tier": ln.tier,
                    "cap": ln.cap,
                    "in_use": ln.in_use,
                    "available": ln.available,
                }
                for name, ln in self._lanes.items()
            },
            "tiers": {
                name: {"cap": t.cap, "in_use": t.in_use, "available": t.available}
                for name, t in self._tiers.items()
            },
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
    """Test hook — drop the singleton so the next get_llm_pool rebuilds."""
    global _pool
    _pool = None
