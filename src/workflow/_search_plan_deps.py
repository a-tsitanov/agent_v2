"""Process-level cache for the plan-execute search dependencies (R2).

Separate from ``_search_deps`` so the new plan-execute flow can build
its own LLM tiers (small ``plan`` model for decomposition) without
disturbing the legacy ReAct deps cache.  The retriever / graph
retriever / chunk repo are shared with ``_search_deps`` (same stores),
so we delegate to it rather than rebuilding clients.

Lock-protected lazy init, mirroring ``_search_deps``.
"""

from __future__ import annotations

import asyncio
from typing import Any

from llama_index.core.llms import LLM

from src.retrieval.llm import build_llm
from src.retrieval.llm_semaphore import wrap_if_needed

_lock = asyncio.Lock()
_state: dict[str, Any] = {"plan_llm": None}


async def get_plan_llm() -> LLM:
    """Small-tier planner LLM (role ``plan``) wrapped in the shared
    BoundedLLM semaphore so it shares the GPU budget with retrieval."""
    async with _lock:
        if _state["plan_llm"] is None:
            from src.config import settings
            _state["plan_llm"] = wrap_if_needed(
                build_llm("plan"),
                max_concurrent=settings.agent.llm_max_concurrent,
            )
    return _state["plan_llm"]


def reset_for_tests() -> None:
    """Test hook — clear the planner LLM cache."""
    _state["plan_llm"] = None
