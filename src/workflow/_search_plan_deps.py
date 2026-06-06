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


_lock = asyncio.Lock()
_state: dict[str, Any] = {"plan_llm": None}


async def get_plan_llm() -> LLM:
    """Small-tier planner LLM (role ``plan``) from the shared pool."""
    from src.retrieval.llm_pool import get_llm_pool
    return get_llm_pool().get("plan")


def reset_for_tests() -> None:
    """Test hook — clear the planner LLM cache."""
    _state["plan_llm"] = None
