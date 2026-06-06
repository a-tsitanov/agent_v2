"""Process-level cache for the plan-execute search dependencies (R2).

Separate from ``_search_deps`` so the new plan-execute flow can build
its own LLM tiers (small ``plan`` model for decomposition) without
disturbing the legacy ReAct deps cache.  The retriever / graph
retriever / chunk repo are shared with ``_search_deps`` (same stores),
so we delegate to it rather than rebuilding clients.

``get_plan_llm`` is a thin wrapper around the shared ``LLMPool``; no
module-local state or lock is needed.
"""

from __future__ import annotations

from llama_index.core.llms import LLM


async def get_plan_llm() -> LLM:
    """Small-tier planner LLM (role ``plan``) from the shared pool."""
    from src.retrieval.llm_pool import get_llm_pool
    return get_llm_pool().get("plan")
