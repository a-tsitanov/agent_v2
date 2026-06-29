# src/analytics/store_query.py
"""Read-only Neo4j query execution for analytics primitives (fail-soft)."""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger


def _run_query(store: Any, cypher: str, params: dict | None = None) -> list[dict]:
    """Sync execution; returns raw rows. Mirrors src/graph/analysis.py::_run_query."""
    return list(store.structured_query(cypher, param_map=params or {}))


async def run_rows(store: Any | None, cypher: str, params: dict | None = None) -> list[dict]:
    """Run ``cypher`` off the event loop; ``[]`` on no-store or any error."""
    if store is None:
        return []
    try:
        return await asyncio.to_thread(_run_query, store, cypher, params)
    except Exception as exc:  # fail-soft like analysis.py
        logger.warning("analytics query failed: {e}", e=exc)
        return []
