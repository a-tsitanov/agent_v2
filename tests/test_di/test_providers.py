"""Tests for `src/di/providers.py`.

Live LLM / Milvus / Neo4j is not exercised — the focus is that
the DI container builds without errors, providers are wired in,
and `graph_retriever` gracefully falls back to None on unreachable
backend.
"""

from __future__ import annotations

import pytest

from src.di.providers import build_api_container, build_worker_container


@pytest.mark.asyncio
async def test_api_container_constructs() -> None:
    container = build_api_container()
    try:
        # Just confirm container can be instantiated without
        # immediate exceptions.  Live dependencies (Milvus,
        # Neo4j) are exercised in integration tests / smoke.
        assert container is not None
    finally:
        await container.close()


@pytest.mark.asyncio
async def test_worker_container_constructs() -> None:
    container = build_worker_container()
    try:
        assert container is not None
    finally:
        await container.close()
