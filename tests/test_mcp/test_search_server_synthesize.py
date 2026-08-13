"""The `synthesize` flag on the MCP-1 search tools (mirrors the REST
``/search/*`` flag — see ``src/api/routes/search_v2.py`` /
``src/models/search.py``).

MCP-1 has its own ``_local_params`` / ``_global_params`` copies, separate
from the FastAPI route helpers, taking a bare query string rather than a
request object — so this flag has to be threaded independently here.

Covers:
  * default (omitted) keeps `synthesize=True` on the built params — no
    existing client sees a behaviour change.
  * `synthesize=False` reaches `OrchestratorParams`/`GlobalSearchParams`
    for all four orchestrated tools (kb_search, kb_global_search,
    kb_drift_search — both legs, kb_auto_search — both legs).

The Temporal client is stubbed (no live cluster) — same pattern as
``tests/test_mcp/test_search_server_dates.py``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.workflow.contracts import GlobalSearchParams, OrchestratorParams, SearchOutcome
from src.workflow.search.router_wf import AutoSearchWorkflow, DriftSearchWorkflow


def _stub_client(outcome: SearchOutcome) -> MagicMock:
    handle = MagicMock()
    handle.result = AsyncMock(return_value=outcome)
    handle.query = AsyncMock(return_value={})
    client = MagicMock()
    client.start_workflow = AsyncMock(return_value=handle)
    return client


def _outcome(mode: str = "local") -> SearchOutcome:
    return SearchOutcome(query="q", mode=mode, answer=f"{mode} answer", latency_ms=1)


def _fake_ctx() -> MagicMock:
    ctx = MagicMock()
    ctx.report_progress = AsyncMock()
    return ctx


# ---------------------------------------------------------------------------
# _local_params / _global_params — direct helper assertions
# ---------------------------------------------------------------------------


def test_local_params_default_synthesize_true():
    from src.mcp.search_server import _local_params

    assert _local_params("q").synthesize is True


def test_local_params_carries_synthesize_false():
    from src.mcp.search_server import _local_params

    assert _local_params("q", synthesize=False).synthesize is False


def test_global_params_default_synthesize_true():
    from src.mcp.search_server import _global_params

    assert _global_params("q").synthesize is True


def test_global_params_carries_synthesize_false():
    from src.mcp.search_server import _global_params

    assert _global_params("q", synthesize=False).synthesize is False
    # drift_mode is independent of synthesize — both should thread through.
    p = _global_params("q", drift_mode=True, synthesize=False)
    assert p.drift_mode is True
    assert p.synthesize is False


# ---------------------------------------------------------------------------
# kb_search — synthesize threaded to OrchestratorParams
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kb_search_default_synthesize_true():
    from src.mcp import search_server

    client = _stub_client(_outcome())
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        result = await search_server.kb_search(query="q", ctx=_fake_ctx())

    assert "error" not in result
    params: OrchestratorParams = client.start_workflow.call_args.args[1]
    assert params.synthesize is True


@pytest.mark.asyncio
async def test_kb_search_synthesize_false_reaches_params():
    from src.mcp import search_server

    client = _stub_client(_outcome())
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        result = await search_server.kb_search(query="q", ctx=_fake_ctx(), synthesize=False)

    assert "error" not in result
    params: OrchestratorParams = client.start_workflow.call_args.args[1]
    assert params.synthesize is False


# ---------------------------------------------------------------------------
# kb_global_search — synthesize threaded to GlobalSearchParams
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kb_global_search_default_synthesize_true():
    from src.mcp import search_server

    client = _stub_client(_outcome("global"))
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        await search_server.kb_global_search(query="q", ctx=_fake_ctx())

    params: GlobalSearchParams = client.start_workflow.call_args.args[1]
    assert params.synthesize is True


@pytest.mark.asyncio
async def test_kb_global_search_synthesize_false_reaches_params():
    from src.mcp import search_server

    client = _stub_client(_outcome("global"))
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        await search_server.kb_global_search(query="q", ctx=_fake_ctx(), synthesize=False)

    params: GlobalSearchParams = client.start_workflow.call_args.args[1]
    assert params.synthesize is False


# ---------------------------------------------------------------------------
# kb_drift_search — synthesize threaded to BOTH legs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kb_drift_search_default_synthesize_true_both_legs():
    from src.mcp import search_server

    client = _stub_client(_outcome("drift"))
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        await search_server.kb_drift_search(query="q", ctx=_fake_ctx())

    call = client.start_workflow.call_args
    assert call.args[0] is DriftSearchWorkflow.run
    local_params, global_params = call.kwargs["args"]
    assert local_params.synthesize is True
    assert global_params.synthesize is True


@pytest.mark.asyncio
async def test_kb_drift_search_synthesize_false_reaches_both_legs():
    from src.mcp import search_server

    client = _stub_client(_outcome("drift"))
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        await search_server.kb_drift_search(query="q", ctx=_fake_ctx(), synthesize=False)

    local_params, global_params = client.start_workflow.call_args.kwargs["args"]
    assert local_params.synthesize is False
    assert global_params.synthesize is False
    # drift_mode must still be forced True regardless of synthesize.
    assert global_params.drift_mode is True


# ---------------------------------------------------------------------------
# kb_auto_search — synthesize threaded to BOTH legs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kb_auto_search_default_synthesize_true_both_legs():
    from src.mcp import search_server

    client = _stub_client(_outcome("local"))
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        await search_server.kb_auto_search(query="q", ctx=_fake_ctx())

    call = client.start_workflow.call_args
    assert call.args[0] is AutoSearchWorkflow.run
    local_params, global_params = call.kwargs["args"]
    assert local_params.synthesize is True
    assert global_params.synthesize is True


@pytest.mark.asyncio
async def test_kb_auto_search_synthesize_false_reaches_both_legs():
    from src.mcp import search_server

    client = _stub_client(_outcome("local"))
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        await search_server.kb_auto_search(query="q", ctx=_fake_ctx(), synthesize=False)

    local_params, global_params = client.start_workflow.call_args.kwargs["args"]
    assert local_params.synthesize is False
    assert global_params.synthesize is False


# ---------------------------------------------------------------------------
# Tool schemas expose `synthesize`
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_all_four_search_tools_schema_expose_synthesize():
    from src.mcp import search_server

    tools = await search_server.mcp._list_tools()
    by_name = {t.name: t for t in tools}
    for name in ("kb_search", "kb_global_search", "kb_drift_search", "kb_auto_search"):
        props = by_name[name].parameters.get("properties", {})
        assert "synthesize" in props, f"{name} missing synthesize param"
