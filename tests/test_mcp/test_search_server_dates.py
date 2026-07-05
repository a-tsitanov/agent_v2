"""Date-interval params on the MCP-1 search tools (mirrors the REST
``/search/local|drift|auto`` bounds — see ``src/api/routes/search_v2.py``).

Covers:
  * bounds threading — calling the tool with ISO dates starts the
    workflow with the matching epoch fields on ``OrchestratorParams``
    (``kb_search``, and the LOCAL leg of ``kb_drift_search`` /
    ``kb_auto_search``; the GLOBAL leg stays undated by design).
  * invalid ISO input returns the tool's error-dict shape instead of
    raising / crashing the tool call.
  * omitted date params keep prior behaviour (all four epoch fields
    stay None — same as before this feature).

The Temporal client is stubbed (no live cluster) — same pattern as
``tests/test_api/test_search_v2_routes.py`` / ``test_analyze_route.py``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.retrieval.date_filters import iso_to_epoch_days
from src.workflow.contracts import GlobalSearchParams, OrchestratorParams, SearchOutcome
from src.workflow.search.orchestrator import SearchOrchestratorWorkflow
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
# kb_search — bounds threading
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kb_search_threads_all_four_date_bounds():
    from src.mcp import search_server

    client = _stub_client(_outcome())
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        result = await search_server.kb_search(
            query="q",
            ctx=_fake_ctx(),
            doc_date_after="2024-01-01",
            doc_date_before="2024-06-30",
            created_after="2024-02-01",
            created_before="2024-05-01",
        )

    assert "error" not in result
    client.start_workflow.assert_awaited_once()
    call = client.start_workflow.call_args
    assert call.args[0] is SearchOrchestratorWorkflow.run
    params: OrchestratorParams = call.args[1]
    assert params.doc_date_after_epoch == iso_to_epoch_days("2024-01-01")
    assert params.doc_date_before_epoch == iso_to_epoch_days("2024-06-30")
    assert params.inserted_after_epoch == iso_to_epoch_days("2024-02-01")
    assert params.inserted_before_epoch == iso_to_epoch_days("2024-05-01")


@pytest.mark.asyncio
async def test_kb_search_omitted_dates_keep_none_epochs():
    """No date params supplied → all four epoch fields stay None (prior
    behaviour, unchanged)."""
    from src.mcp import search_server

    client = _stub_client(_outcome())
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        result = await search_server.kb_search(query="q", ctx=_fake_ctx())

    assert "error" not in result
    params: OrchestratorParams = client.start_workflow.call_args.args[1]
    assert params.doc_date_after_epoch is None
    assert params.doc_date_before_epoch is None
    assert params.inserted_after_epoch is None
    assert params.inserted_before_epoch is None


@pytest.mark.asyncio
async def test_kb_search_invalid_date_returns_error_dict_without_raising():
    from src.mcp import search_server

    client = _stub_client(_outcome())
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        result = await search_server.kb_search(
            query="q", ctx=_fake_ctx(), doc_date_after="not-a-date",
        )

    assert result == {
        "error": "invalid date format, expected YYYY-MM-DD",
        "field": "doc_date_after",
    }
    # Malformed date must short-circuit BEFORE any workflow starts.
    client.start_workflow.assert_not_awaited()


@pytest.mark.asyncio
async def test_kb_search_invalid_date_names_the_right_field():
    from src.mcp import search_server

    client = _stub_client(_outcome())
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        result = await search_server.kb_search(
            query="q", ctx=_fake_ctx(),
            doc_date_after="2024-01-01",  # valid
            created_before="2024-13-40",  # invalid — bad month/day
        )

    assert result == {
        "error": "invalid date format, expected YYYY-MM-DD",
        "field": "created_before",
    }
    client.start_workflow.assert_not_awaited()


# ---------------------------------------------------------------------------
# kb_drift_search — bounds threaded to the LOCAL leg only
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kb_drift_search_threads_bounds_to_local_leg_only():
    from src.mcp import search_server

    client = _stub_client(_outcome("drift"))
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        result = await search_server.kb_drift_search(
            query="q", ctx=_fake_ctx(),
            doc_date_after="2024-01-01", created_before="2024-05-01",
        )

    assert "error" not in result
    client.start_workflow.assert_awaited_once()
    call = client.start_workflow.call_args
    assert call.args[0] is DriftSearchWorkflow.run
    local_params, global_params = call.kwargs["args"]
    assert isinstance(local_params, OrchestratorParams)
    assert isinstance(global_params, GlobalSearchParams)
    assert local_params.doc_date_after_epoch == iso_to_epoch_days("2024-01-01")
    assert local_params.inserted_before_epoch == iso_to_epoch_days("2024-05-01")
    # GlobalSearchParams carries no date-bound fields at all (undated by design).
    assert not hasattr(global_params, "doc_date_after_epoch")


@pytest.mark.asyncio
async def test_kb_drift_search_invalid_date_returns_error_without_raising():
    from src.mcp import search_server

    client = _stub_client(_outcome("drift"))
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        result = await search_server.kb_drift_search(
            query="q", ctx=_fake_ctx(), doc_date_before="bogus",
        )

    assert result == {
        "error": "invalid date format, expected YYYY-MM-DD",
        "field": "doc_date_before",
    }
    client.start_workflow.assert_not_awaited()


@pytest.mark.asyncio
async def test_kb_drift_search_omitted_dates_keep_none_epochs():
    from src.mcp import search_server

    client = _stub_client(_outcome("drift"))
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        await search_server.kb_drift_search(query="q", ctx=_fake_ctx())

    local_params, _global_params = client.start_workflow.call_args.kwargs["args"]
    assert local_params.doc_date_after_epoch is None
    assert local_params.doc_date_before_epoch is None
    assert local_params.inserted_after_epoch is None
    assert local_params.inserted_before_epoch is None


# ---------------------------------------------------------------------------
# kb_auto_search — bounds threaded to the LOCAL leg only
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kb_auto_search_threads_bounds_to_local_leg_only():
    from src.mcp import search_server

    client = _stub_client(_outcome("local"))
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        result = await search_server.kb_auto_search(
            query="q", ctx=_fake_ctx(),
            doc_date_after="2024-01-01", doc_date_before="2024-12-31",
            created_after="2024-02-15", created_before="2024-11-15",
        )

    assert "error" not in result
    client.start_workflow.assert_awaited_once()
    call = client.start_workflow.call_args
    assert call.args[0] is AutoSearchWorkflow.run
    local_params, global_params = call.kwargs["args"]
    assert isinstance(local_params, OrchestratorParams)
    assert isinstance(global_params, GlobalSearchParams)
    assert local_params.doc_date_after_epoch == iso_to_epoch_days("2024-01-01")
    assert local_params.doc_date_before_epoch == iso_to_epoch_days("2024-12-31")
    assert local_params.inserted_after_epoch == iso_to_epoch_days("2024-02-15")
    assert local_params.inserted_before_epoch == iso_to_epoch_days("2024-11-15")
    assert not hasattr(global_params, "doc_date_after_epoch")


@pytest.mark.asyncio
async def test_kb_auto_search_invalid_date_returns_error_without_raising():
    from src.mcp import search_server

    client = _stub_client(_outcome("local"))
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        result = await search_server.kb_auto_search(
            query="q", ctx=_fake_ctx(), created_after="2024/01/01",
        )

    assert result == {
        "error": "invalid date format, expected YYYY-MM-DD",
        "field": "created_after",
    }
    client.start_workflow.assert_not_awaited()


@pytest.mark.asyncio
async def test_kb_auto_search_omitted_dates_keep_none_epochs():
    from src.mcp import search_server

    client = _stub_client(_outcome("local"))
    with patch(
        "src.mcp.search_server.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        await search_server.kb_auto_search(query="q", ctx=_fake_ctx())

    local_params, _global_params = client.start_workflow.call_args.kwargs["args"]
    assert local_params.doc_date_after_epoch is None
    assert local_params.doc_date_before_epoch is None
    assert local_params.inserted_after_epoch is None
    assert local_params.inserted_before_epoch is None


# ---------------------------------------------------------------------------
# kb_global_search — no date params exposed (community summaries undated)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kb_global_search_schema_has_no_date_params():
    from src.mcp import search_server

    tools = await search_server.mcp._list_tools()
    by_name = {t.name: t for t in tools}
    props = by_name["kb_global_search"].parameters.get("properties", {})
    for field in ("doc_date_after", "doc_date_before", "created_after", "created_before"):
        assert field not in props, f"kb_global_search should not expose {field}"
    assert "not applicable to global mode" in by_name["kb_global_search"].description


@pytest.mark.asyncio
async def test_kb_search_and_drift_and_auto_schemas_expose_date_params():
    from src.mcp import search_server

    tools = await search_server.mcp._list_tools()
    by_name = {t.name: t for t in tools}
    for name in ("kb_search", "kb_drift_search", "kb_auto_search"):
        props = by_name[name].parameters.get("properties", {})
        for field in ("doc_date_after", "doc_date_before", "created_after", "created_before"):
            assert field in props, f"{name} missing {field}"
