"""Unit tests for the ``retrieve_subquestion`` activity wiring (R3b).

Two surfaces under test:

1. ``top_entity_name`` — PURE helper that picks the top entity_name from
   a ``graph_search`` observation JSON string.  No I/O, tested directly.
2. The retrieve activity's deterministic graph_walk seeding: after
   ``graph_search`` yields ≥1 entity (and the flag is on), a
   ``graph_walk`` is dispatched with ``start_entity=<top entity>`` and
   its sources are merged (deduped by chunk_id).  Flag-gated + fail-open.

dispatch / the graph_retriever are mocked; no live Temporal / Neo4j.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from llama_index.core.schema import NodeWithScore, TextNode

from src.retrieval import atomic_tools
from src.workflow.contracts import RetrieveParams
from src.workflow.search.activities import retrieve as retrieve_mod
from src.workflow.search.activities.retrieve import (
    retrieve_subquestion,
    top_entity_name,
)

# ── helpers ──────────────────────────────────────────────────────────


def _node(node_id: str, text: str = "x", **md) -> NodeWithScore:
    return NodeWithScore(
        node=TextNode(id_=node_id, text=text, metadata=md),
        score=0.5,
    )


def _gs_obs(entity_names: list[str]) -> str:
    return json.dumps(
        {
            "entities": [
                {"entity_name": n, "entity_type": "Person"}
                for n in entity_names
            ],
            "relations": [],
        },
        ensure_ascii=False,
    )


# ── top_entity_name (pure helper) ───────────────────────────────────


def test_top_entity_name_picks_first_entity():
    obs = _gs_obs(["Иванов", "Петров", "Сидоров"])
    assert top_entity_name(obs) == "Иванов"


def test_top_entity_name_empty_entities_returns_none():
    obs = json.dumps({"entities": [], "relations": []})
    assert top_entity_name(obs) is None


def test_top_entity_name_no_entities_key_returns_none():
    assert top_entity_name(json.dumps({"relations": []})) is None


def test_top_entity_name_garbled_json_returns_none():
    assert top_entity_name("{not valid json") is None


def test_top_entity_name_list_observation_returns_none():
    # vector_search-style observation (a JSON list) has no entities.
    assert top_entity_name(json.dumps([{"chunk_id": "c1"}])) is None


def test_top_entity_name_blank_name_skips_to_next():
    obs = json.dumps(
        {"entities": [{"entity_name": ""}, {"entity_name": "Петров"}]}
    )
    assert top_entity_name(obs) == "Петров"


# ── retrieve activity graph_walk seeding ────────────────────────────


class _DispatchRecorder:
    """Stand-in for ``atomic_tools.dispatch`` recording every call.

    Returns a per-tool ``ToolResult`` from ``responses`` (keyed by tool
    name); raising entries (an Exception instance) are re-raised to
    exercise fail-open paths.
    """

    def __init__(self, responses: dict):
        self.responses = responses
        self.calls: list[tuple[str, dict, dict]] = []

    async def __call__(self, tool_name, tool_kwargs=None, **kw):
        self.calls.append((tool_name, dict(tool_kwargs or {}), kw))
        resp = self.responses.get(tool_name)
        if isinstance(resp, Exception):
            raise resp
        return resp


@pytest.fixture(autouse=True)
def _stub_activity_ctx(monkeypatch):
    """No live Temporal — stub the activity heartbeat/logger context."""
    mock = MagicMock()
    mock.heartbeat = MagicMock()
    mock.logger = MagicMock()
    monkeypatch.setattr(retrieve_mod, "activity", mock)


@pytest.fixture
def _patch_deps(monkeypatch):
    """Stub retriever / graph_retriever DI so the activity runs offline."""
    async def _ret():
        return object()

    async def _gret():
        return object()

    monkeypatch.setattr(retrieve_mod, "get_retriever", _ret)
    monkeypatch.setattr(retrieve_mod, "get_graph_retriever", _gret)


def _result(sources, observation):
    return atomic_tools.ToolResult(sources=sources, observation=observation)


@pytest.mark.asyncio
async def test_graph_walk_seeded_from_top_entity(_patch_deps, monkeypatch):
    rec = _DispatchRecorder({
        "vector_search": _result([_node("v1")], "[]"),
        "graph_search": _result([_node("g1")], _gs_obs(["Иванов", "Петров"])),
        "find_entity_by_name": _result([], _gs_obs([])),
        "graph_walk": _result([_node("w1"), _node("g1")], _gs_obs(["Иванов"])),
    })
    monkeypatch.setattr(atomic_tools, "dispatch", rec)
    monkeypatch.setattr(retrieve_mod.atomic_tools, "dispatch", rec)
    monkeypatch.setattr(retrieve_mod.settings.agent, "graph_walk_enabled", True)
    monkeypatch.setattr(retrieve_mod.settings.agent, "graph_walk_hops", 2)

    res = await retrieve_subquestion(RetrieveParams(subquestion="q", top_k=10))

    walk_calls = [c for c in rec.calls if c[0] == "graph_walk"]
    assert len(walk_calls) == 1
    assert walk_calls[0][1]["start_entity"] == "Иванов"
    assert walk_calls[0][1]["hops"] == 2
    # v1, g1, w1 merged; g1 deduped (came from both graph_search + walk).
    assert {s.chunk_id for s in res.sources} == {"v1", "g1", "w1"}


@pytest.mark.asyncio
async def test_no_graph_walk_when_flag_off(_patch_deps, monkeypatch):
    rec = _DispatchRecorder({
        "vector_search": _result([_node("v1")], "[]"),
        "graph_search": _result([_node("g1")], _gs_obs(["Иванов"])),
        "find_entity_by_name": _result([], _gs_obs([])),
    })
    monkeypatch.setattr(retrieve_mod.atomic_tools, "dispatch", rec)
    monkeypatch.setattr(retrieve_mod.settings.agent, "graph_walk_enabled", False)

    res = await retrieve_subquestion(RetrieveParams(subquestion="q", top_k=10))

    assert not any(c[0] == "graph_walk" for c in rec.calls)
    assert {s.chunk_id for s in res.sources} == {"v1", "g1"}


@pytest.mark.asyncio
async def test_no_graph_walk_when_no_entities(_patch_deps, monkeypatch):
    rec = _DispatchRecorder({
        "vector_search": _result([_node("v1")], "[]"),
        "graph_search": _result([_node("g1")], _gs_obs([])),
        "find_entity_by_name": _result([], _gs_obs([])),
    })
    monkeypatch.setattr(retrieve_mod.atomic_tools, "dispatch", rec)
    monkeypatch.setattr(retrieve_mod.settings.agent, "graph_walk_enabled", True)

    res = await retrieve_subquestion(RetrieveParams(subquestion="q", top_k=10))

    assert not any(c[0] == "graph_walk" for c in rec.calls)
    assert {s.chunk_id for s in res.sources} == {"v1", "g1"}


@pytest.mark.asyncio
async def test_graph_walk_failure_is_fail_open(_patch_deps, monkeypatch):
    rec = _DispatchRecorder({
        "vector_search": _result([_node("v1")], "[]"),
        "graph_search": _result([_node("g1")], _gs_obs(["Иванов"])),
        "find_entity_by_name": _result([], _gs_obs([])),
        "graph_walk": RuntimeError("neo4j down"),
    })
    monkeypatch.setattr(retrieve_mod.atomic_tools, "dispatch", rec)
    monkeypatch.setattr(retrieve_mod.settings.agent, "graph_walk_enabled", True)

    # Must NOT raise — vector + graph_search sources survive.
    res = await retrieve_subquestion(RetrieveParams(subquestion="q", top_k=10))

    assert any(c[0] == "graph_walk" for c in rec.calls)
    assert {s.chunk_id for s in res.sources} == {"v1", "g1"}


def test_pipeline_includes_find_entity_by_name():
    from src.workflow.search.activities.retrieve import _PIPELINE, ALLOWED_TOOLS

    assert "find_entity_by_name" in _PIPELINE
    assert _PIPELINE.index("find_entity_by_name") > _PIPELINE.index("graph_search")
    assert "find_entity_by_name" in ALLOWED_TOOLS
