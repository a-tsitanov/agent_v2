"""Unit tests for src/retrieval/atomic_tools.py.

Each pure function gets mocked retriever / graph_retriever /
chunk_repository — we verify (a) sources-list shape, (b) observation
JSON content, (c) graceful behaviour when an upstream is None
(graph_retriever / chunk_repository optional).
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import pytest
from llama_index.core.schema import NodeWithScore, TextNode

from src.retrieval import atomic_tools


# ── fixtures ────────────────────────────────────────────────────────


def _node(node_id: str, text: str = "x", **md) -> NodeWithScore:
    return NodeWithScore(
        node=TextNode(id_=node_id, text=text, metadata=md),
        score=md.pop("score", 0.5),
    )


class _StubRetriever:
    def __init__(self, nodes):
        self._nodes = nodes
        self.last_query = None

    async def aretrieve(self, query: str):
        self.last_query = query
        return self._nodes


@dataclass
class _StubGraphData:
    entities: list = None
    relations: list = None
    chunks: list = None


class _StubGraphRetriever:
    def __init__(self, data, walk_data=None):
        self._data = data
        self._walk_data = walk_data
        self.last_query = None
        self.last_path_depth = None
        self.last_walk_kwargs = None

    async def aretrieve(self, query: str, *, path_depth=None):
        self.last_query = query
        self.last_path_depth = path_depth
        return self._data

    async def awalk(self, start_entity, *, hops, rel_filter=None):
        self.last_walk_kwargs = {
            "start_entity": start_entity,
            "hops": hops,
            "rel_filter": rel_filter,
        }
        return self._walk_data


class _StubChunkRepo:
    def __init__(self, chunks=None, text=None, raises=None):
        self._chunks = chunks or []
        self._text = text
        self._raises = raises

    async def aget_chunks_by_doc_id(self, doc_id, *, limit, offset):
        if self._raises:
            raise self._raises
        return self._chunks

    async def aread_document_text(self, doc_id, *, max_chars):
        if self._raises:
            raise self._raises
        return self._text


# ── vector_search ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_vector_search_returns_sources_and_observation():
    nodes = [
        _node("c1", "first chunk", doc_id="d1"),
        _node("c2", "second chunk", doc_id="d1"),
    ]
    r = await atomic_tools.vector_search(
        _StubRetriever(nodes), query="hello", top_k=10,
    )
    assert r.sources == nodes
    parsed = json.loads(r.observation)
    assert len(parsed) == 2
    assert parsed[0]["chunk_id"] == "c1"
    assert parsed[0]["text"] == "first chunk"


@pytest.mark.asyncio
async def test_vector_search_top_k_truncates_observation_not_sources():
    nodes = [_node(f"c{i}", "x") for i in range(10)]
    r = await atomic_tools.vector_search(
        _StubRetriever(nodes), query="q", top_k=3,
    )
    # All 10 sources kept (the agent's accumulator may want them all).
    assert len(r.sources) == 10
    # Observation only the first 3 (chat-history weight control).
    assert len(json.loads(r.observation)) == 3


# ── graph_search ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_graph_search_no_retriever_yields_empty():
    r = await atomic_tools.graph_search(None, query="X")
    assert r.sources == []
    assert json.loads(r.observation) == {"entities": [], "relations": []}


@pytest.mark.asyncio
async def test_graph_search_populated():
    chunk_node = _node("g1", "graph chunk", doc_id="d1")
    data = _StubGraphData(
        entities=[{"entity_name": "Иванов", "entity_type": "Person",
                   "description": ""}],
        relations=[{"src_id": "A", "tgt_id": "B", "label": "WORKS_AT",
                    "description": ""}],
        chunks=[chunk_node],
    )
    r = await atomic_tools.graph_search(
        _StubGraphRetriever(data), query="Иванов",
    )
    assert r.sources == [chunk_node]
    parsed = json.loads(r.observation)
    assert parsed["entities"][0]["entity_name"] == "Иванов"
    assert parsed["relations"][0]["label"] == "WORKS_AT"


# ── find_entity_by_id ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_find_entity_by_id_filters_by_type():
    data = _StubGraphData(entities=[
        {"entity_name": "Иванов", "entity_type": "Person"},
        {"entity_name": "Ромашка", "entity_type": "Organization"},
    ])
    r = await atomic_tools.find_entity_by_id(
        _StubGraphRetriever(data), name="any",
        entity_type="Person",
    )
    parsed = json.loads(r.observation)
    assert len(parsed["entities"]) == 1
    assert parsed["entities"][0]["entity_name"] == "Иванов"


@pytest.mark.asyncio
async def test_find_entity_by_id_no_filter_returns_all():
    data = _StubGraphData(entities=[
        {"entity_name": "Иванов", "entity_type": "Person"},
        {"entity_name": "Ромашка", "entity_type": "Organization"},
    ])
    r = await atomic_tools.find_entity_by_id(
        _StubGraphRetriever(data), name="any",
    )
    assert len(json.loads(r.observation)["entities"]) == 2


# ── find_neighbours ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_find_neighbours_returns_entities_and_relations():
    data = _StubGraphData(
        entities=[{"entity_name": "Иванов", "entity_type": "Person"}],
        relations=[{"src_id": "Иванов", "tgt_id": "Ромашка",
                    "label": "WORKS_AT"}],
    )
    r = await atomic_tools.find_neighbours(
        _StubGraphRetriever(data), entity_name="Иванов", hops=1,
    )
    parsed = json.loads(r.observation)
    assert "entities" in parsed and "relations" in parsed
    assert len(parsed["entities"]) == 1
    assert len(parsed["relations"]) == 1


# ── depth / hops → path_depth plumbing ──────────────────────────────


@pytest.mark.asyncio
async def test_graph_search_passes_depth_as_path_depth():
    stub = _StubGraphRetriever(_StubGraphData(entities=[], relations=[]))
    await atomic_tools.graph_search(stub, query="q", depth=3)
    assert stub.last_path_depth == 3


@pytest.mark.asyncio
async def test_graph_search_default_depth_is_one():
    stub = _StubGraphRetriever(_StubGraphData(entities=[], relations=[]))
    await atomic_tools.graph_search(stub, query="q")
    assert stub.last_path_depth == 1


@pytest.mark.asyncio
async def test_find_neighbours_passes_hops_as_path_depth():
    stub = _StubGraphRetriever(_StubGraphData(entities=[], relations=[]))
    await atomic_tools.find_neighbours(stub, entity_name="X", hops=2)
    assert stub.last_path_depth == 2


# ── graph_walk ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_graph_walk_no_retriever_yields_empty():
    r = await atomic_tools.graph_walk(None, start_entity="A")
    assert r.sources == []
    assert json.loads(r.observation) == {"entities": [], "relations": []}


@pytest.mark.asyncio
async def test_graph_walk_passes_hops_and_rel_filter_to_retriever():
    walk = _StubGraphData(
        entities=[{"entity_name": "A", "entity_type": "Person"},
                  {"entity_name": "B", "entity_type": "Person"},
                  {"entity_name": "C", "entity_type": "Person"}],
        relations=[{"src_id": "A", "tgt_id": "B", "label": "KNOWS"},
                   {"src_id": "B", "tgt_id": "C", "label": "KNOWS"}],
        chunks=[],
    )
    stub = _StubGraphRetriever(None, walk_data=walk)
    r = await atomic_tools.graph_walk(
        stub, start_entity="A", hops=2, rel_filter=["KNOWS"],
    )
    # rel_filter + hops forwarded to the retriever's bounded walk.
    assert stub.last_walk_kwargs["start_entity"] == "A"
    assert stub.last_walk_kwargs["hops"] == 2
    assert stub.last_walk_kwargs["rel_filter"] == ["KNOWS"]
    parsed = json.loads(r.observation)
    assert {e["entity_name"] for e in parsed["entities"]} == {"A", "B", "C"}


@pytest.mark.asyncio
async def test_graph_walk_clamps_hops_to_hard_max():
    stub = _StubGraphRetriever(None, walk_data=_StubGraphData(chunks=[]))
    await atomic_tools.graph_walk(stub, start_entity="A", hops=99)
    assert stub.last_walk_kwargs["hops"] == atomic_tools.GRAPH_WALK_MAX_HOPS


@pytest.mark.asyncio
async def test_graph_walk_truncates_entities_to_node_cap():
    cap = atomic_tools.GRAPH_WALK_MAX_NODES
    big = _StubGraphData(
        entities=[{"entity_name": f"E{i}", "entity_type": "Person"}
                  for i in range(cap + 25)],
        relations=[],
        chunks=[],
    )
    stub = _StubGraphRetriever(None, walk_data=big)
    r = await atomic_tools.graph_walk(stub, start_entity="A", hops=2)
    parsed = json.loads(r.observation)
    assert len(parsed["entities"]) == cap


@pytest.mark.asyncio
async def test_graph_walk_carries_chunks_as_sources():
    chunk = _node("g1", "graph chunk", doc_id="d1")
    walk = _StubGraphData(entities=[], relations=[], chunks=[chunk])
    stub = _StubGraphRetriever(None, walk_data=walk)
    r = await atomic_tools.graph_walk(stub, start_entity="A")
    assert r.sources == [chunk]


@pytest.mark.asyncio
async def test_dispatch_routes_to_graph_walk():
    walk = _StubGraphData(
        entities=[{"entity_name": "A", "entity_type": "Person"}],
        relations=[], chunks=[],
    )
    stub = _StubGraphRetriever(None, walk_data=walk)
    r = await atomic_tools.dispatch(
        "graph_walk", {"start_entity": "A", "hops": 2},
        graph_retriever=stub,
    )
    assert json.loads(r.observation)["entities"][0]["entity_name"] == "A"


# ── get_chunks_by_doc_id ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_chunks_by_doc_id_builds_sources():
    chunks = [
        {"chunk_id": "c1", "position": 0, "text": "first",
         "doc_id": "d1", "file_path": "/x"},
        {"chunk_id": "c2", "position": 1, "text": "second",
         "doc_id": "d1", "file_path": "/x"},
    ]
    r = await atomic_tools.get_chunks_by_doc_id(
        _StubChunkRepo(chunks=chunks), doc_id="d1",
    )
    assert len(r.sources) == 2
    assert r.sources[0].node.metadata["doc_id"] == "d1"
    parsed = json.loads(r.observation)
    assert parsed[0]["chunk_id"] == "c1"


@pytest.mark.asyncio
async def test_get_chunks_by_doc_id_no_repo():
    r = await atomic_tools.get_chunks_by_doc_id(None, doc_id="d1")
    assert r.sources == []
    assert "unavailable" in r.observation


@pytest.mark.asyncio
async def test_get_chunks_by_doc_id_swallows_exception():
    r = await atomic_tools.get_chunks_by_doc_id(
        _StubChunkRepo(raises=RuntimeError("pg down")),
        doc_id="d1",
    )
    assert r.sources == []
    parsed = json.loads(r.observation)
    assert "error" in parsed
    assert "pg down" in parsed["error"]


# ── read_full_document ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_read_full_document_returns_text():
    r = await atomic_tools.read_full_document(
        _StubChunkRepo(text="full doc body"),
        doc_id="d1",
    )
    assert r.sources == []
    assert r.observation == "full doc body"


@pytest.mark.asyncio
async def test_read_full_document_missing():
    r = await atomic_tools.read_full_document(
        _StubChunkRepo(text=None), doc_id="missing",
    )
    assert "not found" in r.observation


# ── filter_by_metadata ──────────────────────────────────────────────


def test_filter_by_metadata_by_doc_id():
    sources = [
        _node("c1", doc_id="d1"),
        _node("c2", doc_id="d2"),
        _node("c3", doc_id="d1"),
    ]
    r = atomic_tools.filter_by_metadata(sources, doc_id="d1")
    parsed = json.loads(r.observation)
    assert len(parsed) == 2
    assert {p["chunk_id"] for p in parsed} == {"c1", "c3"}


def test_filter_by_metadata_multi_filter():
    sources = [
        _node("c1", doc_id="d1", department="sales"),
        _node("c2", doc_id="d1", department="legal"),
    ]
    r = atomic_tools.filter_by_metadata(
        sources, doc_id="d1", department="sales",
    )
    parsed = json.loads(r.observation)
    assert len(parsed) == 1
    assert parsed[0]["chunk_id"] == "c1"


# ── dispatcher ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dispatch_routes_to_vector_search():
    nodes = [_node("c1", doc_id="d1")]
    r = await atomic_tools.dispatch(
        "vector_search", {"query": "x", "top_k": 5},
        retriever=_StubRetriever(nodes),
    )
    assert r.sources == nodes


@pytest.mark.asyncio
async def test_dispatch_raises_on_unknown_tool():
    with pytest.raises(ValueError, match="unknown tool"):
        await atomic_tools.dispatch("imaginary_tool", {})


@pytest.mark.asyncio
async def test_dispatch_vector_search_missing_retriever_raises():
    with pytest.raises(ValueError, match="needs a retriever"):
        await atomic_tools.dispatch(
            "vector_search", {"query": "x"},
            retriever=None,
        )


# ── ToolResult shape ────────────────────────────────────────────────


def test_tool_descriptions_cover_all_tools():
    expected = {
        "vector_search", "graph_search", "graph_walk",
        "find_entity_by_id", "find_entity_by_name", "find_neighbours",
        "filter_by_metadata", "get_chunks_by_doc_id", "read_full_document",
    }
    assert set(atomic_tools.TOOL_DESCRIPTIONS) == expected
