"""GraphRetriever.aretrieve must extract entities/relations from the
TextNode-shaped output of LlamaIndex's PG retriever.

The PG retriever (include_text=True) returns plain ``TextNode``s whose
content is "Here are some facts extracted from the provided text:\n\n
{triplet lines}\n\n{source chunk}" — NOT EntityNode/ChunkNode instances.
Before the fix the classifier only matched those class names, so every
node fell through to ``chunks`` and find_neighbours / graph_search
serialised empty entities/relations (MCP-tools bug, 2026-07-03).
"""

from __future__ import annotations

import pytest
from llama_index.core.schema import NodeWithScore, TextNode

from src.graph.retriever import GraphRetriever

FACTS = (
    "Here are some facts extracted from the provided text:\n\n"
    "БРИКС -> AGENDA -> Встреча БРИКС в Южной Африке\n"
    "Россия -> RELATED -> Китай\n\n"
    "Оригинальный текст чанка про саммит."
)


class _FakeRetriever:
    def __init__(self, nodes) -> None:
        self._nodes = nodes

    async def aretrieve(self, query: str):
        return self._nodes


class _FakePGIndex:
    property_graph_store = None

    def __init__(self, nodes) -> None:
        self._nodes = nodes

    def as_retriever(self, *, similarity_top_k, path_depth, include_text):
        return _FakeRetriever(self._nodes)


def _node(text: str) -> NodeWithScore:
    return NodeWithScore(node=TextNode(text=text), score=0.9)


def _build(nodes) -> GraphRetriever:
    return GraphRetriever(_FakePGIndex(nodes), similarity_top_k=10, path_depth=1)


@pytest.mark.asyncio
async def test_textnode_facts_parsed_into_entities_and_relations():
    out = await _build([_node(FACTS)]).aretrieve("БРИКС")

    names = {e["entity_name"] for e in out.entities}
    assert names == {
        "БРИКС", "Встреча БРИКС в Южной Африке", "Россия", "Китай",
    }
    rels = {(r["src_id"], r["label"], r["tgt_id"]) for r in out.relations}
    assert rels == {
        ("БРИКС", "AGENDA", "Встреча БРИКС в Южной Африке"),
        ("Россия", "RELATED", "Китай"),
    }
    # the node itself must STILL be exposed as a chunk (graph_search sources)
    assert len(out.chunks) == 1


@pytest.mark.asyncio
async def test_multi_hop_chain_line_yields_pairwise_relations():
    out = await _build(
        [_node("Here are some facts extracted from the provided text:\n\n"
               "А -> R1 -> Б -> R2 -> В\n\nчанк")],
    ).aretrieve("q")

    rels = {(r["src_id"], r["label"], r["tgt_id"]) for r in out.relations}
    assert rels == {("А", "R1", "Б"), ("Б", "R2", "В")}
    assert {e["entity_name"] for e in out.entities} == {"А", "Б", "В"}


@pytest.mark.asyncio
async def test_plain_chunk_without_facts_stays_chunk_only():
    out = await _build([_node("Просто текст без стрелок.")]).aretrieve("q")
    assert out.entities == []
    assert out.relations == []
    assert len(out.chunks) == 1


@pytest.mark.asyncio
async def test_duplicate_triplets_across_nodes_deduped():
    out = await _build([_node(FACTS), _node(FACTS)]).aretrieve("q")
    rels = [(r["src_id"], r["label"], r["tgt_id"]) for r in out.relations]
    assert len(rels) == len(set(rels)) == 2
    names = [e["entity_name"] for e in out.entities]
    assert len(names) == len(set(names)) == 4
    assert len(out.chunks) == 2
