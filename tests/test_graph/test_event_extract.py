"""Tests for `event` tuple kind parsing in `src/graph/lightrag_parse.py`
and `events_to_graph` in `src/graph/event_extract.py`.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from llama_index.core.graph_stores.types import KG_NODES_KEY, KG_RELATIONS_KEY
from llama_index.core.schema import TextNode

from src.graph.event_extract import events_to_graph
from src.graph.lightrag_extract import LightRAGExtractor
from src.graph.lightrag_parse import ParsedEvent, parse_lightrag_output
from src.graph.lightrag_prompts import COMPLETE_DELIM, TUPLE_DELIM

D, C = TUPLE_DELIM, COMPLETE_DELIM


def _ev_line(*fields: str) -> str:
    return D.join(fields)


def test_parser_recognizes_event_kind() -> None:
    raw = "\n".join(
        [
            _ev_line(
                "event",
                "deal",
                "signed a contract",
                "Romashka;Lutik",
                "2024-03-01",
                "Moscow",
                "affirmed",
            ),
            C,
        ]
    )
    res = parse_lightrag_output(raw, source_chunk_id="c1", file_path="f")
    assert len(res.events) == 1
    e = res.events[0]
    assert e.event_type == "deal"
    assert e.trigger == "signed a contract"
    assert e.participants == ["Romashka", "Lutik"]
    assert e.event_ts == "2024-03-01"
    assert e.location == "Moscow"
    assert e.polarity == "affirmed"
    assert e.source_chunk_id == "c1"
    assert e.file_path == "f"


def test_parser_still_handles_entity_relation_unchanged() -> None:
    raw = "\n".join([_ev_line("entity", "Romashka", "Organization", "a firm"), C])
    res = parse_lightrag_output(raw, source_chunk_id="c1", file_path="f")
    assert len(res.entities) == 1
    assert res.events == []


def test_parser_mixed_entity_relation_event() -> None:
    raw = "\n".join(
        [
            _ev_line("entity", "Romashka", "Organization", "a firm"),
            _ev_line("entity", "Lutik", "Organization", "another firm"),
            _ev_line("relation", "Romashka", "Lutik", "partnership", "They signed a deal."),
            _ev_line(
                "event",
                "deal",
                "signed a contract",
                "Romashka;Lutik",
                "2024-03-01",
                "Moscow",
                "affirmed",
            ),
            _ev_line(
                "event",
                "meeting",
                "held a conference call",
                "Romashka;Lutik;Arbuzov",
                "",
                "",
                "uncertain",
            ),
            C,
        ]
    )
    res = parse_lightrag_output(raw, source_chunk_id="c2", file_path="doc.txt")
    assert len(res.entities) == 2
    assert len(res.relations) == 1
    assert len(res.events) == 2
    e0 = res.events[0]
    assert e0.participants == ["Romashka", "Lutik"]
    e1 = res.events[1]
    assert e1.participants == ["Romashka", "Lutik", "Arbuzov"]
    assert e1.polarity == "uncertain"
    assert e1.event_ts is None
    assert e1.location is None


def test_event_polarity_normalized() -> None:
    raw = "\n".join(
        [
            _ev_line(
                "event",
                "termination",
                "fired",
                "Company;Employee",
                "",
                "",
                "negated",
            ),
            C,
        ]
    )
    res = parse_lightrag_output(raw)
    assert res.events[0].polarity == "negated"


def test_event_optional_fields_absent() -> None:
    """Event line with only 3 fields past kind (no time/location/polarity)."""
    raw = "\n".join(
        [
            _ev_line("event", "meeting", "gathered", "A;B"),
            C,
        ]
    )
    res = parse_lightrag_output(raw)
    assert len(res.events) == 1
    e = res.events[0]
    assert e.event_ts is None
    assert e.location is None
    assert e.polarity == "affirmed"


def test_event_default_entity_relation_parse_unchanged() -> None:
    """Regression: default entity/relation output must parse identically."""
    raw = "\n".join(
        [
            _ev_line("entity", "A", "Concept", "first."),
            _ev_line("entity", "B", "Concept", "second."),
            _ev_line("relation", "A", "B", "causation", "A causes B.", "negated", "2020..2024"),
            C,
        ]
    )
    res = parse_lightrag_output(raw, source_chunk_id="x", file_path="y")
    assert len(res.entities) == 2
    assert len(res.relations) == 1
    assert res.events == []
    rel = res.relations[0]
    assert rel.polarity == "negated"
    assert rel.valid_from == "2020"
    assert rel.valid_to == "2024"


# ── events_to_graph unit tests ───────────────────────────────────────


def _make_ev(**kw) -> ParsedEvent:
    defaults = dict(
        event_type="deal",
        trigger="signed",
        participants=["Romashka", "Lutik"],
        event_ts="2024-03-01",
        location=None,
        polarity="affirmed",
        source_chunk_id="c1",
        file_path="f",
    )
    defaults.update(kw)
    return ParsedEvent(**defaults)


def test_events_to_graph_builds_event_node_and_participant_edges():
    ev = _make_ev()
    nodes, rels = events_to_graph([ev], id_by_name={"Romashka": "id-r", "Lutik": "id-l"})
    assert len(nodes) == 1 and nodes[0].label == "EventOrAction"
    assert nodes[0].properties["event_type"] == "deal"
    rel_types = {r.label for r in rels}
    assert "PARTICIPATED_IN" in rel_types  # event→participants
    assert sum(1 for r in rels if r.label == "PARTICIPATED_IN") == 2


def test_events_to_graph_participants_property_present():
    """Event node must carry a `participants` list property."""
    ev = _make_ev(participants=["A", "B", "C"])
    nodes, _ = events_to_graph([ev], id_by_name={"A": "a1", "B": "b1", "C": "c1"})
    assert nodes[0].properties["participants"] == ["A", "B", "C"]


def test_events_to_graph_orphan_synthesis_for_unknown_participant():
    """Participants not in id_by_name must get a synthesized EntityNode(label='Other')."""
    ev = _make_ev(participants=["Known", "Unknown"])
    nodes, rels = events_to_graph([ev], id_by_name={"Known": "k1"})
    # 1 event node + 1 orphan node
    assert len(nodes) == 2
    orphan = next((n for n in nodes if n.label == "Other"), None)
    assert orphan is not None
    assert orphan.properties["orphan"] is True
    assert sum(1 for r in rels if r.label == "PARTICIPATED_IN") == 2


def test_events_to_graph_participated_in_target_ids():
    """PARTICIPATED_IN edges must resolve to the correct target ids."""
    ev = _make_ev(participants=["Romashka", "Lutik"])
    nodes, rels = events_to_graph([ev], id_by_name={"Romashka": "id-r", "Lutik": "id-l"})
    event_node = nodes[0]
    assert all(r.source_id == event_node.id for r in rels)
    target_ids = {r.target_id for r in rels}
    assert "id-r" in target_ids
    assert "id-l" in target_ids


def test_events_to_graph_source_chunks_property():
    """Event node must carry source_chunks list."""
    ev = _make_ev(source_chunk_id="chunk-42")
    nodes, _ = events_to_graph([ev], id_by_name={"Romashka": "id-r", "Lutik": "id-l"})
    assert nodes[0].properties["source_chunks"] == ["chunk-42"]


# ── gated-extractor integration tests ───────────────────────────────


@dataclass
class _ScriptedLLM:
    responses: list[str]
    calls: list[list] = field(default_factory=list)

    async def achat(self, messages, **_):
        self.calls.append(messages)
        text = self.responses.pop(0) if self.responses else ""

        class _Resp:
            class _Msg:
                content = text

            message = _Msg()

        return _Resp()


def _event_payload() -> str:
    """LLM response with an entity block + one event line."""
    D, C = TUPLE_DELIM, COMPLETE_DELIM
    lines = [
        D.join(["entity", "Romashka", "Organization", "A firm."]),
        D.join(["entity", "Lutik", "Organization", "Another firm."]),
        D.join(["relation", "Romashka", "Lutik", "partnership", "They partnered."]),
        D.join(
            [
                "event",
                "deal",
                "signed a supply contract",
                "Romashka;Lutik",
                "2024-03-01",
                "Moscow",
                "affirmed",
            ]
        ),
        C,
    ]
    return "\n".join(lines)


@pytest.mark.asyncio
async def test_extractor_gated_off_produces_no_event_nodes(monkeypatch) -> None:
    """With extraction_enabled=False (default) no EventOrAction nodes in output."""
    from src.config import settings as _settings

    monkeypatch.setattr(_settings.events, "extraction_enabled", False)

    extractor = LightRAGExtractor(llm=_ScriptedLLM(responses=[_event_payload()]), num_workers=1)
    out = await extractor.acall([TextNode(id_="g1", text="text")])
    nodes = out[0].metadata[KG_NODES_KEY]
    event_nodes = [n for n in nodes if n.label == "EventOrAction"]
    assert event_nodes == [], f"Expected no event nodes when gated off, got {event_nodes}"


@pytest.mark.asyncio
async def test_extractor_gated_on_emits_event_nodes(monkeypatch) -> None:
    """With extraction_enabled=True, EventOrAction nodes appear in output."""
    from src.config import settings as _settings

    monkeypatch.setattr(_settings.events, "extraction_enabled", True)

    extractor = LightRAGExtractor(llm=_ScriptedLLM(responses=[_event_payload()]), num_workers=1)
    out = await extractor.acall([TextNode(id_="g2", text="text")])
    nodes = out[0].metadata[KG_NODES_KEY]
    rels = out[0].metadata[KG_RELATIONS_KEY]
    event_nodes = [n for n in nodes if n.label == "EventOrAction"]
    assert len(event_nodes) == 1
    assert event_nodes[0].properties["event_type"] == "deal"
    part_rels = [r for r in rels if r.label == "PARTICIPATED_IN"]
    assert len(part_rels) == 2
