"""Tests for `src/graph/lightrag_parse.py`."""

from __future__ import annotations

from src.graph.lightrag_parse import (
    _cypher_safe_label,
    _first_keyword,
    _normalize_entity_name,
    ensure_orphan_entities,
    parse_lightrag_output,
    parsed_relations_to_relations,
)
from src.graph.lightrag_prompts import COMPLETE_DELIM, TUPLE_DELIM


def _build(entities: list[tuple[str, str, str]],
           relations: list[tuple[str, str, str, str]]) -> str:
    """Helper: build a LightRAG-format payload from python tuples."""
    lines = []
    for name, etype, desc in entities:
        lines.append(TUPLE_DELIM.join(["entity", name, etype, desc]))
    for src, tgt, kw, desc in relations:
        lines.append(TUPLE_DELIM.join(["relation", src, tgt, kw, desc]))
    lines.append(COMPLETE_DELIM)
    return "\n".join(lines)


# ── normalisation helpers ───────────────────────────────────────────


def test_normalize_ascii_titlecases() -> None:
    assert _normalize_entity_name("basal cell carcinoma") == "Basal Cell Carcinoma"
    assert _normalize_entity_name("BCC") == "Bcc"
    assert _normalize_entity_name('"UV radiation"') == "Uv Radiation"


def test_normalize_preserves_non_ascii() -> None:
    assert _normalize_entity_name("Иванов Иван Петрович") == "Иванов Иван Петрович"
    assert _normalize_entity_name("ООО «Северные технологии»") == "ООО «Северные технологии»".strip("«»")


def test_normalize_strips_whitespace() -> None:
    assert _normalize_entity_name("   ") == ""
    assert _normalize_entity_name("\tfoo bar\n") == "Foo Bar"


def test_cypher_safe_label() -> None:
    assert _cypher_safe_label("causation") == "CAUSATION"
    assert _cypher_safe_label("risk factor") == "RISK_FACTOR"
    assert _cypher_safe_label("treats / mitigates") == "TREATS_MITIGATES"
    assert _cypher_safe_label("") == "RELATED"


def test_first_keyword() -> None:
    assert _first_keyword("causation, risk_factor") == "causation"
    assert _first_keyword(", , empty leading") == "empty leading"
    assert _first_keyword("") == ""


# ── parse: happy path ───────────────────────────────────────────────


def test_parse_happy_path() -> None:
    raw = _build(
        entities=[
            ("Basal Cell Carcinoma", "Concept",
             "The most common type of skin cancer."),
            ("UV Radiation", "Other",
             "Ultraviolet light from the sun that damages skin DNA."),
        ],
        relations=[
            ("UV Radiation", "Basal Cell Carcinoma",
             "causation, risk_factor",
             "UV radiation is a primary risk factor for BCC."),
        ],
    )
    res = parse_lightrag_output(raw, source_chunk_id="c1", file_path="med.txt")
    assert len(res.entities) == 2
    assert res.entities[0].name == "Basal Cell Carcinoma"
    assert res.entities[0].label == "Concept"
    assert "skin cancer" in res.entities[0].properties["description"]
    assert res.entities[0].properties["source_chunk_id"] == "c1"
    assert res.entities[0].properties["file_path"] == "med.txt"

    assert len(res.relations) == 1
    rel = res.relations[0]
    assert rel.source_name == "Uv Radiation"
    assert rel.target_name == "Basal Cell Carcinoma"
    assert rel.keywords == "causation, risk_factor"


# ── parse: tolerance ────────────────────────────────────────────────


def test_parse_strips_thinking_block() -> None:
    raw = (
        "<think>let me think...</think>\n"
        f"entity{TUPLE_DELIM}Foo{TUPLE_DELIM}Concept{TUPLE_DELIM}A thing.\n"
        f"{COMPLETE_DELIM}"
    )
    res = parse_lightrag_output(raw)
    assert len(res.entities) == 1
    assert res.entities[0].name == "Foo"


def test_parse_skips_malformed_lines() -> None:
    raw = "\n".join([
        "Here are the entities:",       # narration → skip
        f"entity{TUPLE_DELIM}X",        # missing fields → skip
        f"entity{TUPLE_DELIM}Y{TUPLE_DELIM}Concept{TUPLE_DELIM}A real entity.",
        f"relation{TUPLE_DELIM}Y",      # missing fields → skip
        f"random text without a delimiter",
        COMPLETE_DELIM,
    ])
    res = parse_lightrag_output(raw)
    assert len(res.entities) == 1
    assert res.entities[0].name == "Y"
    assert res.relations == []


def test_parse_truncated_output_returns_partial() -> None:
    """Generation stopped mid-way; no COMPLETE_DELIM."""
    raw = "\n".join([
        f"entity{TUPLE_DELIM}A{TUPLE_DELIM}Concept{TUPLE_DELIM}First entity.",
        f"entity{TUPLE_DELIM}B{TUPLE_DELIM}Concept{TUPLE_DELIM}Second entity.",
        # Truncated mid-line:
        f"entity{TUPLE_DELIM}C",
    ])
    res = parse_lightrag_output(raw)
    assert len(res.entities) == 2  # A and B; C dropped


def test_parse_dedups_within_chunk() -> None:
    raw = _build(
        entities=[
            ("foo", "Concept", "First mention."),
            ("FOO", "Concept", "Second mention, same name."),
        ],
        relations=[],
    )
    res = parse_lightrag_output(raw)
    # _normalize_entity_name lowercases ASCII before title-case →
    # "foo" and "FOO" both → "Foo" → dedup'd.
    assert len(res.entities) == 1


def test_parse_drops_empty_description() -> None:
    raw = _build(
        entities=[
            ("A", "Concept", ""),  # empty desc → skip
            ("B", "Concept", "Real."),
        ],
        relations=[],
    )
    res = parse_lightrag_output(raw)
    assert [e.name for e in res.entities] == ["B"]


def test_parse_drops_self_loop_relations() -> None:
    raw = _build(
        entities=[("Foo", "Concept", "Just foo.")],
        relations=[("Foo", "Foo", "self", "Self-loop should drop.")],
    )
    res = parse_lightrag_output(raw)
    assert res.relations == []


def test_parse_handles_quoted_kind_keyword() -> None:
    """qwen3 sometimes wraps 'entity' / 'relation' in quotes."""
    raw = (
        f'"entity"{TUPLE_DELIM}X{TUPLE_DELIM}Concept{TUPLE_DELIM}quoted line.\n'
        f"{COMPLETE_DELIM}"
    )
    res = parse_lightrag_output(raw)
    assert len(res.entities) == 1
    assert res.entities[0].name == "X"


# ── post-parse: name → id resolution ────────────────────────────────


def test_parsed_relations_to_relations_resolves() -> None:
    raw = _build(
        entities=[
            ("A", "Concept", "First."),
            ("B", "Concept", "Second."),
        ],
        relations=[
            ("A", "B", "causation", "A causes B."),
        ],
    )
    res = parse_lightrag_output(raw)
    id_by_name = {_normalize_entity_name(e.name): e.id for e in res.entities}
    rels = parsed_relations_to_relations(res.relations, id_by_name)
    assert len(rels) == 1
    assert rels[0].source_id == res.entities[0].id
    assert rels[0].target_id == res.entities[1].id
    assert rels[0].label == "CAUSATION"
    assert rels[0].properties["description"] == "A causes B."


def test_ensure_orphan_entities_for_missing_endpoint() -> None:
    raw = _build(
        entities=[("A", "Concept", "Only A is declared.")],
        relations=[("A", "B", "mentions", "A mentions B.")],
    )
    res = parse_lightrag_output(raw)
    id_by_name = {_normalize_entity_name(e.name): e.id for e in res.entities}
    orphans = ensure_orphan_entities(res.relations, id_by_name)
    assert [e.name for e in orphans] == ["B"]
    assert orphans[0].label == "Other"
    assert orphans[0].properties["orphan"] is True
    # Now id_by_name has B too:
    rels = parsed_relations_to_relations(res.relations, id_by_name)
    assert len(rels) == 1
    assert rels[0].target_id == orphans[0].id
