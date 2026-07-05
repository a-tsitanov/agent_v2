"""Tests for `src/graph/lightrag_parse.py`."""

from __future__ import annotations

import pytest

from src.graph.lightrag_parse import (
    _cypher_safe_label,
    _first_keyword,
    _normalize_entity_name,
    ensure_orphan_entities,
    parse_lightrag_output,
    parsed_relations_to_relations,
)
from src.graph.lightrag_prompts import (
    COMPLETE_DELIM,
    ENTITY_EXTRACTION_SYSTEM,
    TUPLE_DELIM,
)


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
        "random text without a delimiter",
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


# ── #7: relation polarity + temporal validity ───────────────────────


def _rel_line(*fields: str) -> str:
    return TUPLE_DELIM.join(["relation", *fields])


def test_parse_relation_polarity_and_temporal_seven_fields() -> None:
    """7-field relation carries polarity + temporal validity window."""
    raw = "\n".join([
        _rel_line("Ivan", "Plant", "employment",
                  "Ivan was the director of the plant.",
                  "affirmed", "2015..2020"),
        COMPLETE_DELIM,
    ])
    res = parse_lightrag_output(raw)
    assert len(res.relations) == 1
    rel = res.relations[0]
    assert rel.polarity == "affirmed"
    assert rel.valid_from == "2015"
    assert rel.valid_to == "2020"


def test_parse_relation_legacy_five_fields_defaults() -> None:
    """Legacy 5-field relation → polarity defaults affirmed, no window."""
    raw = "\n".join([
        _rel_line("A", "B", "causation", "A causes B."),
        COMPLETE_DELIM,
    ])
    res = parse_lightrag_output(raw)
    rel = res.relations[0]
    assert rel.polarity == "affirmed"
    assert rel.valid_from is None
    assert rel.valid_to is None


def test_parse_relation_polarity_normalised() -> None:
    def polarity_of(raw_pol: str) -> str:
        raw = "\n".join([
            _rel_line("A", "B", "k", "desc.", raw_pol, ""),
            COMPLETE_DELIM,
        ])
        return parse_lightrag_output(raw).relations[0].polarity

    assert polarity_of("negated") == "negated"
    assert polarity_of("NEGATION") == "negated"
    assert polarity_of("uncertain") == "uncertain"
    assert polarity_of("unsure") == "uncertain"
    assert polarity_of("affirmed") == "affirmed"
    assert polarity_of("garbage") == "affirmed"


def test_parse_relation_temporal_open_ended() -> None:
    def window_of(temporal: str) -> tuple[str | None, str | None]:
        raw = "\n".join([
            _rel_line("A", "B", "k", "desc.", "affirmed", temporal),
            COMPLETE_DELIM,
        ])
        rel = parse_lightrag_output(raw).relations[0]
        return rel.valid_from, rel.valid_to

    assert window_of("2015..2020") == ("2015", "2020")
    assert window_of("..2020") == (None, "2020")
    assert window_of("2015..") == ("2015", None)
    assert window_of("") == (None, None)
    assert window_of("2024-03-15") == ("2024-03-15", None)  # bare → from only


def test_extraction_prompt_requests_polarity_and_temporal() -> None:
    """The parser reads polarity/temporal fields — the system prompt MUST
    instruct the LLM to emit them, or extraction silently never populates
    them (and we ship a parser expecting fields that never arrive)."""
    sys = ENTITY_EXTRACTION_SYSTEM.lower()
    assert "polarity" in sys
    assert "temporal" in sys
    assert "7 fields" in sys
    # the format line must list both new fields after the description
    assert "relationship_polarity" in ENTITY_EXTRACTION_SYSTEM
    assert "temporal_validity" in ENTITY_EXTRACTION_SYSTEM


def test_parsed_relations_to_relations_carries_polarity_temporal() -> None:
    raw = "\n".join([
        TUPLE_DELIM.join(["entity", "A", "Concept", "First."]),
        TUPLE_DELIM.join(["entity", "B", "Concept", "Second."]),
        _rel_line("A", "B", "ownership", "A no longer owns B.",
                  "negated", "..2020"),
        COMPLETE_DELIM,
    ])
    res = parse_lightrag_output(raw)
    id_by_name = {_normalize_entity_name(e.name): e.id for e in res.entities}
    rels = parsed_relations_to_relations(res.relations, id_by_name)
    assert len(rels) == 1
    props = rels[0].properties
    assert props["polarity"] == "negated"
    assert props["valid_from"] is None
    assert props["valid_to"] == "2020"


# ── temporal validation: only ISO YYYY[-MM[-DD]] survives ────────────
# (2026-07-05: 40 rels landed in prod with valid_from="2024-XX" and
#  similar LLM improvisations — opaque pass-through poisoned the
#  whats_changed date-window comparisons; now each bound is validated)


def test_parse_temporal_accepts_iso_shapes():
    from src.graph.lightrag_parse import _parse_temporal

    assert _parse_temporal("2015..2020") == ("2015", "2020")
    assert _parse_temporal("2015-03..2020-11") == ("2015-03", "2020-11")
    assert _parse_temporal("2024-03-15") == ("2024-03-15", None)
    assert _parse_temporal("..2020") == (None, "2020")
    assert _parse_temporal("2015..") == ("2015", None)


def test_parse_temporal_rejects_non_iso_bounds():
    from src.graph.lightrag_parse import _parse_temporal

    assert _parse_temporal("2024-XX") == (None, None)          # LLM-заглушка
    assert _parse_temporal("март 2024") == (None, None)        # словесная дата
    assert _parse_temporal("Q1 2024") == (None, None)
    assert _parse_temporal("2024-13-40") == (None, None)       # мусорный месяц/день
    assert _parse_temporal("20240315") == (None, None)         # без дефисов
    # смешанное окно: валидная сторона живёт, мусорная — None
    assert _parse_temporal("2024-XX..2025") == (None, "2025")
    assert _parse_temporal("2015..когда-нибудь") == ("2015", None)


def test_drop_unsupported_dates_requires_year_in_chunk_text():
    """Анти-копипаста дат из промпта: extraction не видит дату документа,
    поэтому дата, чей ГОД не встречается в тексте чанка, физически не могла
    быть извлечена из текста — только скопирована из инструкции/примеров
    (342 ребра с датой из few-shot дошли до прода, 2026-07-05)."""
    from src.graph.lightrag_parse import ParsedRelation, drop_unsupported_dates

    def rel(vf, vt=None):
        return ParsedRelation(
            source_name="A", target_name="B", keywords="k",
            description="d", valid_from=vf, valid_to=vt,
        )

    text = "Договор подписан 15 марта 2024 года, действует до конца 2025."
    rels = [
        rel("2024-03-15"),            # 2024 есть в тексте → живёт
        rel("2024", "2025"),          # оба года в тексте → живут
        rel("2015"),                  # 2015 в тексте нет → None
        rel("2023-01-01", "2025"),    # from дропается, to живёт
        rel(None, None),              # нечего проверять
    ]
    dropped = drop_unsupported_dates(rels, text)
    assert dropped == 2
    assert rels[0].valid_from == "2024-03-15"
    assert rels[1].valid_from == "2024" and rels[1].valid_to == "2025"
    assert rels[2].valid_from is None
    assert rels[3].valid_from is None and rels[3].valid_to == "2025"


# ── event_ts sanity gate ─────────────────────────────────────────────


def test_sanitize_event_ts_rejects_non_temporal() -> None:
    from src.graph.lightrag_parse import _sanitize_event_ts

    bad_values = [
        "affirmed", "uncertain", "empty", "unknown", "Не указано", "неизвестно",
        "52.164866, 32.929911", "Бразилия;Норвегия",
        "Упоминается роль Норвегии как крупнейшего донора в программе НАТО PURL.",
    ]
    for bad in bad_values:
        assert _sanitize_event_ts(bad) is None, f"Failed to reject: {bad}"


@pytest.mark.parametrize(
    "placeholder",
    ["нет времени", "нет даты", "время не указано"],
)
def test_sanitize_event_ts_rejects_new_placeholders(placeholder: str) -> None:
    """Live-data finding: these Russian "no time" placeholders were slipping
    through as verbatim ts phrases (Fix C)."""
    from src.graph.lightrag_parse import _sanitize_event_ts

    assert _sanitize_event_ts(placeholder) is None, f"Failed to reject: {placeholder}"


def test_sanitize_event_ts_keeps_phrases() -> None:
    from src.graph.lightrag_parse import _sanitize_event_ts

    good_values = ["вчера", "6 июля с 12:00 до 18:00 мск", "1 марта 2024", "2024-07-06"]
    for good in good_values:
        assert _sanitize_event_ts(good) == good, f"Failed to keep: {good}"


def test_event_line_with_missing_fields_gets_untimed() -> None:
    # Only 5 fields: participants slid into the ts position — ts must be dropped.
    line = TUPLE_DELIM.join(["event", "meeting", "провели встречу", "Иванов;Петров", "Москва"])
    out = parse_lightrag_output(line + "\n" + COMPLETE_DELIM)
    assert len(out.events) == 1
    assert out.events[0].event_ts is None


def test_full_event_line_keeps_verbatim_ts() -> None:
    line = TUPLE_DELIM.join(["event", "meeting", "провели встречу", "Иванов", "вчера", "Москва", "affirmed"])
    out = parse_lightrag_output(line + "\n" + COMPLETE_DELIM)
    assert out.events[0].event_ts == "вчера"
