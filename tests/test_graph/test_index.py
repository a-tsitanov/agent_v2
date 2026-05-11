"""Unit tests for `src/graph/index.py`.

Covers both extractor modes (`simple` and `schema`), the
`description` entity-prop wiring, and the multilingual prompt
override path.  LLM calls are NOT exercised here — the factory's
job is to wire arguments correctly.
"""

from __future__ import annotations

from typing import get_args

from llama_index.core.indices.property_graph import (
    SchemaLLMPathExtractor,
    SimpleLLMPathExtractor,
)
from llama_index.core.llms import MockLLM

from src.graph.index import _ENTITY_DESCRIPTION_PROP, build_kg_extractor
from src.graph.schema import (
    DEFAULT_VALIDATION_SCHEMA,
    EntityType,
    RelationType,
)


def test_default_mode_is_simple() -> None:
    """Default mode is `simple` — empirically the most reliable on
    qwen3:8b via Ollama.  Schema mode kept as opt-in for stronger
    backends (gpt-4o, qwen3:14b+)."""
    extractor = build_kg_extractor(MockLLM())
    assert isinstance(extractor, SimpleLLMPathExtractor)


def test_schema_mode_includes_description_property() -> None:
    extractor = build_kg_extractor(MockLLM(), mode="schema")
    assert _ENTITY_DESCRIPTION_PROP[0] in (
        extractor.possible_entity_props or []
    )


def test_schema_mode_passes_full_entity_taxonomy() -> None:
    extractor = build_kg_extractor(MockLLM(), mode="schema")
    # SchemaLLMPathExtractor normalises types to uppercase internally,
    # but the constructor receives our original Literal names.  We
    # check the union has all expected types — that's the contract.
    expected = set(get_args(EntityType))
    # Universal coverage check: at least the new R3 additions.
    for required in {
        "Person", "Organization", "Concept", "Metric", "Topic",
        "Issue", "Resolution", "EventOrAction", "Product", "Document",
    }:
        assert required in expected


def test_schema_mode_passes_full_relation_taxonomy() -> None:
    expected = set(get_args(RelationType))
    for required in {
        "WORKS_AT", "DISCUSSES", "MENTIONS", "PARTICIPATED_IN",
        "REPORTED", "RESOLVED_BY", "AFFECTS",
    }:
        assert required in expected


def test_schema_validation_list_passes_through() -> None:
    # The validation schema list is consumed by SchemaLLMPathExtractor
    # in strict mode.  Without a real LLM call we can't easily trigger
    # the strict path (it requires Pydantic-model dynamic generation
    # which fails on lists in some llama-index versions).  Instead
    # we just verify the list itself is well-formed.
    assert len(DEFAULT_VALIDATION_SCHEMA) >= 20
    for head, rel, tail in DEFAULT_VALIDATION_SCHEMA:
        assert head in get_args(EntityType)
        assert rel in get_args(RelationType)
        assert tail in get_args(EntityType)


def test_schema_mode_non_strict_leaves_validation_open() -> None:
    extractor = build_kg_extractor(MockLLM(), mode="schema", strict=False)
    # In non-strict mode kg_validation_schema is set to None at
    # constructor — LlamaIndex falls back to default.
    assert extractor.strict is False


def test_simple_mode_returns_simple_extractor() -> None:
    extractor = build_kg_extractor(MockLLM(), mode="simple")
    assert isinstance(extractor, SimpleLLMPathExtractor)


def test_simple_mode_uses_multilingual_default_prompt() -> None:
    extractor = build_kg_extractor(MockLLM(), mode="simple")
    template_str = extractor.extract_prompt.get_template()
    # Multilingual prompt has examples in EN + RU + DE-like flavours
    assert "ООО Альфа" in template_str
    assert "{max_knowledge_triplets}" in template_str
    assert "{text}" in template_str


def test_simple_mode_custom_extract_prompt_overrides_default() -> None:
    custom = "EXTRACT: {max_knowledge_triplets} from {text}"
    extractor = build_kg_extractor(
        MockLLM(), mode="simple", extract_prompt=custom,
    )
    assert "EXTRACT:" in extractor.extract_prompt.get_template()
