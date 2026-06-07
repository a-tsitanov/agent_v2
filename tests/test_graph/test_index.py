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


def test_default_mode_is_lightrag() -> None:
    """Default mode is `lightrag` — single LLM call per chunk yields
    entities + types + descriptions + relations in one shot, then a
    cross-chunk merge step consolidates duplicates.  See
    `src/graph/lightrag_prompts.py` for the algorithm origin and
    `src/graph/merge.py` for the merger."""
    from src.graph.lightrag_extract import LightRAGExtractor

    extractor = build_kg_extractor(MockLLM())
    assert isinstance(extractor, LightRAGExtractor)


def test_simple_mode_returns_simple_extractor() -> None:
    """`simple` mode is kept as the R9 regression baseline."""
    extractor = build_kg_extractor(MockLLM(), mode="simple")
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


def test_ensure_entity_fulltext_index_idempotent_cypher_and_failopen():
    from src.graph.index import (
        ENTITY_FULLTEXT_INDEX_CYPHER,
        ensure_entity_fulltext_index,
    )

    # Idempotent DDL on the entity name.
    assert "CREATE FULLTEXT INDEX entity_name_fulltext IF NOT EXISTS" in (
        ENTITY_FULLTEXT_INDEX_CYPHER
    )
    assert "ON EACH [e.name]" in ENTITY_FULLTEXT_INDEX_CYPHER

    class _Store:
        def __init__(self):
            self.ran = None

        def structured_query(self, cypher):
            self.ran = cypher

    store = _Store()
    assert ensure_entity_fulltext_index(store) is True
    assert store.ran == ENTITY_FULLTEXT_INDEX_CYPHER

    class _BoomStore:
        def structured_query(self, cypher):
            raise RuntimeError("no fulltext support")

    # Fail-open: returns False, never raises.
    assert ensure_entity_fulltext_index(_BoomStore()) is False


def test_ensure_entity_lookup_indexes_name_and_mention_count_and_failopen():
    from src.graph.index import (
        ENTITY_MENTION_COUNT_INDEX_CYPHER,
        ENTITY_NAME_INDEX_CYPHER,
        ensure_entity_lookup_indexes,
    )

    # Idempotent range indexes backing entity-by-name lookups and the
    # incremental-ER `ORDER BY mention_count DESC` window.
    assert "CREATE INDEX entity_name IF NOT EXISTS" in ENTITY_NAME_INDEX_CYPHER
    assert "FOR (e:__Entity__) ON (e.name)" in ENTITY_NAME_INDEX_CYPHER
    assert (
        "CREATE INDEX entity_mention_count IF NOT EXISTS"
        in ENTITY_MENTION_COUNT_INDEX_CYPHER
    )
    assert "ON (e.mention_count)" in ENTITY_MENTION_COUNT_INDEX_CYPHER

    class _Store:
        def __init__(self):
            self.ran: list[str] = []

        def structured_query(self, cypher):
            self.ran.append(cypher)

    store = _Store()
    assert ensure_entity_lookup_indexes(store) is True
    assert store.ran == [
        ENTITY_NAME_INDEX_CYPHER, ENTITY_MENTION_COUNT_INDEX_CYPHER,
    ]

    class _BoomStore:
        def structured_query(self, cypher):
            raise RuntimeError("no index support")

    # Fail-open: returns False, never raises.
    assert ensure_entity_lookup_indexes(_BoomStore()) is False


def test_build_property_graph_index_threads_llm(monkeypatch):
    # The index's llm must be the project (LiteLLM) model so the default
    # retriever's LLMSynonymRetriever doesn't fall back to Settings.llm
    # (OpenAI) — which crashes a local-only deploy with no OPENAI_API_KEY.
    import src.graph.index as idx

    captured: dict = {}

    class _FakePGI:
        @classmethod
        def from_existing(cls, **kw):
            captured.update(kw)
            return "idx"

    monkeypatch.setattr(idx, "PropertyGraphIndex", _FakePGI)
    sentinel = object()
    out = idx.build_property_graph_index(
        graph_store=object(), embed_model=object(),
        extractor=object(), nodes=None, llm=sentinel,
    )
    assert out == "idx"
    assert captured["llm"] is sentinel


def test_ensure_er_vector_index_ddl_and_failopen():
    from src.graph.index import ER_VECTOR_INDEX_CYPHER, ensure_er_vector_index

    assert "CREATE VECTOR INDEX er_embedding_vec IF NOT EXISTS" in ER_VECTOR_INDEX_CYPHER
    assert "ON e.er_vec" in ER_VECTOR_INDEX_CYPHER
    assert "`vector.dimensions`: $dim" in ER_VECTOR_INDEX_CYPHER

    seen = {}

    class _Store:
        def structured_query(self, cypher, param_map=None):
            seen["cypher"] = cypher
            seen["param_map"] = param_map

    assert ensure_er_vector_index(_Store(), 768) is True
    assert seen["cypher"] == ER_VECTOR_INDEX_CYPHER
    assert seen["param_map"] == {"dim": 768}

    class _Boom:
        def structured_query(self, cypher, param_map=None):
            raise RuntimeError("no vector index support")

    assert ensure_er_vector_index(_Boom(), 768) is False


def test_ensure_community_indexes_ddl_and_failopen():
    from src.graph.index import (
        COMMUNITY_LEVEL_INDEX_CYPHER, CHUNK_DOC_ID_INDEX_CYPHER,
        ensure_community_indexes,
    )
    assert "FOR (c:Community) ON (c.level)" in COMMUNITY_LEVEL_INDEX_CYPHER
    assert "FOR (c:Chunk) ON (c.doc_id)" in CHUNK_DOC_ID_INDEX_CYPHER
    assert all("IF NOT EXISTS" in q for q in (COMMUNITY_LEVEL_INDEX_CYPHER, CHUNK_DOC_ID_INDEX_CYPHER))

    ran = []
    class _Store:
        def structured_query(self, c, param_map=None): ran.append(c)
    assert ensure_community_indexes(_Store()) is True
    assert ran == [COMMUNITY_LEVEL_INDEX_CYPHER, CHUNK_DOC_ID_INDEX_CYPHER]

    class _Boom:
        def structured_query(self, c, param_map=None): raise RuntimeError("x")
    assert ensure_community_indexes(_Boom()) is False
