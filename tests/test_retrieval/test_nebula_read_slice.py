"""Nebula read slice: store-only retriever construction + aretrieve guard."""
from __future__ import annotations

import pytest

from src.graph.nebula_store import entity_vid
from src.graph.retriever import GRAPH_WALK_MAX_HOPS, GraphRetriever, RoundGraphData


class _FakeStore:
    def __init__(self, rows=None, subgraph_rows=None):
        self._rows = rows or []
        self._subgraph_rows = subgraph_rows or []
        self.last_query = None
        self.last_subgraph_call = None
    def structured_query(self, query, param_map=None):
        self.last_query = query
        assert not param_map, "nebula path must not pass param_map"
        return self._rows
    def subgraph(self, vid, hops, *, edge="RELATED"):
        self.last_subgraph_call = (vid, hops, edge)
        return self._subgraph_rows


def test_for_store_builds_without_llamaindex_retriever():
    store = _FakeStore()
    r = GraphRetriever.for_store(store)
    assert r._graph_store is store
    assert r._retriever is None


@pytest.mark.asyncio
async def test_aretrieve_empty_without_retriever():
    r = GraphRetriever.for_store(_FakeStore())
    out = await r.aretrieve("что угодно")
    assert isinstance(out, RoundGraphData)
    assert out.entities == [] and out.relations == [] and out.chunks == []


@pytest.mark.asyncio
async def test_find_by_name_nebula_lookup(monkeypatch):
    # Patch the backend flag on the exact `settings` object retriever.py
    # reads from (its own module-level binding), not `src.config.settings`
    # directly: test_llm_factory.py's `importlib.reload(src.config)` can
    # rebind the latter to a fresh Settings() instance mid-session, which
    # would silently desync the two and leave this test patching an object
    # afind_entities_by_name never reads.
    monkeypatch.setattr(
        "src.graph.retriever.settings.graph.backend", "nebula", raising=False,
    )
    rows = [{"vid": "abc", "name": "Иванов Иван", "label": "PERSON",
             "description": "инженер"}]
    store = _FakeStore(rows=rows)
    r = GraphRetriever.for_store(store)
    out = await r.afind_entities_by_name("Иванов", limit=5)
    # PREFIX match over `entity_name_idx`, not a MATCH scan. `CONTAINS`
    # cannot use an index, so the old form was a full scan of every
    # Entity vertex and failed outright with GraphMemoryExceeded on the
    # production graph — the tool answered `{"entities": []}` for
    # "Украина" while an index lookup found it instantly.
    assert "LOOKUP ON `Entity`" in store.last_query
    assert '`Entity`.name STARTS WITH "Иванов"' in store.last_query
    assert "CONTAINS" not in store.last_query
    assert "MATCH" not in store.last_query
    assert "LIMIT 5" in store.last_query
    assert out.entities == [{"entity_name": "Иванов Иван",
                             "entity_type": "PERSON", "description": "инженер"}]
    assert out.error == ""


@pytest.mark.asyncio
async def test_find_by_name_nebula_ors_the_tokens(monkeypatch):
    monkeypatch.setattr(
        "src.graph.retriever.settings.graph.backend", "nebula", raising=False,
    )
    store = _FakeStore(rows=[])
    await GraphRetriever.for_store(store).afind_entities_by_name("Иванов Москва")
    assert store.last_query.count("STARTS WITH") == 2
    assert " OR " in store.last_query


@pytest.mark.asyncio
async def test_find_by_name_nebula_blank_query_issues_nothing(monkeypatch):
    monkeypatch.setattr(
        "src.graph.retriever.settings.graph.backend", "nebula", raising=False,
    )
    store = _FakeStore(rows=[])
    out = await GraphRetriever.for_store(store).afind_entities_by_name("   ")
    assert store.last_query is None
    assert out.entities == []


@pytest.mark.asyncio
async def test_find_by_name_nebula_reports_a_failure_instead_of_empty(monkeypatch):
    """The defect this replaced: a `GraphMemoryExceeded` came back as an
    empty entity list, which reads as "the graph has no such entity"."""
    monkeypatch.setattr(
        "src.graph.retriever.settings.graph.backend", "nebula", raising=False,
    )

    class _Refusing(_FakeStore):
        def structured_query(self, query, param_map=None):
            raise RuntimeError("nGQL failed: GraphMemoryExceeded: (-2600)")

    out = await GraphRetriever.for_store(_Refusing()).afind_entities_by_name("Украина")
    assert out.entities == []
    assert "GraphMemoryExceeded" in out.error


@pytest.mark.asyncio
async def test_find_by_name_nebula_still_reads_the_older_row_shape(monkeypatch):
    """Accepts the `p` map the previous MATCH form returned, so a caller
    or fake feeding either shape keeps working."""
    monkeypatch.setattr(
        "src.graph.retriever.settings.graph.backend", "nebula", raising=False,
    )
    store = _FakeStore(rows=[{"vid": "a", "p": {"name": "Киев", "label": "CITY"}}])
    out = await GraphRetriever.for_store(store).afind_entities_by_name("Киев")
    assert out.entities == [
        {"entity_name": "Киев", "entity_type": "CITY", "description": ""},
    ]


@pytest.mark.asyncio
async def test_awalk_nebula_uses_subgraph_and_clamps_hops(monkeypatch):
    monkeypatch.setattr(
        "src.graph.retriever.settings.graph.backend", "nebula", raising=False,
    )
    subgraph_rows = [{
        "entities": [
            {"name": "Иванов", "label": "PERSON", "description": "инженер"},
            {"name": "Москва", "label": "CITY", "description": "город"},
        ],
        "relations": [
            {"src": "Иванов", "tgt": "Москва", "label": "WORKS_AT",
             "polarity": "pos", "valid_from": 0, "valid_to": 0},
        ],
    }]
    store = _FakeStore(subgraph_rows=subgraph_rows)
    r = GraphRetriever.for_store(store)
    # hops=999 must be clamped to GRAPH_WALK_MAX_HOPS before hitting subgraph.
    out = await r.awalk("Иванов", hops=999)
    assert store.last_subgraph_call == (entity_vid("Иванов"), GRAPH_WALK_MAX_HOPS, "RELATED")
    assert {e["entity_name"] for e in out.entities} == {"Иванов", "Москва"}
    assert out.relations == [{"src_id": "Иванов", "tgt_id": "Москва",
                              "label": "WORKS_AT", "polarity": "pos",
                              "valid_from": 0, "valid_to": 0}]


@pytest.mark.asyncio
async def test_awalk_nebula_applies_rel_filter(monkeypatch):
    monkeypatch.setattr(
        "src.graph.retriever.settings.graph.backend", "nebula", raising=False,
    )
    subgraph_rows = [{
        "entities": [
            {"name": "Иванов", "label": "PERSON", "description": ""},
            {"name": "Москва", "label": "CITY", "description": ""},
        ],
        "relations": [
            {"src": "Иванов", "tgt": "Москва", "label": "WORKS_AT",
             "polarity": "pos", "valid_from": 0, "valid_to": 0},
            {"src": "Иванов", "tgt": "Москва", "label": "LIVES_IN",
             "polarity": "pos", "valid_from": 0, "valid_to": 0},
        ],
    }]
    store = _FakeStore(subgraph_rows=subgraph_rows)
    r = GraphRetriever.for_store(store)
    out = await r.awalk("Иванов", hops=1, rel_filter=["WORKS_AT"])
    assert [rel["label"] for rel in out.relations] == ["WORKS_AT"]


@pytest.mark.asyncio
async def test_aretrieve_nebula_knn_then_expands(monkeypatch):
    """graph_search under nebula: er_vec kNN picks entities, then subgraph-expand."""
    monkeypatch.setattr(
        "src.graph.retriever.settings.graph.backend", "nebula", raising=False,
    )

    class _Embed:
        async def aget_text_embedding(self, q):
            return [0.1, 0.2, 0.3]

    class _EVS:
        def knn(self, vec, k):
            # EntityVectorStore.knn returns EntityCandidate = TypedDict (a dict),
            # NOT an object with a .name attribute.
            return [{"name": "Герань", "label": "Product"}]

    monkeypatch.setattr(
        "src.ingestion.embeddings.build_embedding_model", lambda: _Embed(), raising=False,
    )
    monkeypatch.setattr(
        "src.graph.entity_vector_store.build_entity_vector_store",
        lambda store: _EVS(), raising=False,
    )

    async def _no_table_hits(_q):
        return []

    subgraph_rows = [{
        "entities": [
            {"name": "Герань", "label": "Product", "description": ""},
            {"name": "Одесса", "label": "CITY", "description": ""},
        ],
        "relations": [
            {"src": "Герань", "tgt": "Одесса", "label": "HIT",
             "polarity": "pos", "valid_from": 0, "valid_to": 0},
        ],
    }]
    store = _FakeStore(subgraph_rows=subgraph_rows)
    r = GraphRetriever.for_store(store)
    monkeypatch.setattr(r, "_entity_table_names", _no_table_hits, raising=False)
    out = await r.aretrieve("удары по Украине", path_depth=1)
    assert {e["entity_name"] for e in out.entities} == {"Герань", "Одесса"}
    assert [rel["label"] for rel in out.relations] == ["HIT"]


@pytest.mark.asyncio
async def test_aretrieve_unions_vector_and_table_seeds(monkeypatch):
    """The walk seeds from BOTH the vector kNN and the entity-table lexical
    hit — a named entity the vector misses still gets walked."""
    monkeypatch.setattr(
        "src.graph.retriever.settings.graph.backend", "nebula", raising=False,
    )
    walked: list[str] = []

    class _Store:
        def structured_query(self, q, param_map=None):
            return []

    r = GraphRetriever.for_store(_Store())

    async def _fake_knn_names(_q):
        return ["Вектор-сущность"]

    async def _fake_table(_q):
        return [{"name": "Таблица-сущность"}]

    async def _fake_walk(name, *, hops=1):
        walked.append(name)
        return RoundGraphData()

    monkeypatch.setattr(r, "_nebula_knn_names", _fake_knn_names, raising=False)
    monkeypatch.setattr(r, "_entity_table_names", _fake_table, raising=False)
    monkeypatch.setattr(r, "awalk", _fake_walk)
    await r.aretrieve("зерно")
    assert "Вектор-сущность" in walked
    assert "Таблица-сущность" in walked


@pytest.mark.asyncio
async def test_aretrieve_vector_outage_does_not_blank_the_lexical_path(monkeypatch):
    """A naive try/except around the whole seeding block would blank both
    paths on a kNN raise. The kNN raise must be caught alone — the table
    seed still reaches the walk."""
    monkeypatch.setattr(
        "src.graph.retriever.settings.graph.backend", "nebula", raising=False,
    )
    walked: list[str] = []

    class _Store:
        def structured_query(self, q, param_map=None):
            return []

    r = GraphRetriever.for_store(_Store())

    async def _boom_knn(_q):
        raise RuntimeError("embed down")

    async def _fake_table(_q):
        return [{"name": "Таблица-сущность"}]

    async def _fake_walk(name, *, hops=1):
        walked.append(name)
        return RoundGraphData()

    monkeypatch.setattr(r, "_nebula_knn_names", _boom_knn, raising=False)
    monkeypatch.setattr(r, "_entity_table_names", _fake_table, raising=False)
    monkeypatch.setattr(r, "awalk", _fake_walk)
    await r.aretrieve("зерно")
    assert walked == ["Таблица-сущность"]


@pytest.mark.asyncio
async def test_entity_table_names_catches_repository_failure(monkeypatch):
    """Exercise `_entity_table_names`'s OWN try/except (not an outer mock
    of the seam itself) — a Postgres/repository failure must not
    propagate, just return []."""

    async def _boom_search(self, query, *, mode="substring", label=None, limit=10):
        raise RuntimeError("pg down")

    monkeypatch.setattr(
        "src.storage.entity_search.EntitySearchRepository.search", _boom_search,
    )

    class _Store:
        def structured_query(self, q, param_map=None):
            return []

    r = GraphRetriever.for_store(_Store())
    assert await r._entity_table_names("q") == []


@pytest.mark.asyncio
async def test_awalk_nebula_fails_open(monkeypatch):
    monkeypatch.setattr(
        "src.graph.retriever.settings.graph.backend", "nebula", raising=False,
    )

    class _BoomStore:
        def subgraph(self, vid, hops, *, edge="RELATED"):
            raise RuntimeError("nebula down")

    r = GraphRetriever.for_store(_BoomStore())
    out = await r.awalk("Иванов", hops=999)
    assert out == RoundGraphData()
