"""Tests for `src/graph/entity_resolution.py`.

Stubs the LLM (verdict-by-pair dict), the embedding model
(deterministic vectors keyed by name), and the graph store
(returns canned existing canonicals).  Verifies the full ER
pipeline: deterministic matches, embedding-based candidates,
LLM judge, hyper-hub clamp, identifier exclusion, name_map
application, incremental ER.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import Any

import pytest
from llama_index.core.base.llms.types import ChatMessage
from llama_index.core.graph_stores.types import (
    KG_NODES_KEY,
    EntityNode,
    Relation,
)
from llama_index.core.schema import TextNode

from src.graph.entity_resolution import (
    ERConfig,
    _deep_normalize,
    _initials_signature,
    _is_cyrillic_name,
    resolve_entities,
)


# ── stubs ───────────────────────────────────────────────────────────


@dataclass
class _EmbeddingStub:
    """Maps `name`-derived prefix → fixed vector.  Tests register
    explicit name→vector entries; everything else returns a hash-
    derived stable but distinct vector.
    """

    table: dict[str, list[float]] = field(default_factory=dict)
    batch_calls: int = 0

    async def aget_text_embedding_batch(self, texts: list[str]) -> list[list[float]]:
        self.batch_calls += 1
        return [self._lookup(t) for t in texts]

    async def aget_text_embedding(self, text: str) -> list[float]:
        return self._lookup(text)

    def _lookup(self, text: str) -> list[float]:
        # text is either "name" (no description) or "name: description".
        # Try exact match first, then prefix-match against table keys.
        if text in self.table:
            return list(self.table[text])
        for prefix, vec in self.table.items():
            # Strip trailing ":" from prefix when matching name-only
            # text — the production code skips the colon for entities
            # without a description.
            stripped = prefix.rstrip(":")
            if text == stripped or text.startswith(prefix):
                return list(vec)
        # Deterministic fallback: 4-dim vector from hash; ensures
        # different unseen names get different vectors.
        h = abs(hash(text)) % 9973
        return [math.sin(h), math.cos(h), math.sin(h * 2), math.cos(h * 2)]


@dataclass
class _ScriptedJudgeLLM:
    """LLM stub that maps (name_a, name_b) → bool (SAME/DIFFERENT).

    Returns a JSON array per achat() call so the production parser
    in `_parse_judge_response` is exercised.
    """

    verdicts: dict[tuple[str, str], bool] = field(default_factory=dict)
    raise_on_call: bool = False
    calls: list[list[ChatMessage]] = field(default_factory=list)

    async def achat(self, messages: list[ChatMessage], **_: Any) -> Any:
        self.calls.append(messages)
        if self.raise_on_call:
            raise RuntimeError("judge timeout simulated")

        user = messages[-1].content or ""
        # Parse all pairs from the user message.
        pair_entries = []
        import re

        for m in re.finditer(
            r"Pair (\d+):\s*\n\s*A:\s*'([^']+)'.*?\n\s*B:\s*'([^']+)'",
            user,
            re.DOTALL,
        ):
            idx = int(m.group(1))
            a = m.group(2)
            b = m.group(3)
            key1 = (a, b)
            key2 = (b, a)
            same = self.verdicts.get(key1, self.verdicts.get(key2, False))
            pair_entries.append({
                "pair": idx, "verdict": "SAME" if same else "DIFFERENT",
            })

        class _Resp:
            class _Msg:
                content = json.dumps(pair_entries)

            message = _Msg()

        return _Resp()


@dataclass
class _GraphStoreStub:
    """Returns canned canonical entities via structured_query."""

    rows: list[dict] = field(default_factory=list)
    raises: bool = False

    def structured_query(self, query: str, param_map: dict | None = None) -> list[dict]:
        if self.raises:
            raise RuntimeError("neo4j down")
        return self.rows


def _ent(
    name: str, label: str = "Concept", desc: str = "",
    mention_count: int = 1,
) -> EntityNode:
    return EntityNode(
        name=name, label=label,
        properties={"description": desc, "mention_count": mention_count},
    )


# ── pure helpers ────────────────────────────────────────────────────


def test_deep_normalize() -> None:
    assert _deep_normalize("Basal Cell Carcinoma") == "basal cell carcinoma"
    assert _deep_normalize("Basal-cell  carcinoma!") == "basal cell carcinoma"
    assert _deep_normalize("Иванов И. И.") == "иванов и и"


def test_initials_signature_full() -> None:
    sig = _initials_signature("Иванов И.И.")
    assert sig is not None
    assert sig[0] == "иванов"


def test_is_cyrillic_name() -> None:
    assert _is_cyrillic_name("Базальноклеточный Рак")
    assert not _is_cyrillic_name("Basal Cell Carcinoma")
    assert not _is_cyrillic_name("BCC")


# ── end-to-end resolve_entities ─────────────────────────────────────


@pytest.mark.asyncio
async def test_cross_lingual_merge() -> None:
    """BCC and Базальноклеточный Рак — same vector + LLM SAME → merge."""
    embed = _EmbeddingStub(table={
        "Basal Cell Carcinoma:": [1.0, 0.0, 0.0, 0.0],
        "Базальноклеточный Рак:": [1.0, 0.0, 0.0, 0.0],  # identical → cosine 1.0
    })
    llm = _ScriptedJudgeLLM(verdicts={
        ("Basal Cell Carcinoma", "Базальноклеточный Рак"): True,
    })
    entities = [
        _ent("Basal Cell Carcinoma", desc="A type of skin cancer.", mention_count=3),
        _ent("Базальноклеточный Рак", desc="Тип рака кожи.", mention_count=5),
    ]
    out_ents, out_rels, name_map = await resolve_entities(
        entities, [], [], llm=llm, embed_model=embed,
    )
    # One canonical entity remains.
    assert len(out_ents) == 1
    # Canonical picked by mention_count → Cyrillic with count 5.
    assert out_ents[0].name == "Базальноклеточный Рак"
    assert "Basal Cell Carcinoma" in out_ents[0].properties["aliases"]
    # name_map keyed by normalised name of the non-canonical.
    assert any("basal" in k.lower() for k in name_map)


@pytest.mark.asyncio
async def test_initialism_deterministic_no_llm() -> None:
    """Иванов И.И. ≡ Иван Иванов via initials regex — no LLM call."""
    embed = _EmbeddingStub()
    llm = _ScriptedJudgeLLM()  # would record any call
    entities = [
        _ent("Иванов И.И.", label="Person", mention_count=4),
        _ent("Иван Иванов", label="Person", mention_count=2),
    ]
    out_ents, _, name_map = await resolve_entities(
        entities, [], [], llm=llm, embed_model=embed,
    )
    assert len(out_ents) == 1
    # Mention_count tiebreak: Иванов И.И. (4) wins over Иван Иванов (2).
    assert out_ents[0].name == "Иванов И.И."
    # No LLM judge call because deterministic confirmed the pair.
    assert llm.calls == []
    assert len(name_map) == 1


@pytest.mark.asyncio
async def test_type_vs_instance_trap_rejected() -> None:
    """Cosine 0.95 but LLM DIFFERENT → not merged."""
    embed = _EmbeddingStub(table={
        "Клиент:": [1.0, 0.0, 0.0, 0.0],
        "Клиент #4521:": [0.999, 0.04, 0.0, 0.0],  # very close
    })
    # Borderline (cosine ≥ HIGH but same-script auto-merge applies);
    # to route to LLM we use a cross-script trick — actually let's
    # use a borderline cosine to ensure LLM is asked.
    embed.table["Клиент:"] = [1.0, 0.0, 0.0, 0.0]
    embed.table["Клиент #4521:"] = [0.80, 0.60, 0.0, 0.0]
    # cosine = 0.80 → in [LOW, HIGH) range → routes to LLM.
    llm = _ScriptedJudgeLLM(verdicts={
        ("Клиент", "Клиент #4521"): False,
    })
    entities = [
        _ent("Клиент", desc="Generic concept of a customer.", mention_count=10),
        _ent("Клиент #4521", desc="A specific customer record.", mention_count=2),
    ]
    out_ents, _, name_map = await resolve_entities(
        entities, [], [], llm=llm, embed_model=embed,
    )
    assert len(out_ents) == 2
    assert name_map == {}


@pytest.mark.asyncio
async def test_deterministic_label_excluded() -> None:
    """Phone/INN/Email labels are SKIPPED — even with cosine 1.0."""
    embed = _EmbeddingStub(table={
        "+74951234567:": [1.0, 0.0, 0.0, 0.0],
        "+74951234568:": [1.0, 0.0, 0.0, 0.0],
    })
    llm = _ScriptedJudgeLLM(verdicts={
        ("+74951234567", "+74951234568"): True,  # would merge if asked
    })
    entities = [
        _ent("+74951234567", label="PhoneNumber"),
        _ent("+74951234568", label="PhoneNumber"),
    ]
    out_ents, _, name_map = await resolve_entities(
        entities, [], [], llm=llm, embed_model=embed,
    )
    assert len(out_ents) == 2  # both kept
    assert llm.calls == []     # LLM never consulted
    assert name_map == {}


@pytest.mark.asyncio
async def test_self_loop_relation_dropped() -> None:
    """When A and B merge, a relation A→B becomes self-loop and is dropped."""
    embed = _EmbeddingStub(table={
        "Basal Cell Carcinoma:": [1.0, 0.0, 0.0, 0.0],
        "Базальноклеточный Рак:": [1.0, 0.0, 0.0, 0.0],
    })
    llm = _ScriptedJudgeLLM(verdicts={
        ("Basal Cell Carcinoma", "Базальноклеточный Рак"): True,
    })
    a = _ent("Basal Cell Carcinoma", mention_count=1)
    b = _ent("Базальноклеточный Рак", mention_count=1)
    self_loop = Relation(
        label="ALIAS_OF", source_id=a.id, target_id=b.id,
        properties={"description": "Same thing."},
    )
    out_ents, out_rels, _ = await resolve_entities(
        [a, b], [self_loop], [], llm=llm, embed_model=embed,
    )
    # When source_id == target_id post-rewrite (same canonical), drop.
    assert all(r.source_id != r.target_id for r in out_rels)


@pytest.mark.asyncio
async def test_hyper_hub_clamp() -> None:
    """Cluster size ≥ hyper_hub_threshold → not merged, marked review."""
    # Build 13 entities that all share the same embedding → one big cluster.
    name_template = "Entity {i}"
    table = {
        f"{name_template.format(i=i)}:": [1.0, 0.0, 0.0, 0.0] for i in range(13)
    }
    embed = _EmbeddingStub(table=table)
    # LLM says SAME for every pair encountered.
    llm = _ScriptedJudgeLLM()  # not consulted because cosine 1.0 = auto-merge
    entities = [_ent(name_template.format(i=i)) for i in range(13)]
    cfg = ERConfig(hyper_hub_threshold=12, high=0.95)
    out_ents, _, name_map = await resolve_entities(
        entities, [], [], llm=llm, embed_model=embed, config=cfg,
    )
    # All 13 kept (not merged).
    assert len(out_ents) == 13
    # name_map empty (no consolidation happened).
    assert name_map == {}
    # er_review_needed flag on every member.
    review_flagged = sum(
        1 for e in out_ents if (e.properties or {}).get("er_review_needed")
    )
    assert review_flagged == 13


@pytest.mark.asyncio
async def test_judge_timeout_treated_as_different() -> None:
    """LLM raises → all borderline pairs default to DIFFERENT."""
    embed = _EmbeddingStub(table={
        "A:": [1.0, 0.0, 0.0, 0.0],
        "B:": [0.80, 0.60, 0.0, 0.0],  # borderline cosine 0.80
    })
    llm = _ScriptedJudgeLLM(raise_on_call=True)
    entities = [_ent("A", desc="ay"), _ent("B", desc="bee")]
    out_ents, _, name_map = await resolve_entities(
        entities, [], [], llm=llm, embed_model=embed,
    )
    assert len(out_ents) == 2  # both kept
    assert name_map == {}


@pytest.mark.asyncio
async def test_embedding_failure_passthrough() -> None:
    """Embed API raises → return inputs unchanged, no ER applied."""

    class _RaisingEmbed:
        async def aget_text_embedding_batch(self, texts):
            raise RuntimeError("embed down")

        async def aget_text_embedding(self, text):
            raise RuntimeError("embed down")

    llm = _ScriptedJudgeLLM()
    entities = [_ent("X"), _ent("Y")]
    out_ents, out_rels, name_map = await resolve_entities(
        entities, [], [], llm=llm, embed_model=_RaisingEmbed(),
    )
    assert out_ents == entities  # identity, not just equal
    assert out_rels == []
    assert name_map == {}


@pytest.mark.asyncio
async def test_incremental_er_from_graph_store() -> None:
    """Stored canonical from prior ingest matches new entity → merge."""
    stored_vec = [1.0, 0.0, 0.0, 0.0]
    embed = _EmbeddingStub(table={
        "BCC:": stored_vec,  # new entity, same vector as stored
    })
    llm = _ScriptedJudgeLLM(verdicts={
        ("BCC", "Базальноклеточный Рак"): True,
        ("Базальноклеточный Рак", "BCC"): True,
    })
    graph_store = _GraphStoreStub(rows=[{
        "name": "Базальноклеточный Рак",
        "labels": ["__Entity__", "__Node__", "Concept"],
        "er_embedding": json.dumps(stored_vec),
        "mention_count": 10,
        "description": "Существующая каноническая запись.",
    }])
    new_entity = _ent("BCC", desc="Abbreviation.", mention_count=2)
    out_ents, _, name_map = await resolve_entities(
        [new_entity], [], [], llm=llm, embed_model=embed,
        graph_store=graph_store,
    )
    # The new BCC should be redirected to the stored canonical.
    assert len(out_ents) == 1
    assert out_ents[0].name == "Базальноклеточный Рак"
    assert "BCC" in (out_ents[0].properties or {}).get("aliases", [])


@pytest.mark.asyncio
async def test_name_map_applied_to_chunk_metadata() -> None:
    """After ER, chunk-level KG_NODES_KEY entities get renamed to canonical."""
    embed = _EmbeddingStub(table={
        "Basal Cell Carcinoma:": [1.0, 0.0, 0.0, 0.0],
        "Базальноклеточный Рак:": [1.0, 0.0, 0.0, 0.0],
    })
    llm = _ScriptedJudgeLLM(verdicts={
        ("Basal Cell Carcinoma", "Базальноклеточный Рак"): True,
    })
    # Chunk-level entities point to per-chunk EntityNodes (separate
    # instances) with the same names.  After ER, both should be
    # renamed to the canonical.
    chunk_a = TextNode(id_="c1", text="...")
    chunk_a.metadata[KG_NODES_KEY] = [_ent("Basal Cell Carcinoma")]
    chunk_b = TextNode(id_="c2", text="...")
    chunk_b.metadata[KG_NODES_KEY] = [_ent("Базальноклеточный Рак")]

    merged = [
        _ent("Basal Cell Carcinoma", mention_count=1),
        _ent("Базальноклеточный Рак", mention_count=3),
    ]
    await resolve_entities(
        merged, [], [chunk_a, chunk_b], llm=llm, embed_model=embed,
    )
    # Both chunk-level entities now carry the canonical name.
    final_names = {
        ent.name
        for chunk in (chunk_a, chunk_b)
        for ent in chunk.metadata[KG_NODES_KEY]
    }
    assert final_names == {"Базальноклеточный Рак"}


@pytest.mark.asyncio
async def test_aliases_preserved() -> None:
    """Cluster members' non-canonical names land in properties['aliases']."""
    embed = _EmbeddingStub(table={
        "Иванов И.И.:": [1.0, 0.0, 0.0, 0.0],
        "Иван Иванов:": [1.0, 0.0, 0.0, 0.0],
    })
    llm = _ScriptedJudgeLLM()  # deterministic pre-pass handles this
    entities = [
        _ent("Иванов И.И.", label="Person", mention_count=5),
        _ent("Иван Иванов", label="Person", mention_count=1),
    ]
    out_ents, _, _ = await resolve_entities(
        entities, [], [], llm=llm, embed_model=embed,
    )
    assert len(out_ents) == 1
    aliases = (out_ents[0].properties or {}).get("aliases") or []
    assert "Иван Иванов" in aliases


@pytest.mark.asyncio
async def test_singletons_get_er_metadata() -> None:
    """Entities not in any cluster still get er_canonical_name +
    er_embedding so the next ingest can match against them."""
    embed = _EmbeddingStub(table={
        "Unique Concept:": [0.1, 0.2, 0.3, 0.4],
    })
    llm = _ScriptedJudgeLLM()
    entities = [_ent("Unique Concept", desc="Solo.")]
    out_ents, _, _ = await resolve_entities(
        entities, [], [], llm=llm, embed_model=embed,
    )
    assert len(out_ents) == 1
    props = out_ents[0].properties or {}
    assert props.get("er_canonical_name") == "Unique Concept"
    assert json.loads(props.get("er_embedding")) == [0.1, 0.2, 0.3, 0.4]


@pytest.mark.asyncio
async def test_load_existing_canonicals_orders_by_mention_count_and_limits():
    """The incremental-ER window must load the most-mentioned canonicals
    first (ORDER BY mention_count DESC) and honour the configured limit —
    so a graph larger than the window can't silently drop hub entities
    from dedup."""
    from src.graph.entity_resolution import _load_existing_canonicals

    captured: dict[str, Any] = {}

    class _Store:
        def structured_query(self, query, param_map=None):
            captured["query"] = query
            captured["param_map"] = param_map
            return []

    out = await _load_existing_canonicals(_Store(), limit=123)

    assert out == []
    assert "ORDER BY mention_count DESC" in captured["query"]
    assert "LIMIT $limit" in captured["query"]
    assert captured["param_map"] == {"limit": 123}


@pytest.mark.asyncio
async def test_load_existing_canonicals_none_store_returns_empty():
    from src.graph.entity_resolution import _load_existing_canonicals

    assert await _load_existing_canonicals(None, limit=10) == []


def _mk_item(name: str, vec: list[float], *, label: str = "Person", desc: str = "d"):
    from src.graph.entity_resolution import _Item, _normalize_entity_name
    return _Item(
        name=name, norm=_normalize_entity_name(name), label=label,
        description=desc, mention_count=3, source="new", embedding=vec,
    )


def test_candidate_pairs_numpy_path_matches_pure_python(monkeypatch):
    """The vectorised cosine path is an optimisation — it must yield the
    EXACT same candidate set as the pure-Python `_cosine` fallback, never
    a behaviour change."""
    import src.graph.entity_resolution as er

    items = [
        _mk_item("Alpha One", [1.0, 0.0, 0.0, 0.0]),
        _mk_item("Alpha Two", [0.99, 0.10, 0.0, 0.0]),   # near-dup of Alpha
        _mk_item("Beta Core", [0.0, 1.0, 0.0, 0.0]),
        _mk_item("Beta Prime", [0.02, 0.98, 0.05, 0.0]),  # near-dup of Beta
        _mk_item("Gamma", [0.0, 0.0, 1.0, 0.0]),
        _mk_item("Delta", [0.0, 0.0, 0.0, 1.0]),
        _mk_item("Beta Two", [0.0, 0.97, 0.10, 0.0]),     # another Beta-ish
        _mk_item("Empty Desc", [0.9, 0.2, 0.0, 0.0], desc=""),  # empty-desc floor
    ]
    cfg = ERConfig()

    auto_np, bord_np = er._candidate_pairs(items, cfg)
    set_np = {(a, b) for a, b, _ in [*auto_np, *bord_np]}

    monkeypatch.setattr(er, "_normalized_matrix", lambda group: None)
    auto_py, bord_py = er._candidate_pairs(items, cfg)
    set_py = {(a, b) for a, b, _ in [*auto_py, *bord_py]}

    assert set_np == set_py


@pytest.mark.asyncio
async def test_cleanup_stored_losers_safe_on_failure():
    """When the repoint+delete query fails (e.g. APOC missing), the loser
    node must be LEFT INTACT — exactly one query attempt per pair, no
    second `DETACH DELETE` that would drop its edges, and no raise."""
    from src.graph.entity_resolution import _cleanup_stored_losers

    calls: list[dict] = []

    class _BoomStore:
        def structured_query(self, query, param_map=None):
            calls.append({"query": query, "param_map": param_map})
            raise RuntimeError("apoc.merge.relationship not found")

    # Must not raise.
    await _cleanup_stored_losers(_BoomStore(), [("Loser A", "Canon A")])

    # Exactly one attempt (the APOC repoint) — no destructive fallback.
    assert len(calls) == 1
    assert "DETACH DELETE" not in calls[0]["query"] or "apoc.merge" in calls[0]["query"]
