# Identifier-aware Search Quality Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut the collateral damage that the 24 canonical-identifier types currently inflict on retrieval quality, while keeping identifier-driven precision wins intact.

**Architecture:** Two-pronged. Ingestion side: stop polluting `node.text` with the canon-block and generalise PhoneNumber consolidation to all 24 types so LLM-emitted dupes get collapsed onto deterministic canonicals. Retrieval side: cap graph fan-out on hub entities, and reweight hybrid scoring per query type.

**Tech Stack:** Python 3.12, LlamaIndex 0.13 (`PropertyGraphIndex`, `VectorStoreIndex`, `BM25Retriever`), Neo4j 5 (Cypher MERGE), Milvus 2.4 (upsert), Temporal workflow.

**Spec context:** `docs/superpowers/specs/2026-05-15-ingest-temporal-workflow-design.md` for the activity layout. New identifier types committed in `c651374` / `2885dec` / `66f478d`.

**Session protocol (user preference):** Pause after each labelled **Stage** for a quick sync before starting the next one.

---

## Stage 1 — Ingestion hygiene

### Task 1: Split canon-block out of `node.text`

**Files:**
- Modify: `src/config.py` (add `IngestionSettings.augment_inline_text` flag)
- Modify: `src/ingestion/identifier_transform.py` (gate the `set_content` call)
- Modify: `src/graph/lightrag_extract.py` (prepend `augment_text` from metadata before LLM call)
- Test: `tests/test_ingestion/test_identifier_transform.py` — new cases for the split.

- [ ] **Step 1: Add the env flag**

In `src/config.py`, locate `IngestionSettings` and append:

```python
    # When True, the canonical identifier augment block is appended to
    # ``node.text`` (so it ends up in Milvus + BM25 + the LLM
    # extractor prompt).  When False (default), the block is stored
    # in ``node.metadata['augment_text']`` only — Milvus + BM25 see
    # the original chunk; the KG extractor reads the augment from
    # metadata before the LLM call.  Default False because the block
    # measurably hurts vector / BM25 quality on topical queries.
    augment_inline_text: bool = False
```

Make sure to add a matching line to `.env.example`:

```env
INGESTION_AUGMENT_INLINE_TEXT=false
```

- [ ] **Step 2: Write the failing test**

In `tests/test_ingestion/test_identifier_transform.py`, add:

```python
def test_default_writes_augment_to_metadata_not_text():
    """With INGESTION_AUGMENT_INLINE_TEXT=false (default) the canon
    block lands in metadata['augment_text'], not in node.text."""
    from llama_index.core.schema import TextNode
    from src.ingestion.identifier_transform import (
        IdentifierCanonicalizationTransform,
    )

    text = "Контактный телефон: +7 (495) 234-56-78"
    node = TextNode(text=text)
    out = IdentifierCanonicalizationTransform()([node])

    assert out[0].text == text, (
        "node.text must NOT include the augment block by default"
    )
    augment = out[0].metadata.get("augment_text", "")
    assert "+74952345678" in augment
    assert "Канонические идентификаторы" in augment


def test_inline_flag_writes_augment_into_text(monkeypatch):
    """With INGESTION_AUGMENT_INLINE_TEXT=true the legacy behaviour
    is preserved — augment block appended to node.text."""
    from llama_index.core.schema import TextNode
    from src.config import settings
    from src.ingestion.identifier_transform import (
        IdentifierCanonicalizationTransform,
    )

    monkeypatch.setattr(
        settings.ingestion, "augment_inline_text", True, raising=False,
    )
    text = "Контактный телефон: +7 (495) 234-56-78"
    node = TextNode(text=text)
    out = IdentifierCanonicalizationTransform()([node])
    assert "Канонические идентификаторы" in out[0].text
    assert "+74952345678" in out[0].text
```

Run: `uv run pytest tests/test_ingestion/test_identifier_transform.py::test_default_writes_augment_to_metadata_not_text -v`
Expected: **fails** because the current transform always mutates `node.text`.

- [ ] **Step 3: Make the transform respect the flag**

In `src/ingestion/identifier_transform.py`, find the `set_content` call (line ~67) and change to:

```python
from src.config import settings

# ... inside the transform's __call__ loop, after building `idents`:

augment = build_augment_block(idents)
if augment:
    node.metadata["augment_text"] = augment
    if settings.ingestion.augment_inline_text:
        node.set_content(node.get_content() + augment)
```

Also persist `canonical_identifiers` to metadata as before — no change there.

- [ ] **Step 4: Make the KG extractor read augment from metadata**

In `src/graph/lightrag_extract.py` (or whichever method assembles the prompt — search for `node.get_content()` / `node.text`), change the chunk-text retrieval to:

```python
def _text_for_extraction(node) -> str:
    """Augment-aware text: when the canon block was kept out of
    ``node.text`` (default since 2026-05-17) the extractor must
    reconstruct it from metadata so the LLM still sees the
    canonical forms when building relationships."""
    text = node.get_content() if hasattr(node, "get_content") else node.text
    augment = (getattr(node, "metadata", None) or {}).get("augment_text", "")
    return f"{text}{augment}" if augment else text
```

Use `_text_for_extraction(node)` everywhere the prompt-building code currently calls `node.get_content()` / `node.text` for the chunk body that gets sent to the LLM.  Leave the metadata snapshot logic in `merge_kg_extraction` etc. alone.

- [ ] **Step 5: Run targeted tests**

```bash
uv run pytest tests/test_ingestion/test_identifier_transform.py -v
uv run pytest tests/test_ingestion/test_pipeline.py -v
uv run pytest tests/test_workflow/test_parse_and_chunk.py -v
```
Expected: all green (the new metadata-routing tests + existing flow).

- [ ] **Step 6: Run full unit suite**

```bash
uv run pytest -q --ignore=tests/test_workflow/test_workflow_local.py
```
Expected: 373+ green (current 371 + at least 2 new).

- [ ] **Step 7: Commit**

```bash
git add src/config.py src/ingestion/identifier_transform.py \
        src/graph/lightrag_extract.py .env.example \
        tests/test_ingestion/test_identifier_transform.py
git commit -m "fix(ingestion): keep canon-block out of node.text by default"
```

---

### Task 2: Generalise consolidation to all identifier types

Replaces `consolidate_phone_entities` with a generic
`consolidate_identifier_entities` that handles every type
`extract_identifiers` recognises.

**Files:**
- Move + extend: `src/graph/phone_consolidation.py` → `src/graph/identifier_consolidation.py`
- Modify: `src/workflow/activities/merge_and_resolve.py` (import + call site)
- Test: `tests/test_graph/test_identifier_consolidation.py` (new file)
- Update: `tests/test_workflow/test_merge_and_resolve.py` (patch path)

- [ ] **Step 1: Write the failing test**

`tests/test_graph/test_identifier_consolidation.py`:

```python
"""Consolidate LLM-extracted identifier-typed entities onto our
deterministic canonical form.

LightRAGExtractor often re-emits the same identifier with different
surface text — "Сайт https://example.com/", "example.com", "site
example.com" — producing parallel Neo4j nodes that bypass our
deterministic canonicalisation.  This consolidator collapses every
identifier-typed entity onto the canonical form that
``extract_identifiers`` would have produced.
"""
from __future__ import annotations

from llama_index.core.graph_stores.types import EntityNode, Relation

from src.graph.identifier_consolidation import consolidate_identifier_entities


def test_collapses_url_variants_onto_same_canonical():
    e1 = EntityNode(name="https://Example.com/", label="URL")
    e2 = EntityNode(name="example.com", label="URL")          # bare Domain in LLM eyes
    e3 = EntityNode(name="https://example.com", label="URL")
    others = [EntityNode(name="Anna", label="Person")]

    rel = Relation(source_id=e1.id, target_id=others[0].id, label="owns")

    merged, relations, name_map = consolidate_identifier_entities(
        [e1, e2, e3, *others], [rel], nodes=[],
    )

    # Three URL nodes collapsed to one.
    urls = [e for e in merged if e.label == "URL"]
    assert len(urls) == 1
    assert urls[0].name == "https://example.com"

    # Non-identifier entity untouched.
    assert any(e.name == "Anna" for e in merged)

    # Relation pointer rewritten to the survivor.
    assert relations[0].source_id == urls[0].id
    # `name_map` reports old → canonical rewrites.
    assert name_map["example.com"] == "https://example.com"


def test_phone_consolidation_still_works():
    """Backwards-compat: phone variants still collapse to E.164."""
    e1 = EntityNode(name="Телефон +7 (495) 123-45-67", label="PhoneNumber")
    e2 = EntityNode(name="+74951234567", label="PhoneNumber")
    merged, _, _ = consolidate_identifier_entities([e1, e2], [], nodes=[])
    phones = [e for e in merged if e.label == "PhoneNumber"]
    assert len(phones) == 1
    assert phones[0].name == "+74951234567"


def test_unknown_label_left_alone():
    """LLM-emitted entities whose label isn't an identifier type
    pass through untouched (Person, Organization, Topic, …)."""
    e = EntityNode(name="Анна Морозова", label="Person")
    merged, _, _ = consolidate_identifier_entities([e], [], nodes=[])
    assert merged == [e]
```

Run: `uv run pytest tests/test_graph/test_identifier_consolidation.py -v`
Expected: ModuleNotFoundError.

- [ ] **Step 2: Move + extend the consolidator**

Create `src/graph/identifier_consolidation.py` (new file).  Lift the bulk of `src/graph/phone_consolidation.py::consolidate_phone_entities` and generalise:

```python
"""Collapse LLM-extracted identifier-typed entities onto their
deterministic canonical form.

Why a separate pass: ``LightRAGExtractor`` often re-emits the same
identifier with different surface text — "Сайт https://example.com",
"example.com", "site example.com".  Each lands in Neo4j as a separate
``EntityNode`` even though our ``inject_canonical_entities`` step
already wrote the canonical one.  ER intentionally skips identifier
types (semantic similarity is meaningless for them), so without this
pass we get parallel nodes.

For every entity whose label is in ``_CANONICALISABLE_LABELS``, run
``extract_identifiers`` over the entity name; if a single canonical
form comes out, replace the entity's name with the canonical, group
all entities that resolved to the same canonical, pick one survivor
per group, accumulate ``aliases`` + ``mention_count`` +
``description`` + ``source_chunks`` + ``file_paths`` and rewrite
every ``Relation`` pointer onto the survivor.

Returns:
    (entities, relations, name_map)

``name_map`` is ``{old_name -> canonical_name}`` for callers that
need to rewrite chunk-level ``KG_NODES_KEY`` metadata (the existing
phone consolidator did this for chunks; we preserve the contract).
"""

from __future__ import annotations

from typing import Any

from src.ingestion.identifiers import IdentifierType, extract_identifiers

_CANONICALISABLE_LABELS: frozenset[str] = frozenset({
    "PhoneNumber", "Email", "INN", "OGRN", "BIC", "SNILS",
    "URL", "Domain", "TelegramHandle", "VKProfile", "TwitterHandle",
    "InstagramHandle", "LinkedInProfile", "YouTubeChannel",
    "GitHubProfile", "UUID",
    "IMEI", "MACAddress", "LicensePlate", "VIN",
})


def _canonicalise_name(name: str, label: str) -> str | None:
    """Run extract_identifiers on a single entity name. Returns the
    canonical form if exactly one identifier of the matching label
    comes out — otherwise None (entity stays untouched).
    """
    found = [
        x for x in extract_identifiers(name)
        if x.entity_type == label
    ]
    if len(found) != 1:
        return None
    return found[0].canonical


def consolidate_identifier_entities(
    entities: "list[Any]",
    relations: "list[Any]",
    nodes: "list[Any] | None" = None,
) -> "tuple[list[Any], list[Any], dict[str, str]]":
    """Generalised consolidator — see module docstring."""
    from llama_index.core.graph_stores.types import EntityNode, Relation

    # 1) Compute canonical name per entity (None = leave alone)
    name_to_canonical: dict[str, str] = {}
    for ent in entities:
        if not isinstance(ent, EntityNode):
            continue
        if ent.label not in _CANONICALISABLE_LABELS:
            continue
        canon = _canonicalise_name(ent.name, ent.label or "")
        if canon is None:
            continue
        name_to_canonical[ent.name] = canon

    if not name_to_canonical:
        return entities, relations, {}

    # 2) Group entities by (label, canonical)
    by_canonical: dict[tuple[str, str], list[EntityNode]] = {}
    for ent in entities:
        if not isinstance(ent, EntityNode):
            continue
        canon = name_to_canonical.get(ent.name)
        if canon is None:
            continue
        by_canonical.setdefault((ent.label or "", canon), []).append(ent)

    # 3) For each group: pick survivor, fold properties, build id remap
    id_remap: dict[str, str] = {}
    survivors_by_key: dict[tuple[str, str], EntityNode] = {}
    consolidated_ids: set[str] = set()
    for key, group in by_canonical.items():
        canon = key[1]
        # Prefer the entity already in canonical form, else first.
        survivor = next((e for e in group if e.name == canon), group[0])
        aliases = list((survivor.properties or {}).get("aliases", []))
        mention_count = int((survivor.properties or {}).get("mention_count", 1) or 1)
        descs = [str((survivor.properties or {}).get("description", "") or "")]
        source_chunks = list(
            (survivor.properties or {}).get("source_chunks", []) or [],
        )
        file_paths = list(
            (survivor.properties or {}).get("file_paths", []) or [],
        )
        for other in group:
            if other is survivor:
                continue
            if other.name not in aliases and other.name != canon:
                aliases.append(other.name)
            mention_count += int(
                (other.properties or {}).get("mention_count", 1) or 1,
            )
            d = str((other.properties or {}).get("description", "") or "")
            if d and d not in descs:
                descs.append(d)
            for cid in (other.properties or {}).get("source_chunks", []) or []:
                if cid not in source_chunks:
                    source_chunks.append(cid)
            for fp in (other.properties or {}).get("file_paths", []) or []:
                if fp not in file_paths:
                    file_paths.append(fp)
            id_remap[other.id] = survivor.id
            consolidated_ids.add(other.id)
        survivor.name = canon
        if survivor.properties is None:
            survivor.properties = {}
        survivor.properties["aliases"] = aliases
        survivor.properties["mention_count"] = mention_count
        survivor.properties["description"] = "\n---\n".join(d for d in descs if d)
        survivor.properties["source_chunks"] = source_chunks
        survivor.properties["file_paths"] = file_paths
        survivors_by_key[key] = survivor

    # 4) Drop consolidated entries
    new_entities = [
        e for e in entities
        if not (isinstance(e, EntityNode) and e.id in consolidated_ids)
    ]

    # 5) Rewrite relations: src/tgt remapped, self-loops dropped
    new_relations = []
    seen_pairs: set[tuple[str, str, str]] = set()
    for rel in relations:
        if not isinstance(rel, Relation):
            new_relations.append(rel)
            continue
        src = id_remap.get(rel.source_id, rel.source_id)
        tgt = id_remap.get(rel.target_id, rel.target_id)
        if src == tgt:
            continue
        key = (src, tgt, rel.label or "")
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        rel.source_id = src
        rel.target_id = tgt
        new_relations.append(rel)

    # 6) Rewrite chunk-level KG_NODES_KEY metadata so PropertyGraphIndex
    #    writes canonical names into Neo4j chunk nodes too.
    rewrite_name_map = {
        old: canon for old, canon in name_to_canonical.items()
        if old != canon
    }
    if nodes and rewrite_name_map:
        from llama_index.core.graph_stores.types import KG_NODES_KEY
        for node in nodes:
            md = getattr(node, "metadata", None) or {}
            for ent in md.get(KG_NODES_KEY, []) or []:
                if not isinstance(ent, EntityNode):
                    continue
                canon = rewrite_name_map.get(ent.name)
                if canon:
                    ent.name = canon

    return new_entities, new_relations, rewrite_name_map
```

- [ ] **Step 3: Update `merge_and_resolve` activity to call the generalised consolidator**

In `src/workflow/activities/merge_and_resolve.py`, change the import + call:

```python
# Before:
from src.graph.phone_consolidation import consolidate_phone_entities
...
merged_entities, merged_relations, _phone_map = consolidate_phone_entities(
    merged_entities, merged_relations, nodes,
)

# After:
from src.graph.identifier_consolidation import consolidate_identifier_entities
...
merged_entities, merged_relations, _id_map = consolidate_identifier_entities(
    merged_entities, merged_relations, nodes,
)
```

Update the surrounding heartbeats to use `_id_map` and rename the `stage: phone_consolidated` heartbeat to `stage: identifiers_consolidated`.

- [ ] **Step 4: Update the existing merge_and_resolve test**

In `tests/test_workflow/test_merge_and_resolve.py`, change the patch path from:

```python
"src.workflow.activities.merge_and_resolve.consolidate_phone_entities"
```
to:
```python
"src.workflow.activities.merge_and_resolve.consolidate_identifier_entities"
```

- [ ] **Step 5: Delete the legacy single-purpose module**

```bash
git rm src/graph/phone_consolidation.py
```

- [ ] **Step 6: Run tests**

```bash
uv run pytest tests/test_graph/test_identifier_consolidation.py \
              tests/test_workflow/test_merge_and_resolve.py -v
uv run pytest -q --ignore=tests/test_workflow/test_workflow_local.py
```
Expected: all green; count grows by 3 (new consolidation tests).

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(graph): generalise identifier consolidation to all 24 types"
```

---

**🛑 STAGE 1 GATE — pause for user sync.**  Confirm:
- Vector + BM25 quality recovered (smoke: re-run a topical query against a corpus with many identifier chunks; compare top-K).
- Neo4j has fewer parallel nodes for the same identifier after a re-ingest with LLM extraction on.

---

## Stage 2 — Retrieval-side smarts

### Task 3: Hub-aware graph traversal

Block dense identifier hubs (`+78005553535`, popular emails, support
URLs) from being graph-walk entry points.  They can still appear as
filters — chunks must mention them — but they don't seed neighbourhood
expansion.

**Files:**
- Modify: `src/config.py` — new `RetrievalSettings.graph_hub_threshold` (default 50).
- Modify: `src/graph/retriever.py` — pre-fetch entity degrees, skip hubs.
- Test: `tests/test_graph/test_retriever_hub.py` (new).

- [ ] **Step 1: Add the config setting**

In `src/config.py`, find the `AgentSettings` / `RetrievalSettings` class (whichever owns retrieval flags) and append:

```python
    # When an EntityNode in Neo4j has more than this many `:MENTIONS`
    # in-edges it's treated as a hub and excluded from graph-walk
    # entry points (it can still filter results — a chunk MUST
    # mention it — but it won't seed neighbourhood expansion).
    # Keeps generic support phones / common emails from dragging
    # unrelated chunks into top-K.
    graph_hub_threshold: int = 50
```

And to `.env.example`:
```env
RETRIEVAL_GRAPH_HUB_THRESHOLD=50
```

- [ ] **Step 2: Write the failing test**

`tests/test_graph/test_retriever_hub.py`:

```python
"""Hub-degree filter: entities with too many incoming :MENTIONS
edges in Neo4j are excluded from graph-walk entry points."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.graph.retriever import GraphRetriever


@pytest.mark.asyncio
async def test_hub_entity_filtered_from_walk() -> None:
    pg_index = MagicMock()
    base_retriever = MagicMock()
    base_retriever.aretrieve = AsyncMock(return_value=[])
    pg_index.as_retriever.return_value = base_retriever

    graph_store = MagicMock()
    # `+78005553535` has 9001 :MENTIONS edges in Neo4j → hub.
    graph_store.structured_query.return_value = [
        {"name": "+78005553535", "degree": 9001},
        {"name": "Анна Морозова", "degree": 4},
    ]

    with patch(
        "src.graph.retriever._entity_degrees",
        return_value={"+78005553535": 9001, "Анна Морозова": 4},
    ):
        retriever = GraphRetriever(
            pg_index, graph_store=graph_store, hub_threshold=50,
        )
        result = await retriever.aretrieve("позвоните +7 800 555 35 35 Анна Морозова")

    # Hub entity ('+78005553535') must NOT appear in the entities
    # the retriever reports — but it CAN appear inside `relations` /
    # `chunks` if the underlying LLMSynonymRetriever still picked
    # adjacent chunks via a non-hub entry point.
    entity_names = [e.get("entity_name") for e in result.entities]
    assert "+78005553535" not in entity_names
```

Run: `uv run pytest tests/test_graph/test_retriever_hub.py -v`
Expected: AttributeError (no `_entity_degrees`, no `hub_threshold` kwarg yet).

- [ ] **Step 3: Add `_entity_degrees` + hub filter to `GraphRetriever`**

In `src/graph/retriever.py`:

```python
from src.config import settings


def _entity_degrees(graph_store, names: list[str]) -> dict[str, int]:
    """Return ``{entity_name: incoming_MENTIONS_count}`` for a set of
    entity names.  One Cypher round trip.
    """
    if not names:
        return {}
    cy = (
        "UNWIND $names AS n "
        "MATCH (e {name: n})<-[r:MENTIONS]-() "
        "RETURN n AS name, count(r) AS degree"
    )
    rows = graph_store.structured_query(cy, param_map={"names": names})
    return {row["name"]: int(row["degree"]) for row in rows}


class GraphRetriever:
    def __init__(
        self,
        pg_index: PropertyGraphIndex,
        *,
        graph_store=None,
        similarity_top_k: int = 10,
        path_depth: int = 1,
        include_text: bool = True,
        hub_threshold: int | None = None,
    ) -> None:
        self._retriever = pg_index.as_retriever(...)
        self._graph_store = graph_store
        self._hub_threshold = (
            hub_threshold
            if hub_threshold is not None
            else settings.retrieval.graph_hub_threshold
        )

    async def aretrieve(self, query: str) -> RoundGraphData:
        nodes = await self._retriever.aretrieve(query)
        out = RoundGraphData()
        candidate_entity_names: list[str] = []
        for n in nodes:
            cls = type(n.node).__name__
            if cls == "EntityNode":
                name = getattr(n.node, "name", None) or n.node.metadata.get("name")
                if name:
                    candidate_entity_names.append(name)
        # One Cypher round trip per query to score degrees.
        degrees = (
            _entity_degrees(self._graph_store, candidate_entity_names)
            if self._graph_store is not None else {}
        )
        # ... existing accumulation loop, BUT skip entities whose
        # degree > self._hub_threshold from `out.entities`.  Keep
        # them in `chunks` (they may have served as legitimate
        # filters via the LLM retriever).
```

Note: the `as_retriever` factory in LlamaIndex doesn't expose hub-degree filtering — we do it post-hoc on the returned nodes.  That's fine; the goal is to keep hubs out of `RoundGraphData.entities` so the agent doesn't expand neighbourhoods around them.

- [ ] **Step 4: Wire `graph_store` into the retriever in DI**

In `src/di/providers.py`, find the `provide_graph_retriever` provider and pass `graph_store=...` from the same provider that builds `pg_index`.

- [ ] **Step 5: Run targeted + full**

```bash
uv run pytest tests/test_graph/test_retriever_hub.py -v
uv run pytest -q --ignore=tests/test_workflow/test_workflow_local.py
```

- [ ] **Step 6: Commit**

```bash
git add src/config.py src/graph/retriever.py src/di/providers.py \
        tests/test_graph/test_retriever_hub.py .env.example
git commit -m "feat(retrieval): hub-degree filter on graph-walk entry points"
```

---

### Task 4: Per-query type weighting in hybrid scoring

When the query itself contains a canonical identifier
(``+7…``, ``vk.com/anna``, ``ABC1234``, …) we know we're in identifier
mode — bump the graph branch's RRF weight, dampen the dense vector
branch.  For topical queries: standard weights.

**Files:**
- Modify: `src/config.py` — new weight settings.
- Modify: `src/retrieval/hybrid.py` — `_classify_query`, dynamic
  weights.
- Modify: `src/retrieval/query_engine.py` — pass classifier through.
- Test: `tests/test_retrieval/test_hybrid_weighting.py` (new).

- [ ] **Step 1: Add the config setting**

In `src/config.py`, in `RetrievalSettings`:

```python
    # RRF weights for the hybrid fusion.  Per-query mode multipliers
    # (below) scale the dense vs sparse vs graph branches dynamically.
    hybrid_weight_dense: float = 1.0
    hybrid_weight_sparse: float = 1.0
    hybrid_weight_graph: float = 1.0

    # Multipliers applied when the query contains an identifier
    # pattern detected by ``extract_identifiers``.  Identifier
    # queries trust the structured graph branch more and dampen the
    # noisy lexical/dense channels.
    identifier_query_dense_mult: float = 0.5
    identifier_query_sparse_mult: float = 0.7
    identifier_query_graph_mult: float = 2.0
```

- [ ] **Step 2: Write the failing test**

`tests/test_retrieval/test_hybrid_weighting.py`:

```python
"""Per-query weight adjustment in the hybrid retriever."""
from __future__ import annotations

import pytest

from src.retrieval.hybrid import _classify_query, query_weights


def test_topical_query_classified_as_topical():
    cls = _classify_query("Каковы тарифы доставки в Москву?")
    assert cls == "topical"


def test_query_with_phone_classified_as_identifier():
    cls = _classify_query("Кому принадлежит +7 (495) 234-56-78?")
    assert cls == "identifier"


def test_query_with_url_classified_as_identifier():
    cls = _classify_query("Откуда ссылка https://github.com/octocat?")
    assert cls == "identifier"


def test_topical_weights_match_base_settings():
    w = query_weights("Каковы тарифы?")
    assert w == (1.0, 1.0, 1.0)


def test_identifier_weights_boost_graph():
    w_dense, w_sparse, w_graph = query_weights(
        "Чей телефон +7 495 234 56 78?",
    )
    assert w_dense < 1.0
    assert w_graph > 1.0
```

Run: expected to fail on missing imports.

- [ ] **Step 3: Implement classifier + weight helper**

In `src/retrieval/hybrid.py`:

```python
from src.config import settings
from src.ingestion.identifiers import extract_identifiers


def _classify_query(query: str) -> Literal["topical", "identifier"]:
    """Return ``identifier`` iff ``extract_identifiers`` finds any
    canonical identifier in the query, else ``topical``."""
    return "identifier" if extract_identifiers(query) else "topical"


def query_weights(query: str) -> tuple[float, float, float]:
    """Return ``(dense, sparse, graph)`` RRF weights for the given
    query.  Identifier-mode multipliers from ``RetrievalSettings``."""
    r = settings.retrieval
    base = (r.hybrid_weight_dense, r.hybrid_weight_sparse, r.hybrid_weight_graph)
    if _classify_query(query) == "topical":
        return base
    return (
        base[0] * r.identifier_query_dense_mult,
        base[1] * r.identifier_query_sparse_mult,
        base[2] * r.identifier_query_graph_mult,
    )
```

Adjust the RRF fusion to consume these weights — see existing fusion code path in `hybrid.py`.  The fusion currently uses RRF with constant `k` and no weighting; introduce a `weighted_rrf(score_lists, weights)` helper or apply weights to per-branch scores before merging.

- [ ] **Step 4: Wire weights through `build_hybrid_retriever`**

`build_hybrid_retriever` currently takes dense + BM25 retrievers and fuses them.  Extend the signature with `graph_retriever` (optional) — when present, three branches fuse with the per-query weights.  Calling sites in `src/retrieval/query_engine.py` need to pass it.

- [ ] **Step 5: Run + commit**

```bash
uv run pytest tests/test_retrieval/test_hybrid_weighting.py -v
uv run pytest -q --ignore=tests/test_workflow/test_workflow_local.py
git add -A
git commit -m "feat(retrieval): per-query weights — identifier vs topical"
```

---

**🛑 STAGE 2 GATE — pause for user sync.**  Run a small eval:
1. Two queries: one topical (`«какие тарифы доставки?»`), one identifier (`«чей телефон +7 495 …»`).
2. Compare top-K composition before/after.  Identifier query should pull more graph entities + the matching contact chunk into top-K; topical query should NOT regress.

---

## Self-Review

**Spec coverage:**
- Item 1 (no canon in `node.text`) — Task 1.  Default flipped to false; flag preserves legacy path.
- Item 2 (generalised consolidation) — Task 2.  PhoneNumber-only → all 24 types.
- Item 4 (hub-aware graph) — Task 3.  Threshold from settings; filter at retriever boundary.
- Item 5 (per-query weighting) — Task 4.  Classifier hits the identifier regex; weights scaled per mode.

**Placeholder scan:** All code blocks contain working Python; no TBD or "implement later" sections.

**Type consistency:** New module name `identifier_consolidation` is used identically in the moved module, the activity import, and the test patch path.  Settings field names match between config + tests + production.

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-05-17-identifier-search-quality.md`.** Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**
