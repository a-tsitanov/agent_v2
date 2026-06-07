# Wiki article quality + source-download links — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve per-entity wiki article quality (rank+cap 1-hop relations, dedup citations per document) and add a deterministic "Источники" section linking to original source downloads via `GET /api/v1/documents/{doc_id}`.

**Architecture:** All changes live in the wiki-editor slice — `src/config.py` (3 settings), `src/graph/wiki_context.py` (graph reads + hash), `src/workflow/wiki/article.py` (render), `src/workflow/wiki/wiki_sweep.py` (wiring). No schema migration; everything additive and flag-tunable. Spec: `docs/superpowers/specs/2026-06-07-wiki-article-quality-and-source-links-design.md`.

**Tech Stack:** Python, pydantic-settings, Neo4j Cypher (via `graph_store.structured_query`), Temporal activities, pytest.

**Decisions (locked):** link target = stable API URL as-is (auth via gateway, out of scope); link text = bare `doc_id`; all 3 parts in scope.

---

### Task 1: Config — relation cap, docs base URL, citation default

**Files:**
- Modify: `src/config.py` (WikiSettings, ~line 435-446)
- Test: `tests/test_config/test_wiki_settings.py` (create; if `tests/test_config/` absent, place at `tests/test_wiki_settings.py`)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_config/test_wiki_settings.py
from src.config import WikiSettings


def test_wiki_settings_new_fields_defaults():
    s = WikiSettings()
    assert s.max_relations == 30
    assert s.docs_base_url == "http://localhost:8000/api/v1"
    assert s.citations_top_k == 8  # bumped from 5


def test_wiki_settings_env_override(monkeypatch):
    monkeypatch.setenv("WIKI_MAX_RELATIONS", "10")
    monkeypatch.setenv("WIKI_DOCS_BASE_URL", "https://kb.internal/api/v1")
    s = WikiSettings()
    assert s.max_relations == 10
    assert s.docs_base_url == "https://kb.internal/api/v1"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_config/test_wiki_settings.py -v`
Expected: FAIL — `AttributeError: 'WikiSettings' object has no attribute 'max_relations'` (and citations_top_k == 5).

- [ ] **Step 3: Edit WikiSettings**

In `src/config.py`, change `citations_top_k` default and add two fields after it:

```python
    citations_top_k: int = Field(default=8, ge=1)
    # Cap on 1-hop relations fed to the article prompt (ranked by neighbour
    # mention_count desc). Bounds prompt size for hub entities.
    max_relations: int = Field(default=30, ge=1)
    # Base URL for source-document download links in the "Источники" section.
    # Points at the documents API (GET {docs_base_url}/documents/{doc_id}).
    docs_base_url: str = "http://localhost:8000/api/v1"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_config/test_wiki_settings.py -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add src/config.py tests/test_config/test_wiki_settings.py
git commit -m "feat(wiki): config for relation cap, docs base url, citations 5->8"
```

---

### Task 2: wiki_context — hash folds doc_ids, relation cap/rank, citation dedup, read_source_docs

**Files:**
- Modify: `src/graph/wiki_context.py`
- Test: `tests/test_graph/test_wiki_context.py` (extend existing)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_graph/test_wiki_context.py`:

```python
from src.graph.wiki_context import read_source_docs


def test_hash_folds_source_doc_ids():
    a = subgraph_hash(_ctx(), source_doc_ids=["d1", "d2"])
    b = subgraph_hash(_ctx(), source_doc_ids=["d1"])
    assert a != b
    # order-independent
    assert subgraph_hash(_ctx(), source_doc_ids=["d2", "d1"]) == a
    # default (no docs) stays backward-compatible and stable
    assert subgraph_hash(_ctx()) == subgraph_hash(_ctx(), source_doc_ids=[])


def test_read_entity_subgraph_passes_max_relations():
    store = MagicMock()
    store.structured_query.return_value = [{
        "name": "X", "label": "Organization", "description": "",
        "qid": "", "page_title": "", "relations": [],
    }]
    read_entity_subgraph(store, "X", max_relations=7)
    _args, kwargs = store.structured_query.call_args
    assert kwargs["param_map"]["max_rel"] == 7


def test_read_source_docs_returns_distinct_ids():
    store = MagicMock()
    store.structured_query.return_value = [{"doc_id": "d1"}, {"doc_id": "d2"}]
    assert read_source_docs(store, "X") == ["d1", "d2"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_graph/test_wiki_context.py -v`
Expected: FAIL — `subgraph_hash() got an unexpected keyword 'source_doc_ids'`, `read_entity_subgraph() got an unexpected keyword 'max_relations'`, `cannot import name 'read_source_docs'`.

- [ ] **Step 3: Implement in `src/graph/wiki_context.py`**

(a) `subgraph_hash` — add `source_doc_ids` param, fold sorted ids in a distinct payload section:

```python
def subgraph_hash(ctx: EntityContext, source_doc_ids=()) -> str:
    """Stable sha256 over the entity's facts AND its source-document set.
    Order-independent on relations and doc ids; independent of qid/page_title.
    Folding doc ids in means a NEW source document (which adds a download
    link) regenerates the article even when no 1-hop relation changed."""
    rels = sorted(
        "\x1f".join((rl, d, nn, rd)) for (rl, d, nn, _nl, rd) in ctx.relations
    )
    docs = "\x1d".join(sorted(source_doc_ids or ()))
    payload = "\x1e".join([ctx.name, ctx.label, ctx.description, *rels, docs])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
```

(b) `_SUBGRAPH_CYPHER` — rank by neighbour mention_count, cap with `$max_rel`
(relations aggregated alone — no chunk OPTIONAL MATCH here, so no cartesian):

```python
_SUBGRAPH_CYPHER = """
MATCH (e:__Entity__ {name: $name})
OPTIONAL MATCH (e)-[r]-(m:__Entity__)
WITH e, r, m, coalesce(m.mention_count, 0) AS mc
ORDER BY mc DESC
WITH e, collect(CASE WHEN m IS NULL THEN NULL ELSE {
    rl: type(r),
    dir: CASE WHEN startNode(r) = e THEN 'out' ELSE 'in' END,
    nn: m.name,
    nl: head([l IN labels(m) WHERE l <> '__Entity__' AND l <> '__Node__']),
    rd: coalesce(r.description, '')
  } END) AS rels
RETURN e.name AS name,
  head([l IN labels(e) WHERE l <> '__Entity__' AND l <> '__Node__']) AS label,
  coalesce(e.description, '') AS description,
  coalesce(e.wikibase_qid, '') AS qid,
  coalesce(e.wiki_page_title, '') AS page_title,
  [x IN rels WHERE x IS NOT NULL][0..$max_rel] AS relations
"""
```

(c) `read_entity_subgraph` — accept and pass `max_relations`:

```python
def read_entity_subgraph(store, name: str, max_relations: int = 30) -> EntityContext:
    rows = store.structured_query(
        _SUBGRAPH_CYPHER, param_map={"name": name, "max_rel": max_relations})
    if not rows:
        raise ValueError(f"entity not found: {name!r}")
    r = rows[0]
    relations = [
        (x["rl"], x["dir"], x["nn"], x.get("nl") or "", x.get("rd") or "")
        for x in (r.get("relations") or [])
    ]
    return EntityContext(
        name=r["name"], label=r.get("label") or "",
        description=r.get("description") or "", wikibase_qid=r.get("qid") or "",
        page_title=(r.get("page_title") or r["name"]), relations=relations,
    )
```

(d) `_CITATIONS_CYPHER` — one chunk per doc_id before LIMIT:

```python
_CITATIONS_CYPHER = """
MATCH (c:Chunk)-[:MENTIONS]->(e:__Entity__ {name: $name})
WITH c.doc_id AS doc_id, collect(c)[0] AS c
RETURN coalesce(c.text, '') AS text, doc_id
ORDER BY doc_id LIMIT $k
"""
```

(e) New `read_source_docs` + Cypher (after `read_citations`):

```python
_SOURCE_DOCS_CYPHER = """
MATCH (c:Chunk)-[:MENTIONS]->(e:__Entity__ {name: $name})
RETURN DISTINCT c.doc_id AS doc_id ORDER BY doc_id
"""


def read_source_docs(store, name: str) -> list[str]:
    """Distinct source-document ids that mention this entity, sorted.
    Used both for the article's download links and folded into the hash."""
    rows = store.structured_query(_SOURCE_DOCS_CYPHER, param_map={"name": name})
    return [row["doc_id"] for row in rows if row.get("doc_id")]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_graph/test_wiki_context.py -v`
Expected: PASS — all old tests (hash determinism/order/qid-ignore, read_entity_subgraph, read_citations pairs) plus the 3 new ones.

- [ ] **Step 5: Commit**

```bash
git add src/graph/wiki_context.py tests/test_graph/test_wiki_context.py
git commit -m "feat(wiki): rank+cap relations, dedup citations per doc, read_source_docs, hash folds doc_ids"
```

---

### Task 3: article — "Источники" section render

**Files:**
- Modify: `src/workflow/wiki/article.py`
- Test: `tests/test_workflow/test_wiki/test_article.py` (extend existing)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_workflow/test_wiki/test_article.py`:

```python
from src.workflow.wiki.article import _fmt_sources, render_bot_section, BOT_START


def test_fmt_sources_builds_download_links():
    out = _fmt_sources(["d1", "d2"], "http://h/api/v1")
    assert "== Источники ==" in out
    assert "[http://h/api/v1/documents/d1 d1]" in out
    assert "[http://h/api/v1/documents/d2 d2]" in out


def test_fmt_sources_empty_when_no_docs_or_no_base():
    assert _fmt_sources([], "http://h/api/v1") == ""
    assert _fmt_sources(["d1"], "") == ""


def test_fmt_sources_strips_trailing_slash_in_base():
    out = _fmt_sources(["d1"], "http://h/api/v1/")
    assert "http://h/api/v1/documents/d1" in out
    assert "api/v1//documents" not in out


class _FakeLLM:
    async def acomplete(self, prompt):
        return "PROSE BODY"


import pytest


@pytest.mark.asyncio
async def test_render_appends_sources_section():
    from src.graph.wiki_context import EntityContext
    ctx = EntityContext(name="X", label="Org", description="d",
                        wikibase_qid="", page_title="X", relations=[])
    out = await render_bot_section(
        ctx, citations=[], llm=_FakeLLM(),
        source_doc_ids=["d1"], docs_base_url="http://h/api/v1")
    assert out.startswith("PROSE BODY")
    assert "== Источники ==" in out
    assert "[http://h/api/v1/documents/d1 d1]" in out


@pytest.mark.asyncio
async def test_render_no_sources_section_when_empty():
    from src.graph.wiki_context import EntityContext
    ctx = EntityContext(name="X", label="Org", description="d",
                        wikibase_qid="", page_title="X", relations=[])
    out = await render_bot_section(
        ctx, citations=[], llm=_FakeLLM(),
        source_doc_ids=[], docs_base_url="http://h/api/v1")
    assert out == "PROSE BODY"
    assert "Источники" not in out
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_workflow/test_wiki/test_article.py -v`
Expected: FAIL — `cannot import name '_fmt_sources'`; `render_bot_section() got an unexpected keyword 'source_doc_ids'`.

- [ ] **Step 3: Implement in `src/workflow/wiki/article.py`**

Add the helper (near the formatting helpers):

```python
def _fmt_sources(doc_ids, base_url: str) -> str:
    """Deterministic '== Источники ==' section with download links to the
    original files. Empty string when there are no docs or no base URL
    (section omitted entirely). Link text is the bare doc_id (UUID)."""
    if not doc_ids or not base_url:
        return ""
    base = base_url.rstrip("/")
    lines = ["== Источники ==", ""]
    for d in doc_ids:
        lines.append(f"* [{base}/documents/{d} {d}] — скачать исходник")
    return "\n".join(lines)
```

Change `render_bot_section` signature + append the section (the source list is
deterministic — NOT fed through the LLM):

```python
async def render_bot_section(ctx, citations, llm,
                             source_doc_ids=(), docs_base_url: str = "") -> str:
    """LLM-render the bot section grounded ONLY in `ctx` (graph facts) and
    `citations`. No prior article prose is passed — anti-drift (spec §5).
    A deterministic '== Источники ==' section with download links is appended
    after the prose (not LLM-generated)."""
    prompt = _PROMPT.format(
        name=ctx.name, label=ctx.label or "entity",
        description=ctx.description or "(none)",
        relations=_fmt_relations(ctx), citations=_fmt_citations(citations),
    )
    resp = await llm.acomplete(prompt)
    prose = str(resp).strip()
    sources = _fmt_sources(source_doc_ids, docs_base_url)
    return f"{prose}\n\n{sources}" if sources else prose
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_workflow/test_wiki/test_article.py -v`
Expected: PASS — new tests plus the existing splice/render tests.

- [ ] **Step 5: Commit**

```bash
git add src/workflow/wiki/article.py tests/test_workflow/test_wiki/test_article.py
git commit -m "feat(wiki): deterministic Источники section with source-download links"
```

---

### Task 4: wiki_sweep — wire source docs + new params into write_entity_article

**Files:**
- Modify: `src/workflow/wiki/wiki_sweep.py` (`write_entity_article`)
- Test: `tests/test_workflow/test_wiki/test_wiki_sweep.py` (extend; assert wiring on the helper, not Temporal)

- [ ] **Step 1: Read the existing sweep test to match its stubbing style**

Run: `sed -n '1,60p' tests/test_workflow/test_wiki/test_wiki_sweep.py`
Note how it patches `build_neo4j_graph_store`, `get_mediawiki`, `get_llm_pool`, and the `wiki_context` helpers. The new test follows the same pattern.

- [ ] **Step 2: Write the failing test**

Add a test asserting `write_entity_article` calls `read_source_docs`, threads
`settings.wiki.max_relations` into `read_entity_subgraph`, hashes with the doc
ids, and passes `source_doc_ids` + `docs_base_url` into `render_bot_section`.
Mirror the existing test's patch targets (patch at
`src.workflow.wiki.wiki_sweep`'s import sites). Example skeleton:

```python
@pytest.mark.asyncio
async def test_write_article_threads_sources_and_max_relations(monkeypatch):
    # ... reuse the file's existing fixtures/stubs for store + mediawiki + llm ...
    # stub read_source_docs -> ["d1"], read_entity_subgraph -> ctx,
    # subgraph_hash -> "newhash" (force the changed branch),
    # capture render_bot_section kwargs.
    # assert: read_entity_subgraph called with max_relations == settings.wiki.max_relations
    # assert: render_bot_section received source_doc_ids == ["d1"]
    #         and docs_base_url == settings.wiki.docs_base_url
```

(Use the same monkeypatch/MagicMock approach already present in this test file;
do not introduce Temporal — `write_entity_article` is an activity body callable
directly.)

- [ ] **Step 3: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_workflow/test_wiki/test_wiki_sweep.py -v`
Expected: FAIL — `read_source_docs` not called / wrong kwargs to render.

- [ ] **Step 4: Implement in `src/workflow/wiki/wiki_sweep.py`**

Update the imports and the body of `write_entity_article`:

```python
    from src.graph.wiki_context import (
        read_entity_subgraph, read_citations, read_source_docs, subgraph_hash)
    ...
    store = build_neo4j_graph_store()
    ctx = read_entity_subgraph(store, name, settings.wiki.max_relations)
    docs = read_source_docs(store, name)
    h = subgraph_hash(ctx, docs)
    # change-detection: skip if subgraph (facts + source set) unchanged.
    cur_hash_rows = store.structured_query(
        "MATCH (e:__Entity__ {name:$n}) RETURN coalesce(e.wiki_hash,'') AS h",
        param_map={"n": name})
    if cur_hash_rows and cur_hash_rows[0]["h"] == h:
        clear_dirty(store, name, h)
        return ArticleOutcome.SKIPPED.value

    cites = read_citations(store, name, settings.wiki.citations_top_k)
    llm = get_llm_pool().get("synthesis")
    bot_md = await render_bot_section(
        ctx, cites, llm=llm,
        source_doc_ids=docs, docs_base_url=settings.wiki.docs_base_url)
    # ... rest (get_mediawiki, splice, upsert, sitelink, persist) unchanged ...
```

- [ ] **Step 5: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_workflow/test_wiki/test_wiki_sweep.py -v`
Expected: PASS.

- [ ] **Step 6: Run the full wiki suite (regression)**

Run: `.venv/bin/python -m pytest tests/test_graph/test_wiki_context.py tests/test_workflow/test_wiki tests/test_config/test_wiki_settings.py -q`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add src/workflow/wiki/wiki_sweep.py tests/test_workflow/test_wiki/test_wiki_sweep.py
git commit -m "feat(wiki): wire source docs, relation cap, doc-id hash into write_entity_article"
```

---

### Task 5: Docs touch-up + final review

**Files:**
- Modify: `docs/runbook/wiki-editor.md` (config table + bot-section description)

- [ ] **Step 1: Update the `WIKI_*` config table**

Add rows for `WIKI_MAX_RELATIONS` (default 30) and `WIKI_DOCS_BASE_URL`
(default `http://localhost:8000/api/v1`); update `WIKI_CITATIONS_TOP_K` default
to 8. In §6 (bot-section contents) add a bullet: "== Источники == — download
links `[{WIKI_DOCS_BASE_URL}/documents/{doc_id} {doc_id}]` to the original files
(endpoint is under `require_api_key`; serve via an authenticated gateway for
browser readers)."

- [ ] **Step 2: Commit**

```bash
git add docs/runbook/wiki-editor.md
git commit -m "docs(wiki): document max_relations, docs_base_url, Источники section"
```

- [ ] **Step 3: Final review**

Dispatch a final code reviewer over the whole change set (Tasks 1-5). Confirm:
no `read_citations`/`subgraph_hash` callers left unmigrated; the auth caveat is
documented; the section is omitted (not empty-headed) when there are no docs.
