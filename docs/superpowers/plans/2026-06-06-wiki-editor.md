# Continuous Wiki Article Editor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Temporal-driven editor that regenerates a per-entity MediaWiki article section from the Neo4j graph — grounded, cited, drift-free — preserving human edits, triggered by ingest dirty-marking + a scheduled sweep.

**Architecture:** New `kb-wiki` Temporal queue hosts `WikiSweepWorkflow`, which selects dirty `__Entity__` nodes and fans out a `write_entity_article` activity per entity: read the 1-hop subgraph + source citations, skip via a content hash if unchanged, else LLM-render a bot section (synthesis lane), splice it between markers into the MediaWiki page (Action API), and update dirty/hash bookkeeping. Ingest marks touched entities dirty; a Temporal Schedule runs the sweep.

**Tech Stack:** Python 3.12, Temporal SDK, httpx (MediaWiki Action API), Neo4j (`structured_query`), LLMPool, pydantic-settings, pytest/pytest-asyncio.

**Spec:** `docs/superpowers/specs/2026-06-06-wiki-editor-design.md`

---

## File Structure

- **Create** `src/graph/wiki_context.py` — `EntityContext`, `read_entity_subgraph`, `read_citations`, `subgraph_hash`.
- **Create** `src/graph/wiki_dirty.py` — `mark_dirty`, `select_dirty`, `clear_dirty` (Cypher helpers).
- **Create** `src/workflow/wiki/__init__.py`, `src/workflow/wiki/article.py` (`render_bot_section`, `splice_bot_section`, markers), `src/workflow/wiki/wiki_sweep.py` (`WikiSweepWorkflow` + activities).
- **Create** `src/storage/mediawiki.py` — `AsyncMediaWiki` (httpx Action API client).
- **Create** `src/workflow/activities/mark_dirty.py` — `mark_entities_dirty` activity.
- **Create** `src/api/routes/admin.py` — `POST /admin/wiki/rebuild`.
- **Create** `scripts/setup_wiki_schedule.py` — idempotent Temporal Schedule.
- **Modify** `src/config.py` (WikiSettings), `src/workflow/worker.py` (register), `src/workflow/document_ingest.py` (hook), `src/workflow/activities/__init__.py` (export), `src/api/__init__.py` or app factory (mount admin router), `.env.example`.
- **Tests** under `tests/test_graph/`, `tests/test_workflow/test_wiki/`, `tests/test_storage/`, `tests/test_config/`.

Run with `uv run pytest`. Reuse: `build_neo4j_graph_store`, `get_llm_pool().get("synthesis")`, `settings.wikibase.bot_user/bot_password`.

---

## Task 1: `WikiSettings` config

**Files:**
- Modify: `src/config.py`
- Test: `tests/test_config/test_settings.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_config/test_settings.py`:

```python
def test_wiki_settings_defaults():
    from src.config import WikiSettings
    s = WikiSettings()
    assert s.enabled is False
    assert s.task_queue == "kb-wiki"
    assert s.activity_concurrency >= 1
    assert s.sweep_batch >= 1
    assert s.sweep_interval_minutes >= 1
    assert s.citations_top_k >= 1
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_config/test_settings.py::test_wiki_settings_defaults -v`
Expected: FAIL — `cannot import name 'WikiSettings'`.

- [ ] **Step 3: Implement**

In `src/config.py`, after `WikibaseSettings` (search for `class WikibaseSettings`), add:

```python
class WikiSettings(BaseSettings):
    """Continuous wiki-article editor (Project A). Generates per-entity
    MediaWiki pages from the Neo4j graph. Opt-in via WIKI_ENABLED."""

    model_config = SettingsConfigDict(
        env_prefix="WIKI_", env_file=".env", extra="ignore",
    )

    enabled: bool = False
    task_queue: str = "kb-wiki"
    activity_concurrency: int = Field(default=4, ge=1)
    sweep_batch: int = Field(default=50, ge=1)
    sweep_interval_minutes: int = Field(default=15, ge=1)
    citations_top_k: int = Field(default=5, ge=1)
    # MediaWiki Action API URL. Empty -> derived from wikibase.base_url
    # + "/w/api.php" by mediawiki_api_url() below.
    mediawiki_api_url: str = ""
```

Then in the `Settings` class add (near the `wikibase` cached_property):

```python
    @cached_property
    def wiki(self) -> WikiSettings:
        return WikiSettings()
```

Add `WikiSettings` to the module `__all__` list (alphabetically, alongside the other `*Settings`).

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_config/test_settings.py::test_wiki_settings_defaults -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/config.py tests/test_config/test_settings.py
git commit -m "feat(config): WikiSettings for continuous wiki editor"
```

---

## Task 2: `subgraph_hash` + `EntityContext` (pure core)

**Files:**
- Create: `src/graph/wiki_context.py`
- Test: `tests/test_graph/test_wiki_context.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_graph/test_wiki_context.py`:

```python
from src.graph.wiki_context import EntityContext, subgraph_hash


def _ctx(**kw):
    base = dict(
        name="ООО Альфа", label="Organization", description="A supplier.",
        wikibase_qid="Q5", page_title="ООО Альфа",
        relations=[
            ("заключила договор", "out", "Договор № 17-К", "Document", "signed"),
            ("контакт", "out", "+74951234567", "PhoneNumber", ""),
        ],
    )
    base.update(kw)
    return EntityContext(**base)


def test_hash_is_deterministic_and_order_independent():
    a = _ctx()
    b = _ctx(relations=list(reversed(_ctx().relations)))  # reordered
    assert subgraph_hash(a) == subgraph_hash(b)


def test_hash_changes_when_a_relation_changes():
    a = _ctx()
    b = _ctx(relations=_ctx().relations + [("платит", "out", "X", "Amount", "")])
    assert subgraph_hash(a) != subgraph_hash(b)


def test_hash_changes_on_description_change():
    assert subgraph_hash(_ctx()) != subgraph_hash(_ctx(description="changed"))


def test_hash_ignores_qid_and_page_title():
    assert subgraph_hash(_ctx(wikibase_qid="Q9", page_title="other")) == \
           subgraph_hash(_ctx())
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_graph/test_wiki_context.py -v`
Expected: FAIL — `No module named 'src.graph.wiki_context'`.

- [ ] **Step 3: Implement the dataclass + hash**

Create `src/graph/wiki_context.py`:

```python
"""Read an entity's 1-hop subgraph from Neo4j and hash it for change
detection. The hash covers only graph FACTS (name/label/description +
relations) — NOT the QID, page title, or citations — so the article is
regenerated exactly when the facts change."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

# (rel_label, direction "out"|"in", neighbor_name, neighbor_label, rel_description)
Relation = tuple[str, str, str, str, str]


@dataclass
class EntityContext:
    name: str
    label: str
    description: str
    wikibase_qid: str
    page_title: str
    relations: list[Relation] = field(default_factory=list)


def subgraph_hash(ctx: EntityContext) -> str:
    """Stable sha256 over the entity's facts. Order-independent on
    relations (sorted), independent of qid/page_title/citations."""
    rels = sorted(
        "|".join((rl, d, nn, rd)) for (rl, d, nn, _nl, rd) in ctx.relations
    )
    payload = "\x1e".join([ctx.name, ctx.label, ctx.description, *rels])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_graph/test_wiki_context.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/graph/wiki_context.py tests/test_graph/test_wiki_context.py
git commit -m "feat(graph): EntityContext + subgraph_hash (change detection)"
```

---

## Task 3: `read_entity_subgraph` + `read_citations` (Cypher)

**Files:**
- Modify: `src/graph/wiki_context.py`
- Test: `tests/test_graph/test_wiki_context.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_graph/test_wiki_context.py`:

```python
from unittest.mock import MagicMock
from src.graph.wiki_context import read_entity_subgraph, read_citations


def test_read_entity_subgraph_builds_context():
    store = MagicMock()
    store.structured_query.return_value = [{
        "name": "ООО Альфа", "label": "Organization",
        "description": "A supplier.", "qid": "Q5", "page_title": "ООО Альфа",
        "relations": [
            {"rl": "заключила договор", "dir": "out",
             "nn": "Договор № 17-К", "nl": "Document", "rd": "signed"},
        ],
    }]
    ctx = read_entity_subgraph(store, "ООО Альфа")
    assert ctx.name == "ООО Альфа" and ctx.wikibase_qid == "Q5"
    assert ctx.relations == [
        ("заключила договор", "out", "Договор № 17-К", "Document", "signed")]
    # page_title falls back to name when the stored prop is empty
    store.structured_query.return_value[0]["page_title"] = ""
    assert read_entity_subgraph(store, "ООО Альфа").page_title == "ООО Альфа"


def test_read_citations_returns_text_docid_pairs():
    store = MagicMock()
    store.structured_query.return_value = [
        {"text": "ООО Альфа заключила…", "doc_id": "d1"},
        {"text": "…контакт +7495…", "doc_id": "d2"},
    ]
    cites = read_citations(store, "ООО Альфа", k=5)
    assert cites == [("ООО Альфа заключила…", "d1"), ("…контакт +7495…", "d2")]
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_graph/test_wiki_context.py -k "read_" -v`
Expected: FAIL — `cannot import name 'read_entity_subgraph'`.

- [ ] **Step 3: Implement**

Append to `src/graph/wiki_context.py`:

```python
_SUBGRAPH_CYPHER = """
MATCH (e:__Entity__ {name: $name})
OPTIONAL MATCH (e)-[r]-(m:__Entity__)
WITH e,
  collect(CASE WHEN m IS NULL THEN NULL ELSE {
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
  [x IN rels WHERE x IS NOT NULL] AS relations
"""

_CITATIONS_CYPHER = """
MATCH (c:Chunk)-[:MENTIONS]->(e:__Entity__ {name: $name})
RETURN coalesce(c.text, '') AS text, coalesce(c.doc_id, '') AS doc_id
LIMIT $k
"""


def read_entity_subgraph(store, name: str) -> EntityContext:
    rows = store.structured_query(_SUBGRAPH_CYPHER, param_map={"name": name})
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


def read_citations(store, name: str, k: int) -> list[tuple[str, str]]:
    rows = store.structured_query(
        _CITATIONS_CYPHER, param_map={"name": name, "k": k})
    return [(row.get("text") or "", row.get("doc_id") or "") for row in rows]
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_graph/test_wiki_context.py -v`
Expected: PASS (all 6).

- [ ] **Step 5: Commit**

```bash
git add src/graph/wiki_context.py tests/test_graph/test_wiki_context.py
git commit -m "feat(graph): read_entity_subgraph + read_citations from Neo4j"
```

---

## Task 4: `splice_bot_section` (pure, anti-drift boundary)

**Files:**
- Create: `src/workflow/wiki/__init__.py` (empty), `src/workflow/wiki/article.py`
- Test: `tests/test_workflow/test_wiki/__init__.py` (empty), `tests/test_workflow/test_wiki/test_article.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_workflow/test_wiki/test_article.py`:

```python
from src.workflow.wiki.article import splice_bot_section, BOT_START, BOT_END


def test_insert_into_pageless_creates_marked_section():
    out = splice_bot_section("", "Hello bot.")
    assert BOT_START in out and BOT_END in out and "Hello bot." in out


def test_preserves_human_content_outside_markers():
    page = "Human intro.\n" + BOT_START + "\nold bot\n" + BOT_END + "\nHuman outro."
    out = splice_bot_section(page, "new bot")
    assert "Human intro." in out and "Human outro." in out
    assert "new bot" in out and "old bot" not in out


def test_idempotent_twice_equals_once():
    once = splice_bot_section("Human.\n", "B")
    twice = splice_bot_section(once, "B")
    assert once == twice


def test_human_only_page_gets_bot_section_prepended_without_loss():
    out = splice_bot_section("Just human text.", "B")
    assert "Just human text." in out and "B" in out and BOT_START in out
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_workflow/test_wiki/test_article.py -k splice -v`
Expected: FAIL — `No module named 'src.workflow.wiki.article'`.

- [ ] **Step 3: Implement**

Create `src/workflow/wiki/__init__.py` (empty) and `src/workflow/wiki/article.py`:

```python
"""Bot-section splice + LLM render for entity wiki articles.

The bot owns ONLY the text between BOT_START/BOT_END markers; everything
outside is human-owned and preserved verbatim. The article is rewritten
from the graph each time (no prior prose fed to the LLM) — see the spec's
anti-drift rationale."""
from __future__ import annotations

import re

BOT_START = "<!-- KB-BOT:START -->"
BOT_END = "<!-- KB-BOT:END -->"

_SECTION_RE = re.compile(
    re.escape(BOT_START) + r".*?" + re.escape(BOT_END), re.DOTALL)


def splice_bot_section(existing_wikitext: str, bot_md: str) -> str:
    """Replace the marked bot section with `bot_md` (wrapped in markers).
    If no markers exist, prepend the bot section, keeping human text below."""
    block = f"{BOT_START}\n{bot_md}\n{BOT_END}"
    if BOT_START in existing_wikitext and BOT_END in existing_wikitext:
        return _SECTION_RE.sub(lambda _m: block, existing_wikitext, count=1)
    if not existing_wikitext.strip():
        return block + "\n"
    return block + "\n\n" + existing_wikitext
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_workflow/test_wiki/test_article.py -k splice -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/workflow/wiki/__init__.py src/workflow/wiki/article.py tests/test_workflow/test_wiki/
git commit -m "feat(wiki): splice_bot_section — bot/human section boundary"
```

---

## Task 5: `render_bot_section` (grounded prompt, mock LLM)

**Files:**
- Modify: `src/workflow/wiki/article.py`
- Test: `tests/test_workflow/test_wiki/test_article.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_workflow/test_wiki/test_article.py`:

```python
import pytest
from unittest.mock import AsyncMock
from src.graph.wiki_context import EntityContext
from src.workflow.wiki.article import render_bot_section


def _ctx():
    return EntityContext(
        name="ООО Альфа", label="Organization", description="A supplier.",
        wikibase_qid="Q5", page_title="ООО Альфа",
        relations=[("заключила договор", "out", "Договор № 17-К", "Document", "signed")],
    )


@pytest.mark.asyncio
async def test_render_grounds_on_facts_and_citations_not_prior_prose():
    captured = {}

    async def fake_acomplete(prompt, *a, **kw):
        captured["prompt"] = str(prompt)
        return "== ООО Альфа ==\nFacts [d1].\n[[Договор № 17-К]]"

    llm = AsyncMock()
    llm.acomplete = fake_acomplete
    cites = [("ООО Альфа заключила договор…", "d1")]
    out = await render_bot_section(_ctx(), cites, llm=llm)

    p = captured["prompt"]
    # Facts + citation snippet present in the prompt:
    assert "Договор № 17-К" in p and "d1" in p and "ООО Альфа заключила" in p
    # The neighbor wiki-link + citation appear in the rendered output:
    assert "[[Договор № 17-К]]" in out and "[d1]" in out
    # Anti-drift: there is no "prior article" channel — the function takes
    # no existing-prose argument (enforced by signature).
    import inspect
    params = set(inspect.signature(render_bot_section).parameters)
    assert "existing" not in params and "prior" not in params
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_workflow/test_wiki/test_article.py -k render -v`
Expected: FAIL — `cannot import name 'render_bot_section'`.

- [ ] **Step 3: Implement**

Append to `src/workflow/wiki/article.py`:

```python
_PROMPT = """\
/no_think
Write a factual encyclopedia section in MediaWiki markup about the entity \
"{name}" ({label}). Use ONLY the facts and source snippets below. Cite every \
statement inline as [doc_id]. Do NOT invent anything. Keep entity names in \
their original language. Link related entities with [[wiki links]].

Entity description: {description}

Facts (relations):
{relations}

Source snippets (for citation):
{citations}

Output ONLY the article section body (no page title heading).
"""


def _fmt_relations(ctx) -> str:
    if not ctx.relations:
        return "(none)"
    lines = []
    for rl, d, nn, nl, rd in ctx.relations:
        arrow = "→" if d == "out" else "←"
        extra = f" — {rd}" if rd else ""
        lines.append(f"- {arrow} {rl}: [[{nn}]] ({nl}){extra}")
    return "\n".join(lines)


def _fmt_citations(cites) -> str:
    if not cites:
        return "(none)"
    return "\n".join(f"[{doc_id}] {text[:300]}" for text, doc_id in cites)


async def render_bot_section(ctx, citations, llm) -> str:
    """LLM-render the bot section grounded ONLY in `ctx` (graph facts) and
    `citations`. No prior article prose is passed — this is the anti-drift
    guarantee (see spec §5)."""
    prompt = _PROMPT.format(
        name=ctx.name, label=ctx.label or "entity",
        description=ctx.description or "(none)",
        relations=_fmt_relations(ctx), citations=_fmt_citations(citations),
    )
    resp = await llm.acomplete(prompt)
    return str(resp).strip()
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_workflow/test_wiki/test_article.py -v`
Expected: PASS (all).

- [ ] **Step 5: Commit**

```bash
git add src/workflow/wiki/article.py tests/test_workflow/test_wiki/test_article.py
git commit -m "feat(wiki): render_bot_section — graph-grounded, cited, drift-free"
```

---

## Task 6: `wiki_dirty` Cypher helpers

**Files:**
- Create: `src/graph/wiki_dirty.py`
- Test: `tests/test_graph/test_wiki_dirty.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_graph/test_wiki_dirty.py`:

```python
from unittest.mock import MagicMock
from src.graph.wiki_dirty import mark_dirty, select_dirty, clear_dirty


def test_mark_dirty_runs_cypher_with_names():
    store = MagicMock()
    mark_dirty(store, ["A", "B"])
    args, kwargs = store.structured_query.call_args
    assert kwargs["param_map"]["names"] == ["A", "B"]
    assert "wiki_dirty = true" in args[0]


def test_mark_dirty_noop_on_empty():
    store = MagicMock()
    mark_dirty(store, [])
    store.structured_query.assert_not_called()


def test_select_dirty_returns_names():
    store = MagicMock()
    store.structured_query.return_value = [{"name": "A"}, {"name": "B"}]
    assert select_dirty(store, limit=10) == ["A", "B"]
    args, kwargs = store.structured_query.call_args
    assert kwargs["param_map"]["limit"] == 10


def test_clear_dirty_sets_hash_and_flags():
    store = MagicMock()
    clear_dirty(store, "A", "deadbeef")
    args, kwargs = store.structured_query.call_args
    assert kwargs["param_map"] == {"name": "A", "hash": "deadbeef"}
    assert "wiki_dirty = false" in args[0]
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_graph/test_wiki_dirty.py -v`
Expected: FAIL — `No module named 'src.graph.wiki_dirty'`.

- [ ] **Step 3: Implement**

Create `src/graph/wiki_dirty.py`:

```python
"""Dirty-flag bookkeeping for the wiki editor (Neo4j __Entity__ props)."""
from __future__ import annotations

_MARK = """
UNWIND $names AS n
MATCH (e:__Entity__ {name: n})
SET e.wiki_dirty = true, e.wiki_dirty_at = datetime()
"""

_SELECT = """
MATCH (e:__Entity__) WHERE e.wiki_dirty = true
RETURN e.name AS name ORDER BY e.wiki_dirty_at LIMIT $limit
"""

_CLEAR = """
MATCH (e:__Entity__ {name: $name})
SET e.wiki_dirty = false, e.wiki_hash = $hash, e.wiki_synced_at = datetime()
"""


def mark_dirty(store, names: list[str]) -> None:
    if not names:
        return
    store.structured_query(_MARK, param_map={"names": names})


def select_dirty(store, limit: int) -> list[str]:
    rows = store.structured_query(_SELECT, param_map={"limit": limit})
    return [r["name"] for r in rows]


def clear_dirty(store, name: str, hash: str) -> None:
    store.structured_query(_CLEAR, param_map={"name": name, "hash": hash})
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_graph/test_wiki_dirty.py -v`
Expected: PASS (4).

- [ ] **Step 5: Commit**

```bash
git add src/graph/wiki_dirty.py tests/test_graph/test_wiki_dirty.py
git commit -m "feat(graph): wiki_dirty mark/select/clear Cypher helpers"
```

---

## Task 7: `AsyncMediaWiki` Action API client

**Files:**
- Create: `src/storage/mediawiki.py`
- Test: `tests/test_storage/test_mediawiki.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_storage/test_mediawiki.py`:

```python
import pytest
from unittest.mock import AsyncMock, MagicMock
from src.storage.mediawiki import AsyncMediaWiki


def _client_returning(payloads):
    """httpx-like async client whose .get/.post return queued JSON payloads."""
    it = iter(payloads)

    async def _call(*a, **kw):
        resp = MagicMock()
        resp.json.return_value = next(it)
        resp.raise_for_status = MagicMock()
        return resp
    c = MagicMock()
    c.get = AsyncMock(side_effect=_call)
    c.post = AsyncMock(side_effect=_call)
    return c


@pytest.mark.asyncio
async def test_get_page_returns_wikitext():
    c = _client_returning([
        {"query": {"pages": {"1": {"revisions": [{"slots": {"main": {"content": "WT"}}}]}}}},
    ])
    mw = AsyncMediaWiki(client=c, api_url="http://x/w/api.php")
    assert await mw.get_page("Title") == "WT"


@pytest.mark.asyncio
async def test_get_missing_page_returns_empty():
    c = _client_returning([{"query": {"pages": {"-1": {"missing": ""}}}}])
    mw = AsyncMediaWiki(client=c, api_url="http://x/w/api.php")
    assert await mw.get_page("Nope") == ""


@pytest.mark.asyncio
async def test_upsert_page_fetches_token_then_edits():
    c = _client_returning([
        {"query": {"tokens": {"csrftoken": "T+\\"}}},     # csrf token
        {"edit": {"result": "Success"}},                  # edit
    ])
    mw = AsyncMediaWiki(client=c, api_url="http://x/w/api.php")
    ok = await mw.upsert_page("Title", "NEW", summary="s")
    assert ok is True
    # the edit POST carried the token + text
    _args, kwargs = c.post.call_args
    data = kwargs.get("data") or {}
    assert data.get("token") == "T+\\" and data.get("text") == "NEW"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_storage/test_mediawiki.py -v`
Expected: FAIL — `No module named 'src.storage.mediawiki'`.

- [ ] **Step 3: Implement**

Create `src/storage/mediawiki.py`:

```python
"""Minimal async MediaWiki Action API client (login + read/edit page +
sitelink). Used by the wiki editor to write entity article pages.

Cookie-session auth via httpx.AsyncClient; CSRF token fetched per edit."""
from __future__ import annotations

from typing import Any

import httpx
from loguru import logger


class AsyncMediaWiki:
    def __init__(self, client: httpx.AsyncClient, api_url: str) -> None:
        self._c = client
        self._api = api_url

    @classmethod
    async def login(cls, api_url: str, user: str, password: str) -> "AsyncMediaWiki":
        client = httpx.AsyncClient(timeout=30.0)
        # 1) login token
        r = await client.get(api_url, params={
            "action": "query", "meta": "tokens", "type": "login", "format": "json"})
        r.raise_for_status()
        ltoken = r.json()["query"]["tokens"]["logintoken"]
        # 2) login
        r = await client.post(api_url, data={
            "action": "login", "lgname": user, "lgpassword": password,
            "lgtoken": ltoken, "format": "json"})
        r.raise_for_status()
        if r.json().get("login", {}).get("result") != "Success":
            raise RuntimeError(f"mediawiki login failed: {r.json()}")
        return cls(client=client, api_url=api_url)

    async def _csrf(self) -> str:
        r = await self._c.get(self._api, params={
            "action": "query", "meta": "tokens", "format": "json"})
        r.raise_for_status()
        return r.json()["query"]["tokens"]["csrftoken"]

    async def get_page(self, title: str) -> str:
        r = await self._c.get(self._api, params={
            "action": "query", "prop": "revisions", "titles": title,
            "rvslots": "main", "rvprop": "content", "format": "json"})
        r.raise_for_status()
        pages = r.json().get("query", {}).get("pages", {})
        for _pid, page in pages.items():
            if "missing" in page:
                return ""
            revs = page.get("revisions") or []
            if revs:
                return revs[0]["slots"]["main"]["content"]
        return ""

    async def upsert_page(self, title: str, wikitext: str, summary: str) -> bool:
        token = await self._csrf()
        r = await self._c.post(self._api, data={
            "action": "edit", "title": title, "text": wikitext,
            "summary": summary, "bot": "1", "token": token, "format": "json"})
        r.raise_for_status()
        result = r.json().get("edit", {}).get("result")
        if result != "Success":
            logger.warning("mediawiki edit non-success title={t} resp={r}",
                           t=title, r=r.json())
            return False
        return True

    async def ensure_sitelink(self, qid: str, title: str) -> bool:
        """Link a Wikibase Item to its MediaWiki article page. Best-effort."""
        if not qid:
            return False
        token = await self._csrf()
        r = await self._c.post(self._api, data={
            "action": "wbsetsitelink", "id": qid,
            "linksite": "kbwiki", "linktitle": title,
            "token": token, "format": "json"})
        r.raise_for_status()
        return "error" not in r.json()

    async def aclose(self) -> None:
        await self._c.aclose()
```

Note on `ensure_sitelink`: `linksite` must match the local wiki's site-global-id. `"kbwiki"` is a placeholder for the dev instance; the Task-9 admin/runbook step documents reading the real `wbGlobalId` from the wiki and setting it (or making it a `WIKI_SITE_GLOBAL_ID` config). The Task-8 activity calls `ensure_sitelink` best-effort and swallows its failure, so a wrong site id never breaks article writing.

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_storage/test_mediawiki.py -v`
Expected: PASS (3).

- [ ] **Step 5: Commit**

```bash
git add src/storage/mediawiki.py tests/test_storage/test_mediawiki.py
git commit -m "feat(storage): AsyncMediaWiki Action API client"
```

---

## Task 8: `mark_entities_dirty` activity + ingest hook

**Files:**
- Create: `src/workflow/activities/mark_dirty.py`
- Modify: `src/workflow/contracts.py`, `src/workflow/activities/__init__.py`, `src/workflow/document_ingest.py`
- Test: `tests/test_workflow/test_wiki/test_mark_dirty.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_workflow/test_wiki/test_mark_dirty.py`:

```python
import pytest
from unittest.mock import MagicMock, patch
from src.workflow.contracts import MarkDirtyIn
from src.workflow.activities.mark_dirty import _dirty_names


def test_dirty_names_includes_entities_and_relation_endpoints():
    payload = MarkDirtyIn(
        entity_names=["A", "B"],
        relation_endpoints=["B", "C", "A"],
    )
    assert _dirty_names(payload) == {"A", "B", "C"}
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_workflow/test_wiki/test_mark_dirty.py -v`
Expected: FAIL — `cannot import name 'MarkDirtyIn'`.

- [ ] **Step 3: Implement the contract + activity**

In `src/workflow/contracts.py`, add (near the other `_Frozen` IO models):

```python
class MarkDirtyIn(_Frozen):
    entity_names: list[str] = Field(default_factory=list)
    relation_endpoints: list[str] = Field(default_factory=list)
```

(If `Field` isn't imported in contracts.py, it is — other models use it.)

Create `src/workflow/activities/mark_dirty.py`:

```python
"""mark_entities_dirty — flag an ingest's entities (and relation endpoints)
for wiki re-write. Best-effort: never raises out (caller ignores failures)."""
from __future__ import annotations

from temporalio import activity

from src.config import settings
from src.graph.store import build_neo4j_graph_store
from src.graph.wiki_dirty import mark_dirty
from src.workflow.contracts import MarkDirtyIn


def _dirty_names(payload: MarkDirtyIn) -> set[str]:
    return set(payload.entity_names) | set(payload.relation_endpoints)


@activity.defn
async def mark_entities_dirty(payload: MarkDirtyIn) -> int:
    if not settings.wiki.enabled:
        return 0
    names = sorted(_dirty_names(payload))
    if not names:
        return 0
    store = build_neo4j_graph_store()
    mark_dirty(store, names)
    activity.logger.info("mark_entities_dirty  count=%d", len(names))
    return len(names)
```

In `src/workflow/activities/__init__.py`: import `mark_entities_dirty` and add it to `MAIN_ACTIVITIES`.

- [ ] **Step 4: Hook into the ingest workflow**

In `src/workflow/document_ingest.py`, after the GraphBuildWorkflow child returns successfully (inside the inner try, after `built = gb_result.built`), add a best-effort dirty-mark. Use `merged` (the `Merged` result) for entity names and relation endpoints. Add this block (guard with a workflow-side import of settings already present):

```python
                # Wiki editor (Project A): flag this doc's entities for
                # article (re)write.  Best-effort — must not fail ingest.
                if settings.wiki.enabled:
                    try:
                        ent_names, endpoints = _wiki_dirty_targets(merged)
                        await workflow.execute_activity(
                            "mark_entities_dirty",
                            MarkDirtyIn(entity_names=ent_names,
                                        relation_endpoints=endpoints),
                            start_to_close_timeout=timedelta(minutes=2),
                            schedule_to_close_timeout=timedelta(minutes=30),
                            retry_policy=_FAST_FOREVER,
                        )
                    except Exception as exc:  # noqa: BLE001 — best-effort
                        log.warning("wiki dirty-mark failed: %s", exc)
```

Add `MarkDirtyIn` to the `contracts` import block in `document_ingest.py`, and add a small pure helper at module level (it reads the merged blob's entity/relation names — keep it deterministic and importable for tests):

```python
def _wiki_dirty_targets(merged: Merged) -> tuple[list[str], list[str]]:
    """Entity names + relation endpoints to flag dirty for the wiki editor.
    Sourced from the Merged summary counts is not enough — read the merged
    entity/relation names already surfaced on `merged`.  We use the
    duplicate_groups + alias maps that Merged carries plus the merged
    entity count is insufficient; in practice the merged blob URI holds the
    full lists.  To avoid re-reading staging here (workflow code is
    sandboxed), pass the names the GraphBuildWorkflow returns."""
    # GraphBuildWorkflow returns names on GraphBuilt (see Task 8b note).
    names = list(getattr(merged, "entity_names", []) or [])
    endpoints = list(getattr(merged, "relation_endpoints", []) or [])
    return names, endpoints
```

> **Task 8b (same commit) — surface names on the contract.** Reading staging from workflow code is unsafe (sandbox). So have `build_property_graph` / `GraphBuildWorkflow` include the written entity names + relation endpoints on the `GraphBuilt` (or `Merged`) contract it already returns. Concretely: add `entity_names: list[str]` and `relation_endpoints: list[str]` (default empty) to the `Merged` model in `contracts.py`, and populate them in `merge_and_resolve` (it already holds `merged_entities` / `merged_relations` in memory — set `entity_names=[e.name for e in merged_entities]` and `relation_endpoints=[r.source_id for r in merged_relations] + [r.target_id for r in merged_relations]`, deduped). Then `_wiki_dirty_targets(merged)` reads them directly. Add a unit test in `tests/test_workflow/test_merge_and_resolve.py` asserting the two lists are populated.

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_workflow/test_wiki/test_mark_dirty.py tests/test_workflow/test_merge_and_resolve.py tests/test_workflow/test_document_ingest_workflow.py -v`
Expected: PASS. (The ingest workflow tests must still pass — the hook is guarded by `settings.wiki.enabled` which is False in tests.)

- [ ] **Step 6: Commit**

```bash
git add src/workflow/contracts.py src/workflow/activities/mark_dirty.py src/workflow/activities/__init__.py src/workflow/document_ingest.py src/workflow/activities/merge_and_resolve.py tests/test_workflow/
git commit -m "feat(wiki): mark_entities_dirty activity + ingest hook + names on Merged"
```

---

## Task 9: `WikiSweepWorkflow` + `write_entity_article` + `select_dirty_entities`

**Files:**
- Create: `src/workflow/wiki/wiki_sweep.py`
- Modify: `src/workflow/contracts.py` (sweep result), `src/workflow/wiki/_deps.py` (lazy MediaWiki singleton)
- Test: `tests/test_workflow/test_wiki/test_wiki_sweep.py`

- [ ] **Step 1: Write the failing test (pure sweep-result aggregation)**

Create `tests/test_workflow/test_wiki/test_wiki_sweep.py`:

```python
from src.workflow.wiki.wiki_sweep import _tally, ArticleOutcome


def test_tally_counts_outcomes():
    res = _tally([
        ArticleOutcome.WRITTEN, ArticleOutcome.SKIPPED,
        ArticleOutcome.WRITTEN, ArticleOutcome.FAILED, None,
    ])
    assert res == {"written": 2, "skipped_unchanged": 1, "failed": 1}
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_workflow/test_wiki/test_wiki_sweep.py -v`
Expected: FAIL — `No module named 'src.workflow.wiki.wiki_sweep'`.

- [ ] **Step 3: Implement deps + activities + workflow**

Create `src/workflow/wiki/_deps.py`:

```python
"""Process-singleton MediaWiki client for wiki activities."""
from __future__ import annotations

import asyncio

from src.config import settings
from src.storage.mediawiki import AsyncMediaWiki

_lock = asyncio.Lock()
_mw: AsyncMediaWiki | None = None


def _api_url() -> str:
    cfg = settings.wiki
    if cfg.mediawiki_api_url:
        return cfg.mediawiki_api_url
    return settings.wikibase.base_url.rstrip("/") + "/w/api.php"


async def get_mediawiki() -> AsyncMediaWiki:
    global _mw
    async with _lock:
        if _mw is None:
            _mw = await AsyncMediaWiki.login(
                _api_url(), settings.wikibase.bot_user,
                settings.wikibase.bot_password.get_secret_value())
    return _mw


def reset_for_tests() -> None:
    global _mw
    _mw = None
```

Create `src/workflow/wiki/wiki_sweep.py`:

```python
"""WikiSweepWorkflow — select dirty entities, (re)write each article."""
from __future__ import annotations

import enum
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from src.config import settings


class ArticleOutcome(str, enum.Enum):
    WRITTEN = "written"
    SKIPPED = "skipped"
    FAILED = "failed"


def _tally(outcomes) -> dict[str, int]:
    res = {"written": 0, "skipped_unchanged": 0, "failed": 0}
    for o in outcomes:
        if o == ArticleOutcome.WRITTEN:
            res["written"] += 1
        elif o == ArticleOutcome.SKIPPED:
            res["skipped_unchanged"] += 1
        elif o == ArticleOutcome.FAILED:
            res["failed"] += 1
    return res


@activity.defn
async def select_dirty_entities(limit: int) -> list[str]:
    from src.graph.store import build_neo4j_graph_store
    from src.graph.wiki_dirty import select_dirty
    return select_dirty(build_neo4j_graph_store(), limit)


@activity.defn
async def write_entity_article(name: str) -> str:
    from src.graph.store import build_neo4j_graph_store
    from src.graph.wiki_context import (
        read_entity_subgraph, read_citations, subgraph_hash)
    from src.graph.wiki_dirty import clear_dirty
    from src.retrieval.llm_pool import get_llm_pool
    from src.workflow.wiki._deps import get_mediawiki
    from src.workflow.wiki.article import render_bot_section, splice_bot_section

    store = build_neo4j_graph_store()
    ctx = read_entity_subgraph(store, name)
    h = subgraph_hash(ctx)
    # change-detection: skip if the subgraph is unchanged since last write.
    cur_hash_rows = store.structured_query(
        "MATCH (e:__Entity__ {name:$n}) RETURN coalesce(e.wiki_hash,'') AS h",
        param_map={"n": name})
    if cur_hash_rows and cur_hash_rows[0]["h"] == h:
        clear_dirty(store, name, h)
        return ArticleOutcome.SKIPPED.value

    cites = read_citations(store, name, settings.wiki.citations_top_k)
    llm = get_llm_pool().get("synthesis")
    bot_md = await render_bot_section(ctx, cites, llm=llm)

    mw = await get_mediawiki()
    title = ctx.page_title
    current = await mw.get_page(title)
    new = splice_bot_section(current, bot_md)
    if new != current:
        await mw.upsert_page(title, new, summary="KB bot: updated from graph")
    await mw.ensure_sitelink(ctx.wikibase_qid, title)
    # persist page title + hash + clear dirty
    store.structured_query(
        "MATCH (e:__Entity__ {name:$n}) SET e.wiki_page_title=$t",
        param_map={"n": name, "t": title})
    clear_dirty(store, name, h)
    return ArticleOutcome.WRITTEN.value


_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2), backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2), maximum_attempts=5)


@workflow.defn
class WikiSweepWorkflow:
    @workflow.run
    async def run(self) -> dict[str, int]:
        log = workflow.logger
        names = await workflow.execute_activity(
            select_dirty_entities, settings.wiki.sweep_batch,
            start_to_close_timeout=timedelta(minutes=2),
            schedule_to_close_timeout=timedelta(minutes=10), retry_policy=_RETRY)
        log.info("wiki sweep  dirty=%d", len(names))
        outcomes: list[str] = []
        for name in names:
            try:
                outcomes.append(await workflow.execute_activity(
                    write_entity_article, name,
                    start_to_close_timeout=timedelta(minutes=10),
                    heartbeat_timeout=timedelta(minutes=5),
                    schedule_to_close_timeout=timedelta(hours=2),
                    retry_policy=_RETRY))
            except Exception as exc:  # noqa: BLE001 — best-effort per entity
                log.warning("write_entity_article failed name=%s: %s", name, exc)
                outcomes.append(ArticleOutcome.FAILED.value)
        result = _tally([ArticleOutcome(o) if o else None for o in outcomes])
        log.info("wiki sweep done  %s", result)
        return result
```

> The `for`-loop awaits sequentially for determinism + bounded fan-out; the synthesis lane + a low `kb-wiki` cap already bound parallelism, and a sweep batch is small. (Parallel `asyncio.gather` over activities is a possible later optimization — not now.)

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_workflow/test_wiki/test_wiki_sweep.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/workflow/wiki/wiki_sweep.py src/workflow/wiki/_deps.py tests/test_workflow/test_wiki/test_wiki_sweep.py
git commit -m "feat(wiki): WikiSweepWorkflow + write_entity_article/select_dirty activities"
```

---

## Task 10: Wire-up — worker pool, admin route, schedule, env

**Files:**
- Modify: `src/workflow/worker.py`, `src/api/routes/admin.py` (create), the FastAPI app factory, `.env.example`
- Create: `scripts/setup_wiki_schedule.py`
- Test: `tests/test_api/test_admin_wiki.py`

- [ ] **Step 1: Register the wiki worker pool**

In `src/workflow/worker.py`, import the wiki workflow + activities and add a worker pool (mirror the `graph_build_worker` block):

```python
from src.workflow.wiki.wiki_sweep import (
    WikiSweepWorkflow, select_dirty_entities, write_entity_article,
)
...
    wiki_worker = Worker(
        client,
        task_queue=settings.wiki.task_queue,
        workflows=[WikiSweepWorkflow],
        activities=[select_dirty_entities, write_entity_article],
        max_concurrent_activities=settings.wiki.activity_concurrency,
    )
    logger.info("temporal worker  wiki_queue={wq}  wiki_concurrency={wc}",
                wq=settings.wiki.task_queue, wc=settings.wiki.activity_concurrency)
```

Add `wiki_worker.run()` to the final `asyncio.gather(...)`.

- [ ] **Step 2: Admin route**

Create `src/api/routes/admin.py`:

```python
"""Admin operations: trigger a wiki sweep (and optional full rebuild)."""
from __future__ import annotations

from fastapi import APIRouter

from src.config import settings
from src.workflow.client import get_temporal_client
from src.workflow.wiki.wiki_sweep import WikiSweepWorkflow

router = APIRouter(prefix="/admin/wiki", tags=["admin"])


@router.post("/rebuild")
async def wiki_rebuild(all: bool = False) -> dict:
    if not settings.wiki.enabled:
        return {"status": "disabled"}
    if all:
        from src.graph.store import build_neo4j_graph_store
        build_neo4j_graph_store().structured_query(
            "MATCH (e:__Entity__) SET e.wiki_dirty = true, "
            "e.wiki_dirty_at = datetime()")
    client = await get_temporal_client()
    handle = await client.start_workflow(
        WikiSweepWorkflow.run, id="wiki-sweep-manual",
        task_queue=settings.wiki.task_queue,
        id_reuse_policy=__import__("temporalio.common", fromlist=["WorkflowIDReusePolicy"]).WorkflowIDReusePolicy.ALLOW_DUPLICATE)
    return {"status": "started", "workflow_id": handle.id}
```

Register the router in the FastAPI app factory (find where other routers like `ingest`/`search_v2` are `app.include_router(...)`) by adding `from src.api.routes import admin` and `app.include_router(admin.router)`.

- [ ] **Step 3: Test the admin route (disabled-path, no infra)**

Create `tests/test_api/test_admin_wiki.py`:

```python
import pytest
from fastapi.testclient import TestClient


def test_wiki_rebuild_returns_disabled_when_off(monkeypatch):
    from src.config import settings
    # WIKI_ENABLED defaults False -> route returns disabled without infra.
    from src.api.app import build_app  # adjust import to the real app factory
    client = TestClient(build_app())
    r = client.post("/admin/wiki/rebuild")
    assert r.status_code == 200 and r.json()["status"] == "disabled"
```

> If the app-factory import path differs, fix it to the real one (grep `FastAPI(` / `include_router`). The point: with `WIKI_ENABLED=false` the endpoint returns `disabled` and touches no Temporal/Neo4j.

- [ ] **Step 4: Schedule script**

Create `scripts/setup_wiki_schedule.py`:

```python
"""Idempotently create/update the Temporal Schedule that runs
WikiSweepWorkflow every WIKI_SWEEP_INTERVAL_MINUTES. No-op if disabled.

Run: uv run python -m scripts.setup_wiki_schedule
"""
from __future__ import annotations

import asyncio
from datetime import timedelta

from loguru import logger
from temporalio.client import (
    Schedule, ScheduleActionStartWorkflow, ScheduleSpec,
    ScheduleIntervalSpec,
)

from src.config import settings
from src.workflow.client import get_temporal_client
from src.workflow.wiki.wiki_sweep import WikiSweepWorkflow

_SCHEDULE_ID = "wiki-sweep"


async def _main() -> None:
    if not settings.wiki.enabled:
        logger.info("WIKI_ENABLED=false — skipping wiki schedule")
        return
    client = await get_temporal_client()
    interval = timedelta(minutes=settings.wiki.sweep_interval_minutes)
    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            WikiSweepWorkflow.run, id="wiki-sweep-scheduled",
            task_queue=settings.wiki.task_queue),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=interval)]),
    )
    try:
        await client.create_schedule(_SCHEDULE_ID, schedule)
        logger.info("created wiki schedule every {m}m",
                    m=settings.wiki.sweep_interval_minutes)
    except Exception:  # already exists -> update
        handle = client.get_schedule_handle(_SCHEDULE_ID)
        await handle.update(lambda _i: schedule)  # type: ignore[arg-type]
        logger.info("updated existing wiki schedule")


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
```

> Verify the exact Temporal Python Schedule import paths against the installed SDK version: `uv run python -c "from temporalio.client import Schedule, ScheduleSpec, ScheduleIntervalSpec, ScheduleActionStartWorkflow; print('ok')"`. If a symbol differs, adjust to the SDK's actual API (the schedule shape is otherwise the same). If the `update` lambda signature differs, use the SDK's documented `handle.update` form.

- [ ] **Step 5: `.env.example`**

Add a block after the Wikibase section:

```
# ── Continuous wiki editor (Project A; opt-in) ──────────────────────
# Generates per-entity MediaWiki article pages from the Neo4j graph.
WIKI_ENABLED=false
WIKI_TASK_QUEUE=kb-wiki
WIKI_ACTIVITY_CONCURRENCY=4
WIKI_SWEEP_BATCH=50
WIKI_SWEEP_INTERVAL_MINUTES=15
WIKI_CITATIONS_TOP_K=5
# Empty -> derived from WIKIBASE_BASE_URL + /w/api.php
WIKI_MEDIAWIKI_API_URL=
```

- [ ] **Step 6: Run the relevant suites**

Run: `uv run pytest tests/test_api/test_admin_wiki.py tests/test_config tests/test_workflow -q`
Expected: PASS. Then sanity-import the worker module: `uv run python -c "import src.workflow.worker; print('worker imports ok')"`.

- [ ] **Step 7: Commit**

```bash
git add src/workflow/worker.py src/api/routes/admin.py scripts/setup_wiki_schedule.py .env.example tests/test_api/test_admin_wiki.py
git commit -m "feat(wiki): register kb-wiki worker, admin rebuild route, schedule, env"
```

---

## Self-Review notes (for the implementer)

- **Spec coverage:** §3 components → Tasks 2-10; §4 dirty/hash → Tasks 2,6,8; §5 generation/splice → Tasks 4,5; §6 queue/schedule/config/admin → Tasks 1,10; §7 idempotency/tests → woven throughout (hash-skip in Task 9, splice idempotency in Task 4, best-effort in Tasks 8,9).
- **Type consistency:** `EntityContext` fields, `subgraph_hash`, `read_entity_subgraph`, `read_citations`, `mark_dirty/select_dirty/clear_dirty`, `splice_bot_section`, `render_bot_section`, `AsyncMediaWiki.{login,get_page,upsert_page,ensure_sitelink}`, `MarkDirtyIn`, `WikiSweepWorkflow`, `ArticleOutcome`, `_tally` — used consistently across tasks.
- **Key risk — Task 8b (names on `Merged`)**: workflow sandbox can't read staging blobs, so entity/relation names MUST be surfaced on the contract by `merge_and_resolve`. Do Task 8b in the SAME commit as Task 8; the ingest hook depends on it. Verify `merge_and_resolve` has `merged_entities`/`merged_relations` with `.name`/`.source_id`/`.target_id` (it does — see `src/workflow/activities/merge_and_resolve.py`).
- **Verify-before-asserting:** Temporal Schedule API (Task 10.4) and the FastAPI app-factory import (Task 10.3) are the two spots whose exact symbols depend on installed versions — the steps include the one-line checks to confirm before relying on them.
```
