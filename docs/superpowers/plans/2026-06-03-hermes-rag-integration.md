# Hermes ↔ RAG Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `kb-llamaindex` usable interactively from Hermes Agent by securing the SSE MCP transport, authoring a `knowledge-base` skill that encodes tool-selection / response templates / memory / multi-turn conventions, and shipping the operator runbook + golden eval scenarios.

**Architecture:** Purely additive. Run the existing MCP-2 (atomic tools, port 9002) and MCP-1 (`kb_search`, port 9001) servers as long-lived SSE services; Hermes connects via `url` + `Authorization: Bearer`. The four "pillars" (multi-turn, memory, templates, problem-orientation) are carried by Hermes natively + one `SKILL.md`. The only code change is enforcing Bearer auth on the SSE transport, which is currently unprotected.

**Tech Stack:** Python 3.12 (uv), FastMCP 3.3.1 (`StaticTokenVerifier`), pytest + pytest-asyncio, PyYAML 6.0.3, agentskills.io `SKILL.md` format.

**Spec:** `docs/superpowers/specs/2026-06-03-hermes-rag-integration-design.md`

---

## Background facts (verified against the code)

- Both MCP servers create a module-level `mcp = FastMCP(name=..., instructions=...)` — `src/mcp/tools_server.py:40` and `src/mcp/search_server.py:38`. Decorators `@mcp.tool()` bind to that instance.
- `src/mcp/_shared.py` has `is_valid_key()` and `assert_api_key_env_set()`, but **nothing enforces auth on incoming SSE requests** — `is_valid_key` is referenced only by a test. The SSE service is currently open.
- `KB_MCP_REQUIRE_AUTH` env (default `"true"`) gates auth; `settings.api.keys_list` holds the configured API keys.
- FastMCP 3.x native auth: `from fastmcp.server.auth import StaticTokenVerifier` → `FastMCP(name=..., auth=StaticTokenVerifier(tokens={token: {...}}))`. It validates the incoming `Authorization: Bearer <token>` header against the token dict keys. Auth is enforced only on HTTP/SSE transports; stdio is unaffected.
- Existing test pattern: `await tools_server.mcp._list_tools()` returns objects with `.name` (`tests/test_mcp/test_tools_server.py`).

## File Structure

- `src/mcp/_shared.py` — **modify**: add `build_sse_auth()` factory.
- `src/mcp/tools_server.py` — **modify**: pass `auth=build_sse_auth()` to `FastMCP(...)`.
- `src/mcp/search_server.py` — **modify**: pass `auth=build_sse_auth()` to `FastMCP(...)`.
- `tests/test_mcp/test_auth.py` — **create**: unit tests for `build_sse_auth()`.
- `integrations/hermes/knowledge-base/SKILL.md` — **create**: the skill (4 pillars).
- `tests/test_mcp/test_hermes_skill.py` — **create**: frontmatter + tool-coverage test.
- `integrations/hermes/config.example.yaml` — **create**: example `~/.hermes/config.yaml`.
- `docs/runbook/hermes.md` — **create**: operator runbook.
- `docs/runbook/mcp.md` — **modify**: cross-link to the Hermes runbook.
- `tests/test_mcp/test_hermes_config_example.py` — **create**: validate the example config.
- `tests/eval/hermes_scenarios.py` — **create**: golden interactive scenario set + coverage test.

---

## Task 1: Enforce Bearer auth on the SSE transport

**Files:**
- Modify: `src/mcp/_shared.py`
- Modify: `src/mcp/tools_server.py:34-48`
- Modify: `src/mcp/search_server.py:30-46`
- Test: `tests/test_mcp/test_auth.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_mcp/test_auth.py`:

```python
"""Unit tests for the SSE auth provider factory."""

from __future__ import annotations

from types import SimpleNamespace

from src.mcp import _shared


def test_build_sse_auth_returns_none_when_disabled(monkeypatch):
    monkeypatch.setenv("KB_MCP_REQUIRE_AUTH", "false")
    monkeypatch.setattr(
        _shared, "settings",
        SimpleNamespace(api=SimpleNamespace(keys_list=["secret-key"])),
    )
    assert _shared.build_sse_auth() is None


def test_build_sse_auth_returns_none_when_no_keys(monkeypatch):
    monkeypatch.setenv("KB_MCP_REQUIRE_AUTH", "true")
    monkeypatch.setattr(
        _shared, "settings",
        SimpleNamespace(api=SimpleNamespace(keys_list=[])),
    )
    assert _shared.build_sse_auth() is None


def test_build_sse_auth_builds_verifier_with_keys(monkeypatch):
    from fastmcp.server.auth import StaticTokenVerifier

    monkeypatch.setenv("KB_MCP_REQUIRE_AUTH", "true")
    monkeypatch.setattr(
        _shared, "settings",
        SimpleNamespace(api=SimpleNamespace(keys_list=["secret-key", "k2"])),
    )
    auth = _shared.build_sse_auth()
    assert isinstance(auth, StaticTokenVerifier)
    assert "secret-key" in auth.tokens
    assert "k2" in auth.tokens
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_mcp/test_auth.py -v`
Expected: FAIL — `AttributeError: module 'src.mcp._shared' has no attribute 'build_sse_auth'`.

- [ ] **Step 3: Add `build_sse_auth()` to `src/mcp/_shared.py`**

Append this function after `is_valid_key` (keep `is_valid_key` — it is still referenced by `tests/test_mcp/test_search_server.py`):

```python
def build_sse_auth() -> Any:
    """Build a FastMCP auth provider for HTTP/SSE transports.

    Returns a ``StaticTokenVerifier`` seeded with the configured API
    keys when ``KB_MCP_REQUIRE_AUTH`` is on and keys exist; otherwise
    ``None`` (auth disabled — stdio/desktop usage).  The verifier
    validates the incoming ``Authorization: Bearer <key>`` header
    against the configured key set.  Enforced only on HTTP/SSE
    transports; stdio is unaffected.
    """
    require = os.environ.get(
        "KB_MCP_REQUIRE_AUTH", "true",
    ).lower() not in {"0", "false", "no"}
    if not require:
        return None
    keys = settings.api.keys_list
    if not keys:
        return None
    from fastmcp.server.auth import StaticTokenVerifier
    return StaticTokenVerifier(
        tokens={
            k: {"sub": "kb-mcp-client", "client_id": "kb"}
            for k in keys
        },
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_mcp/test_auth.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Wire the auth provider into MCP-2 (`tools_server.py`)**

In `src/mcp/tools_server.py`, add `build_sse_auth` to the `_shared` import (line ~34-36):

```python
from src.mcp._shared import (
    assert_api_key_env_set, build_sse_auth, log_banner, parse_args,
)
```

Then pass it to the constructor (line ~40-48):

```python
mcp = FastMCP(
    name="kb-llamaindex-tools",
    instructions=(
        "Atomic retrieval tools over the project knowledge base.  "
        "Each tool returns a JSON-serialisable dict.  Compose them "
        "yourself in your own LLM loop.  For an already-orchestrated "
        "answer, use the sibling MCP-1 server (kb_search) instead."
    ),
    auth=build_sse_auth(),
)
```

- [ ] **Step 6: Wire the auth provider into MCP-1 (`search_server.py`)**

In `src/mcp/search_server.py`, add `build_sse_auth` to the `_shared` import (line ~30-32):

```python
from src.mcp._shared import (
    assert_api_key_env_set, build_sse_auth, log_banner, parse_args,
)
```

Then pass it to the constructor (line ~38-46):

```python
mcp = FastMCP(
    name="kb-llamaindex-search",
    instructions=(
        "High-level search over the project knowledge base.  The "
        "underlying plan-execute-synthesize flow decomposes the "
        "question, retrieves per sub-question in parallel over vector "
        "+ graph, then synthesises a Russian answer with citations."
    ),
    auth=build_sse_auth(),
)
```

- [ ] **Step 7: Run the full MCP test suite to confirm no regression**

Run: `uv run pytest tests/test_mcp/ -v`
Expected: PASS — existing server tests (`test_tools_server.py`, `test_search_server.py`) stay green; `test_auth.py` passes. (Module import builds `mcp` with `auth=None` under the test env, so tool listing is unchanged.)

- [ ] **Step 8: Commit**

```bash
git add src/mcp/_shared.py src/mcp/tools_server.py src/mcp/search_server.py tests/test_mcp/test_auth.py
git commit -m "feat(mcp): enforce Bearer auth on SSE transport via StaticTokenVerifier

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Author the `knowledge-base` SKILL.md

**Files:**
- Create: `integrations/hermes/knowledge-base/SKILL.md`
- Test: `tests/test_mcp/test_hermes_skill.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_mcp/test_hermes_skill.py`:

```python
"""Validates the Hermes knowledge-base skill: frontmatter is present
and the body references every MCP tool the agent can call (so the
decision tree stays exhaustive as tools change)."""

from __future__ import annotations

from pathlib import Path

import yaml

_SKILL = Path("integrations/hermes/knowledge-base/SKILL.md")

# The 6 atomic tools + the kb_search escape hatch.
_REQUIRED_TOOL_NAMES = {
    "vector_search",
    "graph_search",
    "find_entity_by_id",
    "find_neighbours",
    "get_chunks_by_doc_id",
    "read_full_document",
    "kb_search",
}


def _split_frontmatter(text: str) -> tuple[dict, str]:
    assert text.startswith("---\n"), "SKILL.md must start with YAML frontmatter"
    _, fm, body = text.split("---\n", 2)
    return yaml.safe_load(fm), body


def test_skill_file_exists():
    assert _SKILL.is_file(), f"missing {_SKILL}"


def test_skill_frontmatter_has_name_and_description():
    fm, _ = _split_frontmatter(_SKILL.read_text(encoding="utf-8"))
    assert fm["name"] == "knowledge-base"
    assert isinstance(fm["description"], str) and len(fm["description"]) > 20


def test_skill_body_references_every_tool():
    _, body = _split_frontmatter(_SKILL.read_text(encoding="utf-8"))
    missing = {name for name in _REQUIRED_TOOL_NAMES if name not in body}
    assert not missing, f"skill body omits tools: {missing}"


def test_skill_covers_the_four_pillars():
    _, body = _split_frontmatter(_SKILL.read_text(encoding="utf-8"))
    lowered = body.lower()
    for marker in ("tool selection", "response template", "memory", "follow-up"):
        assert marker in lowered, f"skill body missing section marker: {marker!r}"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_mcp/test_hermes_skill.py -v`
Expected: FAIL — `test_skill_file_exists` fails (file missing); others error on read.

- [ ] **Step 3: Create the skill file**

Create `integrations/hermes/knowledge-base/SKILL.md` (the markers `Tool selection`, `Response templates`, `Memory`, `Follow-up` must appear as written so the test passes):

````markdown
---
name: knowledge-base
description: >
  Use when the user asks about company/domain knowledge, internal documents, or
  entities (people, organisations, phone numbers, INN/OGRN/SNILS/email) likely
  stored in the internal knowledge base. Routes questions to the kb-llamaindex
  retrieval tools and formats grounded, cited answers.
---

# Knowledge Base

This skill drives the `kb-llamaindex` retrieval tools (exposed over MCP as
`mcp_kbtools_*` and `mcp_kbsearch_kb_search`). The knowledge base is **stateless**
— it has no memory of the conversation, so you must give each call a
self-contained query and keep the conversational state yourself.

## Tool selection (orient on the problem first)

Pick the tool by the *shape* of the question, not by habit:

- **Known exact identifier** (E.164 phone, INN, OGRN, SNILS, email) →
  `find_entity_by_id(name, entity_type=None)`. Use when the user already names a
  precise identifier.
- **Relationships** ("who is connected to X", "X's surroundings", "X's owner") →
  `find_neighbours(entity_name, hops=1)` for a direct walk, or
  `graph_search(query, depth=2)` when the entity isn't pinned yet.
- **Factual / semantic question** → `vector_search(query, top_k=10)`. If a hit
  needs surrounding context within its source, follow up with
  `get_chunks_by_doc_id(doc_id, limit, offset)`.
- **Need the raw full text** (tables, code, short documents that chunking splits
  badly) → `read_full_document(doc_id, max_chars=20000)`.
- **Hard multi-hop question** you cannot resolve in 2–3 atomic calls → escalate to
  `kb_search(query)`. It runs the full plan-execute-synthesize workflow and
  returns an answer with `citations` and `uncertainties`. Treat it as the
  expensive escape hatch, not the default.

**Canonical anchor:** the local Wikibase is the source of truth for entity
identity. When names conflict, trust the canonical name returned by the graph
tools over a name guessed from free text.

## Response templates (by task type)

The tools return `sources` (and `kb_search` adds `citations` + `uncertainties`).
Format the answer to the task:

- **Factual answer** — the claim, then citations as `[doc_id]` after each
  supported statement. If `kb_search` returned `uncertainties`, add a short
  "Unverified / uncertain" block listing them. Never present a `vector_search`
  hit as certain if the text only partially supports the claim.
- **Entity dossier** — canonical name, type, key attributes, relations (from
  `find_neighbours` / `graph_search`), then the source `doc_id`s. Use when the
  user asks "tell me about X".
- **"What do we know about X"** — a grouped summary across vector + graph results
  (facts, relationships, open questions), each line linked to its `doc_id`.
- **Answer language = the user's question language.** Note: `kb_search` synthesises
  in Russian; if the user wrote in another language, translate its answer.

## Memory (record and reuse `~/.hermes/`)

Use your persistent memory to make the base feel personal across sessions:

- **Record:** the user's domain/role, recurring entities and important `doc_id`s
  they return to, and their preferred answer format.
- **Reuse:** before a tool call, enrich the query with remembered context (entity
  names, typical department, prior `doc_id`s) — this compensates for the stateless
  base. Example: a bare "what's the latest?" becomes a self-contained query about
  the project the user has been asking about.

## Follow-up handling (multi-turn)

Because the base is stateless, **resolve references before every call**. Rewrite
"he / she / it / there / that one" into the concrete entity from earlier in the
conversation, and fold relevant constraints from prior turns into one
self-contained query. Never forward a raw follow-up utterance to a tool.
````

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_mcp/test_hermes_skill.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add integrations/hermes/knowledge-base/SKILL.md tests/test_mcp/test_hermes_skill.py
git commit -m "feat(hermes): knowledge-base skill encoding tool-selection/templates/memory/follow-up

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Operator runbook + example Hermes config

**Files:**
- Create: `integrations/hermes/config.example.yaml`
- Create: `docs/runbook/hermes.md`
- Modify: `docs/runbook/mcp.md` (add a cross-link near the related-runbooks list, lines ~16-19)
- Test: `tests/test_mcp/test_hermes_config_example.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_mcp/test_hermes_config_example.py`:

```python
"""Validates the example ~/.hermes/config.yaml ships a correct,
complete mcp_servers block for both kb-llamaindex servers."""

from __future__ import annotations

from pathlib import Path

import yaml

_CFG = Path("integrations/hermes/config.example.yaml")

_KBTOOLS_INCLUDE = {
    "vector_search",
    "graph_search",
    "find_entity_by_id",
    "find_neighbours",
    "get_chunks_by_doc_id",
    "read_full_document",
}


def test_config_example_exists():
    assert _CFG.is_file(), f"missing {_CFG}"


def test_config_has_both_servers_with_auth_and_tools():
    cfg = yaml.safe_load(_CFG.read_text(encoding="utf-8"))
    servers = cfg["mcp_servers"]

    kbtools = servers["kbtools"]
    assert kbtools["url"].endswith("/sse")
    assert kbtools["headers"]["Authorization"].startswith("Bearer ")
    assert set(kbtools["tools"]["include"]) == _KBTOOLS_INCLUDE

    kbsearch = servers["kbsearch"]
    assert kbsearch["url"].endswith("/sse")
    assert kbsearch["headers"]["Authorization"].startswith("Bearer ")
    assert kbsearch["tools"]["include"] == ["kb_search"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_mcp/test_hermes_config_example.py -v`
Expected: FAIL — `test_config_example_exists` fails (file missing).

- [ ] **Step 3: Create the example config**

Create `integrations/hermes/config.example.yaml`:

```yaml
# Example snippet for ~/.hermes/config.yaml — registers the two
# kb-llamaindex MCP servers with Hermes Agent over SSE.
# Replace <kb-host> with the host running the SSE services and set
# KB_API_KEY in the Hermes process environment.
mcp_servers:
  kbtools:
    url: "http://<kb-host>:9002/sse"
    headers:
      Authorization: "Bearer ${KB_API_KEY}"
    tools:
      include: [vector_search, graph_search, find_entity_by_id,
                find_neighbours, get_chunks_by_doc_id, read_full_document]
      prompts: false
      resources: false
  kbsearch:
    url: "http://<kb-host>:9001/sse"
    headers:
      Authorization: "Bearer ${KB_API_KEY}"
    tools:
      include: [kb_search]
      prompts: false
      resources: false
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_mcp/test_hermes_config_example.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Write the operator runbook**

Create `docs/runbook/hermes.md`:

````markdown
# Hermes Agent integration runbook

Подключение `kb-llamaindex` к [Hermes Agent](https://hermes-agent.nousresearch.com)
(Nous Research) — персистентному серверному агенту. RAG выступает источником
инструментов через MCP по SSE; память, цикл диалога и обучаемость — на стороне
Hermes. Интеграция **аддитивна**: поведение MCP-серверов не меняется.

Связанные runbook'и: [`mcp.md`](mcp.md) (сами MCP-серверы),
[`search-usage.md`](search-usage.md).

## 1. Поднять SSE-сервисы

Рядом с Temporal worker (тот же стек: Milvus, Neo4j, Postgres, LiteLLM):

```bash
# Атомарные тулы (основная поверхность для интерактивного цикла)
uv run python -m src.mcp.tools_server  --transport sse --host 0.0.0.0 --port 9002

# kb_search (тяжёлый escape-hatch для многоходовых вопросов)
uv run python -m src.mcp.search_server --transport sse --host 0.0.0.0 --port 9001
```

**Авторизация:** при `KB_MCP_REQUIRE_AUTH=true` (по умолчанию) сервер требует
заголовок `Authorization: Bearer <key>`, где `<key>` ∈ `API_KEYS`. Без ключей
сервер не стартует (`assert_api_key_env_set`). Для локального stdio-режима
desktop-клиентов auth отключается через `KB_MCP_REQUIRE_AUTH=false`.

## 2. Зарегистрировать в Hermes

Скопировать блок из [`integrations/hermes/config.example.yaml`](../../integrations/hermes/config.example.yaml)
в `~/.hermes/config.yaml`, заменив `<kb-host>` и выставив `KB_API_KEY` в окружении
процесса Hermes. При старте Hermes сам дискаверит тулы и покажет чек-лист; имена
в агенте получают префикс `mcp_kbtools_*` и `mcp_kbsearch_kb_search`.

## 3. Установить скилл

Скилл [`integrations/hermes/knowledge-base/SKILL.md`](../../integrations/hermes/knowledge-base/SKILL.md)
кладётся в `~/.hermes/skills/knowledge-base/SKILL.md` (или публикуется через Skills
Hub). Он учит Hermes выбирать тул под тип задачи, форматировать ответы с
цитатами, пользоваться памятью и резолвить follow-up-реплики в самодостаточные
запросы.

## 4. Smoke-проверка

1. Hermes стартует без ошибок и в списке тулов видны 6 `mcp_kbtools_*` +
   `mcp_kbsearch_kb_search`.
2. Запрос с точным идентификатором (телефон/ИНН) → Hermes зовёт
   `find_entity_by_id` и возвращает досье.
3. Неверный/отсутствующий `KB_API_KEY` → запрос к SSE отклоняется (401).

## 5. Приёмочные сценарии

См. `tests/eval/hermes_scenarios.py` — золотой набор интерактивных сценариев
(по одному на ветку дерева выбора тула + многоходовый follow-up + досье). Прогон
end-to-end через живой Hermes — ручной; критерии: верный тул, применён шаблон,
сработала реформулировка.
````

- [ ] **Step 6: Add a cross-link in `docs/runbook/mcp.md`**

In `docs/runbook/mcp.md`, in the "Связанные runbook'и" list (lines ~16-19), add this bullet:

```markdown
- [`hermes.md`](hermes.md) — подключение MCP-серверов к Hermes Agent (SSE + skill)
```

- [ ] **Step 7: Run the config test again + confirm docs render links**

Run: `uv run pytest tests/test_mcp/test_hermes_config_example.py -v`
Expected: PASS (2 passed).

- [ ] **Step 8: Commit**

```bash
git add integrations/hermes/config.example.yaml docs/runbook/hermes.md docs/runbook/mcp.md tests/test_mcp/test_hermes_config_example.py
git commit -m "docs(hermes): operator runbook + example ~/.hermes/config.yaml

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Golden interactive scenario set

**Files:**
- Create: `tests/eval/hermes_scenarios.py`

This is a structured, in-repo description of the acceptance scenarios plus a
coverage test asserting the set exercises every decision-tree branch. The
end-to-end run through a live Hermes is manual (documented in the runbook); this
file keeps the scenarios versioned and guarantees coverage doesn't silently drop.

- [ ] **Step 1: Write the failing test (the file is also the test)**

Create `tests/eval/hermes_scenarios.py`:

```python
"""Golden interactive scenarios for the Hermes ↔ kb-llamaindex skill.

Each scenario names a user turn, the tool the skill should select, and
the response template that should be applied. The coverage test asserts
the set exercises every decision-tree branch (one per atomic tool, the
kb_search escape hatch, a multi-turn follow-up, and an entity dossier).

End-to-end execution is manual against a live Hermes (see
docs/runbook/hermes.md §5); this file is the versioned source of truth.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Scenario:
    name: str
    user_turn: str
    history: tuple[str, ...]      # prior turns; empty for single-shot
    expected_tool: str            # bare tool name the skill should pick
    expected_template: str        # factual | dossier | what_we_know


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        name="exact_identifier",
        user_turn="Чей это номер +7 495 123-45-67?",
        history=(),
        expected_tool="find_entity_by_id",
        expected_template="dossier",
    ),
    Scenario(
        name="relationship_walk",
        user_turn="Кто связан с ООО «Ромашка»?",
        history=(),
        expected_tool="find_neighbours",
        expected_template="what_we_know",
    ),
    Scenario(
        name="graph_unpinned_entity",
        user_turn="Что известно про связи поставщика из последнего договора?",
        history=(),
        expected_tool="graph_search",
        expected_template="what_we_know",
    ),
    Scenario(
        name="semantic_factual",
        user_turn="Какой порядок согласования отпуска?",
        history=(),
        expected_tool="vector_search",
        expected_template="factual",
    ),
    Scenario(
        name="surrounding_context",
        user_turn="Покажи раздел целиком, откуда это.",
        history=("Какой порядок согласования отпуска?",),
        expected_tool="get_chunks_by_doc_id",
        expected_template="factual",
    ),
    Scenario(
        name="full_document",
        user_turn="Дай полный текст приказа №14, там таблица.",
        history=(),
        expected_tool="read_full_document",
        expected_template="factual",
    ),
    Scenario(
        name="hard_multihop_escalation",
        user_turn="Сравни условия трёх договоров с этим контрагентом и найди расхождения.",
        history=(),
        expected_tool="kb_search",
        expected_template="factual",
    ),
    Scenario(
        name="entity_dossier",
        user_turn="Расскажи всё, что у нас есть про Иванова И.И.",
        history=(),
        expected_tool="find_neighbours",
        expected_template="dossier",
    ),
    Scenario(
        name="multiturn_followup",
        user_turn="А его телефон?",
        history=(
            "Расскажи всё, что у нас есть про Иванова И.И.",
            "Иванов Иван Иванович, менеджер, …",
        ),
        expected_tool="find_entity_by_id",
        expected_template="factual",
    ),
)

# Every tool branch the skill documents must be exercised by ≥1 scenario.
_REQUIRED_TOOL_COVERAGE = {
    "vector_search",
    "graph_search",
    "find_entity_by_id",
    "find_neighbours",
    "get_chunks_by_doc_id",
    "read_full_document",
    "kb_search",
}


def test_scenarios_cover_every_tool_branch():
    covered = {s.expected_tool for s in SCENARIOS}
    missing = _REQUIRED_TOOL_COVERAGE - covered
    assert not missing, f"no scenario exercises: {missing}"


def test_at_least_one_multiturn_scenario():
    assert any(s.history for s in SCENARIOS), "need a follow-up scenario"


def test_templates_are_known():
    allowed = {"factual", "dossier", "what_we_know"}
    bad = {s.expected_template for s in SCENARIOS} - allowed
    assert not bad, f"unknown templates: {bad}"
```

- [ ] **Step 2: Run test to verify it passes**

Run: `uv run pytest tests/eval/hermes_scenarios.py -v`
Expected: PASS (3 passed). (Coverage, multi-turn, and template tests all hold for the scenario set above.)

> Note: this file is created passing because the scenario data and the assertions are authored together. If a future edit removes a tool branch, `test_scenarios_cover_every_tool_branch` fails — that is the guard.

- [ ] **Step 3: Commit**

```bash
git add tests/eval/hermes_scenarios.py
git commit -m "test(eval): golden interactive Hermes scenarios with branch-coverage guard

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Final verification

**Files:** none (verification only).

- [ ] **Step 1: Run the full MCP + eval suites**

Run: `uv run pytest tests/test_mcp/ tests/eval/hermes_scenarios.py -v`
Expected: PASS — all of `test_auth.py`, `test_hermes_skill.py`, `test_hermes_config_example.py`, `hermes_scenarios.py`, plus the pre-existing `test_tools_server.py` / `test_search_server.py`, are green.

- [ ] **Step 2: Confirm no behavioural regression in the servers**

Run: `uv run python -c "import src.mcp.tools_server as t, src.mcp.search_server as s; print('tools auth:', t.mcp.auth); print('search auth:', s.mcp.auth)"`
Expected: with `KB_MCP_REQUIRE_AUTH` unset/true and `API_KEYS` empty in the shell, both print `None` (auth attaches only when keys are configured). No import error.

- [ ] **Step 3: Lint/format check (match repo convention)**

Run: `uv run ruff check src/mcp/ tests/test_mcp/ tests/eval/hermes_scenarios.py`
Expected: PASS (no new violations). Fix any reported issues and re-run.

- [ ] **Step 4: Final commit if anything was fixed**

```bash
git add -A
git commit -m "chore(hermes): lint fixups after integration tasks

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-review (completed by plan author)

**Spec coverage:**
- §Architecture 1 (topology/transport, auth) → Task 1 (auth) + Task 3 (runbook launch commands, config). ✓
- §Architecture 2 (the skill, 4 pillars) → Task 2. ✓
- §Architecture 3 (RAG-side: SSE auth passthrough — confirmed *needed*, the helper was unwired) → Task 1. The "SSE service ops docs" → Task 3 runbook. "No new endpoints/tables" — honoured. ✓
- §Architecture 4 (verification: smoke test, scenario eval, regression, docs) → Task 3 runbook §4, Task 4 scenarios, Task 1/5 regression, Task 3 runbook. ✓
- §Deliverables 1–5 → runbook (T3), SKILL.md (T2), config snippet (T3), conditional auth fix → now unconditional (T1), eval set (T4). ✓
- Open risk 1 (auth passthrough) → resolved: enforced via `StaticTokenVerifier` (T1). Open risk 2 (SSE vs streamable-HTTP) → the example config uses the `/sse` URL form Hermes documents for `sse`; verified manually at smoke-test time (T3 §4). Open risk 3 (Russian synthesis) → addressed in the skill's template rule ("translate kb_search answer to the user's language"). ✓

**Placeholder scan:** No "TBD/TODO/handle edge cases"; the only literal placeholders are intentional config tokens (`<kb-host>`, `${KB_API_KEY}`) explained in-line. ✓

**Type/name consistency:** Tool-name sets are identical across Task 2 (`_REQUIRED_TOOL_NAMES`), Task 3 (`_KBTOOLS_INCLUDE`), and Task 4 (`_REQUIRED_TOOL_COVERAGE`); `build_sse_auth` named identically in helper, both servers, and tests; section markers in the SKILL body (`Tool selection`, `Response templates`, `Memory`, `Follow-up`) match the substrings asserted in `test_skill_covers_the_four_pillars`. ✓
