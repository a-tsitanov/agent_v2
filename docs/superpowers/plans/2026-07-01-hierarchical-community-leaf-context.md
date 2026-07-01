# Hierarchical Community Leaf-Context Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Дать сообществам на ВСЕХ уровнях иерархии валидный контекст для саммаризации, чтобы листовой (самый мелкий) уровень перестал отдавать «input context was not provided».

**Architecture:** Персистить сущностные `IN_COMMUNITY`-рёбра на всех уровнях `detect_hierarchy` (сегодня — только level 0), не трогая `_gather_context` (его существующий фолбэк на member-context тогда сработает для листа). Заскоупить один search-Cypher на level 0, чтобы адресация сообществ по `id` не начала коллизить между уровнями. Затем провалидировать e2e на локальном графе и проверить, использует ли поиск уровни.

**Tech Stack:** Python 3.12, pytest, Neo4j (Cypher, `structured_query`), Temporal-активити, leidenalg/igraph бэкенд, LiteLLM→ollama (gemma4:e4b-it-qat / nomic-embed-text).

## Global Constraints

- `:Community` MERGE всегда keyed на `(id, level)` — `id` уникален ТОЛЬКО в пределах уровня (позиционный индекс membership).
- Порядок записи иерархии — coarsest-first (level ascending): level-`k` MATCH-ит родителя level-`k-1`, который уже записан. НЕ ломать.
- Юнит-тесты БЕЗ живого Neo4j: `_FakeStore` записывает `structured_query(cypher, param_map)` и отдаёт canned-rows (см. `tests/test_graph/test_communities.py`, `tests/test_workflow/test_search_community.py`).
- НЕ менять `_gather_context` (`src/workflow/search/activities/community.py:321`) — фикс только на стороне персистенции + один search-Cypher.
- `MATCH (e:__Entity__ {name})` (не MERGE) — линкуем только СУЩЕСТВУЮЩИЕ сущности, без фантомных нод.
- Прогонять `uv run pytest ...`.

---

### Task 1: Member-рёбра на каждом уровне (ядро фикса)

**Files:**
- Modify: `src/graph/communities.py:201-212` (`_MERGE_SUBCOMMUNITY_CYPHER`)
- Modify: `src/graph/communities.py:648-660` (ветка `else` в цикле персистенции `detect_hierarchy`)
- Test: `tests/test_graph/test_communities.py`

**Interfaces:**
- Produces: `_MERGE_SUBCOMMUNITY_CYPHER` теперь, помимо `PARENT_OF`, чистит старые `IN_COMMUNITY` и линкует членов (`UNWIND $members` → `MATCH (e:__Entity__ {name}) MERGE (e)-[:IN_COMMUNITY]->(c)`). Персистенция level>0 передаёт `"members": comm.members` в params.

- [ ] **Step 1: Написать падающий shape-тест на новый Cypher**

В `tests/test_graph/test_communities.py` добавить:
```python
def test_subcommunity_cypher_writes_member_links():
    """level>0 MERGE теперь и wires PARENT_OF, и линкует членов —
    иначе у листовых сообществ нет источника member-context."""
    from src.graph import communities

    cy = communities._MERGE_SUBCOMMUNITY_CYPHER
    assert "-[:PARENT_OF]->(c)" in cy                      # дендрограмма
    assert "OPTIONAL MATCH (c)<-[old:IN_COMMUNITY]-" in cy  # чистка старых
    assert "UNWIND $members AS member_name" in cy
    assert "MATCH (e:__Entity__ {name: member_name})" in cy
    assert "MERGE (e)-[:IN_COMMUNITY]->(c)" in cy
```

- [ ] **Step 2: Прогнать — убедиться, что падает**

Run: `uv run pytest tests/test_graph/test_communities.py::test_subcommunity_cypher_writes_member_links -q`
Expected: FAIL (`assert "UNWIND $members ..." in cy` — сейчас в sub-cypher этого нет).

- [ ] **Step 3: Расширить `_MERGE_SUBCOMMUNITY_CYPHER`**

Заменить константу `src/graph/communities.py:201-212` на:
```python
_MERGE_SUBCOMMUNITY_CYPHER = """
MERGE (c:Community {id: $community_id, level: $level})
SET c.member_count = $member_count, c.members_hash = $members_hash,
    c.updated = timestamp()
FOREACH (_ IN CASE WHEN $carry_report IS NULL THEN [] ELSE [1] END |
    SET c.report = $carry_report, c.title = $carry_title,
        c.summary = $carry_summary, c.report_vec = $carry_report_vec,
        c.summarized_at = coalesce($carry_summarized_at, timestamp()))
WITH c
MATCH (p:Community {id: $parent_id, level: $level - 1})
MERGE (p)-[:PARENT_OF]->(c)
WITH c
OPTIONAL MATCH (c)<-[old:IN_COMMUNITY]-(:__Entity__)
DELETE old
WITH c
UNWIND $members AS member_name
MATCH (e:__Entity__ {name: member_name})
MERGE (e)-[:IN_COMMUNITY]->(c)
"""
```
И обновить комментарий над ней (194-200): убрать «No entity IN_COMMUNITY links at level > 0» — теперь линкуем на всех уровнях; `PARENT_OF` по-прежнему требует coarsest-first порядок.

- [ ] **Step 4: Передавать `members` в params level>0**

В `src/graph/communities.py`, ветка `else` цикла персистенции (сейчас ~651-659), добавить `members`:
```python
            else:
                # Finer: PARENT_OF edge + member IN_COMMUNITY links (leaf context).
                await asyncio.to_thread(
                    _run_query, store, _MERGE_SUBCOMMUNITY_CYPHER,
                    {
                        "community_id": comm.community_id,
                        "level": comm.level,
                        "member_count": comm.member_count,
                        "members_hash": comm.members_hash,
                        "members": comm.members,
                        "parent_id": comm.parent_id,
                        **carry,
                    },
                )
```

- [ ] **Step 5: Прогнать shape-тест — убедиться, что зелёный**

Run: `uv run pytest tests/test_graph/test_communities.py::test_subcommunity_cypher_writes_member_links -q`
Expected: PASS

- [ ] **Step 6: Обновить hierarchy-materialisation тест (level>0 несёт members + IN_COMMUNITY)**

В `tests/test_graph/test_communities.py::test_detect_hierarchy_materialises_levels_and_parents`, после блока про `PARENT_OF` params (сейчас ~200-204), добавить:
```python
    # level>0 MERGE теперь И wires PARENT_OF, И линкует членов, и передаёт members
    sub_cypher, sub_params = next((c, p) for c, p in store.calls if "PARENT_OF" in c)
    assert "IN_COMMUNITY" in sub_cypher            # тот же MERGE линкует членов
    assert sub_params.get("members")               # непустой список членов на level>0
```
(Существующие ассерты — `prune < first_merge`, `in_comm_idx < parent_idx`, `parent_id=="1"`, `level==1` — не трогать: level-0 `IN_COMMUNITY` по-прежнему пишется первым, инвариант сохраняется.)

- [ ] **Step 7: Прогнать весь файл персистенции — зелёный**

Run: `uv run pytest tests/test_graph/test_communities.py -q`
Expected: PASS (все тесты)

- [ ] **Step 8: Commit**

```bash
git add src/graph/communities.py tests/test_graph/test_communities.py
git commit -m "fix(community): write IN_COMMUNITY member links at every hierarchy level

Leaf (finest) communities had no member links (only level 0 did) and no
finer children, so their summarize context was empty → LLM refusal. Link
members at all levels; _gather_context's existing fallback now grounds them."
```

---

### Task 2: Заскоупить documents-for-communities на level 0 (back-compat)

**Files:**
- Modify: `src/workflow/search/activities/documents.py` (`_DOCS_FOR_COMMUNITIES_CYPHER`)
- Test: `tests/test_workflow/test_search_documents_activity.py`

**Interfaces:**
- Consumes: `:Community {level:0}` (канонический member-уровень).
- Produces: траверс `(:__Entity__)-[:IN_COMMUNITY]->(:Community {level:0})` — по одному сообществу на сущность, как сегодня.

**Почему:** после Task 1 сущность линкуется к сообществу на каждом уровне. `_DOCS_FOR_COMMUNITIES_CYPHER` фильтрует `WHERE comm.id IN $ids`, а `id` уникален только в пределах уровня → без скоупа траверс начнёт коллизить между уровнями и возвращать чужие документы.

- [ ] **Step 1: Написать падающий shape-тест**

В `tests/test_workflow/test_search_documents_activity.py` добавить:
```python
def test_docs_for_communities_cypher_scoped_to_level0():
    """После линковки членов на всех уровнях адресация по comm.id
    коллизит между уровнями; траверс должен быть заскоуплен на level 0."""
    from src.workflow.search.activities import documents

    cy = documents._DOCS_FOR_COMMUNITIES_CYPHER
    assert "-[:IN_COMMUNITY]->(comm:Community {level: 0})" in cy
    assert "comm.id IN $ids" in cy
```

- [ ] **Step 2: Прогнать — убедиться, что падает**

Run: `uv run pytest tests/test_workflow/test_search_documents_activity.py::test_docs_for_communities_cypher_scoped_to_level0 -q`
Expected: FAIL (сейчас `(comm:Community)` без `{level: 0}`).

- [ ] **Step 3: Заскоупить Cypher на level 0**

В `src/workflow/search/activities/documents.py` заменить `_DOCS_FOR_COMMUNITIES_CYPHER` на:
```python
_DOCS_FOR_COMMUNITIES_CYPHER = """
MATCH (c:Chunk)-[:MENTIONS]->(:__Entity__)-[:IN_COMMUNITY]->(comm:Community {level: 0})
WHERE comm.id IN $ids
RETURN DISTINCT c.doc_id AS doc_id
"""
```

- [ ] **Step 4: Прогнать shape-тест — зелёный**

Run: `uv run pytest tests/test_workflow/test_search_documents_activity.py::test_docs_for_communities_cypher_scoped_to_level0 -q`
Expected: PASS

- [ ] **Step 5: Прогнать весь файл — регресс не сломан**

Run: `uv run pytest tests/test_workflow/test_search_documents_activity.py -q`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/workflow/search/activities/documents.py tests/test_workflow/test_search_documents_activity.py
git commit -m "fix(search): scope docs-for-communities traversal to level 0

Community id is unique only per level; once members link at every level,
comm.id IN \$ids collides across levels. Pin the entity→community hop to
level 0 (today's canonical member set)."
```

---

### Task 3: E2E-валидация иерархии на локальном графе

**Files:**
- Use: `.env` (`AGENT_COMMUNITY_MAX_LEVELS`), локальный стек (Neo4j/Temporal/litellm уже подняты), засеянный граф (72 `:__Entity__`, 6 кластеров).
- No code change (валидация); при провале — вернуться к Task 1/2.

**Предпосылка:** локальный стек поднят; worker запущен с `TEMPORAL_COMMUNITY_BACKEND=leidenalg` (иначе TemporalSettings не читает `.env`); API на :8000.

- [ ] **Step 1: (опц.) Baseline — воспроизвести отказы ДО фикса**

Застэшить фикс, поднять уровни, пересобрать, посмотреть отказы на листе:
```bash
git stash push -- src/graph/communities.py src/workflow/search/activities/documents.py
# в .env: AGENT_COMMUNITY_MAX_LEVELS=6 ; перезапустить worker+api
curl -sS -X POST http://localhost:8000/api/v1/admin/communities/rebuild -H "X-API-Key: change-me-strong-key"
```
Через минуту — Cypher (ожидаем refusals>0 на самом мелком уровне):
```cypher
MATCH (c:Community)
RETURN c.level AS level, count(*) AS total,
       sum(CASE WHEN c.summary CONTAINS 'not provided'
                OR c.summary CONTAINS 'cannot be generated' THEN 1 ELSE 0 END) AS refusals
ORDER BY level;
```
Затем вернуть фикс: `git stash pop`.

- [ ] **Step 2: С фиксом — выставить уровни и пересобрать**

В `.env`: `AGENT_COMMUNITY_MAX_LEVELS=6`. Перезапустить host-worker+API (worker — c `TEMPORAL_COMMUNITY_BACKEND=leidenalg`). Затем:
```bash
curl -sS -X POST http://localhost:8000/api/v1/admin/communities/rebuild -H "X-API-Key: change-me-strong-key"
```

- [ ] **Step 3: Проверить member-рёбра на всех уровнях**

```cypher
MATCH (c:Community)
OPTIONAL MATCH (c)<-[:IN_COMMUNITY]-(e:`__Entity__`)
WITH c.level AS level, count(*) AS communities, sum(CASE WHEN e IS NULL THEN 0 ELSE 1 END) AS member_links
RETURN level, communities, member_links ORDER BY level;
```
Expected: `member_links > 0` на КАЖDОМ уровне (раньше — только level 0).

- [ ] **Step 4: Проверить отсутствие отказов на всех уровнях**

```cypher
MATCH (c:Community)
RETURN c.level AS level, count(*) AS total,
       sum(CASE WHEN c.summary CONTAINS 'not provided'
                OR c.summary CONTAINS 'cannot be generated' THEN 1 ELSE 0 END) AS refusals,
       sum(CASE WHEN c.report IS NOT NULL THEN 1 ELSE 0 END) AS with_report
ORDER BY level;
```
Expected: `refusals ≈ 0` на всех уровнях; `with_report == total` на всех уровнях (вкл. лист и level 0/1 — если ребилд дошёл до конца).

- [ ] **Step 5: Вернуть .env**

Вернуть `AGENT_COMMUNITY_MAX_LEVELS` к прежнему значению (1 локально), если не продолжаем эксперименты. Commit не требуется (валидация).

---

### Task 4: Аудит — использует ли поиск уровни иерархии

**Files:**
- Read: `src/config.py:657-662` (`community_max_levels`, `community_dynamic_selection`)
- Read: `src/workflow/search/activities/global_search.py` (`_DESCENT_ROOT_CYPHER`/`_DESCENT_CHILDREN_CYPHER`, чтение по `{level:$level}`)
- Read: `src/workflow/search/activities/documents.py` (level-0 предположение из Task 2)
- Output: короткий раздел «Findings» в конец спеки `docs/superpowers/specs/2026-07-01-hierarchical-community-leaf-context.md`

**Цель (запрос пользователя):** убедиться, что поиск реально ходит по уровням, а не игнорирует иерархию.

- [ ] **Step 1: Определить активный режим выбора сообществ**

Проверить `settings.agent.community_dynamic_selection` (`lexical` | `semantic` | `descent`) и что читает `global_search.py`:
```bash
grep -n "community_dynamic_selection\|_DESCENT\|_GLOBAL_MAP_CYPHER\|level" src/workflow/search/activities/global_search.py
```
Зафиксировать: какой режим и на каких уровнях он берёт `:Community.summary`. `descent` спускается 0→глубже; `lexical`/`semantic` берут фиксированный уровень.

- [ ] **Step 2: Проверить, что descent видит фикснутые мелкие уровни**

Если режим `descent`: убедиться, что `_DESCENT_CHILDREN_CYPHER` идёт по `PARENT_OF` до листа и что у детей теперь есть непустой `summary` (после Task 1). Если режим `lexical`/`semantic` с фиксированным `level`: проверить, какой уровень берётся и не заперт ли он на 0 (тогда иерархия не используется).

- [ ] **Step 3: Сверить documents-for-communities с уровнем выбора**

Task 2 заскоупил `documents.py` на level 0. Если поиск выбирает сообщества на level>0 (`descent`/несколько уровней) и передаёт их `id` в `documents_for_communities`, то level-0-скоуп вернёт неверные документы (id коллизит между уровнями). Зафиксировать как риск: нужен плюмбинг `(id, level)` парой вместо `id IN $ids`. Это follow-up, НЕ в этом плане.

- [ ] **Step 4: Записать findings в спеку**

Дописать в спеку раздел `## Findings аудита поиска (2026-07-01)`: активный режим, использует ли иерархию, и нужен ли follow-up на `(id, level)` в documents-for-communities. Коммитить вместе со спекой:
```bash
git add docs/superpowers/specs/2026-07-01-hierarchical-community-leaf-context.md
git commit -m "docs(community): audit findings — does search use the level hierarchy"
```

---

## Self-Review

**Spec coverage:**
- Решение «member-рёбра на всех уровнях» → Task 1. ✅
- Back-compat `documents.py` (id-коллизия по уровням) → Task 2. ✅
- Валидация (локальный репро MAX_LEVELS=6) → Task 3. ✅
- «Проверить, что поиск использует иерархию» (запрос пользователя) → Task 4. ✅
- Carry-gate — исключён из спеки (проверено: `_READ_OLD_REPORTS_CYPHER` уже фильтрует `report IS NOT NULL`); задачи нет намеренно. ✅
- Cap на member-context — решено НЕ капить (follow-up); задачи нет намеренно. ✅

**Type/name consistency:** `_MERGE_SUBCOMMUNITY_CYPHER`, `_DOCS_FOR_COMMUNITIES_CYPHER`, `comm.members`, `comm.parent_id`, `_FakeStore.calls`, `structured_query(cypher, param_map)` — совпадают с кодом (`communities.py`, `documents.py`) и тест-паттернами.

**Placeholder scan:** нет TODO/«handle edge cases»/«similar to» — весь Cypher и тест-код приведены целиком.
