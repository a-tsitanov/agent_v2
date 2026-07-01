# Иерархические community-summaries — контекст на листовом уровне (fix)

**Дата:** 2026-07-01
**Статус:** дизайн (на ревью)
**Трек:** следствие [[community-detection-offload]] / GraphRAG-иерархия
**Связано:** ADR-0009 (hierarchical-leiden), ADR-0010 (dynamic-community-selection)

## Проблема

При многоуровневой сборке сообществ (`AGENT_COMMUNITY_MAX_LEVELS > 1`, `detect_hierarchy`) **листовой (самый мелкий) уровень получает пустой контекст**, и LLM вместо отчёта отвечает
`"the input context for the knowledge graph community was not provided. therefore, a summary cannot be generated"`.
Фразы нет в коде — это реакция модели на пустой промпт.

### Подтверждено данными прода (7 уровней)

| level | total | refusals | with_report | статус |
|------:|------:|---------:|------------:|--------|
| 6 (лист) | 89 | **73** | 89 | ⛔ отказы |
| 5 | 54 | 0 | 54 | ✅ |
| 4 | 51 | 0 | 51 | ✅ |
| 3 | 51 | 0 | 51 | ✅ |
| 2 | 51 | 0 | 51 | ✅ |
| 1 | 51 | 0 | **0** | ⚠️ не саммаризован |
| 0 (крупный) | 51 | 0 | **0** | ⚠️ не саммаризован |

Локально проблемы нет только потому, что `AGENT_COMMUNITY_MAX_LEVELS=1` → единственный level 0.

### Корневая причина (запинпойнчена в коде)

1. **Сущностные `IN_COMMUNITY`-рёбра пишутся ТОЛЬКО на level 0.** `detect_hierarchy` (`src/graph/communities.py:634-660`): для `level == 0` вызывается `_MERGE_COMMUNITY_CYPHER` (пишет `(e:__Entity__)-[:IN_COMMUNITY]->(c)`, communities.py:188-190); для `level > 0` — `_MERGE_SUBCOMMUNITY_CYPHER` (communities.py:201-212), который пишет только `PARENT_OF` и **member-рёбра не пишет** (и `comm.members` в его params даже не передаётся, communities.py:651-659). При этом `comm.members` есть у КАЖДОГО `CommunityRef` — данные есть, путь level>0 их просто выбрасывает.

2. **Сбор контекста зависит от этих рёбер.** `_gather_context` (`src/workflow/search/activities/community.py:321`):
   - `level == 0` → member-context (`_MEMBER_CONTEXT_CYPHER`, community.py:50): `(c {id,level})<-[:IN_COMMUNITY]-(e:__Entity__)`.
   - `level > 0` → child-reports (`_CHILD_REPORTS_CYPHER`, community.py:69): `(c)-[:PARENT_OF]->(child) WHERE child.report IS NOT NULL`; при отсутствии — **фолбэк на member-context**.

3. **Лист остаётся без источника контекста.** Порядок саммаризации — **finest-first** (`group_specs_by_level`, `community_wf.py:77-92`, `reverse=True`), т.е. лист обрабатывается ПЕРВЫМ. У листа нет более мелких детей (`PARENT_OF`→уровня `level+1` не существует) → child-reports пусты → фолбэк на member-context → но на level>0 member-рёбер нет → **контекст пуст → отказ**. Средние уровни живут за счёт child-reports (даже отказ ребёнка = непустой `report`, поэтому родитель что-то генерит).

**По сути иерархия перевёрнута:** member-рёбра лежат на самом КРУПНОМ уровне, а по логике GraphRAG нужны на ЛИСТОВОМ (лист саммаризуется из сущностей, крупные — агрегируют детей).

### Вторичный симптом: level 0,1 без отчётов (`with_report=0`)

**Причина (подтверждена пользователем): прерванный ребилд.** finest-first → крупные уровни идут последними; терминация/таймаут воркфлоу оставил 0,1 недосчитанными. После полного прогона они досчитываются.

Carry-over-бага здесь НЕТ (проверено): `_READ_OLD_REPORTS_CYPHER` (communities.py:160) фильтрует `c.report IS NOT NULL`, и `needs_report=False` ставится только при реально перенесённом отчёте (communities.py:598 — `if carried and carried.get("report")`). Т.е. сообщество без отчёта не скипается. Отдельная задача carry-gate НЕ нужна.

## Решение (предлагается)

**Писать сущностные `IN_COMMUNITY`-рёбра на ВСЕХ уровнях**, а не только на level 0. Данные (`comm.members`) уже есть в каждом `CommunityRef`. `_gather_context` **не меняется**: level>0 по-прежнему предпочитает child-reports (дёшево, иерархично), а member-рёбра становятся тем фолбэком, который наконец срабатывает для листа. level 0 — как сейчас (member-context).

Итог: лист саммаризуется из сущностей → крупные уровни агрегируют child-reports → вся иерархия заземлена.

### Рассмотренные альтернативы

- **(A) member-рёбра только на глобально-листовом уровне (`level == max`).** Отклонено: в дендрограмме ветви обрываются на разной глубине — «финальное» сообщество сущности не всегда на `max_level`; часть сущностей осталась бы без рёбер.
- **(B) инвертировать: рёбра только на листе + переписать `_gather_context`, чтобы level 0 тоже читал child-reports.** Отклонено: больше изменений в логике сбора + меняется семантика level 0; риск задеть global-search, читающий по уровням.
- **(C, выбрано) рёбра на всех уровнях + заскоупить search-чтения на канонический уровень.** Минимальное изменение логики (только персистенция + один search-Cypher), `_gather_context` не трогаем.

## Архитектура / точечные изменения

### 1. Персистенция иерархии (ядро фикса)
`src/graph/communities.py`:
- `_MERGE_SUBCOMMUNITY_CYPHER` (201-212): добавить блок member-рёбер из `_MERGE_COMMUNITY_CYPHER` (очистка старых `IN_COMMUNITY` + `UNWIND $members` → `MATCH (e:__Entity__ {name}) MERGE (e)-[:IN_COMMUNITY]->(c)`), сохранив существующий `MATCH parent → MERGE PARENT_OF`.
- Цикл персистенции (634-660): в ветке `else` (level>0) передавать `"members": comm.members` в params.
- Level 0 (`_MERGE_COMMUNITY_CYPHER`) — без изменений.

> Порядок записи (coarsest-first, communities.py:626-627) сохраняется — он нужен для `MATCH parent`; member-MERGE от него не зависит.

### 2. Обратная совместимость search (обязательно)
`src/workflow/search/activities/documents.py:26`:
```cypher
MATCH (c:Chunk)-[:MENTIONS]->(:__Entity__)-[:IN_COMMUNITY]->(comm:Community)
```
Константа `_DOCS_FOR_COMMUNITIES_CYPHER` фильтрует `WHERE comm.id IN $ids`. **`id` уникален только В ПРЕДЕЛАХ уровня** (позиционный индекс membership — level-0 «5» и level-3 «5» это РАЗНЫЕ сообщества). Сегодня рёбра только на level 0, поэтому `comm.id IN $ids` однозначен. После фикса сущность линкуется к сообществу на каждом уровне → `comm.id IN $ids` начнёт коллизить между уровнями (и вернёт по несколько `comm` на сущность). **Заскоупить на канонический уровень:** `->(comm:Community {level: 0})`. Поведение сохраняется дословно (сегодняшнее = только level 0). Если поиск начнёт передавать id мелких уровней — нужна плюмбинг (id, level) парой; вынесено в аудит поиска (задача ниже).

Проверить также остальные читатели `IN_COMMUNITY`/`:Community`:
- `global_search.py:44,69,75` — читает по явному `{level: $level}` и `PARENT_OF` → не затронуто, но покрыть тестом.
- `_MEMBER_CONTEXT_CYPHER` (community.py:50) — теперь возвращает участников для любого уровня (это и есть цель).

### 3. Гейт carry-over (вторичный симптом 0,1)
`src/graph/communities.py` (carry-логика ~600-624) и/или `build_summarize_specs` (`community_wf.py:52-73`): **не помечать сообщество как `needs_report=False`, если у него нет перенесённого отчёта** (`carry_report IS NULL`). Т.е. carry-skip допустим только при реально существующем прошлом отчёте. Гарантирует, что крупные уровни досчитываются даже при инкрементальных ребилдах.

### 4. Стоимость / масштаб
- `IN_COMMUNITY`-рёбер станет ≈ `entities × levels` (для 250k × 7 ≈ 1.75M) — для Neo4j приемлемо; `_PRUNE_ALL_CYPHER` чистит всё перед ребилдом, ghost-рёбер не будет.
- `_MEMBER_CONTEXT_CYPHER` для КРУПНЫХ уровней вернёт весь (большой) member-set → тяжёлый промпт. Но крупные уровни используют member-context лишь как ФОЛБЭК (child-reports в приоритете), так что в норме не бьёт. **Открытый вопрос:** ставить ли cap на число участников в member-context (напр. top-N по degree) — см. ниже.

## Валидация

### Локальный репро-harness (перед фиксом — воспроизвести прод)
На засеянном графе (72 сущности, 6 плотных кластеров) поднять `AGENT_COMMUNITY_MAX_LEVELS=6`, `POST /api/v1/admin/communities/rebuild` → ожидаем отказы на листовом уровне (как на проде). Это baseline-репро, которого сейчас нет (локально было `=1`).

### После фикса
- Тот же прогон: у КАЖДОГО уровня (вкл. лист и level 0/1) — непустой не-отказной отчёт.
- Unit: персистенция level>0 создаёт `(e:__Entity__)-[:IN_COMMUNITY]->(c {level>0})` (fake-store).
- Unit: `documents.py` после скоупа возвращает ровно одно `comm` на `(chunk,entity)`.
- Unit: carry-gate — сообщество с `needs_report=False` + `report IS NULL` всё равно попадает в specs.
- Регресс: существующие community-тесты (`tests/test_workflow/test_search_community.py`) зелёные.

## Открытые решения (нужен выбор пользователя)

1. **Cap на member-context для крупных уровней** — ставить top-N участников или оставить полный set (фолбэк-путь, редкий)? Рекомендация: пока НЕ капить, вынести в follow-up, если всплывёт большой промпт.
2. **Level 0,1 без отчётов** — это был прерванный ребилд или carry-баг? Быстрая проверка на проде: есть ли у 0/1 `members_hash` и `needs_report`. Если carry-баг — правим гейт (п.3); если прерывание — фикс не нужен, но гейт всё равно страхует.
3. **Scope изменения:** только summarize-контекст (этот спек) — детекция/бэкенд не трогаем. Подтвердить, что иерархия вообще нужна на проде (иначе быстрый путь — `AGENT_COMMUNITY_MAX_LEVELS=1`, без кода).

## Вне scope
- Инкрементальная поддержка иерархии, партиционирование кластеризации ([[community-detection-offload]] «вне scope»).
- Изменение алгоритма детекции / бэкенда (gds↔leidenalg).
- Дедуп member-context между уровнями.
