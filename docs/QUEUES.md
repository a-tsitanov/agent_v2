# Очереди задач Temporal

Процесс worker'а (`uv run python -m src.workflow.worker`) поднимает
несколько пулов Worker'ов против одного и того же Temporal-клиента,
каждый из которых поллит свою task queue, чтобы нагрузку на GPU / LLM
можно было ограничивать независимо.

| Очередь (поле конфига) | Default | Что хостит | Лимит конкурентности |
| --- | --- | --- | --- |
| `task_queue` | `kb-ingest` | `DocumentIngestWorkflow` + IO/embedding-активности | `TEMPORAL_ACTIVITY_CONCURRENCY` (4) |
| `llm_task_queue` | `kb-ingest-llm` | ТОЛЬКО `extract_kg` (extract-полоса) | `TEMPORAL_LLM_ACTIVITY_CONCURRENCY` (18) |
| `merge_task_queue` | `kb-ingest-merge` | `GraphBuildWorkflow` + `merge_and_resolve` / `build_property_graph` (merge-полоса) | `TEMPORAL_MERGE_ACTIVITY_CONCURRENCY` (14) |
| `search_task_queue` | `kb-search-small` | `SearchOrchestratorWorkflow` + `SubQueryRetrievalWorkflow` + `GlobalSearchWorkflow` + `DriftSearchWorkflow` + `AutoSearchWorkflow` + их активности (plan / retrieve / coverage_check / rerank_sources / route / map_communities / documents_for_communities) | `TEMPORAL_SEARCH_ACTIVITY_CONCURRENCY` (4) |
| `large_task_queue` | `kb-search-large` | ТОЛЬКО `synthesize_answer` (финальный синтез на large-tier, Search R5) — Worker только с активностями, без workflow'ов | `TEMPORAL_LARGE_ACTIVITY_CONCURRENCY` (2) |
| `graph_build_task_queue` | `kb-graph-build` | `CommunityBuildWorkflow` + `detect_communities_activity` / `summarize_community_activity` (ОФФЛАЙН-сборка сообществ GDS-Leiden, Search R6) | `TEMPORAL_GRAPH_BUILD_ACTIVITY_CONCURRENCY` (2) |
| `wiki.task_queue` | `kb-wiki` | `WikiSweepWorkflow` + `select_dirty_entities` / `write_entity_article` (непрерывный пер-сущностный редактор статей MediaWiki) | `WIKI_ACTIVITY_CONCURRENCY` (4) |

## Лимиты очередей против LLMPool — кто на самом деле владеет конкурентностью

К каждой LLM-зависимой активности применяются два независимых ограничителя:

1. **Temporal'овский пер-очередной `max_concurrent_activities`** (таблица выше) —
   сколько активностей этой очереди worker запустит одновременно. Это граница
   **изоляции**: она не даёт одной нагрузке (например, всплеску `extract_kg`)
   занять все слоты, нужные соседней полосе (merge).
2. **Пер-процессный `LLMPool`** (`src/retrieval/llm_pool.py`,
   `LLM_POOL_*`) — настоящий арбитр конкурентности GPU/upstream, общий
   для ingest И search в одном процессе. Он навязывает иерархический
   лимит: глобальный потолок small-tier
   (`LLM_POOL_TIER_SMALL_TOTAL`, default 25) и потолок large-tier
   (`LLM_POOL_TIER_LARGE_TOTAL`, default 8), в сочетании с пер-ролевыми
   потолками полос (`LLM_POOL_LANE_CAPS`: extraction 18, judge 14, search 14,
   plan 4, route 2, retrieve 4, synthesis 8). Полосы small-tier
   намеренно подписаны с избытком (сумма потолков > tier-total), чтобы одна
   роль могла забить GPU, но ни одна не монополизировала его; `LLM_POOL_JUDGE_FLOOR`
   (default 7) резервирует ёмкость, чтобы merge/judge никогда не голодал под
   потоком extraction (правило сайзинга: потолок extraction ≤
   `tier_small_total − judge_floor`).

**Temporal-лимиты ОБЯЗАНЫ быть ≥ соответствующего потолка полосы пула**, чтобы пул
связывал первым — иначе Temporal задросселирует раньше, чем пул успеет
арбитрировать. Именно поэтому лимиты `kb-ingest-llm` / `kb-ingest-merge` были
подняты до 18 / 14 (под потолки полос extraction / judge), а не оставлены на
старой конкурентности-1.

## Выделенная merge-очередь: `kb-ingest-merge`

`extract_kg` и стадия merge (`GraphBuildWorkflow` →
`merge_and_resolve` + `build_property_graph`) раньше делили единую
очередь `kb-ingest-llm` на конкурентности 1. Когда множество документов
ингестятся одновременно, всплеск задач `extract_kg` заполняет эту FIFO-очередь,
и merge документа — поставленный в очередь *позади* всех ожидающих
extract'ов — голодает (head-of-line blocking). Векторная половина
завершается быстро, а графовая половина ждёт, пока рассосётся весь бэклог extract'ов.

**Фикс**: merge получает собственную очередь + пул Worker'ов (`kb-ingest-merge`).
Родительский `DocumentIngestWorkflow` запускает дочерний `GraphBuildWorkflow` на
`merge_task_queue`; его активности `merge_and_resolve` / `build_property_graph`
НЕ несут override `task_queue`, поэтому наследуют очередь child'а и едут по
merge-полосе автоматически. `extract_kg` остаётся прибит к
`kb-ingest-llm`. Теперь extract и merge поллят независимые очереди и
чередуются вместо сериализации через одну FIFO.

**LLM-конкурентностью теперь владеет пер-процессный LLMPool** (`src/retrieval/llm_pool.py`), а не одни только Temporal-лимиты. Temporal-лимиты `llm`/`merge` были подняты до 18/14, чтобы пул связывал первым — они обязаны быть ≥ пер-ролевых потолков полос пула, иначе Temporal задросселировал бы раньше, чем пул получит шанс арбитрировать. Пул навязывает иерархический лимит: глобальный потолок small-tier (default 25, `LLM_POOL_TIER_SMALL_TOTAL`) в сочетании с пер-ролевыми потолками полос (`LLM_POOL_LANE_CAPS`), так что extract и merge динамически чередуются, и GPU остаётся загружен, без монополизации ёмкости любой из ролей. `build_property_graph` остаётся зарегистрирована и в `MAIN_ACTIVITIES` тоже (запись в Neo4j, не GPU-bound), так что развёртывания с одним пулом всё ещё работают.

**Действие оператора при апгрейде**: задайте `TEMPORAL_MERGE_TASK_QUEUE` /
`TEMPORAL_MERGE_ACTIVITY_CONCURRENCY`, если они не дефолтные (держите их ≥
соответствующего потолка полосы пула), и перезапустите worker, чтобы он поллил
новую очередь `kb-ingest-merge`.

## Оффлайн-очередь сборки сообществ графа: `kb-graph-build` (Search R6)

Полностью **развязана / оффлайн** — эта очередь НИКОГДА не задействуется на
горячем пути запроса. Процесс worker'а хостит выделенный пул `Worker` на
`kb-graph-build` (тот же процесс, тот же Temporal-клиент), выполняющий
`CommunityBuildWorkflow` и две его активности:

- `detect_communities_activity` — гоняет Neo4j **GDS Leiden** по
  подграфу `__Entity__` (Cypher-проекция → `gds.leiden.stream`),
  группирует членов по `communityId`, отбрасывает сообщества меньше
  `TEMPORAL_COMMUNITY_MIN_SIZE` (3) и идемпотентно MERGE'ит ноды
  `:Community {id, level, member_count}`, связанные с их членами через
  `(:__Entity__)-[:IN_COMMUNITY]->(:Community)`.
- `summarize_community_activity` — для одного сообщества суммаризирует его
  членов (+ отношения между членами) через LLM **small-tier**
  (`build_llm("retrieve")`) и сохраняет результат в
  `:Community.summary` (идемпотентный MERGE). Батчируемо; workflow
  разворачивает по одному вызову на сообщество с ограниченным параллелизмом
  (`TEMPORAL_COMMUNITY_SUMMARY_PARALLELISM`, default 4).

**Триггеры** (нет пути запроса):
- Admin-эндпоинт `POST /api/v1/admin/communities/rebuild` (основной) —
  запускает workflow на `kb-graph-build`, возвращает id workflow'а.
- Опциональный **Temporal Schedule** — репозиторий пока не конфигурирует
  никакого Temporal Schedule, так что сейчас это ручная/admin-триггерная сборка.
  Чтобы гонять по cron, создайте Schedule (например, через `tctl schedule create`
  или `client.create_schedule`), который запускает `CommunityBuildWorkflow` на
  `kb-graph-build` с входом `DetectCommunitiesParams(min_size=…)`.

**Идемпотентно / инкрементально**: повторный прогон обновляет summary и
членство на существующих нодах `:Community` (MERGE по ключу
`(id, level)`) — он никогда не дублирует сообщества. Путь запроса
не меняется; эти summary пишутся для будущей фазы global-поиска.

**Конкурентность**: держится низкой (`TEMPORAL_GRAPH_BUILD_ACTIVITY_CONCURRENCY`,
default 2), чтобы всплеск summary при пересборке не залил LLM-прокси small-tier.
**Действие оператора при апгрейде**: перезапустите worker, чтобы он поллил
новую очередь `kb-graph-build`.

## Маппинг tier модели ↔ очередь

| Tier | Модель | Очередь | Зачем |
| --- | --- | --- | --- |
| small | LLM роли search (`build_search_llm`) | `kb-search-small` | планировщик, ретрив подзапросов, проверка покрытия, унифицированный rerank — дёшево, дружелюбно к параллелизму |
| large | LLM синтеза (`build_synthesis_llm`) | `kb-search-large` | один тяжёлый финальный синтез на сессию — лимит держится НИЗКИМ, чтобы он никогда не обслуживал много параллельных сессий |

## Очередь синтеза large-tier: `kb-search-large` (Search R5)

Сам `SearchOrchestratorWorkflow` по-прежнему живёт на `kb-search-small`, но
он прибивает финальный `synthesize_answer` к `kb-search-large` через
`workflow.execute_activity("synthesize_answer", …, task_queue=settings.temporal.large_task_queue)`.
Процесс worker'а хостит **отдельный пул `Worker`** на
`kb-search-large` (тот же процесс, тот же Temporal-клиент), регистрирующий ТОЛЬКО
активность `synthesize_answer`, с лимитом
`TEMPORAL_LARGE_ACTIVITY_CONCURRENCY` (default 2). Это изолирует
дорогую модель синтеза на её собственном пуле с низкой конкурентностью, чтобы
параллельные сессии поиска не сваливались на неё кучей, тогда как дешёвая
работа small-tier (plan / retrieve / coverage / rerank) держит свою более высокую
конкурентность на `kb-search-small`.

Пред-синтезный **унифицированный rerank** (`rerank_sources`) гоняется на
`kb-search-small` — bge cross-encoder дёшев относительно большого
LLM синтеза, так что он не оправдывает очередь с низкой конкурентностью.

**Действие оператора при апгрейде**: задайте `TEMPORAL_LARGE_TASK_QUEUE` /
`TEMPORAL_LARGE_ACTIVITY_CONCURRENCY`, если они не дефолтные, и перезапустите
worker, чтобы он поллил новую очередь. (Легаси ReAct-`SearchWorkflow`, который
когда-то синтезировал на small-tier, был удалён в переходе R7b;
plan-execute-оркестратор всегда синтезирует на large-tier на
`kb-search-large`.)

## Переименование search-очереди: `kb-search-llm` → `kb-search-small` (Search R2)

Default search-очереди был переименован с `kb-search-llm` на
`kb-search-small`. Очередь теперь хостит plan-execute-поток small-tier
(планировщик + параллельный ретрив подзапросов) в дополнение к легаси-ReAct
workflow'у, так что имя отражает доминирующий **tier** модели, а не
«любую LLM». Финальный синтез large-tier по-прежнему происходит *внутри*
активности `synthesize_answer` на этой же очереди (отдельной очереди
`kb-search-large` пока нет — она появляется в более поздней фазе).

**Действие оператора при апгрейде**: обновите `TEMPORAL_SEARCH_TASK_QUEUE`, если
он был прибит к старому значению, и перезапустите worker, чтобы он поллил
новое имя очереди. Workflow'ы в полёте на старой очереди дренируются на старом
worker'е; новые сабмиты идут на `kb-search-small`.

### Активности, зарегистрированные на `kb-search-small`
- Общие (`SEARCH_ACTIVITIES`): `coverage_check`, `synthesize_answer`.
  (Легаси-ReAct-активности `agent_reasoning_step`, `tool_execution`,
  `distill_observation` были удалены в переходе R7b.)
- Search-v2 (`SEARCH_V2_ACTIVITIES`): `plan_subquestions`,
  `retrieve_subquestion`, `rerank_sources`, `route_query`,
  `map_communities`, `map_community_partial`, `documents_for_communities`.

Оркестратор переиспользует `synthesize_answer` для финального ответа, так что
ни одна активность синтеза не дублируется.

## Очередь непрерывного wiki-редактора: kb-wiki

Worker хостит пул Worker'ов `kb-wiki`, выполняющий `WikiSweepWorkflow` и две его активности: `select_dirty_entities` (запрашивает Neo4j на сущности, помеченные `wiki_dirty=true`) и `write_entity_article` (генерирует и пишет пер-сущностную секцию статьи MediaWiki). Ingest помечает затронутые сущности `wiki_dirty` через best-effort-хук сразу после графовых записей; Temporal Schedule (`scripts/setup_wiki_schedule.py`) или admin-маршрут `POST /admin/wiki/rebuild` запускает sweep, который перегенерирует управляемую ботом секцию статьи MediaWiki для каждой грязной сущности из графа (заземлённую и со ссылками, без дрейфа) и пропускает неизменившиеся сущности через хэш подграфа. Фича включается опционально через `WIKI_ENABLED`. В отличие от любой другой очереди здесь, конфиг wiki-очереди живёт на `WikiSettings` (env-префикс `WIKI_`), а НЕ на `TemporalSettings` — имя очереди задаётся `WIKI_TASK_QUEUE` (default `kb-wiki`), а конкурентность лимитируется через `WIKI_ACTIVITY_CONCURRENCY` (default 4). Генерация статей едет по полосе synthesis LLMPool, так что делит тот же иерархический бюджет GPU, что и синтез поиска. **Действие оператора при апгрейде**: перезапустите worker, чтобы он поллил новую очередь `kb-wiki`.
