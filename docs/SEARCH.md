# Подсистема поиска — глубокий справочник

> ГЛУБОКИЙ повествовательный справочник о том, как запрос становится
> ответом. Для обзорных диаграмм потока (Mermaid + отрендеренный D2) см.
> [`SEARCH-FLOW.md`](SEARCH-FLOW.md); для однопараграфных описаний функций
> и быстрого справочника по переменным окружения см.
> [`FEATURES.md`](FEATURES.md#2-search). Использование/runbook:
> [`runbook/search-usage.md`](runbook/search-usage.md). Топология очередей:
> [`QUEUES.md`](QUEUES.md).

Поиск — это набор **надёжных воркфлоу Temporal**, запускаемых из
`src/api/routes/search_v2.py`. Каждый режим возвращает один и тот же
внутренний `SearchOutcome`, проецируемый на публичный `SearchResponse`
обработчиком маршрута (`_outcome_to_response`). Нигде в живом пути нет
открытого цикла ReAct «LLM выбирает следующий инструмент» — каждый
конвейер извлечения детерминирован; единственные LLM-вызовы — это
маршрутизатор, планировщик, шаги small-tier на каждое
сообщество/подвопрос и финальный синтезатор.

## Входной слой

`src/api/routes/search_v2.py` — **единственный** HTTP-слой поиска. Все
маршруты требуют `X-API-Key` и потребляют общий `SearchRequest`
(`src/models/search.py`: `query`, `top_k`, `history`, `answer_template`,
плюс обратно-совместимые поля фильтров). Режим выбирается *эндпоинтом*, а
не полем запроса.

**Поля запроса** (`SearchRequest`):

| Поле | Тип | По умолчанию | Эффект |
| --- | --- | --- | --- |
| `query` | `str` | — | вопрос |
| `top_k` | `int` | 10 | top-k чанков (переопределяет `AGENT_TOP_K`) |
| `history` | `list[ConversationTurn]` | `[]` | предыдущие ходы (управляются клиентом); пусто = single-shot, без контекстуализации |
| `answer_template` | `str \| None` | `None` | формирует ФОРМУ синтезированного ответа — именованный или инлайновый шаблон; пусто/None → дефолтная русскоязычная преамбула (поведение без изменений) — см. [Шаблоны ответа](#шаблоны-ответа-answer_template) |
| `synthesize` | `bool` | `True` | `False` пропускает финальный large-model синтез — `answer` возвращается `""`, всё остальное (`sources`, `citations`, `step_stats`, …) не меняется. Для клиента, который сам собирает ответ из `sources` и не хочет платить за синтез, который всё равно выбросит |

Остальные поля (`mode`, `department`, `doc_type_filter`, `created_after`
/ `created_before`, `response_type`, `include_references`, …) сохранены
для обратной совместимости и не потребляются plan-execute / GraphRAG
потоками.

| Эндпоинт | Воркфлоу | Очередь оркестрации | Tier синтеза |
| --- | --- | --- | --- |
| `POST /api/v1/search/local` | `SearchOrchestratorWorkflow` | `kb-search-small` | `kb-search-large` |
| `POST /api/v1/search/global` | `GlobalSearchWorkflow` | `kb-search-small` | `kb-search-large` (REDUCE) |
| `POST /api/v1/search/drift` | `DriftSearchWorkflow` | `kb-search-small` | `kb-search-large` (global REDUCE) |
| `POST /api/v1/search/auto` | `AutoSearchWorkflow` | `kb-search-small` | по выбранному потоку |
| `POST /api/v1/admin/communities/rebuild` | `CommunityBuildWorkflow` | `kb-graph-build` | н/д (офлайн-построение) |

Воркфлоу запускаются на `settings.temporal.search_task_queue`
(`kb-search-small`) с переиспользованием id `ALLOW_DUPLICATE`; маршрут
ожидает `handle.result()` и мапит результат. `SearchResponse.mode` несёт
эффективный режим (`local` / `global` / `drift`).

> Два MCP-слоя тоже добираются до поиска: `kb_search` из MCP-1 запускает
> `SearchOrchestratorWorkflow` (local plan-execute); MCP-2
> (`src/mcp/tools_server.py`) предоставляет атомарные инструменты
> извлечения in-process с **per-call** переопределениями — см.
> [Per-call depth/hops (слой MCP)](#per-call-depthhops-слой-mcp).

---

## Local — plan-execute (`SearchOrchestratorWorkflow`)

`src/workflow/search/orchestrator.py`. Тонкий координатор plan → fan-out →
coverage → rerank → synthesize. Единственные LLM-вызовы — это
предварительный планировщик и финальный синтезатор; всё между ними —
детерминированное извлечение.

```
question (+ optional history)
  │
  ▼  0. contextualize_query        (only if history present + enabled)
  │     follow-up → standalone question  (model_copy(query=…))
  ▼  1. plan_subquestions          (small "plan" model)
  ├─ "sub A" ─▶ SubQueryRetrievalWorkflow (child) ─┐
  ├─ "sub B" ─▶ SubQueryRetrievalWorkflow (child) ─┤  asyncio.gather
  └─ …(≤ max_subqueries, default 5)               ─┘  (parallel)
                                                      │
                  2. merge + dedup by chunk_id  ◀─────┘
                                                      │
                  3. coverage gate (bounded loop)
                                                      │
                  4. rerank_sources (bge cross-encoder, ONE pass)
                                                      │
                  5. synthesize_answer (large tier, kb-search-large)
                                                      ▼  SearchOutcome (mode="local")
```

### 0. Контекстуализация (история диалога)

Когда `params.history` непустой **и** `params.contextualize_enabled`
(разрешается при запуске из `AGENT_CONVERSATION_HISTORY_ENABLED`),
оркестратор СНАЧАЛА запускает активность `contextualize_query`
(`activities/contextualize.py`). Она переписывает follow-up в
самостоятельный вопрос, используя недавние ходы (small-модель уровня
`route`, `/no_think`), ограниченные `history_max_turns` /
`history_max_chars`, после чего `params.model_copy(query=…)` заставляет
*весь* нижестоящий конвейер использовать самостоятельный запрос без иных
правок.

- **Безопасно к replay**: решение о включении фиксируется в params при
  запуске (`contextualize_enabled`), а не читается из конфига внутри
  воркфлоу, так что изменение конфига не может сломать переигрываемый
  воркфлоу.
- **Fail-open**: пустая история, отсутствие пригодных ходов или любая
  LLM-ошибка возвращают исходный запрос без изменений. Инертна
  (пропускается), когда история пуста, так что single-shot вызывающие не
  затрагиваются.
- Управляемая клиентом история (без серверных сессий) сохраняет поиск
  stateless.

### 1. План — `plan_subquestions`

Small-модель «plan» раскладывает составной вопрос на атомарные
подвопросы (парсинг нумерованных / маркированных списков / JSON-массива,
с ограничением `max_subqueries`). Атомарные вопросы и **любой** сбой
планировщика возвращают `[query]` (fail-safe — поиск никогда не
блокируется на планировщике).

### 2. Fan-out — `SubQueryRetrievalWorkflow`

По одному дочернему воркфлоу на подвопрос, выполняемые параллельно через
`asyncio.gather` над `execute_child_workflow` (детерминированные id
дочерних `…-sub-{i}`). Каждый дочерний (`subquery_wf.py`) вызывает
единственную активность `retrieve_subquestion` и дедуплицирует свои
источники по `chunk_id`. Никакого агентного / выбирающего инструмент
LLM-вызова — план зафиксировал инструменты заранее. См.
[Детерминированный конвейер извлечения](#детерминированный-конвейер-извлечения).

Оркестратор объединяет источники всех дочерних и дедуплицирует по
`chunk_id` (`merge_subquery_sources`). На каждый дочерний записывается
одна step-статистика для телеметрии (переиспользует форму
`AgenticStepStatDict`, так что модель ответа мапится без изменений).

### 3. Проверка покрытия (ограниченный цикл)

После merge, пока `coverage_check_enabled` и остаётся бюджет раундов
(`max_coverage_rounds`, по умолчанию 1), оркестратор запускает ОДНУ
проверку `coverage_check` на small-tier по собранным доказательствам
(`build_evidence(merged)`). Чистые хелперы
`should_run_coverage_round` / `build_evidence`
(`src/workflow/search/_coverage.py`) решают, по какой ветке идти:

- **complete** (нет пробела) → переход к rerank.
- **named gap + остаётся бюджет** → выдать этот пробел как ОДИН
  дополнительный `SubQueryRetrievalWorkflow` (id дочернего `…-cov-{n}`),
  пере-слить его источники (dedup по `chunk_id`), уменьшить бюджет,
  добавить step-статистику покрытия и пере-проверить (по-прежнему
  ограниченно).

Это добавляет обнаружение пробелов, которого простому fan-out не хватает
для многосоставных вопросов.

- **Ограничено**: максимум `max_coverage_rounds` дополнительных раундов —
  никакого бесконечного цикла, даже если пробел сохраняется.
- **Fail-open**: любая ошибка в проверке ИЛИ в дополнительном раунде
  извлечения → переход прямо к синтезу. Активность `coverage_check` сама
  по себе fail-open (возвращает `complete=True` на собственных внутренних
  ошибках), так что капризный вызов полноты никогда не сможет заблокировать
  ответ.

### 4. Единый rerank — `rerank_sources`

Перед синтезом оркестратор со-ранжирует **объединённые** графовые и
векторные чанки за ОДИН проход bge cross-encoder
(`BAAI/bge-reranker-v2-m3`, переиспользуется из
`src/retrieval/reranker.py`, кэшируется в процессе), так что две модальности
оцениваются друг против друга, а не конкатенируются по порядку ретривера.
Чанк, всплывающий из обеих модальностей, дедуплицируется (первый
побеждает) чистым хелпером `prepare_rerank_pool` (`activities/rerank.py`)
перед reranking, так что загрузку модели можно юнит-тестировать без
чекпойнта ~1 ГБ. `top_n` из `TEMPORAL_RERANK_TOP_N` (по умолчанию 5).
Выполняется на малой очереди поиска.

- **Fail-open**: любая ошибка rerank → откат к объединённому пулу,
  **ограниченному** до `rerank_top_n` (чистый `cap_synth_sources`).
  Ограничение важно: неограниченный откат мог бы превысить таймаут
  start_to_close у `synthesize_answer`. Отображаемые `SearchOutcome.sources`
  остаются ПОЛНЫМ объединённым пулом (цитаты без изменений); обрезается
  только *контекст* синтеза.
- **`synthesize=False` → rerank НЕ выполняется.** Единственный потребитель
  результата rerank — `build_synthesize_call`; отображаемые
  `SearchOutcome.sources` — это объединённый пул в обоих режимах (см. выше),
  поэтому пропуск rerank не меняет ничего наблюдаемого для клиента, но
  убирает лишний проход cross-encoder'а по всему пулу (GPU, таймаут 3 мин)
  ради результата, который никто не читает. Инвариант «`sources` одинаковы
  при `synthesize=True` и `False`» закреплён тестом
  `test_outcome_sources_identical_with_and_without_synthesis`.

### 5. Синтез — large-tier

`synthesize_answer` закреплён за `TEMPORAL_LARGE_TASK_QUEUE`
(`kb-search-large`) с `use_synthesis_llm=True` (большой
`build_synthesis_llm`). Отдельный пул `Worker` с низкой конкурентностью
(`TEMPORAL_LARGE_ACTIVITY_CONCURRENCY`, по умолчанию 2) опрашивает эту
очередь, так что тяжеловесная модель синтеза не задавливается
параллельными сессиями. Спецификация вызова (очередь + tier) строится
чистым хелпером `build_synthesize_call`, юнит-тестируемым вне Temporal.

`SearchOutcome` возвращает ответ, полный объединённый пул источников,
различные `doc_ids` (`distinct_doc_ids`, пропуская частичные результаты
сообществ), статистику по шагам, цитаты, неопределённости и латентность.

### Шаблоны ответа (`answer_template`)

Опциональное поле запроса `answer_template` (`SearchRequest`,
`src/models/search.py`) позволяет вызывающему задать **ФОРМУ**
синтезированного ответа. Оно протягивается request →
`_local_params` / `_global_params` (`search_v2.py`) →
`OrchestratorParams` / `GlobalSearchParams` → `SynthesizeParams` →
активность `synthesize_answer`, которая строит инструкцию синтеза через
`src/retrieval/answer_template.py::build_query` (шаблон обрамляется вокруг
вопроса). Поскольку синтез общий, поле применяется ко **всем режимам**
(local / global / drift).

`answer_template` — это либо:

- **именованный** шаблон — разрешается из
  `prompts/answer_templates/<name>.md` (в комплекте идёт шаблон
  `dossier`); например `answer_template: "dossier"`;
- либо **инлайновая** строка-шаблон, используемая дословно.

- **Дефолт**: пусто/None (по умолчанию) → используется существующая
  русскоязычная преамбула (`ru_query`); поведение для всех текущих
  вызывающих не меняется.
- **Безопасность**: имя с разделителями путей, неизвестные имена или
  имена длиннее 64 символов трактуются как инлайн (произвольный файл
  никогда не читается — защита от path-traversal); шаблоны ограничены
  8000 символами.
- Структурированный / JSON-вывод (возврат разобранного объекта) отложен
  (вариант b, не реализован).

### Детерминированный конвейер извлечения

`retrieve_subquestion` (`activities/retrieve.py`) выполняет
ФИКСИРОВАННУЮ последовательность инструментов для одного подвопроса,
переиспользуя `atomic_tools.dispatch` (тот же путь кода, что и у
MCP-сервера). Сбой в одном инструменте логируется и **не** топит
активность — то, что нашли другие инструменты, всё равно возвращается.

`_PIPELINE = (vector_search, graph_search, find_entity_by_name)`, затем
засеянный `graph_walk`:

| Инструмент | Бэкенд | Возвращает | Примечания |
| --- | --- | --- | --- |
| `vector_search` | Milvus (HNSW) | top-k чанков по сходству эмбеддингов | плотный baseline; доступен всегда |
| `graph_search` | Neo4j нативный векторный kNN по эмбеддингам сущностей + `LLMSynonymRetriever` | найденные сущности + соседи (`depth` triplet-hops) + связанные чанки | сопоставление сущностей — это **индексированный** нативный векторный kNN (масштабируется на большой граф) плюс один вызов small-LLM для синонимов; сущности ER-канонизированы при приёме |
| `find_entity_by_name` | Neo4j полнотекстовый индекс по `__Entity__.name` | сущности по (частичному) имени | ловит опечатки / частичные имена, которые `graph_search` может вытеснить из ранжирования |
| `graph_walk` | Neo4j переменной длины `(e)-[*1..hops]-` | ограниченная окрестность (≤50 узлов / ≤100 рёбер) | явный N-hop обход; **с двойным засевом** (ниже) |

**Глубина соседей `graph_search`.** `graph_search` передаёт
`depth = settings.agent.graph_search_path_depth`
(`AGENT_GRAPH_SEARCH_PATH_DEPTH`, по умолчанию 1, ограничение 1–3) в
`path_depth` ретривера. Глубина 1 = найденные узлы + непосредственные
связи; поднимите, чтобы расширить графовый контекст без изменения кода.
(Исторически это было жёстко закреплено на 1; теперь это операторская
ручка, а также per-call MCP-переопределение.) Ширина кандидатов —
`graph_similarity_top_k` (`AGENT_GRAPH_SIMILARITY_TOP_K`, по умолчанию 20),
так что именованная сущность не вытесняется из ранжирования на большом
графе.

**Двойной засев обхода (dual walk-seed).** После того как `graph_search`
(и `find_entity_by_name`) отработали, активность детерминированно
засевает ограниченный `graph_walk` — без LLM-выбора инструмента. Чистый
хелпер `top_entity_name` выбирает топовую сущность из наблюдения
`graph_search` (сущности приходят в порядке ранга сходства), а
`_walk_seeds` решает, какими будут засевы:

- **single-seed** (`graph_walk_dual_seed=False`): топовая сущность
  graph_search, иначе топовая сущность полнотекста.
- **dual-seed** (по умолчанию, `AGENT_GRAPH_WALK_DUAL_SEED=True`):
  объединение *обеих* (дедуплицированное, graph_search первым), так что
  сущность, найденная полнотекстом (частичное имя / опечатка), всё равно
  вносит свою окрестность, даже когда `graph_search` уже что-то вернул.

Каждый засев диспетчеризует `graph_walk(start_entity, hops=graph_walk_hops)`;
чанки обхода сливаются в накопленные источники (тот же набор `seen` по
chunk_id). Под флагом `AGENT_GRAPH_WALK_ENABLED` (по умолчанию вкл).
**Fail-open на каждый засев**: сбой парсинга, отсутствующий засев или
ошибка хранилища пропускают этот обход и возвращают остальное без
изменений — активность никогда не бросает исключение на обходе. Поскольку
этот парсинг недетерминирован, он живёт в АКТИВНОСТИ, а не в теле
`@workflow.run`.

`graph_walk` **жёстко ограничен** (никогда не безграничен), что
обеспечивается и в Cypher, и в маппинге строк: `hops ≤ GRAPH_WALK_MAX_HOPS`
(3), `≤ GRAPH_WALK_MAX_NODES` (50), `≤ GRAPH_WALK_MAX_EDGES` (100). Один
ограниченный Cypher (`MATCH (e {name:$name})-[r*1..hops]-(m)` с серверным
`LIMIT`, fallback без APOC) не даёт многошаговому обходу взорвать контекст
синтеза. Он диспетчеризуемый, но НЕ входит в фиксированный `_PIPELINE`
(ему нужен явный `start_entity`); именно засев выше его активирует.

---

## Global — GraphRAG map-reduce (`GlobalSearchWorkflow`)

`src/workflow/search/global_wf.py`. Отвечает на вопросы уровня корпуса /
тематические через MAP-REDUCE по офлайн-**отчётам** сообществ, не
извлекая отдельные чанки.

```
question (+ optional history)
  │
  ▼  0. contextualize_query        (if history + enabled; skipped under drift)
  ▼  1. map_communities — SELECT which communities to map over
  │       strategy: lexical | semantic | descent  (capped at max_communities)
  ├─ community 1 ─▶ map_community_partial (small tier) ─┐
  ├─ community 2 ─▶ map_community_partial (small tier) ─┤  gather (fan-out bounded by LLM_POOL_N)
  └─ …                                                 ─┘
        off-topic communities self-drop ('НЕТ' → score 0)
                                                         │
        surviving partials → SerializedNode             │
        (chunk_id = "community:<id>")                    │
                                                         ▼
  2. documents_for_communities (doc_ids behind surviving communities)
  3. REDUCE: synthesize_answer ONCE  (large tier, kb-search-large)
                                                         ▼  SearchOutcome (mode="global")
```

- **MAP** (`map_community_partial`, `activities/global_search.py`)
  выполняется на `kb-search-small` (small-модель уровня `retrieve`);
  конкурентность ограничена глобальным семафором `LLM_POOL_N`. Каждое не
  относящееся к теме сообщество само сообщает буквальное `НЕТ` → score 0
  и отбрасывается (`is_relevant_partial`).
- **REDUCE** — это существующий `synthesize_answer`, закреплённый за
  большой очередью ровно как в локальном потоке (`build_reduce_call`).
- Сборка map-spec / reduce-context / reduce-call — чистые хелперы
  (`build_map_specs`, `partials_to_sources`, `build_reduce_call`) —
  юнит-тестируются без живого окружения Temporal.
- `documents_for_communities` разрешает `doc_ids`, стоящие за уцелевшими
  сообществами, для ссылок на документы в ответе.
- **Fail-safe**: ошибки хранилища / LLM дают пустые результаты (ответ без
  доказательств), а не бросают исключение; `_coerce_global_params`
  защищает от data-конвертера, вернувшего обычный dict.

### Стратегии отбора сообществ

`map_communities` переключается по `params.community_selection`
(`AGENT_COMMUNITY_DYNAMIC_SELECTION`, по умолчанию `lexical`):

- **`lexical`** (`_map_communities_lexical` + чистый `rank_summaries`) —
  читает все `:Community.summary` для уровня, ранжирует по пересечению
  слов запроса, ограничивает до `limit`. Детерминирован, без LLM, без
  векторной зависимости. Ничьи разрешаются по порядку Cypher (сначала
  крупнейшее сообщество).
- **`semantic`** (`select_communities_semantic`) — эмбеддит запрос, затем
  kNN по индексу `community_report_vec` (`db.index.vector.queryNodes`),
  ближайшие первыми, ограничено до `limit`.
- **`descent`** (`select_communities_descent`) — динамический отбор
  GraphRAG: эмбеддит запрос, начинает с **самого грубого** уровня (0),
  ранжирует фронтир по косинусу запроса против `report_vec`, спускается по
  `PARENT_OF` в релевантных детей и собирает **самые мелкие** релевантные
  сообщества вплоть до `budget`. С защитой от циклов; если ничто не
  достигает листа (например, существует только уровень 0), откатывается к
  топ-`budget` корням по косинусу.

Обе векторные стратегии **fail-open к lexical** при пустом результате или
любой ошибке, так что переключение ручки отбора никогда не затвердеет в
сбой.

### Сообщества строятся офлайн

Сообщества + отчёты создаются `CommunityBuildWorkflow` (ниже) на
`kb-graph-build`, полностью отвязанные от горячего пути запроса.

---

## Drift — сначала local, потом global (`DriftSearchWorkflow`)

`src/workflow/search/router_wf.py`. **Ограниченный** механизм
local-затем-global — ровно один локальный проход + один глобальный проход,
без открытого цикла.

```
question (+ optional history)
  │
  ▼  0. contextualize_query ONCE  (then children get history CLEARED)
  ▼  1. SearchOrchestratorWorkflow (child, id …-local)   → concrete chunk evidence
  ▼  2. GlobalSearchWorkflow (child, id …-global, drift_mode=True)
  │       seeded with the local sources
  │            └─ REDUCE merges local sources AHEAD of community partials
  │               (dedup by chunk_id) so local evidence leads
  │
  ├─ global fails / times out ─▶ _drift_local_fallback(local)
  │                               degrade to the local answer
  ▼  merge local + global doc_ids
     SearchOutcome (mode="drift" either way)
```

- **Контекстуализировать один раз**: drift сам переписывает follow-up,
  затем передаёт самостоятельный запрос ОБОИМ детям с `history=[]`, так
  что ни один ребёнок не перезапускает контекстуализацию. Пустая история ⇒
  пропуск (дети ведут себя как раньше).
- **Засев drift**: глобальный ребёнок диспетчеризуется с `drift_mode=True`
  и локальными `outcome.sources` как `drift_seed`. В режиме drift контекст
  REDUCE — это `merge_subquery_sources([drift_seed, partials])` — локальные
  источники ведут, частичные результаты сообществ расширяют,
  дедуплицированы по chunk_id. Результат помечается `"drift"`.
- **Грациозный откат**: если глобальный ребёнок бросает исключение
  (ChildWorkflowError / таймаут / сбой активности), `_drift_local_fallback`
  возвращает локальный результат с сохранённым `mode` = `"drift"` — запрос
  деградирует до локального ответа вместо падения. Это страховочная сетка
  drift.
- При успехе финальные `documents` результата — это объединение local +
  global doc_ids с сохранением порядка (`merge_doc_ids`).

---

## Auto — маршрутизируемая диспетчеризация (`AutoSearchWorkflow`)

`src/workflow/search/router_wf.py`. Выбирает режим, затем диспетчеризует
соответствующий воркфлоу как дочерний.

1. `route_query` (`activities/route.py`, small-модель уровня `route`)
   классифицирует вопрос в `local` / `global` / `drift`. Промпт просит
   одно слово; чистый хелпер `classify_route` распознаёт ПЕРВУЮ известную
   метку в (возможно обёрнутом) ответе.
2. `dispatch_for_route` (чистый) мапит метку → хэндл воркфлоу
   (`local` → orchestrator, `global` → global, `drift` → drift), по
   умолчанию **local** для любой неизвестной метки.
3. Диспетчеризация как дочернего воркфлоу (`…-local` / `…-global` /
   `…-drift`). `get_state` раскрывает выбранный маршрут для наблюдаемости.

**Fail-safe маршрутизация**: `route_query` возвращает `route="local"` при
ЛЮБОЙ ошибке или нераспарсиваемом ответе, и `dispatch_for_route` тоже по
умолчанию выбирает local — так что капризный маршрутизатор деградирует к
безопасному, самому дешёвому, всегда заземлённому на чанках потоку.

---

## Офлайн-построение сообществ (`CommunityBuildWorkflow`)

`src/workflow/search/community_wf.py` +
`src/workflow/search/activities/community.py`. **Полностью отвязано /
офлайн** — выполняется на собственной очереди `kb-graph-build`, НИКОГДА на
горячем пути запроса. Запускается через
`POST /api/v1/admin/communities/rebuild` (fire-and-forget, возвращает id
воркфлоу); Temporal Schedule мог бы вызывать тот же воркфлоу (в репозитории
пока ничего не подключено).

```
1. detect_communities_activity   (GDS Leiden over __Entity__)
     max_levels == 1 → single-level (detect_communities)
     max_levels  > 1 → full dendrogram hierarchy (detect_hierarchy)
   → MERGE :Community {id, level, member_count} + (:__Entity__)-[:IN_COMMUNITY]->
   → coarse→finer (:Community)-[:PARENT_OF]->(:Community) for hierarchy
   → ensure the community_report_vec index ONCE (fail-open → degrades to lexical)
2. summarize fan-out, FINEST-level-first (bounded by community_summary_parallelism)
   per community: structured report {title, summary, findings:[{statement, importance}]}
     level 0  → from member entities/relations
     level>0  → from CHILD reports (composed bottom-up; falls back to members)
   → embed (title + summary) → MERGE report / title / summary / report_vec on :Community
```

- **Обнаружение** проецирует подграф `__Entity__` в in-memory GDS-граф и
  запускает **GDS Leiden** (`gds.leiden.stream`). Сообщества меньше
  `community_min_size` отбрасываются. **Fail-safe**: любая ошибка
  GDS/Cypher (или отсутствие хранилища) → `[]`, никогда не пробрасывается
  через активность.
- **Резюме** создаются LLM **small-tier** (`build_llm("retrieve")` — никогда
  большой моделью синтеза), парсятся из JSON толерантно (`_parse_report`
  всегда выдаёт *что-то* для сохранения).
- **Упорядочивание по уровням**: воркфлоу группирует спецификации по
  уровню и обрабатывает самые мелкие первыми (`group_specs_by_level`), так
  что дочерние отчёты грубого родителя уже существуют, когда он
  компонует собственный (`_CHILD_REPORTS_CYPHER`). Барьер уровня означает
  «этот уровень завершён», а не «все дети присутствуют»; частичные сбои
  деградируют, а идемпотентные перезапуски залечивают.
- **Инкрементально**: сообщество, перенесённое без изменений из прошлого
  построения (`needs_report=False`), пропускается — его отчёт уже сохранён.
  Перезапуск идемпотентен (MERGE по ключам `(id, level)`), обновляя
  резюме / состав без дублирования узлов.

**Схема `:Community`** (аддитивная — ни одна существующая
метка/свойство не затрагивается):
- Метка `:Community`; свойства `id` (Leiden communityId), `level` (0 =
  самый грубый), `member_count`, `title`, `summary`, `report`
  (структурированный отчёт в JSON), `report_vec` (нативный эмбеддинг,
  может быть не задан при сбое эмбеддинга), `summarized_at`.
- Связи `(:__Entity__)-[:IN_COMMUNITY]->(:Community)` и дендрограмма
  coarse→finer `(:Community)-[:PARENT_OF]->(:Community)`.
- Ограничение уникальности `community_key` на `(id, level)` обеспечивает
  MERGE; range-индексы на `Community.level` и `Chunk.doc_id` обеспечивают
  глобальное чтение / обход community→document (`ensure_community_indexes`).

> **Заметка о GDS**: Cypher `gds.graph.project` / `gds.leiden.stream` /
> `gds.graph.drop` нацелен на API Neo4j GDS 2.x, но **не верифицирован
> против живой установки GDS** (в dev-песочнице нет Neo4j/GDS — тесты
> мокают хранилище + строки GDS). Все строки GDS/Cypher изолированы как
> константы в начале `src/graph/communities.py`, чтобы фикс версии был
> изменением одного файла. Провалидируйте, прежде чем полагаться на это в
> продакшене.

---

## Per-call depth/hops (слой MCP)

HTTP-эндпоинты `/api/v1/search/*` НЕ имеют per-request поля depth/hops —
`SearchRequest` раскрывает только `query`, `top_k`, `history`, а локальный
конвейер читает depth/hops из `AgentSettings`.

**Per-call** переопределения живут на атомарных инструментах MCP-2
(`src/mcp/tools_server.py`), где вызывающий управляет ретриверами напрямую:

- `graph_search(query, depth=1)` — `depth` (1–3, с ограничением) задаёт
  расширение соседей на вызов (мапится в `path_depth` ретривера).
- `find_neighbours(entity_name, hops=1)` — `hops` (1–3, с ограничением)
  задаёт глубину соседей на вызов.
- `graph_walk(start_entity, hops=2, rel_filter=None)` — `hops` на вызов,
  жёстко ограничен `GRAPH_WALK_MAX_HOPS`.

Они оборачивают те же `atomic_tools`, что использует детерминированный
конвейер, так что дефолты конфига (`AGENT_GRAPH_SEARCH_PATH_DEPTH`,
`AGENT_GRAPH_WALK_HOPS`) и per-call MCP-аргументы делят один бэкенд.

---

## Что деградирует против жёсткого падения

Живой путь сильно fail-open, так что капризная зависимость никогда не
блокирует ответ; немногочисленные точки жёсткого падения — это сами
обработчики маршрутов.

| Компонент | При сбое |
| --- | --- |
| `contextualize_query` | использовать сырой запрос (деградация) |
| `plan_subquestions` | `[query]` — единственный подвопрос (деградация) |
| один инструмент извлечения | логируется, результаты других инструментов всё равно возвращаются (деградация) |
| засев `graph_walk` | пропустить этот обход, сохранить остальное (деградация на засев) |
| `coverage_check` / доп. раунд | переход прямо к синтезу (деградация) |
| `rerank_sources` | объединённый пул, ограниченный до `rerank_top_n` (деградация) |
| `route_query` | по умолчанию `local` (деградация) |
| глобальный проход drift | `_drift_local_fallback` → локальный ответ, mode остаётся `drift` (деградация) |
| `map_communities` semantic/descent | откат к lexical (деградация) |
| глобальный MAP / хранилище сообществ | пусто → ответ без доказательств (деградация) |
| обнаружение / резюмирование сообществ | `[]` / не сохранено, следующее построение примирит (деградация) |
| `synthesize_answer` | пробрасывается → маршрут возвращает HTTP 500 (**жёсткое падение**) |
| Temporal start/await в маршруте | маршрут возвращает HTTP 500 (**жёсткое падение**) |

---

## Ручки конфигурации

Ручки поиска живут на `AgentSettings` (префикс `AGENT_`, `src/config.py`)
и `TemporalSettings` (префикс `TEMPORAL_`). См.
[`FEATURES.md`](FEATURES.md#config-quick-reference-new-feature-env-vars)
для подмножества новых функций.

| Переменная окружения | Настройка | По умолчанию | Эффект |
| --- | --- | --- | --- |
| `AGENT_TOP_K` | `top_k` | 10 | дефолтный top-k чанков (request `top_k` переопределяет) |
| `AGENT_MAX_SUBQUERIES` | `max_subqueries` | 5 | ограничивает параллельный fan-out подзапросов (и стоимость планировщика) |
| `AGENT_COVERAGE_CHECK_ENABLED` | `coverage_check_enabled` | true | включает проверку покрытия в оркестраторе |
| `AGENT_MAX_COVERAGE_ROUNDS` | `max_coverage_rounds` | 1 | ограничивает доп. раунды подвопросов покрытия (0–3) |
| `AGENT_CONVERSATION_HISTORY_ENABLED` | `conversation_history_enabled` | true | контекстуализирует follow-up-ы (инертна без истории) |
| `AGENT_HISTORY_MAX_TURNS` | `history_max_turns` | 6 | недавние ходы, подаваемые в контекстуализацию |
| `AGENT_HISTORY_MAX_CHARS` | `history_max_chars` | 4000 | бюджет символов для окна истории |
| `AGENT_GRAPH_WALK_ENABLED` | `graph_walk_enabled` | true | включить детерминированный засев `graph_walk` |
| `AGENT_GRAPH_WALK_HOPS` | `graph_walk_hops` | 2 | запрашиваемое число hops обхода (ограничено `GRAPH_WALK_MAX_HOPS`=3) |
| `AGENT_GRAPH_WALK_DUAL_SEED` | `graph_walk_dual_seed` | true | засевать обход из сущности graph_search + полнотекста |
| `AGENT_GRAPH_SEARCH_PATH_DEPTH` | `graph_search_path_depth` | 1 | глубина соседей `graph_search` (1–3) |
| `AGENT_GRAPH_SIMILARITY_TOP_K` | `graph_similarity_top_k` | 20 | число кандидатов графового ретривера |
| `AGENT_GLOBAL_MAX_COMMUNITIES` | `global_max_communities` | 20 | ограничивает сообщества, входящие в глобальный MAP |
| `AGENT_COMMUNITY_MAX_LEVELS` | `community_max_levels` | 1 | глубина иерархии Leiden для материализации (1 = один уровень) |
| `AGENT_COMMUNITY_DYNAMIC_SELECTION` | `community_dynamic_selection` | lexical | отбор global/drift: `lexical`\|`semantic`\|`descent` |
| `TEMPORAL_SEARCH_TASK_QUEUE` | `search_task_queue` | `kb-search-small` | очередь, где живёт оркестратор + дочерние подзапросы |
| `TEMPORAL_LARGE_TASK_QUEUE` | `large_task_queue` | `kb-search-large` | выделенная очередь для large-tier `synthesize_answer` |
| `TEMPORAL_LARGE_ACTIVITY_CONCURRENCY` | `large_activity_concurrency` | 2 | низкий лимит, чтобы синтез не задавливался |
| `TEMPORAL_RERANK_TOP_N` | `rerank_top_n` | 5 | top-N bge cross-encoder в синтез |
| `TEMPORAL_GRAPH_BUILD_TASK_QUEUE` | `graph_build_task_queue` | `kb-graph-build` | очередь офлайн-построения сообществ |
| `TEMPORAL_GRAPH_BUILD_ACTIVITY_CONCURRENCY` | `graph_build_activity_concurrency` | 2 | низкий лимит, чтобы всплеск резюме при перестройке не залил прокси |
| `TEMPORAL_COMMUNITY_SUMMARY_PARALLELISM` | `community_summary_parallelism` | 4 | fan-out резюмирования по сообществам |
| `TEMPORAL_COMMUNITY_MIN_SIZE` | `community_min_size` | 3 | сообщества меньше этого игнорируются |

Tier-ы моделей `route` / `plan` / `retrieve` / синтеза настраиваются через
`LITELLM_ROLE_TIERS` (дефолты из `_DEFAULT_ROLE_TIERS`); `route`, `plan`,
`retrieve` и роли community/contextualise мапятся на small-tier, синтез —
на large-tier.

---

## Статус модулей

- **Активно**: `src/retrieval/reranker.py` (`BAAI/bge-reranker-v2-m3`,
  единый rerank), `src/retrieval/atomic_tools.py` (бэкенд инструментов),
  `src/retrieval/llm.py` (`build_llm` роль→tier).
- **Не подключено**: `src/retrieval/hybrid.py` — кандидат-эксперимент
  BM25+dense; прогоните бенчмарк через `tests/eval/` перед интеграцией.
