# Архитектура

`kb-llamaindex` — это RAG-сервис с несколькими хранилищами и
надёжным (durable) исполнением. Он принимает документы на любом языке,
нормализует граф знаний к русскому языку (имена сущностей + описания +
связи), сохраняя при этом текст чанков на языке оригинала ради точности
цитирования, и предоставляет четыре режима поиска (`local` / `global` /
`drift` / `auto`) с возрастающей агентной сложностью.

Этот документ — **высокоуровневая карта**. Более глубокие документы
детализируют каждый слой:

| Документ | Что охватывает |
|---|---|
| [`INGEST.md`](INGEST.md) | Конвейер приёма (ingest) — активности, очереди, staging по схеме claim-check, деградация |
| [`SEARCH-FLOW.md`](SEARCH-FLOW.md) / [`SEARCH.md`](SEARCH.md) | Четыре режима поиска + детерминированный конвейер извлечения + GraphRAG map-reduce |
| [`QUEUES.md`](QUEUES.md) | Очереди задач Temporal + лимиты конкурентности по каждой очереди |
| [`FEATURES.md`](FEATURES.md) | Каждая функция: что / зачем / как + управляющая переменная окружения |
| [`MODELS.md`](MODELS.md) | Рекомендации по моделям для каждой роли + процедура замены |
| [`DEPLOYMENT.md`](DEPLOYMENT.md) | Docker-стек + эксплуатация |
| [`runbook/`](runbook/) | Эксплуатационные сценарии (mcp, search-usage, multimodel, analytics, wikibase, wiki-editor, er-native-vector-knn) |

> Диаграмма: [`diagrams/system_architecture.svg`](diagrams/system_architecture.svg)
> (исходник [`diagrams/system_architecture.d2`](diagrams/system_architecture.d2)).
> Диаграммы отдельных потоков лежат рядом со своими документами (`diagrams/ingest_flow.*`,
> `diagrams/search_modes.*`, `diagrams/kb_search_flow.*`).

---

## 1. Компоненты с высоты птичьего полёта

Два долгоживущих процесса плюс набор stateful-бэкендов:

- **API** (`src/api/`) — FastAPI на `:8000`. Тонкий HTTP-слой
  (`/search/*`, `/ingest`, `/documents/{id}`, `/admin/*`), аутентификация
  через `X-API-Key`. Маршруты валидируют, запускают воркфлоу Temporal и
  стримят результаты — бизнес-логика живёт в модулях
  workflow/activity/graph, а не в маршрутах.
- **Воркер Temporal** (`src/workflow/worker.py`) — содержит определения
  воркфлоу и все активности в нескольких пулах `Worker` (по одному на
  каждую очередь задач) в рамках одного процесса. Надёжное исполнение:
  автоматические повторы, heartbeat-ы, идемпотентные активности,
  безопасный к replay код.
- **Серверы MCP** (`src/mcp/`) — два опциональных слоя (stdio + HTTP/SSE),
  которые предоставляют поиск внешним LLM-клиентам (OpenWebUI / Claude
  Desktop / Cursor): **MCP-1** `:9001` (`kb_search`, запускает воркфлоу
  поиска) и **MCP-2** `:9002` (8 атомарных in-process инструментов
  извлечения). См. [`runbook/mcp.md`](runbook/mcp.md).

Сквозные компоненты:

- **Прокси LiteLLM** `:4000` — единый шлюз к моделям. Все вызовы чата и
  эмбеддингов идут через него (модели по ролям: extraction / judge /
  search / synthesis; многоязычные эмбеддинги).
- **LLMPool** (`src/retrieval/llm_pool.py`) — per-process регулятор
  конкурентности, который стоит *выше* лимитов очередей Temporal:
  потолок на каждый tier (small = ёмкость GPU, large = бюджет API) плюс
  отдельные дорожки (lanes) на каждую роль, захватываемые сначала по
  дорожке, затем глобально по tier. Лимиты Temporal заданы с запасом, так
  что именно пул является реальным арбитром конкурентных LLM-вызовов.

---

## 2. Хранилища данных — что в каждом

| Хранилище | Что хранит | Подключение (по умолчанию) |
|---|---|---|
| **Milvus** | Векторный индекс чанков — одна запись на чанк (`id, text, embedding, metadata`). `text` — это чанк **на языке оригинала**; эмбеддинги получены из многоязычной embed-модели через LiteLLM. ANN-индекс — **HNSW** (`MILVUS_INDEX_TYPE`, `FLAT` для точного). | `MILVUS_HOST:MILVUS_PORT` (`localhost:19530`); коллекция `MILVUS_COLLECTION` |
| **Neo4j** | Property-граф **и** два нативных индекса в одном хранилище: узлы `:__Entity__:<Type>` + типизированные связи (имена/описания **на русском** после слияния; связи несут осмысленный `weight` = счётчик co-occurrence + дискретные `tags`), узлы `:Chunk`, связанные через `(:Chunk)-[:MENTIONS]->(:__Entity__)`, **нативный векторный индекс** по эмбеддингам сущностей (`graph_search` kNN; плюс `er_vec` для ER на нативных векторах), **полнотекстовый индекс** по `__Entity__.name` (`find_entity_by_name`), а также иерархия `:Community` + отчёты (`community_report_vec`) для глобального поиска. | `NEO4J_URI` (`bolt://localhost:7687`) |
| **Postgres** | Таблица заданий/статусов `documents` (doc_id → статус → метаданные) и `ingest_metrics` (длительности по каждой активности + теги моделей по ролям для аналитики). | `POSTGRES_*` |
| **MinIO** | Загруженные исходные файлы (отдаются обратно через `GET /documents/{id}`) **и** staging-блобы по схеме claim-check — тяжёлое состояние приёма (распарсенные узлы, KG, слитые сущности), сериализованное через pickle и передаваемое между активностями по URI. | `MINIO_*` (консоль `:9001`, S3 API `:9000`) |
| **Wikibase / MediaWiki** | Курируемый **канонический якорь**: self-hosted Wikibase Item на каждую сущность (`push_wikibase`, opt-in) + страницы статей MediaWiki на каждую сущность, создаваемые непрерывным редактором wiki. WDQS предоставляет SPARQL-эндпоинт. | контейнеры `wikibase` / `wdqs` |

Вспомогательная инфраструктура: **etcd** + **MinIO** обслуживают Milvus;
**wikibase-mysql** (MariaDB) обслуживает Wikibase; **temporal**
(+ **temporal-ui** `:8080`) — бэкенд надёжного исполнения; **Prometheus** +
**Grafana** — наблюдаемость. См. [`DEPLOYMENT.md`](DEPLOYMENT.md) /
`docker-compose.yml`.

**Прод-топология:** `Dockerfile` + `docker-compose.prod.yml` поднимают в
одном compose всё приложение (API + воркер) вместе с бэкендами и redis,
**исключая** litellm/ollama — они внешние и доступны через
`LITELLM_BASE_URL`. Wikibase прячется за `--profile wikibase`.

Сбросить всё: `uv run python -m scripts.wipe_db --yes`.

---

## 3. Путь приёма (ingest)

`POST /ingest` загружает файл в MinIO, вставляет строку `pending` в
Postgres, снимает снапшот имён моделей по ролям и запускает надёжный
**`DocumentIngestWorkflow`** (очередь `kb-ingest`). Воркфлоу выполняет
фиксированную последовательность активностей; тяжёлое состояние
путешествует как **блобы MinIO по URI** (claim-check), так что по
payload-ам Temporal едут только небольшие контракты.

**Опциональный классификатор:** между `fetch_source` и `parse_and_chunk`
может стоять шаг `classify_document` (opt-in `CLASSIFIER_ENABLED`,
`src/ingestion/classifier.py`) — детерминированные правила + LLM-гейт
отсеивают мусор; документ-мусор завершается новым статусом `skipped`
вместо прохода через конвейер. `force=true` на `/ingest` обходит правила
и форсирует приём.

Две половины, намеренно разделённые:

- **Векторная половина** — `fetch_source` → (`classify_document`) →
  `parse_and_chunk` (разбиение → детерминированная канонизация
  идентификаторов → опциональный перевод на русский) → `index_vector`
  (эмбеддинг → Milvus). Если это падает, приём падает (`mark_failed`).
- **Графовая половина** — `inject_canonical` (сущности-идентификаторы в
  Neo4j) → `extract_kg` (LightRAG, один LLM-вызов на чанк, очередь
  `kb-ingest-llm`) → дочерний **`GraphBuildWorkflow`** (очередь
  `kb-ingest-merge`): `merge_and_resolve` (межчанковое слияние →
  консолидация телефонов → разрешение сущностей) → `build_property_graph`
  (upsert в Neo4j).

**Деградация:** если графовая половина бросает исключение / истекает по
таймауту, родитель ловит это и выставляет `graph_status = "vector_only"` —
документ всё ещё доступен для векторного поиска, граф просто пропускается
(а вместе с ним и `push_wikibase`). Граф — это дополнение, а не
блокирующий фактор.

Best-effort хвосты: `mark_entities_dirty` (помечает сущности для
редактора wiki) и `push_wikibase` (проецирование в якорь Wikibase, только
если граф завершился), затем `finalize` записывает финальный статус +
`ingest_metrics`.

**Опциональный контроль допуска (admission control)** (opt-in
`INGEST_ADMISSION_ENABLED`, `INGEST_ADMISSION_MAX_INFLIGHT`) — singleton
`IngestSchedulerWorkflow` (`src/workflow/ingest_scheduler.py`,
`src/workflow/admission.py`) допускает к исполнению не более K документов
одновременно, проводя каждый до завершения по FIFO. Когда выключен,
документы запускаются сразу, как раньше.

Полная таблица активностей, диаграмма последовательности, staging-контракты → [`INGEST.md`](INGEST.md).

---

## 4. Путь поиска

`POST /search/{local,global,drift,auto}` — все четыре являются надёжными
воркфлоу Temporal, запускаемыми из `src/api/routes/search_v2.py` и
разделяющими одну форму `SearchRequest` / `SearchResponse` (включая
управляемую клиентом `history` для многоходовых диалогов).

| Режим | Воркфлоу | Форма |
|---|---|---|
| `local` | `SearchOrchestratorWorkflow` | планирование подвопросов → параллельный fan-out `SubQueryRetrievalWorkflow` → merge/dedup → проверка покрытия → bge rerank → синтез на large-tier |
| `global` | `GlobalSearchWorkflow` | GraphRAG map-reduce по отчётам сообществ (MAP на small-tier по каждому сообществу → REDUCE на large-tier один раз) |
| `drift` | `DriftSearchWorkflow` | локальный проход, затем глобальное расширение, засеянное локальными источниками; деградирует до локального ответа, если глобальный падает |
| `auto` | `AutoSearchWorkflow` | `route_query` классифицирует → диспетчеризует local/global/drift (fail-safe → local) |

Ключевые свойства:

- **Детерминированное извлечение, а не цикл ReAct.** Каждый подвопрос
  выполняет фиксированную последовательность инструментов — `vector_search`
  (Milvus) → `graph_search` (Neo4j нативный векторный kNN по сущностям +
  LLM-синонимы) → `find_entity_by_name` (Neo4j полнотекст) → `graph_walk`
  (ограниченный N-hop обход, засеянный из двух источников — топ
  graph_search и полнотекстовой сущности). Результаты сливаются и
  дедуплицируются по `chunk_id`. (Старый путь Self-RAG / ReAct был
  **удалён** при переходе на R7b.)
- **История диалога** — когда присутствует `history`, активность
  `contextualize_query` один раз в начале воркфлоу переписывает
  follow-up в самостоятельный вопрос.
- **Иерархические сообщества** — global/drift выбирают по Leiden-**иерархии**
  узлов `:Community` со структурированными отчётами через
  лексический / семантический-kNN / спуск-по-иерархии отбор. Leiden
  запускается **взвешенным** (по `weight` связей = co-occurrence).
  Сообщества строятся **офлайн** через `CommunityBuildWorkflow` (очередь
  `kb-graph-build`, запускается админом), полностью отвязанные от горячего
  пути запроса.
- **Вывод только на русском** — `synthesize_answer` оборачивает запрос
  инструкцией выдавать ответ на русском, так что язык ответа совпадает с
  нормализацией графа независимо от языка исходных чанков.
- **Шаблон ответа** — поле запроса `answer_template`
  (`src/retrieval/answer_template.py`) задаёт форму синтезируемого ответа:
  именованный шаблон из `prompts/answer_templates/<name>.md` или inline-текст;
  пустое значение → шаблон по умолчанию.

**Анализ графа (admin):** набор read-only эндпоинтов
`/admin/graph/{stats,pagerank,components,shortest-path}`
(`src/graph/analysis.py`), опирающихся на GDS, даёт сводную статистику,
PageRank, компоненты связности и кратчайшие пути по графу знаний — вне
горячего пути запроса.

Режимы, инструменты извлечения, отбор сообществ → [`SEARCH-FLOW.md`](SEARCH-FLOW.md)
и [`SEARCH.md`](SEARCH.md).

---

## 5. Надёжное исполнение и очереди

Воркер содержит по одному пулу `Worker` на каждую очередь задач, чтобы
давление по GPU / LLM на одну нагрузку не могло заморить другую (блокировка
головы очереди, head-of-line blocking). Очереди:

| Очередь | Что содержит | Лимит (по умолчанию) |
|---|---|---|
| `kb-ingest` | `DocumentIngestWorkflow` + активности IO/эмбеддинга | 4 |
| `kb-ingest-llm` | только `extract_kg` (дорожка extract) | 18 |
| `kb-ingest-merge` | `GraphBuildWorkflow` + merge/build (дорожка merge) | 14 |
| `kb-search-small` | воркфлоу поиска + активности plan/retrieve/coverage/rerank/route/map | 4 |
| `kb-search-large` | только `synthesize_answer` (финальный синтез на large-tier) | 2 |
| `kb-graph-build` | `CommunityBuildWorkflow` (офлайн-сообщества GDS-Leiden) | 2 |
| `kb-wiki` | `WikiSweepWorkflow` (непрерывный редактор MediaWiki по сущностям) | 4 |

Лимиты Temporal по каждой очереди ограничивают, сколько активностей
*планируется*; per-process **LLMPool** затем ограничивает, сколько
LLM-вызовов реально выполняется конкурентно (потолок tier + дорожки
ролей). Лимиты держатся ≥ потолков дорожек пула, чтобы пул арбитрировал
первым. Полное обоснование + ручки `TEMPORAL_*_ACTIVITY_CONCURRENCY` → [`QUEUES.md`](QUEUES.md).

---

## 6. Якорь знаний и редактор wiki

Помимо RAG-хранилищ, сущности попадают в курируемый слой идентичности:

- **Популятор Wikibase** (`push_wikibase`, opt-in `WIKIBASE_ENABLED`) —
  приём создаёт/патчит Wikibase Item на каждую сущность с ключом
  `wikibase_qid`, сворачивая сущности типа идентификатор в
  external-id-утверждения; запрашивается через WDQS SPARQL.
- **Непрерывный редактор wiki** (`WikiSweepWorkflow`, очередь `kb-wiki`,
  opt-in `WIKI_ENABLED`) — приём помечает затронутые сущности
  `wiki_dirty`; запланированный проход (sweep) переписывает управляемую
  ботом секцию статьи MediaWiki по каждой сущности **только из фактов
  графа** (anti-drift, с цитированием), сохраняя правки людей и пропуская
  неизменившиеся сущности по хешу подграфа.

→ [`runbook/wikibase.md`](runbook/wikibase.md),
[`runbook/wiki-editor.md`](runbook/wiki-editor.md), [`FEATURES.md`](FEATURES.md#3-knowledge-anchors).

---

## 7. Наблюдаемость

- **Temporal UI** `:8080` — таймлайны воркфлоу/активностей, повторы,
  падения.
- **Prometheus** + **Grafana** — воркер экспортирует метрики Temporal SDK
  через Prometheus-экспортёр (`src/workflow/worker.py::_build_runtime`,
  под флагом `METRICS_ENABLED`); дашборды Grafana (Ingest Overview,
  Version compare, Run drill-down) читают их плюс таблицу Postgres
  `ingest_metrics`.
- **`ingest_metrics`** — длительности по каждой активности + теги моделей
  по ролям (зафиксированы при запуске), так что дашборды атрибутируют
  каждый шаг точной модели, которая его выполнила, даже после замены
  модели.
- **Оценка качества ответов** — `tests/eval/` оценивает ответы эндпоинтов
  (полнота фактов/сущностей, точность цитирования, граница галлюцинаций)
  детерминированно и офлайн.

→ [`runbook/analytics.md`](runbook/analytics.md).

---

## 8. Конфигурация

Все настройки проходят через `src/config.py` (pydantic-settings, читает
`.env`), с пространствами имён по подсистемам: `API_`, `MILVUS_`, `NEO4J_`,
`POSTGRES_`, `MINIO_`, `LITELLM_`, `TEMPORAL_`, `INGESTION_`, `AGENT_`,
`LLM_POOL_`, `WIKIBASE_` / `WIKI_`, `METRICS_`. Тумблеры новых функций
(ER на нативных векторах, история диалога, dual walk-seed, иерархические
сообщества, тип индекса Milvus) перечислены в
[`FEATURES.md`](FEATURES.md#config-quick-reference-new-feature-env-vars);
процедура замены модели — в [`MODELS.md`](MODELS.md).

---

## 9. Ещё не подключено

- Гибридный ретривер (BM25 + векторный RRF) существует в
  `src/retrieval/hybrid.py`, но не входит в живой путь извлечения.
- Мультитенантная изоляция данных — `department` проходит через
  метаданные, но не применяется на этапе извлечения.
- Настроенное Temporal **Schedule** для перестроек сообществ/wiki (сегодня
  они запускаются админом).
