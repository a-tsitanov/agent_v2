# Возможности

Справочник по каждой возможности kb-llamaindex: что это, зачем существует, как работает и какие env/config это контролируют. Потоки процессов описаны в [`INGEST.md`](INGEST.md) и [`SEARCH-FLOW.md`](SEARCH-FLOW.md); операторские плейбуки — в [`runbook/`](runbook/).

**Легенда:** 🆕 = добавлено в работе по масштабированию/GraphRAG за 2026-06 (подробный разбор в [§ Глубокий разбор новых возможностей](#глубокий-разбор-новых-возможностей)). Все 🆕 поведения search/ER/community **opt-in, с дефолтами, равными прежнему поведению**.

---

## 1. Инжест

### Парсинг и чанкинг
LlamaIndex `IngestionPipeline`: ридер извлекает документ, сплиттер (`SentenceSplitter` или `SemanticSplitterNodeParser`) производит чанки (`INGESTION_CHUNK_SIZE` / `_OVERLAP`). На выходе — чанки `BaseNode`, несущие текст + метаданные. — `ingestion/pipeline.py`, `activities/parse_and_chunk.py`

### Многоязычный перевод
Опциональная трансформация перевода на русский (на документ или на чанк по порогу размера), чтобы разнородный корпус нормализовался для эмбеддинга/экстракции, при этом **имена сущностей остаются на языке оригинала**. Метаданные перевода вычищаются перед нижестоящими хранилищами. — `runbook/multimodel.md`

### Детерминированная канонизация идентификаторов
24 типа структурных идентификаторов (телефоны→E.164, ИНН/ОГРН/БИК с контрольными суммами, email, URL, почтовые адреса через libpostal, даты, суммы, IMEI/MAC/VIN/госномера …) извлекаются **без LLM**, хранятся в метаданных чанка, дописываются в текст чанка (чтобы LLM видела канонические формы) и предварительно инжектятся как ноды `:__Entity__` **перед** KG-экстракцией. Гарантирует дедуп идентификаторов даже когда LLM извлекает дословное упоминание. — `ingestion/identifiers.py`, `ingestion/identifier_transform.py`, `activities/inject_canonical.py`

### Классификатор входных документов 🆕
Опциональный гейт перед инжестом, отсеивающий мусорные документы: сначала детерминированные правила (расширение/размер), затем LLM-гейт по превью документа. `force=true` на `POST /ingest` обходит правила; отсеянные документы получают новый статус `skipped`. Fail-soft → при ошибке документ идёт в инжест. По умолчанию выключено (`CLASSIFIER_ENABLED`). — `ingestion/classifier.py`, `activities/classify_document.py`

### KG-экстракция LightRAG
На каждый чанк — один LLM-вызов (`LightRAGExtractor`, портированный из HKUDS/LightRAG) выдаёт типизированные сущности (имя + тип + описание в 1–2 предложения) и связи (src + tgt + ключевые слова + описание + **полярность** + **временная валидность** — 7-польный кортеж) одним структурированным ответом. Многоязычный промпт; `/no_think` для qwen3. — `graph/lightrag_extract.py`, `graph/lightrag_prompts.py`, `activities/extract_kg.py`

### Entity Resolution (ER) 🆕(native-vector)
Кросс-чанковая + кросс-документная дедупликация семантически равных сущностей («BCC» ≡ «Базальноклеточный рак»; «Иванов И.И.» ≡ «Иван Иванов»). Конвейер: слияние имён внутри батча → консолидация телефонов → **ER** (`resolve_entities`): генерация кандидатов (векторизованный косинус + перекрытие имён), LLM-судья для пограничных пар (с кэшем Neo4j `:ERVerdict`), кластеризация union-find, зажим гипер-хабов, выбор канонического. Метки типов-идентификаторов исключаются (у них детерминированная канонизация). См. [§ Native-vector ER](#native-vector-er-) о новом пути kNN. — `graph/entity_resolution.py`, `activities/merge_and_resolve.py`

### Пакетная консолидация графа 🆕
`scripts/reresolve_graph.py` — прогон ER по **всему** графу для слияния дублей смысловых сущностей, накопившихся между ингестами (per-ingest ER сравнивает только новую пачку). Переиспользует `resolve_entities` как чистую функцию решения через read-only proxy; применяет мёржи через `_cleanup_stored_losers` с **сохранением типов связей** (в отличие от `merge_identifier_duplicates`, который плющит в `RELATED_TO`). Dry-run по умолчанию. — `scripts/reresolve_graph.py`, [`runbook/reresolve-graph.md`](runbook/reresolve-graph.md)

### Сборка property-графа
Объединённые сущности/связи делаются upsert в Neo4j с рёбрами `(:Chunk)-[:MENTIONS]->(:__Entity__)`; эмбеддинги сущностей пишутся в нативный векторный индекс; гарантируются fulltext- и range-индексы. — `activities/build_property_graph.py`, `graph/index.py`

### Взвешенные связи и теги 🆕
Связи KG теперь несут осмысленный `weight` (= число различных совместных упоминаний, было константой 1.0), дискретные `tags` и агрегированные `mention_count`/`source_chunks`. Детекция сообществ Leiden теперь работает **взвешенно** по `r.weight`. — `graph/merge.py`, `graph/communities.py`

### Полярность и временная валидность связей 🆕
Каждая связь несёт **логическую полярность** `polarity` (`affirmed` / `negated` / `uncertain`) — так отрицания («Иванов **больше не** директор») и сомнения («предположительно владеет») не выглядят в графе как утверждённые факты — и **окно временной валидности** `valid_from` / `valid_to` (скалярные ISO-строки; вложенные map Neo4j в свойствах не хранит). Заполняет экстракция-LLM (default `affirmed` / пусто, когда текст молчит); merge агрегирует полярность мажоритарным голосом, а окно расширяет до самого широкого наблюдавшегося (`min valid_from`, `max valid_to`). — `graph/lightrag_parse.py`, `graph/lightrag_prompts.py`, `graph/merge.py`

### Тюнинг детекции сообществ (Leiden) 🆕
Разрешение Leiden и параллелизм GDS вынесены в ручки `TEMPORAL_COMMUNITY_LEIDEN_GAMMA` (>1 → больше мелких сообществ; <1 → меньше крупных) и `TEMPORAL_COMMUNITY_LEIDEN_CONCURRENCY`. Снимаются из конфига в `DetectCommunitiesParams` на старте воркфлоу (детерминизм Temporal). — `graph/communities.py`, [`runbook/leiden-diagnostics.md`](runbook/leiden-diagnostics.md)

### Backfill `doc_id` на legacy-чанки 🆕
`scripts/backfill_doc_id.py` доустанавливает `doc_id` чанкам, проиндексированным до того, как `index_vector` начал помечать ноды (иначе `get_chunks_by_doc_id` их не находит): сопоставляет `file_path` → `doc_id` по Postgres и переиндексирует обычным путём LlamaIndex, сохраняя текст и эмбеддинг. Dry-run по умолчанию. — `storage/backfill.py`, [`runbook/doc-id-backfill.md`](runbook/doc-id-backfill.md)

### Мультимодель и аналитика
Имена моделей по ролям снимаются при сабмите и пишутся по каждой активности в таблицу Postgres `ingest_metrics` (длительности + теги версий), так что дашборды отражают точную модель, выполнявшую каждый шаг. — `runbook/multimodel.md`, `runbook/analytics.md`

### Аналитика по графу (analytical-query layer, Waves 0–3) 🆕
Количественно-структурные вопросы к графу («сколько», «кто самый центральный», «что изменилось») через `POST /api/v1/analyze` и MCP-тул `kb_analyze`: LLM-планировщик раскладывает вопрос на ≤`ANALYTICS_MAX_STEPS` вызовов из каталога **42 детерминированных примитивов** (Cypher по Neo4j: счётчики, центральность, сообщества, досье, динамика по `document_date`, качество графа, риск/прогноз), LLM-синтез собирает ответ. Каждый ответ несёт `provenance` с точным Cypher и сырыми строками — численные факты читать из него, а не из `answer`. Центральность/link-prediction/risk предвычисляются офлайн-джобом `POST /admin/graph/materialize` (запускать после bulk-ингеста). Опциональный мониторинг Arc-2 (`MONITOR_*`, выключен по умолчанию): периодический свип в пуле `monitor` — новые связи, рост risk, burst событий, доставка алертов в webhook. — `src/analytics/`, `src/workflow/analytics/`, [`runbook/graph-analytics.md`](runbook/graph-analytics.md)

---

## 2. Поиск

Четыре режима, детерминированный конвейер инструментов, реранкинг и покрытие описаны в [`SEARCH-FLOW.md`](SEARCH-FLOW.md). Сводка:

- **local** — план → параллельный детерминированный retrieve (vector_search, graph_search, find_entity_by_name, graph_walk) → проверка покрытия → реранк bge → синтез на large-уровне.
- **global** — map-reduce по отчётам сообществ.
- **drift** — сначала local, затем global, с мягким fallback 🆕.
- **auto** — роутер классифицирует запрос и диспетчеризует один режим.
- **Реранкер** — кросс-энкодер bge-reranker-v2-m3, топ-N на синтез.
- **Проверка покрытия** — обнаруживает пробел в доказательствах и запускает один дополнительный целевой раунд.

### Шаблонизированные ответы (Track 6) 🆕
Поле запроса `answer_template` задаёт форму синтезированного ответа: именованный шаблон из `prompts/answer_templates/<name>.md` (например, `dossier`) либо инлайновый текст. Пустое значение → дефолтный русскоязычный вывод. — `retrieval/answer_template.py`

Новые поведения поиска: [история диалога](#история-диалога-), [двойной walk-seed](#двойной-walk-seed-), [drift fallback](#drift-мягкий-fallback-), [индексы сообществ](#индексы-сообществ-).

---

## 3. Якоря знаний

### Канонический якорь Wikibase
Самостоятельно размещённый инстанс Wikibase — это курируемый слой идентичности: инжест проецирует в него сущности/связи (`push_wikibase`, best-effort), создавая/патча Items с ключом `wikibase_qid`, сворачивая сущности типов-идентификаторов в external-id statements. По умолчанию выключено (`WIKIBASE_ENABLED`). — `runbook/wikibase.md`, `activities/push_wikibase.py`

### Непрерывный wiki-редактор (Project A)
Превращает сущности Neo4j в per-entity статьи MediaWiki. Инжест помечает затронутые сущности `wiki_dirty`; запланированный `WikiSweepWorkflow` (очередь `kb-wiki`) переписывает бот-секцию между маркерами **только по фактам графа** (анти-дрейф), сохраняя правки человека, с пропуском по content-hash для неизменённых сущностей. По умолчанию выключено (`WIKI_ENABLED`). — `runbook/wiki-editor.md`

---

## 4. Платформа

### Долговечные воркфлоу Temporal + очереди
Инжест и поиск — это долговечные воркфлоу с автоматическими retry, heartbeat и идемпотентными активностями. Выделенные очереди изолируют всплески LLM от работы по записи в Neo4j/слиянию: `kb-ingest`, `kb-ingest-llm`, `kb-ingest-merge`, `kb-search-small`, `kb-search-large`, `kb-graph-build`, `kb-wiki`. — [`QUEUES.md`](QUEUES.md)

### LLMPool (пер-процессная конкурентность)
Единый пер-процессный пул владеет конкурентностью LLM с иерархическими гейтами — потолок уровня (small=ёмкость GPU, large=бюджет API) и полосы по ролям (extraction/judge/search/…), захватываемые сначала по полосе, затем глобально по уровню, — так что лимиты очередей Temporal могут быть щедрыми, а фактические одновременные LLM-вызовы соответствуют GPU. — `retrieval/llm_pool.py`

### Документ-уровневый контроль допуска 🆕
Синглтон-воркфлоу `IngestSchedulerWorkflow` допускает к обработке не более K документов одновременно и прогоняет каждый до завершения (FIFO), чтобы хвост слияния одного документа не голодал за экстрактами более новых документов. По умолчанию выключено (`INGEST_ADMISSION_ENABLED`, `INGEST_ADMISSION_MAX_INFLIGHT`). — `workflow/admission.py`, `workflow/ingest_scheduler.py`

### Тулкит анализа графа (read-only) 🆕
Admin-эндпоинты `/admin/graph/{stats,pagerank,personalized-pagerank,components,shortest-path}` (за API-ключом) поверх GDS: счётчики сущностей/связей, p50/p99 степени, группы дублирующихся имён, число сообществ (`stats`); взвешенный PageRank; **персонализированный (seed-смещённый) PageRank** — важность относительно заданных сущностей (`personalized-pagerank`); слабо-связные компоненты (`components`); кратчайший путь между двумя сущностями по имени (`shortest-path`). Те же функции доступны как MCP-инструменты на MCP-2 (`graph_pagerank`, `graph_personalized_pagerank`, `graph_components`, `graph_shortest_path`, `graph_stats`). — `graph/analysis.py`, `api/routes/graph_admin.py`, `mcp/tools_server.py`

### Claim-check staging
Тяжёлое состояние (ноды, сущности) сериализуется в MinIO и передаётся между активностями по URI; в полезной нагрузке Temporal путешествуют только небольшие контракты; осиротевшие блобы упавших прогонов подметаются. — `workflow/staging.py`

### MCP-серверы
Две MCP-поверхности открывают поиск для OpenWebUI / Claude Desktop / Cursor: MCP-1 (`kb_search` через воркфлоу поиска Temporal) и MCP-2 (атомарные инструменты ретрива в процессе). — `runbook/mcp.md`

### Продакшен docker-compose + Dockerfile (Track 1) 🆕
`docker-compose.prod.yml` поднимает всё приложение (api/worker/mcp + бэкенды + redis) минус litellm/ollama (внешние через `LITELLM_BASE_URL`); wikibase — за `--profile wikibase`. — `docker-compose.prod.yml`

### Scale-bench harness 🆕
Синтетический набор бенчмарков без продакшен-данных (`tests/eval/scale/`), который очерчивает обрывы масштабирования (генерация кандидатов ER O(N²), Milvus FLAT против HNSW, стоимость хабов в graph_walk, охват native-vector ER против окна), генерируя реалистичные формы данных локально. — `tests/eval/scale/README.md`

---

## Глубокий разбор новых возможностей

### Native-vector ER 🆕
**Проблема:** инкрементальный ER загружал максимум окно из 5000 сущностей на инжест; при 250k канонических это окно достигает ~2 % истинных ближайших матчей (замерено), так что новые упоминания молча фрагментируются в дубли — деградируя каждый режим поиска.
**Исправление (opt-in):** хранить ER-эмбеддинг как нативный вектор Neo4j (`er_vec`) + индекс `er_embedding_vec` и заменить загрузку окна на пер-сущностный kNN `db.index.vector.queryNodes` по **всему графу**. Замерено: ~96 % recall при ~6 мс/запрос против ~2 % у окна.
**Также исправлено:** загрузка окна теперь `ORDER BY mention_count DESC` (хаб-сущности всегда в окне); генерация кандидатов векторизована (~118× — BLAS-косинус `_normalized_matrix` + пер-итемный кэш токенов); очистка сохранённых проигравших безопасна по бездействию (без молчаливой потери рёбер).
**Включить (после бэкапа Neo4j):**
```bash
python -m scripts.backfill_er_vector --no-dry-run   # parse er_embedding JSON → er_vec + build index
AGENT_ER_USE_NATIVE_VECTOR_KNN=true                 # restart ingest worker
```
По умолчанию OFF. — runbook [`runbook/er-native-vector-knn.md`](runbook/er-native-vector-knn.md), `graph/entity_resolution.py`

### История диалога 🆕
**Проблема:** `/search` был без состояния — у уточнений («а что по цене?») не было референта.
**Исправление:** клиент передаёт `history` (ходы); small-LLM активность `contextualize_query` переписывает уточнение в **самодостаточный вопрос** один раз в начале каждого воркфлоу (только когда история непуста); `params.model_copy(query=…)` заставляет весь конвейер использовать его. Управляется клиентом (без серверных сессий, остаётся без состояния / безопасно к replay); гейт включения резолвится в момент сабмита (`contextualize_enabled`). Drift контекстуализирует один раз и очищает историю дочерних. — `activities/contextualize.py`
**Config:** `AGENT_CONVERSATION_HISTORY_ENABLED` (по умолчанию true, инертен без истории), `AGENT_HISTORY_MAX_TURNS` (6), `AGENT_HISTORY_MAX_CHARS` (4000). — `activities/contextualize.py`

### Иерархические сообщества + динамический выбор 🆕
**Проблема:** плоские сообщества уровня 0 + короткие сводки + O(N) лексическое ранжирование на Python — слабо, семантически слепо («GPU»≠«видеокарта») и пересуммаризируется целиком при каждой сборке.
**Исправление (в стиле GraphRAG, opt-in):**
- **Иерархия** — один прогон GDS Leiden с `includeIntermediateCommunities` материализует многоуровневые `:Community` + рёбра `PARENT_OF` (уровень 0 = самый грубый, обратная совместимость; `members_hash` на сообщество). — `graph/communities.py::detect_hierarchy`
- **Структурированные отчёты** — `{title, summary, findings:[{statement, importance}]}` генерируются снизу вверх (уровень 0 из членов, уровень>0 из дочерних отчётов), embed в нативный индекс `community_report_vec`. — `activities/community.py`
- **Инкрементально** — сообщество, у которого `(level, members_hash)` не изменился, **переносит свой отчёт** (без LLM); сборка идёт **уровень за уровнем, от самого мелкого**, и суммаризирует только изменившиеся сообщества. — `community_wf.py`
- **Динамический выбор** для global/drift — **v1 семантический** (kNN по `community_report_vec`) и **v2 спуск** (старт с самого грубого, ранжирование по косинусу, спуск по `PARENT_OF` в релевантные дочерние → самые мелкие релевантные), с **лексическим fallback** при пустоте/ошибке. — `activities/global_search.py`
**Config:** `AGENT_COMMUNITY_MAX_LEVELS` (по умолчанию 1 = один уровень/как сейчас; повысьте, чтобы построить иерархию), `AGENT_COMMUNITY_DYNAMIC_SELECTION` (`lexical`|`semantic`|`descent`, по умолчанию `lexical` = как сейчас). Постройте иерархию через admin-триггер пересборки сообществ, затем переключите выбор. Спека/план в [`superpowers/specs`](superpowers/); бэклог (рекурсивное огрубление, claims) в [`superpowers/backlog-graph-scale.md`](superpowers/backlog-graph-scale.md).

### Двойной walk-seed 🆕
`graph_walk` сидится из **обеих** топ-сущностей — `graph_search` и `find_entity_by_name` — когда они различаются (результаты дедуплицируются по chunk_id), так что найденная по fulltext сущность вносит свою окрестность даже когда `graph_search` уже что-то вернул. `AGENT_GRAPH_WALK_DUAL_SEED` (по умолчанию on). — `activities/retrieve.py`

### Drift: мягкий fallback 🆕
Если global-проход drift-запроса падает/таймаутится, запрос **деградирует до локального ответа** (режим сохраняется `"drift"`) вместо провала всего запроса. — `search/router_wf.py::_drift_local_fallback`

### Индексы сообществ 🆕
Range-индексы на `Community.level` (чтение глобальной сводки) и `Chunk.doc_id` (обход сообщество→документ) — индекс `community_level` обязателен несмотря на композитное ограничение `(id,level)` (композитное не обслуживает выборку только по level). — `graph/index.py::ensure_community_indexes`

---

## Быстрый справочник по конфигу (env-переменные новых возможностей)

| Env | По умолчанию | Эффект |
|---|---|---|
| `AGENT_ER_USE_NATIVE_VECTOR_KNN` | false | ER kNN по всему графу (после бэкфилла) вместо окна 5000 |
| `AGENT_ER_VECTOR_KNN_K` | 20 | соседей на новую сущность (native ER) |
| `AGENT_CONVERSATION_HISTORY_ENABLED` | true | контекстуализировать уточнения, когда передан `history` |
| `AGENT_HISTORY_MAX_TURNS` / `_CHARS` | 6 / 4000 | ограничивают историю, подаваемую в контекстуализацию |
| `AGENT_GRAPH_WALK_DUAL_SEED` | true | сидировать graph_walk из graph_search + fulltext |
| `AGENT_COMMUNITY_MAX_LEVELS` | 1 | глубина иерархии Leiden для материализации (1 = как сейчас) |
| `AGENT_COMMUNITY_DYNAMIC_SELECTION` | lexical | выбор сообществ для global/drift: lexical \| semantic \| descent |
| `MILVUS_INDEX_TYPE` | HNSW | ANN-индекс чанков (FLAT для точного) — применяется при (пере)создании |
| `CLASSIFIER_ENABLED` | false | отсев мусорных документов перед инжестом (правила + LLM-гейт; `force=true` обходит правила) |
| `INGEST_ADMISSION_ENABLED` | false | документ-уровневый контроль допуска через синглтон IngestSchedulerWorkflow |
| `INGEST_ADMISSION_MAX_INFLIGHT` | — | макс. число документов в обработке одновременно (FIFO) |
