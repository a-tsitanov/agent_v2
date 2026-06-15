# Конвейер инжеста

Как документ становится доступным для поиска: оркестрируемый Temporal поток инжеста, выполняемые блоки, очереди, на которых они работают, и способ передачи тяжёлого состояния между ними.

> Диаграммы: Mermaid (ниже, редактируется как текст) + отрендеренный обзор D2 в [`diagrams/ingest_flow.svg`](diagrams/ingest_flow.svg) (исходник [`diagrams/ingest_flow.d2`](diagrams/ingest_flow.d2)).
> Справочник по очередям: [`QUEUES.md`](QUEUES.md). Архитектура верхнего уровня: [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Кратко

`POST /ingest` загружает файл в **MinIO**, записывает строку в статусе pending в **Postgres**, делает снимок имён моделей по ролям и запускает долговечный воркфлоу **`DocumentIngestWorkflow`** на очереди `kb-ingest`. Воркфлоу выполняет фиксированную последовательность активностей, передавая тяжёлое состояние (распарсенные ноды, KG-ноды, объединённые сущности) между ними как **MinIO-блобы по URI** (claim-check) — в полезной нагрузке Temporal путешествуют только небольшие контракты. **Векторная половина** (parse → embed → Milvus) и **графовая половина** (extract KG → merge/ER → Neo4j) разделены, так что медленная или упавшая сборка графа деградирует до `graph_status="vector_only"` вместо потери всего инжеста.

Опционально включаются два механизма (оба opt-in, по умолчанию выключены): **классификатор входных документов** (`CLASSIFIER_ENABLED`) — шаг между `fetch_source` и `parse_and_chunk`, который может коротко замкнуть воркфлоу в терминальный статус `skipped`, не запуская парсинг/индексацию/граф; и **допуск документов** (`INGEST_ADMISSION_ENABLED`) — когда включён, `/ingest` не запускает `DocumentIngestWorkflow` напрямую, а отправляет документ в синглтон-планировщик `IngestSchedulerWorkflow`, который ограничивает число одновременно обрабатываемых документов (см. [Допуск документов](#допуск-документов-admission-control)).

## Поток (верхний уровень)

```mermaid
flowchart TD
    A["POST /ingest<br/>upload→MinIO, insert pending→Postgres,<br/>snapshot per-role models"] --> WF["DocumentIngestWorkflow<br/>(queue: kb-ingest)"]

    WF --> S1["1. fetch_source<br/>download/cache; status=processing"]
    S1 --> CLS{"classify_document<br/>(опц., CLASSIFIER_ENABLED)<br/>rules + LLM-гейт по превью"}
    CLS -- "ingest (или fail-soft)" --> S2["2. parse_and_chunk<br/>read→split→identifier-canon→(translate)<br/>→ parsed.pkl"]
    CLS -- "skip (force обходит rules)" --> SK["mark_skipped<br/>status=skipped + причина,<br/>чистка staging, без parse/index/graph"]
    S2 --> S3["3. index_vector<br/>embed + insert → Milvus"]
    S2 --> S4["4. inject_canonical<br/>upsert identifier entities → Neo4j"]
    S2 --> S5["5. extract_kg<br/>LightRAG: entities+relations per chunk<br/>(queue: kb-ingest-llm) → kg.pkl"]

    S5 --> GBW["6. GraphBuildWorkflow (child)<br/>(queue: kb-ingest-merge)"]
    subgraph GBW_inner [" "]
      GB1["6a. merge_and_resolve<br/>LightRAG merge → phone consolidate → ER<br/>→ merged.pkl"]
      GB2["6b. build_property_graph<br/>upsert entities/relations + indexes → Neo4j"]
      GB1 --> GB2
    end
    GBW --> GB1

    GB2 --> S7["7. mark_entities_dirty<br/>(best-effort) flag entities for wiki editor"]
    S7 --> S8["8. push_wikibase<br/>(best-effort, only if graph completed)"]
    S8 --> S9["9. finalize<br/>status=completed/vector_only,<br/>clean staging, write ingest_metrics→Postgres"]

    GBW -. "child fails / times out" .-> VO["graph_status = vector_only<br/>(vector half kept, graph skipped)"]
    VO --> S7
    WF -. "vector half fails" .-> MF["mark_failed<br/>status=failed, clean up, re-raise"]

    S3 --> S9
```

## Последовательность (задействованные хранилища)

```mermaid
sequenceDiagram
    autonumber
    actor U as Client
    participant API as /ingest route
    participant T as Temporal (DocumentIngestWorkflow)
    participant MinIO
    participant PG as Postgres
    participant Milvus
    participant Neo4j
    participant LLM as LiteLLM (via LLMPool)

    U->>API: POST file
    API->>MinIO: put_object(file)
    API->>PG: insert_pending(doc_id)
    API->>T: start DocumentIngestWorkflow(IngestParams + model snapshots)
    T->>MinIO: fetch_source (download/cache)
    T->>PG: status = processing
    T->>T: parse_and_chunk (split + identifier-canon)
    T->>MinIO: write parsed.pkl
    T->>Milvus: index_vector (embed + insert)
    T->>Neo4j: inject_canonical (identifier entities)
    T->>LLM: extract_kg (extraction lane) — 1 call/chunk
    T->>MinIO: write kg.pkl
    Note over T: GraphBuildWorkflow (child, kb-ingest-merge)
    T->>LLM: merge_and_resolve (judge lane: merge + ER)
    T->>MinIO: write merged.pkl
    T->>Neo4j: build_property_graph (upsert + indexes)
    T->>Neo4j: mark_entities_dirty (best-effort, wiki)
    T->>Neo4j: push_wikibase (best-effort)
    T->>PG: finalize (status + ingest_metrics per activity)
    T->>MinIO: delete staging prefix
```

## Стадии

| # | Активность | Очередь | Что делает | Вход → Выход | Файл |
|---|---|---|---|---|---|
| 1 | `fetch_source` | kb-ingest | Идемпотентная загрузка из MinIO (кэширует локально); Postgres → `processing` | `IngestParams` → `Ctx` | `activities/fetch_source.py` |
| 1.5 | `classify_document` | kb-ingest | **Opt-in** (`CLASSIFIER_ENABLED`, по умолч. false). Два слоя: (a) детерминированные правила — расширение / размер / пустой файл (`classifier.py::apply_rules`), которые обходит флаг `force`; (b) LLM-гейт по ограниченному превью файла (`classify_with_llm`, `astructured_predict`, small-уровень). Любая ошибка → fail-soft в ingest (ложный skip дороже). При skip воркфлоу замыкается на `mark_skipped` | `Ctx` → решение ingest/skip | `activities/classify_document.py`, `ingestion/classifier.py` |
| 2 | `parse_and_chunk` | kb-ingest | Чтение → разбиение (`chunk_size`/`overlap`) → **канонизация идентификаторов** (телефоны→E.164, ИНН/ОГРН…) → опциональный перевод; вычистка метаданных перевода; pickle нод | `Ctx` → `Parsed` (`nodes_uri`) | `activities/parse_and_chunk.py` |
| 3 | `index_vector` | kb-ingest | Срезает метаданные сверх лимита Milvus → embed → вставка в **Milvus**; восстанавливает метаданные на нодах в памяти | `Parsed` → `Indexed` | `activities/index_vector.py` |
| 4 | `inject_canonical` | kb-ingest | Upsert одной `:__Entity__` на каждый `(type, canonical)` идентификатор в **Neo4j** ДО LLM-экстракции (чтобы дословные упоминания от LLM всё равно дедуплицировались) | `Parsed` → `Injected` | `activities/inject_canonical.py` |
| 5 | `extract_kg` | **kb-ingest-llm** | **Экстрактор LightRAG**: один LLM-вызов на чанк → сущности + связи (src+tgt+keywords+описание+**полярность**+**временная валидность**) в метаданных чанка; сводит статистику | `Parsed` → `KGExtracted` (`kg.pkl`) | `activities/extract_kg.py` |
| 6a | `merge_and_resolve` | **kb-ingest-merge** | Кросс-чанковое **слияние LightRAG** → **консолидация телефонов** → **ER** (`resolve_entities`: LLM-судья + кэш вердиктов + native-vector kNN/окно) | `KGExtracted` → `Merged` (`merged.pkl`) | `activities/merge_and_resolve.py` |
| 6b | `build_property_graph` | kb-ingest-merge | Срезает небезопасные для Neo4j метаданные → строит PG-индекс (Chunk + `MENTIONS` + сущности/связи) → upsert в **Neo4j** → гарантирует индексы | `Merged` → `GraphBuilt` | `activities/build_property_graph.py` |
| 7 | `mark_entities_dirty` | kb-ingest | Best-effort: помечает имена объединённых сущностей для непрерывного wiki-редактора (Project A) | `MarkDirtyIn` → count | `activities/mark_dirty.py` |
| 8 | `push_wikibase` | kb-ingest | Best-effort (только если граф `completed`): проецирует сущности/связи в локальный якорь Wikibase | `Merged` → `WikibasePushed` | `activities/push_wikibase.py` |
| 9 | `finalize` | kb-ingest | Финальный статус в Postgres; удаление staging-префикса + локальной директории; запись `ingest_metrics` по каждой активности (длительности + теги моделей по ролям) | `FinalizeIn` → `IngestResult` | `activities/finalize.py` |
| — | `mark_skipped` | kb-ingest | **Opt-in** (срабатывает только при `classify_document` → skip): пишет терминальный статус `skipped` + причину, чистит staging; парсинг/индексация/граф не выполняются | `MarkSkippedIn` | `activities/finalize.py` |
| — | `mark_failed` | kb-ingest | При падении векторной половины: статус `failed`, очистка, повторный проброс | `MarkFailedIn` | `activities/finalize.py` |

`6a`/`6b` выполняются внутри **дочернего воркфлоу `GraphBuildWorkflow`** (`graph_build.py`), так что медленная LLM-работа по графу имеет свои собственные retry/timeout и метрики и может быть отменена без перезапуска векторной половины.

## Две половины + деградация

- **Векторная половина** (1–3): fetch → parse/chunk → embed → Milvus. Если падает/таймаутится → `mark_failed`, инжест проваливается.
- **Графовая половина** (5–6): extract KG → merge/ER → Neo4j, внутри дочернего воркфлоу. Если поднимается исключение (`ActivityError`/`ChildWorkflowError`) → перехватывается → **`graph_status = "vector_only"`**: документ по-прежнему доступен для векторного поиска, граф просто пропускается. `push_wikibase` тогда тоже пропускается (он завязан на `completed`).

Терминальные статусы документа: **`completed`** (обе половины успешны), **`vector_only`** (граф деградировал), **`failed`** (упала векторная половина) и **`skipped`** (новый — документ отклонён классификатором на шаге `classify_document`, парсинг/индексация/граф не выполнялись).

## Классификатор входных документов (opt-in)

**Track 2**, включается через env `CLASSIFIER_ENABLED` (по умолчанию false). Когда включён, между `fetch_source` (шаг 1) и `parse_and_chunk` (шаг 2) на очереди `kb-ingest` выполняется активность `classify_document`, решающая, стоит ли вообще пускать документ в дорогостоящий конвейер.

- **Два слоя.** (a) Детерминированные правила (`ingestion/classifier.py::apply_rules`) — проверки по расширению файла, размеру, пустоте. (b) LLM-гейт по ограниченному превью файла (`classify_with_llm`, `astructured_predict`, small-уровень).
- **Флаг `force`.** `Form`-параметр `force=true` на `POST /ingest`, снимаемый в `IngestParams.force`, **обходит детерминированные правила** — чтобы оператор мог протолкнуть документ, который правила иначе бы отсеяли.
- **Skip → `mark_skipped`.** При решении skip воркфлоу коротко замыкается на новую активность `mark_skipped` (`activities/finalize.py`): пишется новый терминальный статус Postgres `skipped` + причина, чистится staging; парсинг/индексация/граф **не выполняются**.
- **Fail-soft.** Любая ошибка классификатора по умолчанию трактуется как ingest (ложный skip теряет хороший документ — это дорогая ошибка).
- **Детерминизм Temporal.** Флаг классификатора снимается в `IngestParams` в момент сабмита (как wiki-флаг); воркфлоу ветвится на `params.classifier_enabled` и никогда не читает settings напрямую.

Файлы: `workflow/activities/classify_document.py`, `ingestion/classifier.py`, `workflow/activities/finalize.py` (`mark_skipped`).

## Допуск документов (admission control)

**Track 5**, включается через env `INGEST_ADMISSION_ENABLED` (по умолчанию false), `INGEST_ADMISSION_MAX_INFLIGHT` (по умолчанию 1). Решает проблему насыщения очередей: хвост слияния одного документа не должен голодать за десятками всплесков экстракции более новых документов.

- **Когда включён**, `POST /ingest` **не** запускает `DocumentIngestWorkflow` напрямую. Вместо этого он делает signal-with-start синглтон-воркфлоу `IngestSchedulerWorkflow` (фиксированный workflow id `ingest-scheduler`, `WorkflowIDConflictPolicy.USE_EXISTING`, `start_signal="submit"`).
- **Планировщик** допускает не более `max_inflight` (K) документов одновременно и доводит каждый `DocumentIngestWorkflow` (как дочерний) **до завершения** прежде чем допустить следующий, FIFO.
- **K=1** = строгое «закончи один, прежде чем начать следующий». **K=2–3** перекрывает I/O-стадии одного документа с GPU-стадией другого, ограничивая глубину очереди.
- **Когда выключен** (по умолчанию), `/ingest` запускает воркфлоу напрямую ровно как раньше — изменений в поведении нет.

Чистая логика планирования — `workflow/admission.py::AdmissionState`; Temporal-оболочка — `workflow/ingest_scheduler.py`.

## Claim-check staging (MinIO)

Тяжёлое состояние никогда не путешествует в полезной нагрузке Temporal (лимит 2 МБ) — оно сериализуется в MinIO и передаётся по URI:

| Блоб | Производит | Потребляет |
|---|---|---|
| `{run_id}/parsed.pkl` (list[BaseNode]) | parse_and_chunk | index_vector, inject_canonical, extract_kg |
| `{run_id}/kg.pkl` (nodes + KG metadata) | extract_kg | merge_and_resolve |
| `{run_id}/merged.pkl` (entities, relations, nodes) | merge_and_resolve | build_property_graph, push_wikibase |

`finalize` (или `mark_failed`) удаляет префикс `{run_id}/`; `cleanup_orphans()` подметает блобы упавших прогонов старше 24 ч. (`workflow/staging.py`)

## Очереди, воркеры, конкурентность LLM

Раздельные очереди не дают всплеску LLM заморить голодом работу по записи в Neo4j/слиянию (блокировка головы очереди):

| Очередь | Конкурентность активностей | Что выполняет |
|---|---|---|
| `kb-ingest` | 4 | воркфлоу + IO-активности (fetch, parse, index_vector, inject, mark_dirty, push_wikibase, finalize) |
| `kb-ingest-llm` | 18 | только `extract_kg` |
| `kb-ingest-merge` | 14 | `GraphBuildWorkflow` + merge_and_resolve + build_property_graph |

Поверх покраздельных лимитов Temporal **пер-процессный `LLMPool`** (`retrieval/llm_pool.py`) управляет фактической конкурентностью LLM иерархическими гейтами: потолок **уровня** (small=ёмкость GPU, large=бюджет API) и **полосы по ролям** (extraction/judge/…), захватываемые сначала по полосе, затем глобально по уровню. Так что Temporal может запланировать 18 `extract_kg`, но пул допускает лишь столько одновременных LLM-вызовов, сколько способен обслужить GPU. См. [`QUEUES.md`](QUEUES.md) + [`runbook/multimodel.md`](runbook/multimodel.md).

## Канонизация идентификаторов (детерминированная, до LLM)

24 типа идентификаторов (телефоны→E.164, ИНН/ОГРН с контрольными суммами, email, URL, почтовые адреса через libpostal, даты, суммы, …) извлекаются **детерминированно** в `parse_and_chunk` (без LLM), хранятся в метаданных чанка И дописываются в текст чанка, чтобы LLM видела канонические формы в потоке. Затем `inject_canonical` делает upsert их как нод `:__Entity__` **перед** `extract_kg`, так что даже если LLM извлечёт дословную строку телефона, она дедуплицируется на каноническую ноду. (`ingestion/identifiers.py`, `ingestion/identifier_transform.py`)

## Снимки мультимодели

Имена моделей по ролям (`extraction`/`judge`/`search`) снимаются в момент `POST /ingest` и протягиваются `IngestParams → FinalizeIn`, так что `ingest_metrics` записывает точную модель, выполнявшую каждую активность, даже если модели меняются между сабмитами — пересборка не нужна. (`api/routes/ingest.py`, `activities/finalize.py`; runbook [`runbook/multimodel.md`](runbook/multimodel.md))
