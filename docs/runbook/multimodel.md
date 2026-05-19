# Multimodel · GraphBuildWorkflow runbook

Самодостаточный гид по фиче, которая шипалась в спринте `feature/multimodel-and-child` (7 коммитов поверх analytics). Содержит ссылки на конкретные файлы/строки и code-выдержки — читать линейно сверху вниз.

> **Если ты пришёл за «куда смотреть когда сломалось»** — иди в [§ 11. Troubleshooting](#11-troubleshooting).
> **Если хочешь смокнуть end-to-end** — [§ 9. Smoke verification](#9-smoke-verification).

---

## 1. Overview — что изменилось

До спринта проект использовал **одну глобальную LLM** для всех LLM-вызовов: extract_kg, ER-judge, агент, Self-RAG synth, переводчик. После спринта добавлены три независимые роли — `extraction`, `judge`, `search`. Каждая может указать свою модель через env-переменную; если оставить пустой — fallback на `LITELLM_LLM_MODEL`.

Параллельно вынесли тяжёлую graph-часть workflow (`merge_and_resolve` + `build_property_graph`) в отдельный **child Temporal workflow** `GraphBuildWorkflow`. Это дало три практических профита, описанных в [§ 4](#4-graphbuildworkflow--зачем-child-workflow).

Совокупно эти две вещи делают возможным то, ради чего user пришёл: **per-activity model в `ingest_metrics`** (Postgres-таблица из analytics-спринта) — теперь каждая строка таблицы знает, какая модель **реально использовалась** для именно этой активности. См. [§ 7](#7-ingest_metrics--per-activity-model).

**Связь с другими runbook'ами:**
- [`analytics.md`](analytics.md) — Grafana-дашборды, как читать `ingest_metrics`, version_tag-механика
- [`wikibase.md`](wikibase.md) — Wikibase-population (parallel feature, не пересекается)
- [`../MODELS.md`](../MODELS.md) — verseguidance по выбору модели per role

---

## 2. Terminology cheat-sheet

| Термин | Что значит |
|---|---|
| **Role** | Назначение LLM-вызова: `extraction` / `judge` / `search`. Не модель, а *роль*. |
| **Per-role factory** | `build_extraction_llm()` / `build_judge_llm()` / `build_search_llm()` в `src/retrieval/llm.py:56-65`. |
| **Parent workflow** | `DocumentIngestWorkflow` — основной flow, запускается с `POST /api/v1/ingest`. ID-формат: `ingest-{doc_id}`. |
| **Child workflow** | `GraphBuildWorkflow` — содержит `merge_and_resolve` + `build_property_graph`. ID-формат: `graph-{doc_id}`. |
| **vector_only fallback** | Когда graph-часть упала (LLM down, ER timeout) — parent ловит ошибку, помечает `graph_status="vector_only"`, документ остаётся доступен для vector-поиска, KG не строится. |
| **Snapshot моделей** | API в момент `/ingest` снимает текущие значения `LITELLM_*_MODEL` и кладёт их в `IngestParams` — поэтому смена env между ingest'ами отражается только на новых документах. |
| **`models_per_role`** | dict `{"extraction": "...", "judge": "...", "search": "..."}` который extractor получает в момент finalize, чтобы заполнить `model` колонку. |

---

## 3. Per-role LLM configuration

### 3.1 Env-переменные

| Env var | Default | Используется в |
|---|---|---|
| `LITELLM_LLM_MODEL` | `gpt-4o-mini` | Глобальный fallback. Если per-role env не задана — используется эта. |
| `LITELLM_EXTRACTION_MODEL` | `""` (fallback) | `extract_kg`, `parse_and_chunk` (translator), CLI `python -m src.ingestion.run` |
| `LITELLM_JUDGE_MODEL` | `""` (fallback) | `merge_and_resolve` — ER LLM-judge + cross-chunk merge summary |
| `LITELLM_SEARCH_MODEL` | `""` (fallback) | DI-injected LLM → `/api/v1/agent`, `/selfrag`, `/legacy/agent` |

Полная таблица в [`docs/MODELS.md` § "Per-role LLMs"](../MODELS.md#per-role-llms).

### 3.2 Где определены поля в config

[`src/config.py:117-140`](../../src/config.py):

```python
llm_model: str = "qwen3:8b"
# Per-role overrides — empty ("") means "use ``llm_model``".  Keeps
# single-model deployments simple; cap into a per-role model when
# the operator wants the cheap/fast model for high-volume judge
# calls while keeping a stronger model for extraction or the
# user-facing answer agent.
extraction_model: str = ""
judge_model: str = ""
search_model: str = ""

def model_for(self, role: LLMRole) -> str:
    """Return the configured model name for ``role`` with fallback
    to ``llm_model`` when the role-specific field is empty."""
    override = {
        "extraction": self.extraction_model,
        "judge":      self.judge_model,
        "search":     self.search_model,
    }[role]
    return override or self.llm_model
```

`LLMRole` экспортируется на module-level: `Literal["extraction", "judge", "search"]` ([`src/config.py:20`](../../src/config.py)).

### 3.3 Factory wrapper'ы

[`src/retrieval/llm.py:43-65`](../../src/retrieval/llm.py):

```python
def build_llm(role: LLMRole | None = None) -> LLM:
    """role=None ⇒ legacy fallback to settings.litellm.llm_model.
    role="..."  ⇒ uses model_for(role)."""
    cfg = settings.litellm
    model = cfg.model_for(role) if role else cfg.llm_model
    return _build(model)


def build_extraction_llm() -> LLM: return build_llm("extraction")
def build_judge_llm()      -> LLM: return build_llm("judge")
def build_search_llm()     -> LLM: return build_llm("search")
```

`build_llm()` без аргумента сохранён намеренно: это legacy entry-point для diag-скриптов (`scripts/diag_kg*.py`), которые ничего не знают про роли.

### 3.4 Какой callsite на какую роль маппится

| Файл:строка | Вызов | Role | Почему |
|---|---|---|---|
| [`src/workflow/activities/extract_kg.py:92`](../../src/workflow/activities/extract_kg.py) | `build_extraction_llm()` | extraction | KG triple extraction — нужна "глубина чтения" |
| [`src/workflow/activities/parse_and_chunk.py:40`](../../src/workflow/activities/parse_and_chunk.py) | `build_extraction_llm()` | extraction | Опциональный translator — тоже full-chunk reading |
| [`src/workflow/activities/merge_and_resolve.py:98`](../../src/workflow/activities/merge_and_resolve.py) | `build_judge_llm()` | judge | ER pair-wise judge + cross-chunk merge summary, высокий call-volume |
| [`src/di/providers.py:50-58`](../../src/di/providers.py) | `build_search_llm()` | search | DI-injected → все search-routes; latency-sensitive |
| [`src/ingestion/run.py:56-63`](../../src/ingestion/run.py) | `build_extraction_llm()` | extraction | CLI translator |

---

## 4. GraphBuildWorkflow — зачем child workflow

### 4.1 Топология

```
POST /api/v1/ingest
        │
        ▼
DocumentIngestWorkflow (parent, queue=kb-ingest)
   ├─ fetch_source
   ├─ parse_and_chunk            [role=extraction]
   ├─ index_vector
   ├─ inject_canonical
   ├─ extract_kg                 [role=extraction, queue=kb-ingest-llm]
   ├─ GraphBuildWorkflow         ◀────── child, awaited
   │      ↓ runs on queue=kb-ingest-llm
   │   ┌─ merge_and_resolve     [role=judge]
   │   └─ build_property_graph  [Neo4j writes]
   │      ↓ возвращает GraphBuildResult(merged, built)
   │      ↓ при ошибке → ChildWorkflowError → graph_status=vector_only
   ├─ push_wikibase             (skipped если vector_only)
   └─ finalize:
         fetch parent history
         fetch child history    ◀────── BEST-EFFORT (нет если vector_only)
         parse_activity_timings(..., models_per_role={...})
         insert_metrics(rows)
```

### 4.2 Три практических профита

**(1) Независимый retry/timeout.** Stuck merge_and_resolve не валит весь ingest. Retry-policy задана на уровне child workflow, не parent'а.

**(2) Раздельная visibility в Temporal UI.** В `http://localhost:8080`:
- `ingest-{doc_id}` — parent, видна vector-половина (fetch/parse/index/inject/extract) проходящая за секунды
- `graph-{doc_id}` — child рядом, своя timeline где каждый ER-judge LLM-call виден отдельно с его длительностью

**(3) Чистый vector_only fallback.** Parent ловит `ChildWorkflowError` (а не только `ActivityError`) → автоматически downgrade'ит `graph_status="vector_only"` → продолжает ingest.

### 4.3 Code-выдержка: parent зовёт child

[`src/workflow/document_ingest.py:170-186`](../../src/workflow/document_ingest.py):

```python
workflow.upsert_memo({"stage": "graph_build_child"})
log.info("→ GraphBuildWorkflow (child, queue=%s)",
         settings.temporal.llm_task_queue)
# merge_and_resolve + build_property_graph now run as
# a Temporal child workflow so they get independent
# retry / visibility / scheduling.  Parent awaits — keeps
# "ingest complete" semantics simple.  ChildWorkflowError
# is caught below alongside ActivityError so a stuck
# child still downgrades to vector_only without failing
# the whole document.
gb_result = await workflow.execute_child_workflow(
    GraphBuildWorkflow.run, kg,
    id=f"graph-{params.doc_id}",
    task_queue=settings.temporal.llm_task_queue,
    parent_close_policy=ParentClosePolicy.REQUEST_CANCEL,
    id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
)
merged = gb_result.merged
built = gb_result.built
```

И сразу под этим — except, который покрывает обе ошибки ([line 190](../../src/workflow/document_ingest.py)):

```python
except (ActivityError, ChildWorkflowError) as exc:
    log.warning("graph stage failed, downgrading to vector_only: %s", exc)
    graph_status = "vector_only"
```

### 4.4 Code-выдержка: child workflow

[`src/workflow/graph_build.py:51-86`](../../src/workflow/graph_build.py):

```python
@workflow.defn
class GraphBuildWorkflow:
    @workflow.run
    async def run(self, kg: KGExtracted) -> GraphBuildResult:
        log = workflow.logger
        workflow.upsert_memo({...})

        merged: Merged = await workflow.execute_activity(
            "merge_and_resolve", kg,
            result_type=Merged,
            start_to_close_timeout=timedelta(hours=1),
            heartbeat_timeout=timedelta(minutes=5),
            schedule_to_close_timeout=timedelta(hours=24),
            retry_policy=_HEAVY_FOREVER,
        )

        built: GraphBuilt = await workflow.execute_activity(
            "build_property_graph", merged,
            result_type=GraphBuilt,
            start_to_close_timeout=timedelta(hours=1),
            heartbeat_timeout=timedelta(minutes=5),
            schedule_to_close_timeout=timedelta(hours=24),
            retry_policy=_FAST_FOREVER,
        )
        return GraphBuildResult(merged=merged, built=built)
```

Возвращает `GraphBuildResult` — composition `Merged + GraphBuilt`. См. [`src/workflow/contracts.py:118-128`](../../src/workflow/contracts.py).

### 4.5 Регистрация worker'а

[`src/workflow/worker.py:81-96`](../../src/workflow/worker.py):

```python
main_worker = Worker(
    client,
    task_queue=settings.temporal.task_queue,
    workflows=[DocumentIngestWorkflow],          # ← parent here
    activities=MAIN_ACTIVITIES,
    max_concurrent_activities=settings.temporal.activity_concurrency,
)
# GraphBuildWorkflow runs on the LLM queue alongside merge_and_resolve
# + build_property_graph activities so the GPU-cap (concurrency=1)
# serialises the heavy work — child workflow dispatch itself is
# lightweight, the activities inside are the real LLM load.
llm_worker = Worker(
    client,
    task_queue=settings.temporal.llm_task_queue,
    workflows=[GraphBuildWorkflow],              # ← child here
    activities=LLM_ACTIVITIES,
    max_concurrent_activities=settings.temporal.llm_activity_concurrency,
)
```

`build_property_graph` зарегистрирован в обоих пулах (см. [`src/workflow/activities/__init__.py`](../../src/workflow/activities/__init__.py)) чтобы child мог её клеймить локально без cross-queue dispatch.

---

## 5. Code map — где что лежит

### 5.1 Новые файлы (этот спринт)

| Файл | Что |
|---|---|
| [`src/workflow/graph_build.py`](../../src/workflow/graph_build.py) | Определение `GraphBuildWorkflow` |
| [`src/observability/role_map.py`](../../src/observability/role_map.py) | `ACTIVITY_TO_ROLE` static map |
| [`tests/test_workflow/test_graph_build_workflow.py`](../../tests/test_workflow/test_graph_build_workflow.py) | Child workflow integration test |
| [`tests/test_retrieval/test_llm_factory.py`](../../tests/test_retrieval/test_llm_factory.py) | 3 wrapper + legacy fallback tests |

### 5.2 Изменённые файлы

| Файл | Что изменилось |
|---|---|
| [`src/config.py`](../../src/config.py) | `LLMRole` literal, `LiteLLMSettings.{extraction,judge,search}_model` + `model_for(role)` |
| [`src/retrieval/llm.py`](../../src/retrieval/llm.py) | Полностью переписан: `build_llm(role)` + 3 wrapper'а |
| [`src/workflow/contracts.py`](../../src/workflow/contracts.py) | `IngestParams` + `FinalizeIn` пополнены `extraction_model/judge_model/search_model`; новый `GraphBuildResult` |
| [`src/workflow/document_ingest.py`](../../src/workflow/document_ingest.py) | Inline `merge_and_resolve + build_property_graph` → `execute_child_workflow`; `except (ActivityError, ChildWorkflowError)` |
| [`src/workflow/worker.py`](../../src/workflow/worker.py) | `llm_worker` теперь хостит `GraphBuildWorkflow` |
| [`src/workflow/activities/__init__.py`](../../src/workflow/activities/__init__.py) | `build_property_graph` зарегистрирована в обоих пулах |
| [`src/workflow/activities/{extract_kg,parse_and_chunk,merge_and_resolve}.py`](../../src/workflow/activities/) | Переключены на role-specific factories |
| [`src/workflow/activities/finalize.py`](../../src/workflow/activities/finalize.py) | `_persist_ingest_metrics` теперь fetch'ит parent + child history |
| [`src/api/routes/ingest.py`](../../src/api/routes/ingest.py) | Snapshot per-role моделей + 3 новых Search Attributes |
| [`src/observability/ingest_metrics_extractor.py`](../../src/observability/ingest_metrics_extractor.py) | `models_per_role` kwarg + role-map lookup |
| [`scripts/setup_db.py`](../../scripts/setup_db.py) | Регистрация `ExtractionModel/JudgeModel/SearchModel` Search Attributes |
| [`infra/grafana/dashboards/02-version-compare.json`](../../infra/grafana/dashboards/02-version-compare.json) | `model_A/model_B` колонки + "Per-stage model usage" panel |

---

## 6. Walkthrough: что происходит на POST /ingest

Step-by-step с указанием файлов:

### Step 1: HTTP-приём в API

[`src/api/routes/ingest.py:58-180`](../../src/api/routes/ingest.py) — `upload_document`:

1. Auth: `X-API-Key` → `require_api_key` middleware
2. Multipart валидация (filename, content-type)
3. Upload в MinIO bucket `kb-uploads` (`storage.put_object` ← `src/storage/minio.py`)
4. `pg.insert_pending(doc_id, s3_uri, ...)` — insert строки в Postgres `documents` со status='pending' ([`src/storage/postgres.py`](../../src/storage/postgres.py))
5. **Snapshot per-role моделей** (это новое):
   ```python
   cfg = settings.litellm
   model = cfg.llm_model                  # global fallback
   extraction_model = cfg.model_for("extraction")
   judge_model = cfg.model_for("judge")
   search_model = cfg.model_for("search")
   ```
6. `start_workflow(DocumentIngestWorkflow.run, IngestParams(..., extraction_model, judge_model, search_model, version_tag, env), search_attributes={"VersionTag", "ExtractionModel", "JudgeModel", "SearchModel", "Env"})`
7. Возвращает `202 + {"job_id": doc_id}` — клиент уходит, дальше всё async

### Step 2: Parent DocumentIngestWorkflow (queue=kb-ingest)

[`src/workflow/document_ingest.py`](../../src/workflow/document_ingest.py):

| Stage | Что делает | LLM? |
|---|---|---|
| `fetch_source` | Скачивает файл из MinIO в локальный temp dir ([fetch_source.py](../../src/workflow/activities/fetch_source.py)) | — |
| `parse_and_chunk` | LlamaIndex IngestionPipeline: parse + semantic chunking + опц. translator ([parse_and_chunk.py](../../src/workflow/activities/parse_and_chunk.py)) | `extraction` |
| `index_vector` | BGE-M3 embeddings → Milvus collection ([index_vector.py](../../src/workflow/activities/index_vector.py)) | embedding only |
| `inject_canonical` | Regex для 24 identifier-типов (ИНН, IMEI, …) ([inject_canonical.py](../../src/workflow/activities/inject_canonical.py)) | — |
| `extract_kg` | LightRAG-style KG-извлечение (на `kb-ingest-llm` queue) ([extract_kg.py](../../src/workflow/activities/extract_kg.py)) | `extraction` |

### Step 3: Child GraphBuildWorkflow (queue=kb-ingest-llm)

[§ 4.4](#44-code-выдержка-child-workflow) выше показал код. Что внутри по существу:

1. **`merge_and_resolve` activity** ([merge_and_resolve.py](../../src/workflow/activities/merge_and_resolve.py)):
   - Читает `KGExtracted` (новые ноды документа) из staging-blob (MinIO)
   - Зовёт `merge_kg_extraction` ([`src/graph/merge.py`](../../src/graph/merge.py)) — orthographic dedup внутри документа
   - Зовёт `consolidate_phone_entities` ([`src/graph/phone_consolidation.py`](../../src/graph/phone_consolidation.py)) — E.164 нормализация
   - Зовёт **`resolve_entities`** ([`src/graph/entity_resolution.py`](../../src/graph/entity_resolution.py)) — это и есть **сличение новых сущностей с уже-сохранёнными в Neo4j** (см. [§ 8](#8-entity-resolution--как-новые-ноды-сличаются-с-существующими-graph-нодами))
   - Пишет результат в staging-blob, возвращает `Merged`

2. **`build_property_graph` activity** ([build_property_graph.py](../../src/workflow/activities/build_property_graph.py)):
   - Читает `Merged` из staging
   - Через LlamaIndex `PropertyGraphIndex` делает `MERGE` в Neo4j (`MERGE (n:__Entity__ {name: ...}) SET ...`)
   - Возвращает `GraphBuilt(entities, relations)`

Child возвращает `GraphBuildResult(merged=..., built=...)` обратно в parent.

### Step 4: Parent продолжает после child

- `push_wikibase` ([push_wikibase.py](../../src/workflow/activities/push_wikibase.py)) — только если `graph_status=="completed"` И wikibase enabled. См. [`wikibase.md`](wikibase.md).
- **`finalize`** ([finalize.py](../../src/workflow/activities/finalize.py)):
  1. UPDATE `documents.status`
  2. Cleanup staging-blobs из MinIO
  3. Rmtree локального temp dir
  4. **`_persist_ingest_metrics(payload)`** ← см. [§ 7](#7-ingest_metrics--per-activity-model)

---

## 7. `ingest_metrics` — per-activity model

### 7.1 Зачем

`ingest_metrics` — Postgres-таблица из analytics-спринта ([`analytics.md`](analytics.md)). Одна строка на (workflow, activity, attempt) с длительностью + tag-метаданными. До multimodel'а `model` колонка была одинаковая для всех 8 строк ingest'а (snapshot `LITELLM_LLM_MODEL` на submit). Теперь — **per-row**, отражает фактически использованную модель.

### 7.2 Маппинг activity → role → model

[`src/observability/role_map.py`](../../src/observability/role_map.py):

```python
ACTIVITY_TO_ROLE: Final[dict[str, LLMRole | None]] = {
    "fetch_source":         None,           # MinIO fetch — no LLM
    "parse_and_chunk":      "extraction",   # translation path uses extraction LLM
    "index_vector":         None,           # BGE-M3 embeddings, not LLM chat
    "inject_canonical":     None,           # regex identifier injection
    "extract_kg":           "extraction",   # LightRAG KG extraction
    "merge_and_resolve":    "judge",        # ER LLM-judge + cross-chunk merge
    "build_property_graph": None,           # Neo4j writes only
    "push_wikibase":        None,           # MediaWiki REST, no LLM
    "finalize":             None,           # Postgres + cleanup only
}
```

`None` ⇒ `model=NULL` в Postgres — честно, никакая LLM не звалась.

### 7.3 Extractor — resolve логика

[`src/observability/ingest_metrics_extractor.py:91-104`](../../src/observability/ingest_metrics_extractor.py):

```python
# Resolve the per-row model: lookup the role this
# activity uses, then pull the snapshotted model for that
# role; fall back to the default ``model`` argument when
# the per-role snapshot is empty; emit NULL for non-LLM
# activities (role=None — fetch, embed, regex, etc.).
role = ACTIVITY_TO_ROLE.get(name)
if role is None:
    row_model: str | None = None
else:
    row_model = (
        models_per_role.get(role) or model or None
    )
```

### 7.4 Где finalize тянет ОБЕ history

[`src/workflow/activities/finalize.py:88-145`](../../src/workflow/activities/finalize.py):

```python
async def _persist_ingest_metrics(payload: FinalizeIn) -> None:
    try:
        client = await get_temporal_client()
        info = activity.info()
        models_per_role = {
            "extraction": payload.extraction_model,
            "judge":      payload.judge_model,
            "search":     payload.search_model,
        }

        # 1. Parent history (the vector half + push_wikibase + finalize'
        # so-far).
        parent_handle = client.get_workflow_handle(
            info.workflow_id, run_id=info.workflow_run_id,
        )
        parent_history = await parent_handle.fetch_history()
        rows = parse_activity_timings(parent_history, ...,
                                      models_per_role=models_per_role)

        # 2. Child history — best-effort.  When graph_status="vector_only"
        # the child never ran, so the fetch yields "not found"; that's
        # fine and we just skip the merge-side rows.
        child_id = f"graph-{payload.ctx.doc_id}"
        try:
            child_handle = client.get_workflow_handle(child_id)
            child_history = await child_handle.fetch_history()
            child_rows = parse_activity_timings(child_history, ...,
                                                models_per_role=models_per_role)
            rows.extend(child_rows)
        except Exception as exc:
            activity.logger.info("ingest_metrics: child history fetch skipped (%s)", exc)
```

### 7.5 Что увидишь в Postgres после ingest'а

```sql
SELECT activity_name, model, version_tag, duration_ms
  FROM ingest_metrics
 WHERE doc_id = '<job_id>'::uuid
 ORDER BY started_at;
```

Пример (с `LITELLM_JUDGE_MODEL=gpt-4o-2024-08-06` override):

```
activity_name        | model              | version_tag    | duration_ms
---------------------+--------------------+----------------+------------
fetch_source         |                    | mm-judge-swap  |          34
parse_and_chunk      | gpt-4o-mini        | mm-judge-swap  |         100
index_vector         |                    | mm-judge-swap  |         619
inject_canonical     |                    | mm-judge-swap  |         141
extract_kg           | gpt-4o-mini        | mm-judge-swap  |        5462
merge_and_resolve    | gpt-4o-2024-08-06  | mm-judge-swap  |         487
build_property_graph |                    | mm-judge-swap  |         460
push_wikibase        |                    | mm-judge-swap  |           3
```

Видно: extraction (`extract_kg`, `parse_and_chunk`) → gpt-4o-mini; judge (`merge_and_resolve`) → gpt-4o-2024-08-06; всё non-LLM → NULL.

---

## 8. Entity Resolution — как новые ноды сличаются с существующими graph-нодами

Это происходит в `merge_and_resolve` (внутри child workflow), в функции `resolve_entities` файла [`src/graph/entity_resolution.py:1036`](../../src/graph/entity_resolution.py). Логика **не менялась** в этом спринте — она была написана раньше, просто переехала в child workflow. Но раз ты спросил — вот подробности.

### 8.1 Алгоритм (12 шагов)

Из docstring модуля [`src/graph/entity_resolution.py:14-38`](../../src/graph/entity_resolution.py):

```
1. Filter eligible labels (drop deterministic identifiers —
   Phone/Email/INN/etc. already canonicalised).
2. (incremental) Load canonical entities + embeddings from Neo4j.
3. Compute embeddings for new entities (single batched call).
4. Deterministic pre-pass — initialism regex, exact-normalised
   after stripping diacritics / punctuation.
5. Candidate pairs — same-label top-K cosine neighbors above LOW.
6. Auto-merge — cosine ≥ HIGH AND same script (both ASCII or
   both Cyrillic).  Cross-script always routes to LLM.
7. LLM-judge borderline pairs (batched 10, JSON YES/NO/UNSURE).
8. Union-find → connected components.
9. Verify large clusters (≥ `max_cluster_size`) via one LLM call;
   drop low-confidence members.
10. Hyper-hub clamp — clusters ≥ `hyper_hub_threshold` not
    auto-merged; flagged `er_review_needed` instead.
11. Pick canonical per cluster, consolidate descriptions via
    `_maybe_summarize_descriptions` from `merge.py`.
12. Build name_map, rewrite chunk-level KG_NODES_KEY metadata
    and merged_relations, drop self-loops, re-aggregate.
```

**Conservative ER** — LLM-judge default'ит на `DIFFERENT` при timeout / failure. False-negative (не смерджили хотя надо) лучше чем false-positive (смерджили двух разных людей → KG испорчен).

### 8.2 Load existing canonicals — выдержка

[`src/graph/entity_resolution.py:976-1004`](../../src/graph/entity_resolution.py):

```python
async def _load_existing_canonicals(
    graph_store: Any | None,
) -> list[_Item]:
    """Read Neo4j entities with `er_canonical_name` and their stored
    embedding.  Returns empty when graph_store is None or any error
    occurs (incremental ER is best-effort — without it we still do
    within-batch ER).
    """
    if graph_store is None:
        return []
    try:
        rows = await asyncio.to_thread(
            graph_store.structured_query,
            """
            MATCH (n:__Entity__)
            WHERE n.er_canonical_name IS NOT NULL
            RETURN n.name AS name,
                   labels(n) AS labels,
                   n.er_embedding AS er_embedding,
                   coalesce(n.mention_count, 1) AS mention_count,
                   coalesce(n.description, '') AS description
            LIMIT 5000
            """,
        )
    except Exception as exc:
        logger.warning("ER load existing canonicals failed: {err}", err=exc)
        return []
    ...
```

Загружает до 5000 канонических сущностей с embedding'ами из Neo4j. Дальше в `resolve_entities` они кладутся в одну корзину с новыми и идут через embedding cosine + LLM-judge pipeline.

### 8.3 Когда сличение фактически произойдёт

Внутри `resolve_entities` ([line 1109](../../src/graph/entity_resolution.py)):

```python
stored_items = await _load_existing_canonicals(graph_store)
```

Затем `stored_items` сливаются с новыми из текущего документа в one big list, после чего KNN-cosine + judge pipeline решает кто duplicate чего. Если `Иван Иванов (stored)` и `Иванов И.И. (new)` дали cosine 0.92 + same-script — auto-merge. Если cosine 0.7 — borderline → LLM-judge с моделью из `LITELLM_JUDGE_MODEL`.

### 8.4 Что увидишь в worker-логе

Из реального ingest'а:

```
2026-05-19 22:38:39.857 DEBUG  src.graph.entity_resolution:resolve_entities:1149
  ER judge-pair  'Семантический Разделитель' (label=Concept) ↔ 'Разделитель Предложений' (label=Concept)  cosine=0.644
2026-05-19 22:38:40.821 INFO  src.graph.entity_resolution:resolve_entities:1156
  ER judge-verdict  'Семантический Разделитель' vs 'Разделитель Предложений' = DIFFERENT
2026-05-19 22:38:40.821 INFO  src.graph.entity_resolution:resolve_entities:1214
  ER cluster  canonical='Анна Морозова'  size=2  members=['Анна Морозова (stored)', 'Морозова А. С.']  has_new=True
2026-05-19 22:38:40.823 INFO  src.graph.entity_resolution:resolve_entities:1309
  ER done  new_entities=3  canonical_clusters=1  merged=1  review=0
```

Третья строка — пример **именно того что ты спрашивал**: 'Морозова А. С.' (новая из текущего документа) смерджилась с 'Анна Морозова (stored)' (уже в Neo4j). Canonical name выбран как 'Анна Морозова'. Когда дальше `build_property_graph` напишет в Neo4j — обновит существующую ноду 'Анна Морозова' с новыми chunk-references, не создаст вторую.

### 8.5 Запись обратно в Neo4j

После ER кластеризации `resolve_entities` возвращает `name_map: dict[str, str]` который применяется к chunk-level KG metadata. Затем `build_property_graph` идёт в Neo4j через LlamaIndex `PropertyGraphIndex`. Каждый узел делается через `MERGE (n:__Entity__ {name: $name})` — Cypher's `MERGE` это upsert: создаст ноду если её нет, прочитает если есть. Через тот же MERGE добавляются property (`mention_count++`, `er_canonical_name`, embedding если меняется).

---

## 9. Smoke verification

End-to-end проверка multimodel-фичи. Предполагает что compose уже поднят (`docker compose -p kb-llamaindex ps` → 13 healthy сервисов).

```bash
# 0. Pre-flight: setup_db регистрирует новые SA + создаёт ingest_metrics
.venv/bin/python -m scripts.setup_db
# Ожидаем: "search-attrs registered names=['VersionTag', 'Model',
# 'ExtractionModel', 'JudgeModel', 'SearchModel', 'Env']"

# 1. Submit batch A с дефолтными моделями
nohup .venv/bin/python -m src.workflow.worker > /tmp/worker.log 2>&1 &
nohup .venv/bin/uvicorn src.api.main:app --port 8002 > /tmp/api.log 2>&1 &
sleep 5
curl -F file=@docs/bruno/samples/sample.txt \
     -H "X-API-Key: dev-local-key" -H "X-Version-Tag: smoke-A" \
     http://127.0.0.1:8002/api/v1/ingest

# Дождаться "workflow done" в /tmp/worker.log

# 2. Сменить ОДНУ роль (judge) и перезапустить
pkill -f "src.workflow.worker"; pkill -f "uvicorn.*8002"
sleep 3
LITELLM_JUDGE_MODEL=gpt-4o-2024-08-06 nohup .venv/bin/python -m src.workflow.worker > /tmp/worker.log 2>&1 &
LITELLM_JUDGE_MODEL=gpt-4o-2024-08-06 nohup .venv/bin/uvicorn src.api.main:app --port 8002 > /tmp/api.log 2>&1 &
sleep 5

# 3. Submit batch B с новым judge
curl -F file=@docs/bruno/samples/sample.txt \
     -H "X-API-Key: dev-local-key" -H "X-Version-Tag: smoke-B" \
     http://127.0.0.1:8002/api/v1/ingest

# 4. Проверить per-activity model в Postgres
docker exec kb-llamaindex-postgres-1 psql -U postgres -d kb_llamaindex -c "
SELECT activity_name, model, version_tag
  FROM ingest_metrics
 WHERE version_tag IN ('smoke-A', 'smoke-B')
 ORDER BY activity_name, version_tag"
```

**Ожидаемый результат:**

```
activity_name        | model              | version_tag
---------------------+--------------------+-------------
extract_kg           | gpt-4o-mini        | smoke-A      ← extraction, не менялся
extract_kg           | gpt-4o-mini        | smoke-B      ← тот же
merge_and_resolve    | gpt-4o-mini        | smoke-A      ← judge default
merge_and_resolve    | gpt-4o-2024-08-06  | smoke-B      ← judge swap!
parse_and_chunk      | gpt-4o-mini        | smoke-A
parse_and_chunk      | gpt-4o-mini        | smoke-B
fetch_source         |                    | smoke-A      ← NULL (no LLM)
fetch_source         |                    | smoke-B
...
```

**Что валидирует:**
- ✅ Per-role snapshot работает (только judge изменился)
- ✅ Other LLM activities не затронуты
- ✅ Non-LLM activities получают NULL
- ✅ Child workflow history (merge_and_resolve, build_property_graph) сливается с parent в один список

5. **Visual check в Temporal UI** — `http://localhost:8080` → найди `ingest-<job_id>` → должна быть nested `graph-<job_id>` со своими timing'ами.

6. **Grafana compare** — `http://localhost:3001/d/kb-ingest-version-compare/` → выбери `smoke-A` vs `smoke-B` → delta-таблица покажет `model_A` / `model_B` колонки.

---

## 10. Observability — что смотреть когда

### 10.1 Temporal UI (http://localhost:8080)

- `ingest-{doc_id}` — parent workflow. Видна полная цепочка активностей + ChildWorkflowExecutionStarted/Completed events для graph-стадии.
- `graph-{doc_id}` — child workflow. Отдельная execution с двумя активностями (merge_and_resolve + build_property_graph). Открой Pending Activities если ER завис.

### 10.2 Grafana dashboards (http://localhost:3001)

| Dashboard | Что показывает |
|---|---|
| `kb-ingest-overview` | Live: pollers, slots, p50/p95 per activity, throughput, failed rate, recent ingests |
| `kb-ingest-version-compare` | Two tag side-by-side + delta table + **per-stage model usage** (наш Stage 5 add) |
| `kb-ingest-run-drilldown` | Один doc_id: bar per activity duration, timeline table |

### 10.3 Postgres queries

```sql
-- Какие модели использовались на стейдже X?
SELECT activity_name, model, COUNT(*) AS n
  FROM ingest_metrics
 WHERE activity_name = 'merge_and_resolve'
 GROUP BY activity_name, model;

-- Latency per role-model
SELECT model, AVG(duration_ms)::int, COUNT(*)
  FROM ingest_metrics
 WHERE activity_name = 'extract_kg' AND model IS NOT NULL
 GROUP BY model;

-- Какие version_tag'и есть в БД (для template-vars)
SELECT DISTINCT version_tag FROM ingest_metrics ORDER BY version_tag;
```

### 10.4 Prometheus (http://localhost:9092)

`temporal_activity_execution_latency_*` — histogram per `activity_type` label. Per-model graphs пока **не** доступны через Prometheus — Temporal SDK не лейблит метрики моделью. Если нужна per-model latency aggregation — иди в Grafana → PG datasource.

---

## 11. Troubleshooting

| Симптом | Причина | Действие |
|---|---|---|
| `ingest_metrics.model = NULL` для `extract_kg` строки | API сабмитил **без** snapshot'а (старый процесс) или env пустой и `llm_model` тоже | `psql -c "SELECT version_tag FROM ingest_metrics ..."` — если `unspecified` → старый процесс. Перезапустить API из multimodel worktree |
| `merge_and_resolve.model = gpt-4o-mini` хотя сменил `LITELLM_JUDGE_MODEL` | API не перезапущен после env-change (snapshot снимается в момент submit) | `pkill -f uvicorn` + restart API. Сабмит ДО рестарта получит старую модель — это by design (snapshot at submit time) |
| Two distinct workflow IDs `ingest-X` и `graph-X` для одного ingest'а | Нормально — child workflow живёт отдельной execution | Это not bug. См. [§ 4.2](#42-три-практических-профита) |
| `graph_status="vector_only"` после ingest'а | Child workflow failed (LLM down, ER timeout, Neo4j unreachable) | Открой `graph-{doc_id}` в Temporal UI — там видна activity которая упала + её stack. Это **best-effort** — документ доступен для vector-поиска |
| Worker занят на старых workflow'ах из других worktree'ов | Несколько worker'ов из разных worktree подключены к одному Temporal cluster и конкурируют за task queue | `lsof -p <pid>` → `cwd` показывает worktree. Убей `kill <pid>` тот что не нужен |
| `ingest_metrics: child history fetch skipped` warning в worker log | Path был `vector_only` → child не запускался → `get_workflow_handle("graph-X").fetch_history()` вернул NotFound | Это норма — warning'у не валит pipeline; metrics-rows для parent части записываются нормально |
| Temporal SA registration в `setup_db` падает с "advanced visibility unsupported" | Postgres visibility store не поддерживает custom SA | Не блокирует — `ingest_metrics` path работает без SA. Можно проигнорить, либо мигрировать Temporal на ES visibility (большой шаг) |

---

## 12. Cross-references

### 12.1 Связанные runbook'и
- [`docs/runbook/analytics.md`](analytics.md) — Grafana dashboards setup, `ingest_metrics` schema, version_tag mechanics
- [`docs/runbook/wikibase.md`](wikibase.md) — Wikibase population (orthogonal feature)

### 12.2 Архитектурные документы
- [`docs/MODELS.md`](../MODELS.md) — per-role model guidance, escalation path
- [`docs/architecture.html`](../architecture.html) — визуальная карта (5 sections)
- [`docs/architecture.d2`](../architecture.d2) / [`docs/architecture.svg`](../architecture.svg) — D2 source + рендер

### 12.3 Specs
- `docs/superpowers/specs/2026-05-15-ingest-temporal-workflow-design.md` — original Temporal workflow design
- `docs/superpowers/plans/2026-05-18-multimodel-and-child-merge-workflow.md` — этого спринта

### 12.4 Тесты как живая документация
- [`tests/test_retrieval/test_llm_factory.py`](../../tests/test_retrieval/test_llm_factory.py) — 5 case'ов: legacy fallback + 3 wrapper + per-role fallback
- [`tests/test_observability/test_ingest_metrics_extractor.py`](../../tests/test_observability/test_ingest_metrics_extractor.py) — 9 case'ов: empty, single, retry, in-flight, per-role resolution, fallback, models_per_role omitted
- [`tests/test_workflow/test_graph_build_workflow.py`](../../tests/test_workflow/test_graph_build_workflow.py) — child chain test
- [`tests/test_workflow/test_document_ingest_workflow.py`](../../tests/test_workflow/test_document_ingest_workflow.py) — 5 случаев включая `test_graph_failure_via_child_downgrades`

### 12.5 Ключевые env vars (общий список)
```env
LITELLM_LLM_MODEL=gpt-4o-mini           # global fallback for all roles
LITELLM_EXTRACTION_MODEL=               # role override (empty = fallback)
LITELLM_JUDGE_MODEL=
LITELLM_SEARCH_MODEL=
LITELLM_FUNCTION_CALLING=true           # disable for non-tool-calling models

# Analytics layer (from analytics-grafana sprint)
ANALYTICS_VERSION_TAG=unspecified       # default if X-Version-Tag header missing
ANALYTICS_ENV_NAME=dev-local            # propagated to ingest_metrics.env

# Temporal queues (unchanged)
TEMPORAL_TASK_QUEUE=kb-ingest
TEMPORAL_LLM_TASK_QUEUE=kb-ingest-llm
TEMPORAL_LLM_ACTIVITY_CONCURRENCY=1     # GPU serialization cap
```
