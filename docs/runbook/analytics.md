# Runbook по аналитике ingest

> Этот runbook — про **метрики ingest-конвейера** (Prometheus / Grafana /
> Postgres `ingest_metrics`). Аналитические **запросы к графу знаний**
> (`/api/v1/analyze`, каталог 42 примитивов, материализация, мониторинг Arc-2)
> — в [`graph-analytics.md`](graph-analytics.md).

## 1. Обзор

Тайминги каждой активности ingest-workflow собираются двумя независимыми путями, дублирующими друг друга:

1. **Live aggregated (Prometheus).** Temporal Python SDK воркера выставляет встроенный Prometheus-exporter на `host:9090/metrics`. Prometheus (в compose) скрейпит его каждые 15с. Метрики (`temporal_activity_execution_latency_*`, `_succeed_endtoend_latency_count`, `_execution_failed`) автоматически labeled by `activity_type` / `task_queue`.
2. **Frozen per-run (Postgres).** Активность `finalize` в конце каждого workflow читает её собственный history через `WorkflowHandle.fetch_history()`, парсит пары `Scheduled → Started → Completed/Failed/TimedOut/Canceled` и пишет одну строку в Postgres `ingest_metrics` на каждую (activity, attempt). Best-effort: сбой не валит workflow.

Grafana (`:3001`) подключена к обоим источникам и обслуживает три дашборда: **Ingest Overview** (live, Prom), **Version compare** (PG), **Run drill-down** (PG).

Ссылка на план: `docs/superpowers/plans/2026-05-18-grafana-analytics.md` (если перенесён в репо) или `~/.claude/plans/merry-scribbling-thimble.md`.

## 2. Поднятие (bring-up)

```bash
docker compose -p kb-llamaindex up -d prometheus grafana
sleep 5
curl -sf http://localhost:9092/-/healthy && echo "prom OK"
curl -sf http://localhost:3001/api/health && echo "grafana OK"
```

Авто-provisioning сразу загружает datasource'ы (`prom-kb`, `pg-kb`) и три дашборда в папку `kb-llamaindex` (см. `infra/grafana/provisioning/`).

## 3. Воркер — Prometheus exporter

Воркер должен запускаться с включённым флагом `METRICS_ENABLED=true` (по умолчанию). Тогда при старте увидим в логах:

```
temporal worker  prometheus exporter listening on 0.0.0.0:9090
```

Prometheus тянет с `host.docker.internal:9090` (compose service `prometheus` имеет `extra_hosts: host.docker.internal:host-gateway` — работает на Docker Desktop / macOS / Linux). Проверка:

```bash
curl http://localhost:9092/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health}'
# temporal-worker  health=up
# prometheus-self  health=up
```

Если `temporal-worker.health=down` — воркер не запущен или порт 9090 занят. Активность-метрики появляются после первого реального ingest'а (histogram bucket'ы инициализируются по факту).

## 4. Тегирование версий (version tagging)

Каждый ingest снабжается тегом `version_tag` (строка, произвольная). Сабмит:

```bash
# Per-request (header):
curl -F file=@doc.txt -H "X-API-Key: $API_KEY" \
     -H "X-Version-Tag: qwen3-baseline" \
     http://localhost:8000/api/v1/ingest

# Per-batch (env default — если хочется не повторять header):
export ANALYTICS_DEFAULT_VERSION_TAG=qwen3-baseline
# … 20 ingest'ов без header'а — все попадут в этот snapshot

# Поменяли модель:
export LITELLM_LLM_MODEL=llama3.3:70b
export ANALYTICS_DEFAULT_VERSION_TAG=llama-3.3-baseline
# перезапустить API + worker → batch №2 с новым тегом
```

> ⚠️ Имя env-переменной — `ANALYTICS_DEFAULT_VERSION_TAG`, НЕ `ANALYTICS_VERSION_TAG`.
> Поле `AnalyticsSettings.default_version_tag` имеет prefix `ANALYTICS_` без alias
> (`src/config.py`), поэтому pydantic-settings читает именно
> `ANALYTICS_DEFAULT_VERSION_TAG`. Старое имя `ANALYTICS_VERSION_TAG` (всё ещё
> фигурирует в `.env.example`) молча игнорируется → ingest получит default
> `unspecified`. Для разовых сабмитов надёжнее header `X-Version-Tag`.

В Postgres `ingest_metrics` поле `model` snapshot'ится из конфига LiteLLM **на
момент сабмита**: глобальный default берётся из `settings.litellm.effective_base`,
а per-role значения (`extraction` / `judge` / `search`) — из
`settings.litellm.model_for(role)` (см. `src/api/routes/ingest.py`). Поэтому смена
модели в env автоматически отражается в данных. Per-activity разбивка — в §6a.

`env` — это `ANALYTICS_ENV_NAME` (default `dev-local`). Используется для разделения dev / staging / prod в одной БД, если нужно.

## 5. Smoke (end-to-end проверка)

```bash
# 1. Поднять стек
docker compose -p kb-llamaindex up -d
python -m scripts.setup_db   # создаст ingest_metrics + зарегистрирует search attrs
uv run python -m src.workflow.worker &
uvicorn src.api.main:app --port 8000 &

# 2. Submit под версией A
curl -F file=@tests/test_ingestion/fixtures/sample.txt \
     -H "X-API-Key: dev-local-key" \
     -H "X-Version-Tag: alpha" \
     http://localhost:8000/api/v1/ingest

# 3. Дождаться "workflow done", проверить Postgres
psql -h localhost -U postgres -d kb_llamaindex -c \
  "SELECT activity_name, duration_ms FROM ingest_metrics \
   WHERE version_tag='alpha' ORDER BY started_at"
# Ожидаем 8 строк — fetch_source, parse_and_chunk, index_vector,
# inject_canonical, extract_kg, merge_and_resolve, build_property_graph,
# push_wikibase

# 4. То же под версией B (после смены model env), затем:
open http://localhost:3001/d/kb-ingest-version-compare/
# В шапке выбрать version_a=alpha, version_b=beta → delta-табличка покажет
# что изменилось.
```

## 6. Дашборды

| URL slug | Заголовок | Источник данных | Назначение |
|---|---|---|---|
| `/d/kb-ingest-overview` | Ingest · Overview (live) | Prom + PG | Поллеры, слоты, p50/p95 по активностям, throughput, доля failed, таблица недавних ingest'ов |
| `/d/kb-ingest-version-compare` | Ingest · Version compare | PG | Бар-чарт avg duration A vs B + дельта-таблица avg + p95 |
| `/d/kb-ingest-run-drilldown` | Ingest · Run drill-down | PG | Один doc_id: бар по стадиям + таблица timeline |

Все три auto-loaded из `infra/grafana/dashboards/*.json` через provisioning. Чтобы поправить — отредактировать JSON и `docker compose restart grafana` (либо вызвать `POST /api/admin/provisioning/dashboards/reload`).

## 6a. Колонка model по активностям (multimodel-плагин)

Каждая строка `ingest_metrics` несёт `model` — модель, **фактически использованную** именно для этой активности. Резолвится в момент `finalize` через `src/observability/role_map.py:ACTIVITY_TO_ROLE`:

| Активность | LLM-роль | Колонка `model` |
|---|---|---|
| `parse_and_chunk` | extraction | snapshot LITELLM_EXTRACTION_MODEL (с fallback на LITELLM_LLM_MODEL) |
| `extract_kg` | extraction | то же |
| `merge_and_resolve` | judge | snapshot LITELLM_JUDGE_MODEL |
| `fetch_source` / `index_vector` / `inject_canonical` / `build_property_graph` / `push_wikibase` / `finalize` | — | `NULL` (LLM не звался) |

Snapshot моделей делает API в момент `/ingest` (см. `src/api/routes/ingest.py`), значения летят в `IngestParams` → `FinalizeIn` → `parse_activity_timings(..., models_per_role={...})`. Поэтому смена `LITELLM_*_MODEL` в env **между** ingest'ами немедленно отражается на новых строках, а старые сохраняют исторический model.

Compare-дашборд (`/d/kb-ingest-version-compare/`) теперь показывает `model_A` / `model_B` в delta-таблице (отдельный JSON-update Stage 5) и отдельную таблицу `Per-stage model usage` для side-by-side debug'а.

**Каверзный случай — child workflow.** Активности `merge_and_resolve` и `build_property_graph` живут в `GraphBuildWorkflow` (child от `DocumentIngestWorkflow`). Их event-history лежит в **отдельной** workflow execution (`graph-{doc_id}`). `finalize._persist_ingest_metrics` явно тянет обе истории и мерджит в один список. Если parent выпал в `vector_only` (child failed), child-history просто не существует — fetch swallow'ит ошибку и в `ingest_metrics` ляжет только parent-side rows.

## 7. Хранение (retention)

| Слой | Retention |
|------|-----------|
| Prometheus | 30 дней (`--storage.tsdb.retention.time=30d` в compose) |
| Postgres `ingest_metrics` | **навсегда** до явной очистки |
| Temporal event history | 24 часа (по умолчанию) |

Postgres — единственный источник для долгосрочных сравнений. Если нужна очистка:

```sql
DELETE FROM ingest_metrics WHERE completed_at < NOW() - INTERVAL '6 months';
```

## 8. Замечание для Linux

На Linux compose `extra_hosts: host.docker.internal:host-gateway` маппит на gateway-IP контейнера. Если не работает (старый Docker), заменить в `infra/prometheus/prometheus.yml` на `172.17.0.1:9090` или прописать явный bridge-network gateway.

## 9. Диагностика проблем

| Симптом | Причина | Действие |
|---|---|---|
| `temporal-worker target = down` в Prom | Воркер не запущен / port 9090 занят / `METRICS_ENABLED=false` | Проверить `pgrep -af src.workflow.worker`, `lsof -i:9090`, `env | grep METRICS_ENABLED` |
| `ingest_metrics persist failed (best-effort)` в worker-логе | Temporal SDK не достал history (cluster moment) / PG down | Не критично — workflow прошёл; следующий ingest напишет. Долгосрочное чинить root cause (PG / Temporal) |
| Grafana panel "No data" на Prom-запросе | Метрика ещё не materialized: первый ingest нужен | Прогнать smoke; histogram bucket появится после первого `_observe()` |
| Compare-дашборд показывает пустую таблицу | Один из выбранных тегов отсутствует в `ingest_metrics` | `SELECT DISTINCT version_tag FROM ingest_metrics` — выбрать существующие |
| Drill-down `$doc_id` не предлагает свежий ingest | Кэш Grafana template-variable 30s | F5 / `Reload variables` в шапке дашборда |
| Search attribute `VersionTag` не виден в Temporal UI | Visibility-store без advanced support (Postgres-only) | Не блокирует analytics — ingest_metrics всё равно пишет тег |

## 10. Дальнейшие улучшения

- **Тайминги под-стадий внутри активности.** Внутри `extract_kg` сейчас единый блок. Прокинуть `time.perf_counter` через `activity.heartbeat({"sub_stage": "llm", "ms": ...})` для разбивки на `llm_call_ms` / `parse_ms`.
- **Самозамер для finalize.** `fetch_history()` внутри finalize не видит свой Completed-event → строка finalize не пишется. Добавить локальный `perf_counter` для отдельной MetricRow.
- **Алерты.** Grafana alerting на `p95_extract_kg > 30s` и rate `temporal_activity_execution_failed > 0`.
- **Связка с LangFuse.** Соединить LangFuse trace_id с `workflow_run_id` через label, чтобы из drill-down прыгать в LLM-trace.
