# Руководство по развёртыванию

Пошагово от чистой машины до работающего стека
`kb-llamaindex`.  Покрывает локальную разработку (macOS / Linux), staging и
операционные регуляторы, нужные для движения к продакшену.

Стек **оркестрируется Temporal**: API ставит в очередь Temporal'овский
`DocumentIngestWorkflow`, а отдельный процесс worker'а поллит Temporal'овские
task queue, чтобы делать работу.  Топологию очередей + пер-очередную
конкурентность см. в `docs/QUEUES.md`.  Архитектуру системы см. в
`docs/ARCHITECTURE.md`.

> **Два пути развёртывания.**  Разделы 1–14 описывают **dev / host-process**
> сетап: `docker-compose.yml` поднимает backing-хранилища + Temporal **и**
> `litellm`, а API и worker гоняются **на хосте** (`uv run ...`).  Для
> **продакшена** есть отдельный путь — `docker-compose.prod.yml` +
> корневой `Dockerfile`, которые контейнеризуют **само приложение** (api /
> worker / mcp) и **выносят** `litellm`/`ollama` на внешние хосты.  См.
> раздел **0. Продакшен: контейнеризованный compose** ниже.

---

## 0. Продакшен: контейнеризованный compose

В корне репозитория лежат `Dockerfile` + `docker-compose.prod.yml` —
полный продакшен-путь, поднимающий **всё приложение целиком в compose**
(в отличие от dev-сетапа из разделов 4/7, где app гоняется на хосте).

### 0a. Образ (`Dockerfile`)

Один uv-based образ на **все** роли приложения (api / worker / mcp);
конкретная роль выбирается `command`'ом в compose.  База — `python:3.12-slim`,
зависимости ставятся `uv sync --frozen --no-dev`.  Default `CMD` поднимает API
(`uvicorn src.api.main:app --host 0.0.0.0 --port 8000`).  `.dockerignore`
исключает `.git` / `.venv` / worktrees / `tests` / `docs`, но **оставляет
`prompts/`** (там лежат шаблоны ответов, нужные в рантайме).

### 0b. Стек (`docker-compose.prod.yml`)

Поднимает сервисы приложения:

| Сервис | Порт (operator-facing) | Назначение |
|---|---|---|
| `api` | `:8000` (`API_PORT`) | FastAPI |
| `worker` | — (только метрики `9090..9096`) | Temporal-worker, все 7 пулов; сплит на масштаб через `WORKER_GROUPS` |
| `mcp-search` | `:9001` | MCP-сервер поиска |
| `mcp-tools` | `:9002` | MCP-сервер инструментов |

…плюс все backend'ы в том же compose: `postgres`, `temporal` + `temporal-ui`,
`etcd`, `minio`, `milvus`, `neo4j` (apoc + gds), `prometheus`, `grafana` и
`redis` (LLM-кеш; используется, как только смержится `feature/redis-llm-cache`).

Ключевые отличия от dev-`docker-compose.yml`:

* **Приложение в контейнерах.**  api / worker / mcp собираются из корневого
  `Dockerfile` и гоняются в compose — на хосте `uv run ...` не нужен.
* **`litellm` и `ollama` ИСКЛЮЧЕНЫ.**  Они живут на отдельных/внешних хостах;
  приложение ходит на них через `LITELLM_BASE_URL` (**обязательная** env —
  compose упадёт с ошибкой, если она не задана).
* **App↔backend трафик идёт по compose-internal DNS** (по именам сервисов);
  наружу публикуются только operator-facing порты (без коллизий).
* **Wikibase-стек** (`wikibase`, `wikibase-mysql`, `wdqs`) спрятан за compose-
  профиль `wikibase` — **opt-in**, по умолчанию выключен.
* **Prometheus** использует `infra/prometheus/prometheus.prod.yml`, который
  скрейпит пул-порты worker-контейнера `worker:9090..9096`.

### 0c. Env (`.env.prod.example`)

Скопируйте шаблон в `.env` (compose читает `.env` по умолчанию) и заполните —
**ротируйте каждый дефолтный credential**:

```env
# ── ВНЕШНИЙ LLM-прокси (litellm/ollama НЕ в этом compose) ──
LITELLM_BASE_URL=http://your-litellm-host:4000   # REQUIRED — compose упадёт, если пусто
LITELLM_API_KEY=sk-change-me
LITELLM_EMBEDDING_DIM=1536        # MUST совпадать с коллекцией Milvus

# ── API ──
API_KEYS=change-me-strong-key
API_PORT=8000

# ── Секреты / credentials (РОТИРУЙТЕ) ──
NEO4J_PASSWORD=change-me
POSTGRES_PASSWORD=change-me
MINIO_ACCESS_KEY=change-me
MINIO_SECRET_KEY=change-me
GRAFANA_ADMIN_PASSWORD=change-me

# ── Память Neo4j (под хост; Leiden на большом KG прожорлив до heap) ──
NEO4J_HEAP_MAX=4G
NEO4J_PAGECACHE=2G

# ── Temporal (512 = прод-пол; НЕИЗМЕНЯЕМ после первого init) ──
TEMPORAL_NUM_HISTORY_SHARDS=512

# ── Opt-in фичи ──
CLASSIFIER_ENABLED=false
INGEST_ADMISSION_ENABLED=false
INGEST_ADMISSION_MAX_INFLIGHT=1
WIKI_ENABLED=false
LLM_CACHE_ENABLED=false           # консьюмится, когда смержится feature/redis-llm-cache

# ── Топология worker'а ──
WORKER_GROUPS=                    # пусто = один контейнер тянет все 7 пулов; задайте подмножество (напр. llm) для сплита

# ── Оверрайды хост-портов (избежать коллизий со сторонними стеками) ──
# MINIO_CONSOLE_PORT=9101   # дефолт 9101, чтобы не клэшить с mcp-search :9001
# PROMETHEUS_PORT=9092
# GRAFANA_PORT=3001
```

### 0d. Команды

```bash
# Core (без wikibase)
docker compose -f docker-compose.prod.yml up -d

# С wikibase-стеком (opt-in профиль)
docker compose -f docker-compose.prod.yml --profile wikibase up -d
```

### 0e. Прод-харденинг

Заметки по харденингу — инлайн в самом compose.  Захардененный прод гоняет
`temporalio/server` + одноразовую schema-миграцию через `temporal-sql-tool`
(вместо образа `auto-setup`) и даёт Temporal **собственный** Postgres.  Общий
прод-чеклист (auth-ротация, персистентность, супервизия, сетевая изоляция)
см. в разделе **13** ниже — он применим к обоим путям.

> **Статус валидации.**  `docker compose config` проходит; `docker build
> --check` проходит.  Полный build образа в dev **не** прогонялся (тянет
> PyPI) — прогоните его в вашем окружении перед первым деплоем.

---

## 1. Предварительные требования

| Инструмент | Версия | Зачем |
|---|---|---|
| **Docker + Compose** | 24+ | Все backing-хранилища + Temporal гоняются в контейнерах |
| **Python** | 3.11 или 3.12 | API + worker |
| **uv** | 0.4+ | Пакетный менеджер проекта (`uv run ...`) |
| **OpenAI API key** | — | upstream LiteLLM (default `large`-tier = `gpt-4o-mini`) |

Опционально (только если гоняете локальную модель вместо OpenAI):

| Инструмент | Версия | Зачем |
|---|---|---|
| **Ollama** / vLLM | 0.5+ | Локальная LLM `small`-tier + embedding'и |

---

## 2. Клонирование и установка

```bash
git clone <repo-url> kb-llamaindex
cd kb-llamaindex

# uv reads pyproject.toml + uv.lock, creates .venv automatically.
uv sync
```

Проверка:

```bash
uv run python -c "import llama_index, fastapi, temporalio; print('ok')"
```

---

## 3. Конфигурация окружения

Скопируйте пример-файл и отредактируйте значения:

```bash
cp .env.example .env
$EDITOR .env
```

Критичные регуляторы:

```env
# ── OpenAI upstream (REQUIRED if litellm_config.yaml points at openai/*)
OPENAI_API_KEY=sk-...

# ── API auth (rotate per environment)
API_KEYS=dev-local-key

# ── Two physical model tiers (you manage exactly TWO names).  Every
# logical role resolves to one of these (small = high-volume local,
# large = final synthesis only).  See docs/MODELS.md.
LITELLM_MODEL_SMALL=gemma4:e4b
LITELLM_MODEL_LARGE=gpt-4o-mini

# ── Embedding dim must match the model AND Milvus: 1536 for
# text-embedding-3-small, 768 for nomic-embed-text, 3072 for -3-large.
LITELLM_EMBEDDING_MODEL=text-embedding-3-small
LITELLM_EMBEDDING_DIM=1536
MILVUS_DIM=1536          # MUST match LITELLM_EMBEDDING_DIM

# ── Russian normalisation of the knowledge graph (set false to
# skip the LLM translation cost on ingest; graph stays in source language)
INGESTION_TRANSLATE_TO_RUSSIAN=true
INGESTION_TRANSLATION_CONCURRENCY=4

# ── Opt-in subsystems (default OFF) ────────────────────────────────
WIKIBASE_ENABLED=false   # push canonical entities into self-hosted Wikibase
WIKI_ENABLED=false       # continuous per-entity MediaWiki article editor
```

Переход на сетап с локальной моделью → см. `docs/MODELS.md`.

---

## 4. Поднимите стек хранилищ + оркестрации

```bash
docker compose up -d
```

Это стартует следующие контейнеры:

| Контейнер | Хост-порт(ы) | Назначение | Healthcheck |
|---|---|---|---|
| `etcd` | — | Метаданные Milvus | `etcdctl endpoint health` |
| `minio` | 9000, 9001 (console) | Object-store Milvus + пользовательские загрузки (bucket `kb-uploads`) | `/minio/health/live` |
| `milvus` | 19530, 9091 | Векторный индекс (HNSW по умолчанию) | `:9091/healthz` |
| `neo4j` | 7474 (web), 7687 (bolt) | Property graph (+ APOC + GDS) | `:7474` spider-проба |
| `postgres` | 5432 | Таблица статусов задач + метрики ingest; также бэкит Temporal (отдельные DB) | `pg_isready` |
| `temporal` | 7233 | Движок workflow'ов (образ `auto-setup`) | — (зависит от postgres) |
| `temporal-ui` | 8080 | Temporal Web UI | — |
| `litellm` | 4000 | LLM-шлюз (читает `OPENAI_API_KEY`) | `/health/liveliness` |
| `prometheus` | 9092 → 9090 | Скрейпит метрики worker'а + Temporal | — |
| `grafana` | 3001 → 3000 | Дашборды | — |
| `wikibase-mysql` | — | MariaDB, бэкящая Wikibase/MediaWiki | `mariadb-admin ping` |
| `wikibase` | 8181 → 80 | Wikibase + MediaWiki (opt-in цель) | `Special:Version` |
| `wdqs` | 8989 → 9999 | Wikibase Query Service (SPARQL) | — (может флапать; опционален в рантайме) |

Заметки:
* Контейнеры `wikibase` / `wikibase-mysql` / `wdqs` всегда стартуют со
  стеком, но **используются** только когда `WIKIBASE_ENABLED=true` или
  `WIKI_ENABLED=true`.  У `wdqs` нет healthcheck'а, и он известен тем, что флапает на
  бутстрапе — он опционален в рантайме и не блокирует ingest/search.
* Temporal здесь делит app-инстанс `postgres` (отдельные DB `temporal` /
  `temporal_visibility`).  `NUM_HISTORY_SHARDS` дефолтится на 512 (
  прод-пол) и **неизменяем после первого init кластера** — задайте
  `TEMPORAL_NUM_HISTORY_SHARDS=4` на маленькой dev-машине *до* первого
  бута.  См. комментарии по прод-харденингу в `docker-compose.yml`.
* **Worker и API гоняются на хосте** (не в compose).  Prometheus
  скрейпит хост-worker через `host.docker.internal` (экспортер на
  `METRICS_BIND_ADDRESS`, default `0.0.0.0:9090`).  Это **dev-путь**: app
  на хосте, `litellm` — в compose.  Продакшен-путь контейнеризует app и
  выносит `litellm`/`ollama` наружу — см. раздел **0** выше.

Дождитесь, пока всё станет healthy:

```bash
docker compose ps
# Core STATUS columns should show "healthy"
```

Ручная проба:

```bash
curl -fsS http://localhost:4000/health/liveliness   # LiteLLM
curl -fsS http://localhost:9091/healthz             # Milvus
docker exec kb-llamaindex-postgres-1 pg_isready -U postgres
```

---

## 5. Инициализация схем

`scripts/setup_db.py` идемпотентен — безопасно перезапускать.

```bash
uv run python -m scripts.setup_db
```

Что он делает:
* Создаёт таблицу `documents` (+ индексы по status / department) и таблицу
  `ingest_metrics` в Postgres.
* Пингует Milvus (коллекция создаётся лениво `MilvusVectorStore`
  при первом insert'е).
* Обеспечивает существование upload-bucket'а MinIO (`MINIO_BUCKET`, default `kb-uploads`).
* Регистрирует кастомные Search Attributes Temporal, используемые слоем аналитики
  (no-op / предупреждение, если Postgres visibility-store их не поддерживает).
* Neo4j не нужна схема — лейблы и индексы эмитятся в момент insert'а
  через `PropertyGraphIndex`.

### 5b. (Опционально) Бутстрап Wikibase + wiki-расписания

Только если `WIKIBASE_ENABLED=true` и/или `WIKI_ENABLED=true`.

```bash
# One-time Wikibase bootstrap: creates base-class Items + Properties,
# provisions the runtime bot account, caches QIDs/PIDs into Neo4j.
uv run python -m scripts.setup_wikibase
#   --dry-run        report planned creates without writing
#   --refresh-cache  re-pull existing QIDs/PIDs into the Neo4j cache only

# Create/refresh the Temporal Schedule that runs WikiSweepWorkflow every
# WIKI_SWEEP_INTERVAL_MINUTES.  No-op when WIKI_ENABLED=false.
uv run python -m scripts.setup_wiki_schedule
```

---

## 6. Smoke-тест LLM-шлюза

Подтвердите, что прокси LiteLLM достаёт свой upstream с ключом, который вы вставили:

```bash
curl -fsS http://localhost:4000/v1/chat/completions \
  -H "Authorization: Bearer sk-litellm-stub" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"Reply with one word: OK"}],"max_tokens":10}'

curl -fsS http://localhost:4000/v1/embeddings \
  -H "Authorization: Bearer sk-litellm-stub" \
  -H "Content-Type: application/json" \
  -d '{"model":"text-embedding-3-small","input":"smoke test"}' \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print('dim:', len(d['data'][0]['embedding']))"
```

Ожидается: ответ `OK` и `dim: 1536`.  Если видите 401 — `OPENAI_API_KEY`
неверен / отсутствует.  Если видите "connection refused" — 
контейнер litellm лежит (`docker compose logs litellm`).

---

## 7. Запустите API + worker

Два долгоживущих хост-процесса.  Гоняйте каждый в своём терминале (или используйте
`tmux` / `systemd` в продакшене):

```bash
# Terminal 1 — API
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload

# Terminal 2 — Temporal worker (hosts ALL queue pools in one process)
uv run python -m src.workflow.worker
```

Единственный процесс worker'а хостит семь пулов Worker'ов против одного и того же
Temporal-клиента — ingest (`kb-ingest`), extract (`kb-ingest-llm`), merge
(`kb-ingest-merge`), search (`kb-search-small`), синтез large-tier
(`kb-search-large`), оффлайн-сборка сообществ (`kb-graph-build`) и
wiki-редактор (`kb-wiki`).  Полную таблицу см. в `docs/QUEUES.md`.  На
бутстрапе он логирует каждую очередь + её лимит конкурентности; при `METRICS_ENABLED=true`
он также стартует экспортер Prometheus на `METRICS_BIND_ADDRESS`.

Sanity:

```bash
curl -fsS http://localhost:8000/health        # {"status":"ok", ...}
```

> **Заметка**: API использует `--reload` для удобства разработки.  Отключите в
> продакшене (`gunicorn -k uvicorn.workers.UvicornWorker` с несколькими
> worker'ами).  Для развёртывания на несколько машин гоняйте отдельные процессы
> worker'ов и наводите каждый на очереди, которые он должен поллить — держите LLM-полосы
> (`kb-ingest-llm` / `kb-ingest-merge`) на GPU-машине (см. docstring
> модуля в `src/workflow/worker.py`).

---

## 8. Запустите первый ingest

Эндпоинт ingest сохраняет файл в MinIO, вставляет Postgres-строку
`documents` и **запускает Temporal'овский `DocumentIngestWorkflow`**; 
worker затем гоняет его end-to-end (fetch → parse/chunk → embed/index →
extract KG → merge graph).

### 8a. Через API (curl)

```bash
curl -fsS -X POST http://localhost:8000/api/v1/ingest \
  -H "X-API-Key: dev-local-key" \
  -F "file=@/path/to/document.txt;type=text/plain" \
  -F "department=demo"

# → {"job_id": "abc-...uuid..."}
```

Поллинг статуса:

```bash
curl -fsS http://localhost:8000/api/v1/ingest/<job_id> \
  -H "X-API-Key: dev-local-key"
# → {"status": "pending" → "processing" → "completed"}
#   (a vector-indexed doc whose graph half failed shows "vector_only")
```

### 8b. Тестовый медицинский корпус

```bash
uv run python -m scripts.ingest_medical
```

Конвертирует `tests/eval/corpora/medical/medical.json` → корпус-файл,
загружает его через `/api/v1/ingest` и поллит, пока не завершится.

---

## 9. Smoke-тест поиска

```bash
# Local — plan-execute (the default mode)
curl -fsS -X POST http://localhost:8000/api/v1/search/local \
  -H "X-API-Key: dev-local-key" \
  -H "Content-Type: application/json" \
  -d '{"query":"какие риски развития рака кожи?","top_k":10}' \
  | python3 -m json.tool

# Auto — router picks local/global/drift
curl -fsS -X POST http://localhost:8000/api/v1/search/auto \
  -H "X-API-Key: dev-local-key" \
  -H "Content-Type: application/json" \
  -d '{"query":"какие риски развития рака кожи?","top_k":10}' \
  | python3 -m json.tool

# Global/drift need community summaries first (offline build, kb-graph-build):
curl -fsS -X POST http://localhost:8000/api/v1/admin/communities/rebuild \
  -H "X-API-Key: dev-local-key"
```

Ожидается: русский ответ + `sources[].content` (текст чанка на оригинальном
языке) + `documents[]` (download-ссылки на использованные исходные файлы).  Латентность
model-bound (доминирует tier большого синтеза).  Эндпоинты + тюнинг:
`docs/runbook/search-usage.md`.

---

## 10. Прогон тестового набора

```bash
uv run pytest -q
```

Тестовый набор оффлайн (stub-LLM, stub-Milvus / Neo4j) — безопасно гонять
в CI без каких-либо backing-сервисов.

Для детерминированного eval'а качества ответов (тоже оффлайн):

```bash
uv run pytest tests/eval/ -q
```

Для живого end-to-end eval'а против реального API:

```bash
uv run python -m tests.eval.run_answer_eval \
  --no-golden --medical-sample 5 --endpoints search,agent \
  --search-timeout 180 --agentic-timeout 900
```

---

## 11. Операции сброса / очистки

### Полный сброс (ядерный)

```bash
uv run python -m scripts.wipe_db --yes
```

`scripts/wipe_db.py` теперь очищает **workflow'ы Temporal и страницы MediaWiki**
в дополнение к хранилищам данных.  По порядку он:

* **Temporal** — терминирует каждый RUNNING workflow, затем удаляет каждую
  execution (открытую + закрытую) в namespace, так что Temporal совпадает с
  очищенными хранилищами.  *(Best-effort; fail-open, если сервер лежит.)*
* **Postgres** — `TRUNCATE documents`.
* **Milvus** — дропает сконфигурированную коллекцию.
* **Neo4j** — `MATCH (n) DETACH DELETE n` + дропает не-системные индексы.
* **MediaWiki** — удаляет статьи wiki-редактора (каждую
  страницу основного namespace кроме `Main Page`).  *(Best-effort; логирует предупреждение
  и продолжает, если wiki-стек лежит или admin-логин падает.  НЕ
  трогает Wikibase Items/Properties — они принадлежат `setup_wikibase`; 
  пер-сущностные QID уже были удалены с очисткой Neo4j.)*
* **Файловая система** — чистит `API_UPLOAD_DIR` и `INGESTION_CACHE_DIR`.
* Перезапускает `setup_db`, чтобы пересоздать схемы.

Флаги:

| Флаг | Эффект |
|---|---|
| `--yes` / `-y` | Пропустить интерактивное подтверждение (CI / скрипты) |
| `--keep-temporal` | Не терминировать/удалять execution'ы workflow'ов Temporal |
| `--keep-wiki` | Не удалять статьи MediaWiki |
| `--keep-files` | Не трогать upload-директорию / кеш ingest |
| `--no-setup` | Пропустить запуск `setup_db` после очистки |

API / worker не нужно останавливать — они увидят пустые хранилища на
следующем запросе.  Любой ingest в полёте упадёт на середине (его
строка `documents` исчезает, а его workflow терминируется).

### Только переингест (сохранить схемы + Temporal + wiki)

```bash
uv run python -m scripts.wipe_db --yes --keep-files --keep-temporal --keep-wiki
# Then re-upload your docs
```

---

## 12. Смена моделей

Детальные процедуры свопа — в `docs/MODELS.md`.  Два сценария:

* **OpenAI → локальный (Ollama/vLLM)**: отредактируйте `docker/litellm_config.yaml`
  (наведите tier'ы `small`/`large` на `ollama_chat/...`), задайте
  `LITELLM_MODEL_SMALL` / `LITELLM_MODEL_LARGE`, задайте `MILVUS_DIM` /
  `LITELLM_EMBEDDING_DIM` под новую embedding-модель (например, 768 для
  `nomic-embed-text`), очистите Milvus (`scripts/wipe_db.py`), затем
  `docker compose up -d --force-recreate litellm`.
* **Эскалировать одну роль на large-tier**: задайте
  `LITELLM_ROLE_TIERS='{"plan":"large"}'` в `.env`, перезапустите API + worker.
  Переингест не требуется (embedding-модель не менялась).

---

## 13. Чеклист продакшена

Сверх локальной разработки:

| Пункт | Действие |
|---|---|
| **Auth** | Ротируйте `API_KEYS` на каждое окружение. Добавьте реальный master_key + базу данных для LiteLLM (см. комментарии в `docker/litellm_config.yaml`). |
| **Харденинг Temporal** | Образ `auto-setup` пере-гоняет настройку схемы на каждом бутстрапе — переключитесь на `temporalio/server` + мигрируйте схему один раз через `temporal-sql-tool`. Дайте Temporal СОБСТВЕННЫЙ Postgres (он тяжёл по записи истории). Никогда не бампайте образ Temporal на месте на кластере с живыми данными. См. комментарии в `docker-compose.yml`. |
| **Персистентность** | Монтируйте Docker-тома на долговечное хранилище. Запланируйте бэкапы Postgres + Neo4j. У Milvus собственный snapshot-инструментарий. |
| **Логирование** | `API_LOG_JSON=true` для структурированных логов. Агрегируйте через loki / cloudwatch. |
| **Супервизор процессов** | Замените `uv run uvicorn` на `gunicorn -k uvicorn.workers.UvicornWorker` (4+ worker'а). Гоняйте Temporal-worker под `supervisord` / `systemd` с политикой рестарта. |
| **Масштабирование** | Гоняйте больше процессов worker'ов (они делят одни task queue — Temporal балансирует нагрузку). Для GPU-сплита гоняйте отдельные процессы worker'ов, прибитые к LLM-полосам. Пер-очередная конкурентность: `docs/QUEUES.md`. API-worker'ы stateless за балансировщиком нагрузки. |
| **LLM-конкурентность** | Реальной LLM-конкурентностью владеет пер-процессный `LLMPool` (`LLM_POOL_*`), а не Temporal-лимиты очередей — сайзьте `LLM_POOL_TIER_SMALL_TOTAL` / `LLM_POOL_TIER_LARGE_TOTAL` под ваш GPU + upstream-бюджет. Держите Temporal-лимиты `*_ACTIVITY_CONCURRENCY` ≥ соответствующего потолка полосы пула. |
| **Health-пробы** | `/health` — путь liveness. Для readiness дополнительно пробьте известный закешированный search-запрос. |
| **Сетевая изоляция** | Postgres, Neo4j, Milvus, Temporal, LiteLLM все биндятся на публичные порты по умолчанию в `docker-compose.yml` — ограничьте до VPC / приватной сети в продакшене. |
| **Дисциплина переингеста** | Смена `INGESTION_CHUNK_SIZE`, embedding-модели или `MILVUS_DIM` инвалидирует корпус — очистите и переингестите. |
| **Стоимость перевода** | На тяжёлом мультиязычном корпусе задайте `INGESTION_TRANSLATE_TO_RUSSIAN=false`. Граф остаётся на исходном языке; межъязыковая дедупликация сущностей деградирует. |

---

## 14. Траблшутинг

| Симптом | Вероятная причина | Фикс |
|---|---|---|
| Worker не стартует, логирует ошибку конфига модели LiteLLM на бутстрапе | `validate_litellm_models` поймал плохое имя модели до того, как побежала любая активность | Поправьте `docker/litellm_config.yaml` / `LITELLM_MODEL_*`, перезапустите worker |
| Ingest застрял на `pending`/`processing`, без прогресса | Temporal-worker не запущен или не поллит правильные очереди | Запустите `uv run python -m src.workflow.worker`; проверьте Temporal UI (`:8080`) на застрявшие/упавшие workflow'ы |
| `Property values can only be of primitive types or arrays thereof` в worker'е | небезопасные метаданные просочились в `PropertyGraphIndex` | Уже пофикшено в metadata-стриппере; подтяните последнее |
| 401 на каждый `/v1/chat/completions` | `OPENAI_API_KEY` отсутствует в env контейнера litellm | Отредактируйте `.env`, `docker compose up -d --force-recreate litellm` |
| `/api/v1/search` возвращает 500 с "vector dim mismatch" | Embedding-модель сменилась, но коллекция Milvus не была пересоздана | `scripts.wipe_db` + переингест |
| Документ оканчивается в статусе `vector_only` | Векторная половина проиндексирована, но графовая/merge-половина упала | Инспектируйте прогон `GraphBuildWorkflow` в Temporal UI; переингест |
| Worker бесконечно ест LLM-вызовы, без завершения | Конкурентность перевода/извлечения против tier-лимита upstream | Понизьте `INGESTION_TRANSLATION_CONCURRENCY` / тюньте лимиты `LLM_POOL_*` |

---

## 15. Куда смотреть

| Нужно | Откройте это |
|---|---|
| Понять всю картину | `docs/ARCHITECTURE.md` |
| Топология task-queue Temporal + конкурентность | `docs/QUEUES.md` |
| Трассировать один запрос end-to-end | `docs/SEARCH.md` (архитектура) · `docs/runbook/search-usage.md` (использование) |
| Поменять LLM / embedding-модель | `docs/MODELS.md` |
| Инспектировать workflow'ы / ретраи | Temporal Web UI на `http://localhost:8080` |
| Диагностировать KG-извлечение | `scripts/diag_kg.py`, `scripts/diag_kg_medical.py` |
| Инспектировать состояние ingest-пайплайна | `docker exec kb-llamaindex-postgres-1 psql -U postgres -d kb_llamaindex -c "SELECT id, status, error FROM documents ORDER BY created_at DESC LIMIT 10;"` |
| Инспектировать сущности Neo4j | Neo4j Browser на `http://localhost:7474` (bolt `:7687`) |
| Инспектировать чанки Milvus | `pymilvus` REPL: `MilvusClient('http://localhost:19530').query(collection_name='kb_llamaindex', filter='doc_id=="..."', output_fields=["*"])` |
