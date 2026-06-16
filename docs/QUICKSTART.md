# QUICKSTART — быстрый запуск kb-llamaindex

Минимальный путь «с нуля до работающего поиска»: поднять инфраструктуру → инициализировать схемы → запустить воркер и API → проверить ингестом и поиском. Подробности по каждому шагу — в [`DEPLOYMENT.md`](DEPLOYMENT.md); концепции — в [`CONCEPTS.md`](CONCEPTS.md).

> TL;DR (всё на дефолтах для локалки):
> ```bash
> cp .env.example .env                            # 1. конфиг (отредактируй ключи — см. §2)
> make up                                         # 2. backends (healthy) + schema init
> make models                                     # 3. (опц.) префетч reranker-модели (~1 GB BGE)
> uv run python -m src.workflow.worker &          # 4. воркер (host)
> uv run uvicorn src.api.main:app --port 8000 &   # 5. API (host)
> ```
>
> `make models` скачивает BGE-reranker (~1 GB) заранее, чтобы первый `/search` не завис на загрузке модели.
>
> **Apple Silicon / arm64:** образы `wikibase` и `wdqs` — только amd64. Включи в Docker Desktop «Use Rosetta for x86/amd64 emulation» и выдели Docker ≥ 4–6 GB RAM (Milvus standalone требователен к памяти).

---

## 1. Предусловия

- **Docker** (Docker Desktop / Compose v2) — для стека хранилищ.
- **uv** (менеджер пакетов Python) — запуск кода: `uv sync` ставит зависимости.
- Доступ к LLM: либо ключ OpenAI-совместимого провайдера, либо локальный LiteLLM-конфиг (см. §2).

---

## 2. Шаг 1 — конфигурация (`.env`)

```bash
cp .env.example .env
```

Обязательно проверь/задай эти параметры (остальное в `.env.example` имеет рабочие дефолты для локалки):

| Переменная | Зачем | Замечание |
|---|---|---|
| `API_KEYS` | Ключи для `X-API-Key` (через запятую) | Без неё `/ingest` и `/search` вернут 401 |
| `OPENAI_API_KEY` / `LITELLM_API_KEY` | Доступ к моделям через LiteLLM-шлюз | Либо реальный ключ, либо локальные модели в `docker/litellm_config.yaml` |
| `LITELLM_MODEL_SMALL` / `LITELLM_MODEL_LARGE` | Модели для tier'ов small/large | По умолчанию `gpt-4o-mini` |
| `LITELLM_EMBEDDING_MODEL` / `LITELLM_EMBEDDING_DIM` | Embed-модель и её размерность | **`LITELLM_EMBEDDING_DIM` обязан совпадать с `MILVUS_DIM`** |
| `MILVUS_DIM` | Размерность вектора в Milvus | Должна равняться `LITELLM_EMBEDDING_DIM` (напр. 1536) |
| `POSTGRES_PASSWORD`, `NEO4J_PASSWORD`, `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` | Доступ к хранилищам | Дефолты compose подходят для локалки |

> ⚠️ Самая частая ошибка старта — рассинхрон `MILVUS_DIM` ≠ `LITELLM_EMBEDDING_DIM`: ингест упадёт на вставке в Milvus.

---

## 3. Шаг 2 — поднять инфраструктуру

```bash
make up                     # compose up + schema init (Postgres + MinIO + Temporal attrs) — одна команда
# или, если нужен полный контроль:
# docker compose up -d
# docker compose ps           # дождись healthy у neo4j / milvus / postgres / minio / temporal / litellm
# make init                   # отдельно инициализировать схемы
```

Сервисы и порты:

| Сервис | Порт | Назначение |
|---|---|---|
| FastAPI (запустим в §5) | `:8000` | HTTP API |
| Temporal UI | `:8080` | История воркфлоу, replay, дебаг |
| LiteLLM | `:4000` | Шлюз к моделям |
| Neo4j | `:7474` (HTTP) / `:7687` (Bolt) | Граф |
| Milvus | `:19530` | Векторы чанков |
| Postgres | `:5432` | Статусы заданий + `ingest_metrics` |
| MinIO | `:9000` (S3) / `:9001` (консоль) | Исходники + claim-check staging |
| Grafana | `:3001` | Дашборды аналитики |

> `wdqs` (SPARQL) может «флапать» при старте — он опционален для рантайма.

---

## 4. Шаг 3 — инициализировать схемы

```bash
make init
```

Создаёт таблицы Postgres (`documents`, `ingest_metrics`), гарантирует бакет MinIO и регистрирует search-атрибуты Temporal. Идемпотентно — безопасно повторять. (`make up` уже делает это автоматически; `make init` полезен для повторного прогона без рестарта контейнеров.)

---

## 5. Шаг 4 — запустить воркер и API

```bash
uv run python -m src.workflow.worker &              # durable-воркфлоу + активности (все очереди)
uv run uvicorn src.api.main:app --port 8000 &       # HTTP API
```

> После любой правки `.env` — **перезапусти оба процесса**, иначе новые настройки не подхватятся.

---

## 6. Шаг 5 — проверить (smoke)

```bash
# Ингест документа
curl -X POST http://localhost:8000/api/v1/ingest \
     -F "file=@tests/test_ingestion/fixtures/sample.txt" \
     -H "X-API-Key: <твой_ключ_из_API_KEYS>"
# Дождись завершения DocumentIngestWorkflow в Temporal UI (:8080)

# Поиск
curl -X POST http://localhost:8000/api/v1/search/local \
     -H "X-API-Key: <твой_ключ>" -H "Content-Type: application/json" \
     -d '{"query": "о чём документ?", "top_k": 5}'
```

Режимы поиска: `/search/local`, `/search/global`, `/search/drift`, `/search/auto`. Скачать исходник: `GET /api/v1/documents/{doc_id}` (под `X-API-Key`).

---

## 7. Включение опциональных возможностей (матрица)

Большинство фич **уже включены по умолчанию**. «Спят» (opt-in) три вещи — выставь в `.env` и перезапусти воркер:

| Что включить | Переменные | Дефолт | Примечание |
|---|---|---|---|
| **Native-vector ER** (kNN по всему графу вместо окна 5000) | `AGENT_ER_USE_NATIVE_VECTOR_KNN=true` (+ `AGENT_ER_VECTOR_KNN_K=20`) | `false` | На **существующем** графе сперва прогнать backfill: `uv run python -m scripts.backfill_er_vector --no-dry-run`, и только потом флаг. На пустом графе можно сразу `true` (`er_vec` пишется при ингесте). |
| **Иерархические сообщества + умный отбор** | `AGENT_COMMUNITY_MAX_LEVELS=3`, `AGENT_COMMUNITY_DYNAMIC_SELECTION=semantic` | `1`, `lexical` | `>1` уровень → многоуровневый Leiden; `semantic`/`descent` вместо `lexical` |
| **Редактор wiki** (статьи MediaWiki по сущностям) | `WIKI_ENABLED=true` | `false` | `WIKI_DOCS_BASE_URL` уже верный для ссылок «Источники». Для sitelink'ов: `WIKIBASE_ENABLED=true` + `uv run python -m scripts.setup_wikibase`, и `WIKI_SITE_GLOBAL_ID` под реальный id вики |

Уже включено по умолчанию: ER (judge + verdict-cache), история диалога (`AGENT_CONVERSATION_HISTORY_ENABLED`), dual walk-seed, Milvus HNSW.

Полный разбор каждой фичи — [`FEATURES.md`](FEATURES.md); по моделям/ролям — [`MODELS.md`](MODELS.md); по очередям — [`QUEUES.md`](QUEUES.md).

---

## 8. Сброс (чистый слейт)

```bash
uv run python -m scripts.wipe_db --yes
```

Терминирует воркфлоу Temporal + чистит Postgres / Milvus / Neo4j / MediaWiki-страницы / локальные файлы и пересоздаёт схемы. Флаги: `--keep-temporal`, `--keep-wiki`, `--keep-files`, `--no-setup`.

---

## 9. Опционально — wiki / Wikibase

Только если нужны wiki-статьи и/или структурный якорь Wikibase:

```bash
make wiki-setup
# Одна команда: поднимает wikibase + wikibase-mysql (профиль), создаёт бот-аккаунт
# внутри контейнера и запускает bootstrap классов/свойств (идемпотентно).
# Wikibase нужно ~90 сек на первый старт — make wiki-setup ждёт healthy.

uv run python -m scripts.setup_wiki_schedule       # (опц.) Temporal Schedule для авто-свипа редактора
```

> **Apple Silicon / arm64:** образы `wikibase` и `wdqs` — только amd64. Убедись, что в Docker Desktop включено «Use Rosetta for x86/amd64 emulation», иначе контейнеры не стартуют.

Детали: [`runbook/wikibase.md`](runbook/wikibase.md), [`runbook/wiki-editor.md`](runbook/wiki-editor.md).
