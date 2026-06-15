# Search — памятка по использованию и тюнингу

Краткий практический гид по текущей поверхности поиска (после R7b cutover):
`POST /api/v1/search/{local,global,drift,auto}` + admin-триггер
`POST /api/v1/admin/communities/rebuild`. Архитектурные детали — в
[`docs/SEARCH.md`](../SEARCH.md). (Легаси `/search`,`/agent`,`/selfrag`
удалены в R7b cutover.)

Все запросы: заголовок `X-API-Key`, тело `{"query": "...", "top_k": 10}`,
ответ `{query, answer, mode, sources[], documents[], latency_ms}`. Поле
`documents[]` — это `{doc_id, url}` ссылки на скачивание оригиналов
(`GET /api/v1/documents/{doc_id}`, см. §«Скачивание источника» ниже); `sources[]`
несёт чанки `{doc_id, chunk_id, content, score}`.

Поля тела, которые **реально потребляются** воркфлоу: `query`, `top_k`,
`history` (см. §«История диалога»). Остальные поля `SearchRequest`
(`mode`, `department`, `doc_type_filter`, `created_after/before`,
`response_type`, `include_references`, `user_id`) приняты для обратной
совместимости, но текущими plan-execute / GraphRAG-флоу **игнорируются** —
не полагайтесь на них для фильтрации.

## Выбор режима + параметры (стрелка = эффект при увеличении)

| Эндпоинт | Когда | Что делает | Ключевые параметры (влияние) |
|---|---|---|---|
| `/search/local` | Конкретный факт из 1–нескольких чанков: «кто/что/когда/сколько у X», поиск сущности, документа, значения, определения. **Дефолт.** Пример: «какой диагноз у Иванова?» | plan → параллельный retrieve по под-вопросам → coverage-gate → rerank → синтез | `top_k`↑ кандидатов→полнее/шумнее; `AGENT_MAX_SUBQUERIES`(5)↑ шире декомпозиция, дороже; `AGENT_COVERAGE_CHECK_ENABLED`/`AGENT_MAX_COVERAGE_ROUNDS`(1)↑ добивает пробелы, +задержка; `TEMPORAL_RERANK_TOP_N`(5)↑ полнее контекст синтеза, но дольше/риск timeout; `HF_RERANK_MODEL`/`HF_OFFLINE` — реранкер (битый offline-кэш→rerank fail-open); `LITELLM_MODEL_SMALL` (plan/retrieve/coverage), `LITELLM_MODEL_LARGE` (синтез) |
| `/search/global` | Корпус-уровневый/тематический/агрегирующий вопрос: «обобщи», «основные темы/категории во всех документах». **Требует** `admin/communities/rebuild`. Пример: «какие категории заболеваний в базе?» | map-reduce по community-summaries (small MAP → large REDUCE) | `AGENT_GLOBAL_MAX_COMMUNITIES`(20)↑ больше сообществ в MAP→полнее/дороже; конкурентность MAP ограничена `LLM_POOL_N`; **свежесть/качество зависят от rebuild**: `TEMPORAL_COMMUNITY_MIN_SIZE`(3)↑ отбрасывает мелкие сообщества, `TEMPORAL_COMMUNITY_SUMMARY_PARALLELISM`(4); `LITELLM_MODEL_SMALL` (партиалы), `LITELLM_MODEL_LARGE` (REDUCE-синтез). `top_k` почти не влияет (ретрив не по чанкам) |
| `/search/drift` | Нужны и факт, И широкий контекст: «расскажи про X и как он связан с остальным». Дороже (2 прохода). | local-проход → global-расширение с локальными источниками как seed | **Объединяет параметры local + global** (оба прохода). Локальные источники подмешиваются в REDUCE-контекст → `TEMPORAL_RERANK_TOP_N` и `AGENT_GLOBAL_MAX_COMMUNITIES` вместе определяют размер финального контекста (риск timeout выше) |
| `/search/auto` | Не знаете режим / смешанный трафик / клиент не классифицирует. Fail-safe→local. Пример: «сравни подходы к лечению BCC». | `route_query` (small) классифицирует → диспатчит local/global/drift | Роль `route`→`LITELLM_MODEL_SMALL` (классификация; сбой→`local`). Дальше действуют параметры **выбранного** режима. +1 small-LLM вызов |

### Сквозные параметры (влияют на любой режим)
- `TEMPORAL_LARGE_ACTIVITY_CONCURRENCY` (2) — одновременные синтезы; низкий → при многих запросах синтезы встают в очередь (head-of-line).
- `TEMPORAL_SEARCH_ACTIVITY_CONCURRENCY` (4) — параллельные сессии на small-очереди.
- `AGENT_LLM_MAX_CONCURRENT` (8) — общий семафор LLM-вызовов (защита GPU/прокси).

### История диалога (`history`)
Многоходовой диалог: клиент сам ведёт историю и шлёт её в каждом запросе.
```json
{
  "query": "А какие у него побочные эффекты?",
  "top_k": 10,
  "history": [
    {"role": "user", "content": "Что известно про препарат Цисплатин?"},
    {"role": "assistant", "content": "Цисплатин — это…"}
  ]
}
```
- `history` — список `{role, content}` (`role` = `user` | `assistant`). Пусто → одиночный запрос без контекстуализации.
- Прокидывается в local- и global-флоу (`_local_params` / `_global_params` →
  `ConversationTurnDict`). Контекстуализация (переписывание текущего вопроса с
  учётом истории) включается флагом `AGENT_CONVERSATION_HISTORY_ENABLED`; если он
  выключен — `history` принимается, но игнорируется.
- История **client-managed**: сервер её не хранит между запросами.

### ⚠️ Граф-глубина / hops — это НЕ параметры HTTP-запроса
Per-call knobs `graph_search.depth` и `find_neighbours.hops` существуют только в
**MCP-инструментах** (`src/mcp/tools_server.py`), не в HTTP `/search/*`. В HTTP-API
глубина similarity-обхода графа задаётся **конфигом** `AGENT_GRAPH_SEARCH_PATH_DEPTH`
(default 1, clamp 1–3 — см. `src/config.py` / `src/graph/retriever.py`), а не полем
тела запроса. Тело `SearchRequest` поля глубины/hops не содержит.

### ⚠️ Правило перезапуска
Все `AGENT_*` / `TEMPORAL_*` / `LITELLM_*` / `HF_*` читаются **на submit-time**
(`_local_params`/`_global_params` в API) или **на старте воркера**, не в рантайме
воркфлоу (replay-safety Temporal). После правки `.env` — **перезапустить API и
воркер**, иначе изменения не подхватятся.

## Примеры запросов

```bash
# 0. (для global/drift/auto при первом использовании) — построить сообщества
curl -s -X POST localhost:8000/api/v1/admin/communities/rebuild -H "X-API-Key: $KEY"
# → {"workflow_id":"community-build-…","status":"started"}  (идёт минуты, offline)

# 1. LOCAL — конкретный факт из документов
curl -s -X POST localhost:8000/api/v1/search/local \
  -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"query":"Какой диагноз и какие препараты назначены пациенту Иванову И.И.?","top_k":10}'

# 2. GLOBAL — обзор по всему корпусу (нужны communities)
curl -s -X POST localhost:8000/api/v1/search/global \
  -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"query":"Какие основные категории заболеваний встречаются во всех документах?","top_k":10}'

# 3. DRIFT — факт + широкий контекст вокруг него
curl -s -X POST localhost:8000/api/v1/search/drift \
  -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"query":"Что известно про препарат Цисплатин и в каких ещё схемах лечения он упоминается?","top_k":10}'

# 4. AUTO — пусть роутер сам выберет режим
curl -s -X POST localhost:8000/api/v1/search/auto \
  -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"query":"Сравни подходы к лечению BCC в разных документах","top_k":10}'
```

Форма ответа (одинаковая для всех режимов):

```json
{
  "query": "...",
  "answer": "Ответ на русском с цитированием источников…",
  "mode": "local",            // local | global | drift  (auto подставит фактический)
  "sources": [
    {"doc_id": "D-123", "chunk_id": "…", "content": "…", "score": 0.87}
  ],
  "documents": [
    {"doc_id": "D-123", "url": "/api/v1/documents/D-123"}
  ],
  "latency_ms": 4213
}
```

## Скачивание источника (`documents[]` → оригинал)
Каждый `doc_id` из `sources[]` / `documents[]` можно скачать как исходный файл:

```bash
# documents[].url уже относительный: "/api/v1/documents/<doc_id>"
curl -s -OJ localhost:8000/api/v1/documents/<doc_id> -H "X-API-Key: $KEY"
```

`GET /api/v1/documents/{doc_id}` стримит оригинал из MinIO (URI берётся из
`documents.path` в Postgres) с `Content-Disposition: attachment` и корректным
`filename*` (RFC 6266). Legacy-документы с локальным путём (до MinIO) отдаются
с диска, если файл ещё на месте. Коды: `401` (нет/битый ключ), `404` (нет
документа или источник недоступен), `503` (MinIO недоступен).

## Wiki-rebuild (отдельный admin-триггер, не путать с communities)
`POST /api/v1/admin/communities/rebuild` (выше) строит граф-сообщества для
`global`/`drift`. Это **другой** триггер, чем wiki-editor:

> ⚠️ Путь wiki-rebuild — `/admin/wiki/rebuild` **без** префикса `/api/v1`. В отличие
> от search/ingest/documents, `admin.router` подключён в `src/api/main.py` без
> `/api/v1` и сам несёт `prefix="/admin/wiki"`. (Эндпоинт сейчас **не** требует
> `X-API-Key` — на нём нет `require_api_key`-зависимости в коде.)

```bash
# Прогнать wiki-sweep по «грязным» сущностям:
curl -s -X POST localhost:8000/admin/wiki/rebuild
# Пометить ВСЕ сущности грязными и пересобрать статьи целиком:
curl -s -X POST 'localhost:8000/admin/wiki/rebuild?all=true'
# → {"status":"started","workflow_id":"wiki-sweep-manual"}  | {"status":"disabled"} если WIKI_ENABLED=false
```

`?all=true` сначала ставит `wiki_dirty=true` на каждый `:__Entity__` в Neo4j,
затем запускает `WikiSweepWorkflow` на очереди `kb-wiki`. Без флага пересобираются
только уже помеченные грязными сущности. При `WIKI_ENABLED=false` возвращает
`{"status":"disabled"}` и ничего не делает.

Подсказки по подбору режима:
- запрос #1 (один пациент) на `/global` даст размытый обзор — берите `local`;
- запрос #2 («во всех документах») на `/local` упрётся в `top_k` чанков и не обобщит — берите `global`;
- не уверены между local/global (#4) — `/auto` (сам выберет, при сбое откатится на `local`).

## Если поиск «висит»
Чаще всего стопор в синтезе. Диагностика (Temporal UI `localhost:8080` или
Python-клиент): `get_state` query показывает фазу (`rerank` vs `synthesize`);
в Pending Activities у `synthesize_answer` смотрите Scheduled-vs-Started + Last
Heartbeat + Last Failure. Типовые причины: нет поллера на `kb-search-large`
(воркер не поднят / не та очередь), насыщение `large_activity_concurrency`,
зависший large-LLM, либо stale-воркер. После правок env/кода — **передеплой
воркера**.
