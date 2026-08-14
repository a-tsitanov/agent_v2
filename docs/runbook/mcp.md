# MCP servers runbook

Гид по трём MCP-серверам (Model Context Protocol) которые экспозируют kb-llamaindex как tool-source для внешних LLM-клиентов (OpenWebUI, Claude Desktop, Cursor, Continue, …).

> **⚠️ ОБНОВЛЕНО (R7b cutover) — MCP-1 раздел частично устарел.** MCP-1
> `kb_search` теперь submit'ит **`SearchOrchestratorWorkflow`** (plan-execute,
> local mode) на очередь `kb-search-small`, а НЕ удалённый `SearchWorkflow`.
> Сигнатура — фактически `kb_search(query)` (параметры `mode` /
> `max_iterations` и режимы `simple`/`agent`/`selfrag` УДАЛЕНЫ; ReAct/Self-RAG
> больше нет). Текущая модель поиска: [`search-usage.md`](search-usage.md) +
> [`../SEARCH.md`](../SEARCH.md). Раздел MCP-2 (atomic tools) ниже актуален.
> (Полная переработка MCP-1 прозы — TODO.)
>
> **`synthesize: bool = True`** — все четыре orchestrated tools (`kb_search`,
> `kb_global_search`, `kb_drift_search`, `kb_auto_search`) принимают этот
> параметр, зеркалящий `SearchRequest.synthesize` из HTTP-слоя (см.
> [`../SEARCH.md`](../SEARCH.md)). `False` пропускает финальный large-model
> синтез (и предшествующий ему rerank — его результат никто не читает) —
> `answer` возвращается `""`. Смысл: клиент, который сам собирает ответ из
> `sources`, перестаёт платить (временем и токенами large-tier модели) за
> синтез, который всё равно выбросит. По умолчанию `True` — существующие
> клиенты поведения не меняют.
>
> **⚠️ Что из результата теряется.** В отличие от HTTP-слоя, MCP-1 отдаёт
> клиенту ВЕСЬ `SearchOutcome` (`_outcome_to_dict`), поэтому разница видна:
>
> | Поле результата | При `synthesize=false` |
> |---|---|
> | `sources`, `documents`, `step_stats`, `query`, `mode`, `latency_ms` | **без изменений** |
> | `answer` | `""` |
> | `citations` | `[]` — продукт синтеза |
> | `uncertainties` | `[]` — продукт синтеза |
> | `refinement_rounds` | `0` — продукт синтеза |
>
> Причина: ветка пропуска строит `SynthesizeResult(text="")`, а
> `citations` / `uncertainties` / `refinement_rounds` в `SearchOutcome`
> берутся именно из неё. Если клиенту нужны цитаты — синтез пропускать
> нельзя, их надо собирать самому из `sources`.

> **Цель архитектуры:** дать оператору три режима интеграции — "получить готовый ответ" (MCP-1), "взять примитивы и собрать loop своим LLM" (MCP-2), или "точные внешние цифры без единого обращения к LLM" (MCP-3). Защита GPU реализована на двух уровнях (MCP-3 GPU не использует вовсе — только Postgres): Temporal-queue для MCP-1, BoundedLLM-семафор для MCP-2.

Связанные runbook'и:
- [`search-usage.md`](search-usage.md) — текущие режимы `/search/{local,global,drift,auto}`, параметры, тюнинг
- [`multimodel.md`](multimodel.md) — откуда берётся search-role LLM
- [`analytics.md`](analytics.md) — Grafana для search-workflow latency
- [`hermes.md`](hermes.md) — подключение MCP-серверов к Hermes Agent (SSE + skill)

---

## 1. Three-server overview

| Server | Tool surface | Транспорт | Идёт через | Кто типично подключается |
|---|---|---|---|---|
| **MCP-1** (`src/mcp/search_server.py`) | 5 tools: `kb_search` (local plan-execute), `kb_global_search` (map-reduce по сообществам), `kb_drift_search` (local→global), `kb_auto_search` (роутер), `kb_analyze` (аналитика по графу: 42 примитива, plan→compute→synthesize — см. [`graph-analytics.md`](graph-analytics.md)) | stdio + HTTP/SSE | **Temporal workflows** (search-очередь) | OpenWebUI как готовый ассистент; non-LLM-developer clients |
| **MCP-2** (`src/mcp/tools_server.py`) | atomic retrieval (8): `vector_search`, `graph_search`, `graph_walk`, `find_entity_by_id`, `find_entity_by_name`, `find_neighbours`, `get_chunks_by_doc_id`, `read_full_document` + read-only GDS analysis (Track 7b, 5): `graph_pagerank`, `graph_personalized_pagerank`, `graph_components`, `graph_shortest_path`, `graph_stats` + channel-side series (2, канал-сторона для MCP-3 `stat_align`): `topic_trend`, `polarity_evolution` | stdio + **Streamable HTTP** (`/mcp`) | прямой Python in-process | Claude Desktop / Cursor / Continue с собственным LLM-loop'ом |
| **MCP-3** (`src/mcp/stats_server.py`) | exact statistics (3): `stat_indicators_search` (каталог/поиск индикаторов), `stat_series` (значения одного индикатора), `stat_align` (сведение двух рядов на общую сетку — арифметика без LLM) | stdio + **Streamable HTTP** (`/mcp`) | прямой Python in-process, plain Postgres, **никакого LLM** | Claude Desktop / Cursor / любой клиент, которому нужны точные числа, а не синтез |

Запуск одной командой; `--transport` переключает режим: MCP-1 — `stdio|sse`,
MCP-2 / MCP-3 — `stdio|http` (Streamable HTTP, эндпоинт `/mcp`; SSE здесь заменён).

---

## 2. Запуск

### Stdio (для Desktop клиентов)

В Claude Desktop / Cursor конфиге:

```json
{
  "mcpServers": {
    "kb-search": {
      "command": "uv",
      "args": [
        "run", "python", "-m", "src.mcp.search_server",
        "--transport", "stdio"
      ],
      "cwd": "/path/to/kb-llamaindex",
      "env": {
        "KB_MCP_REQUIRE_AUTH": "false"
      }
    },
    "kb-tools": {
      "command": "uv",
      "args": [
        "run", "python", "-m", "src.mcp.tools_server",
        "--transport", "stdio"
      ],
      "cwd": "/path/to/kb-llamaindex",
      "env": {
        "KB_MCP_REQUIRE_AUTH": "false"
      }
    },
    "kb-stats": {
      "command": "uv",
      "args": [
        "run", "python", "-m", "src.mcp.stats_server",
        "--transport", "stdio"
      ],
      "cwd": "/path/to/kb-llamaindex",
      "env": {
        "KB_MCP_REQUIRE_AUTH": "false"
      }
    }
  }
}
```

В Cursor: settings.json → `mcp.servers`, та же структура. В Continue: `~/.continue/config.json` под секцией `experimental.mcp`.

### HTTP (для OpenWebUI и web-клиентов)

```bash
# MCP-1 (search) — legacy SSE
uv run python -m src.mcp.search_server --transport sse  --host 0.0.0.0 --port 9001
# MCP-2 (tools)  — Streamable HTTP (эндпоинт /mcp)
uv run python -m src.mcp.tools_server  --transport http --host 0.0.0.0 --port 9002
# MCP-3 (stats)  — Streamable HTTP (эндпоинт /mcp)
uv run python -m src.mcp.stats_server  --transport http --host 0.0.0.0 --port 9003
```

OpenWebUI Admin Settings → MCP servers → URL `http://localhost:9001/sse`
(search, SSE), `http://localhost:9002/mcp` (tools, Streamable HTTP) и
`http://localhost:9003/mcp` (stats, Streamable HTTP).

### Docker (recommended для prod)

(Stage 5 follow-up — `docker-compose.yml` пока не модифицирован; запускается напрямую на хосте. См. § 7.)

---

## 3. MCP-1: `kb_search` tool

### 3.1 Контракт

```
kb_search(
    query: str,
    mode: "simple" | "agent" | "selfrag" = "agent",
    max_iterations: int = 8,
    max_refinements: int = 3,
) -> {
    "answer": str,                  # финальный ответ на русском
    "mode": str,                    # обратно тот же mode
    "query": str,                   # исходный query
    "sources": [
        {"chunk_id": str, "doc_id": str, "text": str, "score": float},
        ...
    ],
    "citations": [                   # только selfrag
        {"claim": str, "chunk_id": str}, ...
    ],
    "uncertainties": [               # только selfrag
        {"topic": str, "reason": str}, ...
    ],
    "refinement_rounds": int,       # 0 для simple/agent, 0..max для selfrag
    "step_stats": [                  # ReAct loop telemetry (agent/selfrag)
        {"step": int, "tool_name": str, "tool_args": {...},
         "observation_summary": str}, ...
    ],
    "latency_ms": int,
}
```

### 3.2 Что происходит под капотом

1. MCP клиент шлёт `tools/call kb_search {query, mode, ...}`
2. MCP-1 сервер ([`src/mcp/search_server.py:55`](../../src/mcp/search_server.py)) submits Temporal workflow `SearchWorkflow.run` на queue `kb-search-llm`
3. Параллельно: poll'ит workflow query `get_state` каждые ~300ms и форвардит как MCP `notifications/progress` (фаза + текущий tool + sources count)
4. SearchWorkflow внутри Temporal:
   - mode=simple: 1× `tool_execution(vector_search)` → `synthesize_answer`
   - mode=agent: цикл `agent_reasoning_step` + `tool_execution` до `submit_answer` или `max_iterations` или repeat-guard
   - mode=selfrag: тот же цикл + `synthesize_answer` в reflective режиме (draft → [NEED] markers → re-retrieve → redraft до `max_refinements`)
5. Workflow result → выход tool'а

### 3.3 Cancellation

MCP клиент закрывает соединение (закрытая вкладка / `notifications/cancelled`) → fastmcp pours `asyncio.CancelledError` в `kb_search` корутину → tool ловит её и зовёт `handle.cancel()` на Temporal workflow. GPU освобождается, недосчитанные шаги не выполняются.

### 3.4 Concurrency защита

Temporal queue `kb-search-llm` имеет cap `TEMPORAL_SEARCH_ACTIVITY_CONCURRENCY=4` (см. [`src/config.py`](../../src/config.py)). 5-й одновременный MCP-вызов **ждёт** в очереди Temporal — клиент видит "медленный response" а не 429/500.

---

## 4. MCP-2: 6 atomic tools

### 4.1 Tools

| Tool | Сигнатура | Что делает | Файл |
|---|---|---|---|
| `vector_search` | `(query, top_k=10)` | BM25+dense+RRF, top_k chunks | [`atomic_tools.py:69`](../../src/retrieval/atomic_tools.py) |
| `graph_search` | `(query, depth=2)` | KG walk → entities + relations | [`atomic_tools.py:89`](../../src/retrieval/atomic_tools.py) |
| `find_entity_by_id` | `(name, entity_type=None)` | Exact name lookup | [`atomic_tools.py:113`](../../src/retrieval/atomic_tools.py) |
| `find_neighbours` | `(entity_name, hops=1)` | 1-2 hop walk | [`atomic_tools.py:135`](../../src/retrieval/atomic_tools.py) |
| `get_chunks_by_doc_id` | `(doc_id, limit=50, offset=0)` | All chunks one doc in order | [`atomic_tools.py:159`](../../src/retrieval/atomic_tools.py) |
| `read_full_document` | `(doc_id, max_chars=20000)` | Raw pre-chunk file text | [`atomic_tools.py:211`](../../src/retrieval/atomic_tools.py) |

**`filter_by_metadata` намеренно НЕ exposed** — она оперирует in-process accumulator, что бессмысленно через stateless MCP-1/MCP-2 границу.

### 4.2 Что внутри

Каждый tool — тонкий wrapper в [`src/mcp/tools_server.py`](../../src/mcp/tools_server.py) вокруг соответствующей функции из `src/retrieval/atomic_tools.py`. Никакого Temporal — прямой Python call в том же процессе.

```python
@mcp.tool()
async def vector_search(query: str, top_k: int = 10) -> dict:
    r = await atomic_tools.vector_search(await _r(), query=query, top_k=top_k)
    return {"sources": json.loads(r.observation)}
```

`_r()` — ленивая DI bootstrap первого вызова: строит Milvus retriever, BGE embed model, Neo4j-graph retriever (optional), Postgres ChunkRepository, BoundedLLM-обёрнутую search-role LLM. Кэширует на module level — последующие вызовы reuse'ят.

### 4.3 Concurrency защита

LLM-используют `graph_search` (через LLMSynonymRetriever нормализацию query) и `find_entity_by_id` / `find_neighbours`. Все идут через `BoundedLLM` с семафором `settings.agent.llm_max_concurrent` (default 8). 20 параллельных `graph_search`-вызовов → 8 idёт через GPU, 12 ждут в семафоре.

`vector_search` / `get_chunks_by_doc_id` / `read_full_document` — pure retrieval/IO, не упираются в LLM, идут параллельно (ограничены только Milvus/PG connection pool).

### 4.4 Channel-side series — вход для MCP-3 `stat_align`

| Tool | Сигнатура | Что делает | Файл |
|---|---|---|---|
| `topic_trend` | `(topic, granularity="month", since=None, until=None)` | Частота упоминаний темы/сущности по периодам (по дате чанка) — канальный ряд ВНИМАНИЯ | [`dynamics.py`](../../src/analytics/primitives/dynamics.py) |
| `polarity_evolution` | `(name=None, rel_type=None)` | Как менялась полярность связей сущности во времени — канальный ряд ОЦЕНКИ | [`dynamics.py`](../../src/analytics/primitives/dynamics.py) |

В отличие от §4.1, эти два tool'а — не wrapper'ы над `atomic_tools.py`, а тонкие обёртки над analytics-каталожными примитивами `topic_trend` / `polarity_evolution` из [`src/analytics/primitives/dynamics.py`](../../src/analytics/primitives/dynamics.py) (используют тот же `_deps["graph_store"]`, что и GDS-tools из §4.1, не Milvus/`atomic_tools.py`). Оба примитива **зарегистрированы** в analytics `CATALOG` ([`dynamics.py:176`](../../src/analytics/primitives/dynamics.py) — `topic_trend`, [`dynamics.py:184`](../../src/analytics/primitives/dynamics.py) — `polarity_evolution`) и потому достижимы двумя путями: напрямую через MCP-2 (tools ниже) и через planner-путь `kb_analyze` → `POST /api/v1/analyze`.

**⚠️ `topic_trend` не работает на nebula — а nebula это прод-дефолт (`GRAPH_BACKEND`).** Примитив вызывает `run_rows(store, cypher, {"topic": ...})`, а `NebulaGraphStore.structured_query` бросает `NotImplementedError` на непустом `param_map` ([`nebula_store.py:268-272`](../../src/graph/nebula_store.py)); `run_rows` fail-soft'ит любое исключение в `[]` ([`store_query.py:23-25`](../../src/analytics/store_query.py)). Раньше это давало `rows: []` для ЛЮБОЙ темы — неотличимо от «про это никогда не писали». `polarity_evolution` этим НЕ затронут: он идёт через `build_dynamics_graph_ops`, чья nebula-реализация подставляет литералы в nGQL и `param_map` не передаёт.

**Guard закрывает только MCP-2, не CATALOG-путь.** `_TOPIC_TREND_UNSUPPORTED_BACKENDS` ([`tools_server.py:502`](../../src/mcp/tools_server.py)) проверяется внутри `_topic_trend()` ([`tools_server.py:525`](../../src/mcp/tools_server.py)) — это код самого MCP-2 tool'а `topic_trend`, и на nebula он возвращает `{"error": ...}` с явным упоминанием бэкенда вместо тихого нуля. Но `kb_analyze` (и, соответственно, `POST /api/v1/analyze`) идёт через CATALOG и зовёт примитив `topic_trend` из `dynamics.py` напрямую — этот guard там не стоит. На nebula такой вызов молча получает `rows: []` от `run_rows`. **Вывод для оператора:** пустой результат `topic_trend` от `kb_analyze` на nebula означает «бэкенд не может ответить», а не «тему никогда не упоминали» — тот же вопрос через MCP-2 `topic_trend` покажет это явной ошибкой. Снять ограничение целиком = научить nebula биндить nGQL-параметры, после чего guard-константа и обе ветки этого абзаца отмирают.

**Фильтр `since`/`until`.** Примитив не принимает границы дат — окно применяется к уже полученным бакетам, по **пересечению границ бакета с окном**, а не сравнением строк. Для каждого ключа периода `_period_bounds(period, granularity)` даёт первый и последний день бакета, и бакет остаётся, если он с окном пересекается (`end >= since` и `start <= until`). Два следствия, оба намеренные:

- Пересечение, а не вложенность: бакет, наполовину торчащий за границу окна, всё равно содержит упоминания изнутри окна, и выбросить его значило бы молча их потерять. Квартал `2026-Q2` попадает в окно `since=2026-06-25` целиком.
- Сравнение идёт по датам, а не по ISO-строкам. Прежняя реализация сравнивала обрезанные строки периода и ломалась на кварталах: `'Q'` сортируется после всех цифр, поэтому `"2026-Q2"` оказывался больше любой границы вида `"YYYY-MM"` — под `since` проходили все кварталы, под `until` не проходил ни один. Исправлено в `8d2a1dc`; регрессия закрыта тестами в `tests/test_mcp/test_tools_server_trend.py`.

Это КАНАЛ-сторона сравнения «о чём писали» vs «что показал опрос»: клиент берёт `rows` отсюда и `rows` из MCP-3 `stat_series`, передаёт оба в MCP-3 `stat_align` (см. §5.1). Строки `topic_trend` отдаются в `stat_align` **как есть**: помимо `period` (человекочитаемая метка — `"2026-Q1"`, `"2026-03"`) и `mentions` каждая строка несёт `period_start` (первый день бакета, ISO-дата) и `value` (= `mentions`) — ровно те два ключа, которые читает `stat_align`. Преобразование живёт здесь, потому что формат метки и granularity знает этот модуль; MCP-3 знать чужой формат меток не должен.

`polarity_evolution` в `stat_align` **напрямую не подаётся**: его строки — `{period, polarity, n}`, то есть разбивка по полярностям (несколько строк на период), а не ряд, и `period` там всегда месяц. Единого `value` в них нет; какую полярность считать «рядом» — решение клиента, и tool его за клиента не принимает.

---

## 5. MCP-3: 3 exact-statistics tools

**Назначение:** точные внешние числа (опросы, официальные ряды) — без синтеза, без единого обращения к LLM, без чтения графа или Milvus. MCP-3 читает только таблицы `stat_indicator` / `stat_observation` в Postgres напрямую через [`src/storage/stats.py:StatsRepository`](../../src/storage/stats.py). Контракт числовых данных отличается от семантического: там «немного не то» — приемлемый ответ, здесь — нет, поэтому арифметика (`stat_align`) намеренно живёт отдельно от модели, в чистых функциях [`src/stats/align.py`](../../src/stats/align.py).

### 5.1 Tools

| Tool | Сигнатура | Что делает | Файл |
|---|---|---|---|
| `stat_indicators_search` | `(query=None, source=None, limit=20)` | Каталог источников (без аргументов) → список индикаторов источника (`source`) → триграм-поиск по названию/вопросу (`query`) | [`stats_server.py`](../../src/mcp/stats_server.py) |
| `stat_series` | `(indicator_id, since=None, until=None, dims=None)` | Значения одного индикатора по времени, последняя ревизия на период; плюс `warnings` | [`stats_server.py`](../../src/mcp/stats_server.py) |
| `stat_align` | `(series_a, series_b, granularity="week", value_kind_a="share", value_kind_b="share", max_lag=4)` | Сведение двух рядов на общую сетку, z-score, поиск лучшего лага, `gap`/`divergence`/`correlation` | [`stats_server.py`](../../src/mcp/stats_server.py) |

**`stat_indicators_search` — три режима намеренно за одним tool'ом.** Пустой `query` и пустой `source` — не ошибка, а вызов "я ещё не знаю, что тут есть": ответ — каталог источников. Триграм-поиск находит опечатки/варианты написания, но не синонимы, поэтому неудачно угаданный `query` возвращает пусто и неотличим от "такой статистики нет" — заранее показать каталог для клиента дешевле, чем гадать.

**`dims` в `stat_series` — три разных запроса, не два.** `dims={"region": "Москва"}` — вхождение (`jsonb @>`): строки, несущие этот разрез, независимо от прочих измерений. `dims={}` — строгое равенство: **только безразмерные** строки. `dims` не передан — вообще без фильтра, то есть ВСЕ разрезы сразу; для размерного индикатора это несколько чисел на период, а не ряд, и тогда в `warnings` приходит `multiple_dims_cuts`. Раньше `{}` и «не передан» вели себя одинаково, и `stat_align` усреднял разрезы внутри бакета, выдавая среднее как точное значение индикатора.

**`stat_align` ничего не читает** — оба ряда приходят аргументами. Клиент сам достаёт канал-сторону через MCP-2 (`topic_trend` / `polarity_evolution`) и подтягивает индикаторную сторону отсюда (`stat_series`), затем передаёт оба в `stat_align`. Это держит границу чистой и не подпускает модель к арифметике.

### 5.2 Запуск

```bash
uv run python -m src.mcp.stats_server --transport stdio
uv run python -m src.mcp.stats_server --transport http --port 9003
```

Порт по умолчанию — **9003** (MCP-1 = 9001, MCP-2 = 9002). Таймаут на tool — 120s (не 1800s как у MCP-2: это тонкий Postgres-read, не тяжёлый graph walk).

---

## 6. Auth

Все три сервера используют тот же `API_KEYS` env что и FastAPI route handlers (см. [`src/api/auth.py`](../../src/api/auth.py) и [`src/config.py:ApiSettings.keys_list`](../../src/config.py)).

**Stdio**: env-vars передаются через конфиг клиента (Claude Desktop "env" поле). API_KEYS не нужен если `KB_MCP_REQUIRE_AUTH=false`.

**HTTP/SSE**: header `Authorization: Bearer <token>` ожидается, token проверяется в `is_valid_key()` ([`src/mcp/_shared.py`](../../src/mcp/_shared.py)).

**Strict mode**: при boot'е (`assert_api_key_env_set()`) сервер падает если `KB_MCP_REQUIRE_AUTH=true` (default) И `API_KEYS` пуст — защита от случайно-открытых портов.

```bash
# Development без auth:
export KB_MCP_REQUIRE_AUTH=false
uv run python -m src.mcp.tools_server --transport http --port 9002

# Production:
export KB_MCP_REQUIRE_AUTH=true
export API_KEYS=dev-local-key,prod-key-2
uv run python -m src.mcp.tools_server --transport http --port 9002
```

---

## 7. Тюнинг — где какой knob

| Knob | Где | Default | Эффект |
|---|---|---|---|
| `TEMPORAL_SEARCH_ACTIVITY_CONCURRENCY` | env (config.py:TemporalSettings) | 4 | Сколько одновременных search-сессий MCP-1 может крутиться |
| `AGENT_LLM_MAX_CONCURRENT` | env (config.py:AgentSettings) | 8 | Семафор-cap на LLM-вызовы (MCP-2 + везде) |
| `AGENT_MAX_ITERATIONS` | env, передаётся client'ом в `kb_search` | 8 | Макс шагов ReAct loop'а |
| `AGENT_MAX_REFINEMENTS` | env, через `kb_search` | 3 | Макс рефайн-раундов в selfrag |
| `KB_MCP_REQUIRE_AUTH` | env | true | Strict auth gate на boot |
| `LITELLM_SEARCH_MODEL` | env | qwen3:8b fallback | Модель для search-role (см. multimodel.md) |

**Рекомендации тюнинга:**
- **Single GPU, low traffic**: `TEMPORAL_SEARCH_ACTIVITY_CONCURRENCY=2`, `AGENT_LLM_MAX_CONCURRENT=4` — серилизуй жёстко.
- **Multi-GPU**: подними оба × число GPU.
- **OpenAI / cloud LLM**: `AGENT_LLM_MAX_CONCURRENT=16+`, упор только в rate-limit'ы провайдера.

---

## 8. Live smoke

```bash
# 1. Pre-flight: основной стек (Temporal, Milvus, Neo4j, Postgres, MinIO)
docker compose -p kb-llamaindex up -d
uv run python -m scripts.setup_db
uv run python -m src.workflow.worker &       # включает kb-search-llm queue

# 2. MCP-1 stdio smoke — tools/list через stdin
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' | \
  KB_MCP_REQUIRE_AUTH=false uv run python -m src.mcp.search_server --transport stdio
# Expect: {"result":{"tools":[{"name":"kb_search",...}]}}

# 3. MCP-2 stdio smoke — все 6 tools должны быть в списке
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' | \
  KB_MCP_REQUIRE_AUTH=false uv run python -m src.mcp.tools_server --transport stdio
# Expect: tools = ["vector_search", "graph_search", "find_entity_by_id",
#                  "find_neighbours", "get_chunks_by_doc_id",
#                  "read_full_document"]

# 4. MCP-3 stdio smoke — 3 tools должны быть в списке
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' | \
  KB_MCP_REQUIRE_AUTH=false uv run python -m src.mcp.stats_server --transport stdio
# Expect: tools = ["stat_indicators_search", "stat_series", "stat_align"]

# 5. HTTP smoke (после ingest какого-нибудь doc'а)
KB_MCP_REQUIRE_AUTH=false uv run python -m src.mcp.tools_server \
  --transport http --host 127.0.0.1 --port 9002 &
curl -X POST http://127.0.0.1:9002/mcp  # Streamable HTTP endpoint
kill %1

# 6. End-to-end через MCP-1 → Temporal:
KB_MCP_REQUIRE_AUTH=false uv run python -m src.mcp.search_server \
  --transport sse --host 127.0.0.1 --port 9001 &
# в браузере открыть http://localhost:8080 (Temporal UI) → submit query через
# OpenWebUI → workflow `mcp-search-<uuid>` появится с progress events
```

---

## 9. Tests

```bash
# Unit suites (no Temporal needed)
uv run pytest tests/test_mcp/ tests/test_retrieval/test_atomic_tools.py \
              tests/test_retrieval/test_llm_semaphore.py -v
# Workflow integration (skips if Temporal port 7233 не доступен)
uv run pytest tests/test_workflow/test_search_workflow.py -v
```

**Известный flake**: совместный прогон MCP + SearchWorkflow в одной pytest-сессии может ловить `ImportError: cannot import name 'claw_state'` (beartype + fastmcp + Temporal sandbox import collision). Разделяй suites — обе passing'ят индивидуально.  Документация: [`tests/conftest.py`](../../tests/conftest.py).

---

## 10. Troubleshooting

| Симптом | Причина | Действие |
|---|---|---|
| MCP server падает на старте с `API_KEYS env is empty` | Strict auth (default) + нет ключей | Set `API_KEYS=dev-local-key` или `KB_MCP_REQUIRE_AUTH=false` |
| `kb_search` зависает > 30 минут | Temporal SearchWorkflow застрял (extract_kg upstream падает?) | Temporal UI → `mcp-search-<id>` → посмотреть failed activity. Cancel руками если нужно: `temporal workflow cancel -w mcp-search-...` |
| `graph_search` возвращает empty | Neo4j unreachable, fallback на `None` | Лог `MCP-2: graph_retriever disabled (Neo4j down?)`. `docker compose ps neo4j` — проверить статус |
| MCP-2 LLM-tool отвечает медленно (10+ сек на простой запрос) | LLM-семафор занят — много параллельных вызовов или ingest конкурирует | Grafana `kb-llamaindex/01-ingest-overview` дашборд — увидеть peak. Поднять `AGENT_LLM_MAX_CONCURRENT` если GPU справляется |
| `kb_search` возвращает пустой answer для нормального query | `simple` mode и vector retriever пустой (нет docs) или semafor пустой | Sanity: `curl /api/v1/search` напрямую с тем же query — должен дать тот же результат |
| OpenWebUI не видит MCP server в Settings | URL не правильный или transport mismatch | OpenWebUI ждёт `/sse` endpoint. `http://host:9001/sse` (не `http://host:9001` голый) |
| MCP-1 progress notifications не приходят клиенту | fastmcp version mismatch или транспорт не поддерживает | stdio: некоторые клиенты игнорят progress. HTTP/SSE: должно работать. Проверить `pip show fastmcp` ≥ 2.0 |
| `stat_indicators_search` без аргументов возвращает пустой каталог | В `stat_indicator`/`stat_observation` ещё ничего не загружено | Данные заливаются только через `POST /api/v1/statistics/load` ([`src/api/routes/stats_data.py`](../../src/api/routes/stats_data.py)) — отдельного загрузчика/скрапера в проекте нет. Проверить: `SELECT count(*) FROM stat_indicator` |
| `stat_align` возвращает `low_overlap:*` в `warnings` | Меньше `STATS_MIN_OVERLAP` (default 8) общих периодов после ресемплинга | Ожидаемо для коротких/редких рядов — корреляция намеренно не считается, не баг |
| `stat_series` возвращает `multiple_dims_cuts` в `warnings` | У индикатора есть разрезы (`dims`), а запрос их не сузил — на один период приходится несколько строк | Это НЕ ряд: передав такое в `stat_align`, вы получите среднее по разрезам, поданное как точное число. Сузить: `dims={"region": "Москва"}`, либо `dims={}` — строго безразмерные строки |
| `topic_trend` отвечает `{"error": "... nebula ..."}` | Ожидаемо: на nebula (прод-дефолт) tool не работает вовсе — см. §4.4 | Не «нет данных». Либо `GRAPH_BACKEND=neo4j`, либо канальную сторону сравнения брать иначе. Пустой `rows` тут был бы хуже ошибки |
| `kb_analyze` (`POST /api/v1/analyze`) молча даёт `topic_trend: rows: []` на nebula, без ошибки | CATALOG-путь зовёт примитив `topic_trend` напрямую, минуя MCP-2 guard `_TOPIC_TREND_UNSUPPORTED_BACKENDS` — см. §4.4 | Не «тему не упоминали» — «бэкенд не может ответить». Перепроверить тот же запрос через MCP-2 `topic_trend`, там это видно как явная ошибка |

---

## 11. Cross-references

- **Search subsystem deep-dive**: [`search-usage.md`](search-usage.md)
- **Per-role LLM (search role)**: [`multimodel.md`](multimodel.md)
- **`atomic_tools.py` reference**: `src/retrieval/atomic_tools.py` (290 строк, 7 функций + dispatch)
- **`SearchWorkflow`**: `src/workflow/search_workflow.py`
- **`BoundedLLM`**: `src/retrieval/llm_semaphore.py`
- **Stats subsystem**: `src/stats/align.py` (чистая арифметика), `src/storage/stats.py` (`StatsRepository`), `src/mcp/stats_server.py`
- **MCP protocol spec**: https://spec.modelcontextprotocol.io/specification/
- **fastmcp docs**: https://gofastmcp.com/
