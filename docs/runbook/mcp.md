# MCP servers runbook

Гид по двум MCP-серверам (Model Context Protocol) которые экспозируют kb-llamaindex как tool-source для внешних LLM-клиентов (OpenWebUI, Claude Desktop, Cursor, Continue, …).

> **⚠️ ОБНОВЛЕНО (R7b cutover) — MCP-1 раздел частично устарел.** MCP-1
> `kb_search` теперь submit'ит **`SearchOrchestratorWorkflow`** (plan-execute,
> local mode) на очередь `kb-search-small`, а НЕ удалённый `SearchWorkflow`.
> Сигнатура — фактически `kb_search(query)` (параметры `mode` /
> `max_iterations` и режимы `simple`/`agent`/`selfrag` УДАЛЕНЫ; ReAct/Self-RAG
> больше нет). Текущая модель поиска: [`search-usage.md`](search-usage.md) +
> [`../SEARCH.md`](../SEARCH.md). Раздел MCP-2 (atomic tools) ниже актуален.
> (Полная переработка MCP-1 прозы — TODO.)

> **Цель архитектуры:** дать оператору два режима интеграции — "получить готовый ответ" (MCP-1) или "взять примитивы и собрать loop своим LLM" (MCP-2). Защита GPU реализована на двух уровнях: Temporal-queue для MCP-1, BoundedLLM-семафор для MCP-2.

Связанные runbook'и:
- [`search-usage.md`](search-usage.md) — текущие режимы `/search/{local,global,drift,auto}`, параметры, тюнинг
- [`multimodel.md`](multimodel.md) — откуда берётся search-role LLM
- [`analytics.md`](analytics.md) — Grafana для search-workflow latency

---

## 1. Two-server overview

| Server | Tool surface | Транспорт | Идёт через | Кто типично подключается |
|---|---|---|---|---|
| **MCP-1** (`src/mcp/search_server.py`) | 1 tool: `kb_search(query)` | stdio + HTTP/SSE | **Temporal `SearchOrchestratorWorkflow`** (plan-execute, local) | OpenWebUI как готовый ассистент; non-LLM-developer clients |
| **MCP-2** (`src/mcp/tools_server.py`) | 6 tools: `vector_search`, `graph_search`, `find_entity_by_id`, `find_neighbours`, `get_chunks_by_doc_id`, `read_full_document` | stdio + HTTP/SSE | прямой Python in-process | Claude Desktop / Cursor / Continue с собственным LLM-loop'ом |

Оба сервера запускаются одной командой; параметр `--transport stdio|sse` переключает режим.

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
    }
  }
}
```

В Cursor: settings.json → `mcp.servers`, та же структура. В Continue: `~/.continue/config.json` под секцией `experimental.mcp`.

### HTTP/SSE (для OpenWebUI и web-клиентов)

```bash
uv run python -m src.mcp.search_server --transport sse --host 0.0.0.0 --port 9001
uv run python -m src.mcp.tools_server  --transport sse --host 0.0.0.0 --port 9002
```

OpenWebUI Admin Settings → MCP servers → URL `http://localhost:9001/sse` (и `:9002/sse`).

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

---

## 5. Auth

Оба сервера используют тот же `API_KEYS` env что и FastAPI route handlers (см. [`src/api/auth.py`](../../src/api/auth.py) и [`src/config.py:ApiSettings.keys_list`](../../src/config.py)).

**Stdio**: env-vars передаются через конфиг клиента (Claude Desktop "env" поле). API_KEYS не нужен если `KB_MCP_REQUIRE_AUTH=false`.

**HTTP/SSE**: header `Authorization: Bearer <token>` ожидается, token проверяется в `is_valid_key()` ([`src/mcp/_shared.py`](../../src/mcp/_shared.py)).

**Strict mode**: при boot'е (`assert_api_key_env_set()`) сервер падает если `KB_MCP_REQUIRE_AUTH=true` (default) И `API_KEYS` пуст — защита от случайно-открытых портов.

```bash
# Development без auth:
export KB_MCP_REQUIRE_AUTH=false
uv run python -m src.mcp.tools_server --transport sse --port 9002

# Production:
export KB_MCP_REQUIRE_AUTH=true
export API_KEYS=dev-local-key,prod-key-2
uv run python -m src.mcp.tools_server --transport sse --port 9002
```

---

## 6. Тюнинг — где какой knob

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

## 7. Live smoke

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

# 4. HTTP/SSE smoke (после ingest какого-нибудь doc'а)
KB_MCP_REQUIRE_AUTH=false uv run python -m src.mcp.tools_server \
  --transport sse --host 127.0.0.1 --port 9002 &
curl -X POST http://127.0.0.1:9002/sse  # выдаст stream начало
kill %1

# 5. End-to-end через MCP-1 → Temporal:
KB_MCP_REQUIRE_AUTH=false uv run python -m src.mcp.search_server \
  --transport sse --host 127.0.0.1 --port 9001 &
# в браузере открыть http://localhost:8080 (Temporal UI) → submit query через
# OpenWebUI → workflow `mcp-search-<uuid>` появится с progress events
```

---

## 8. Tests

```bash
# Unit suites (no Temporal needed)
uv run pytest tests/test_mcp/ tests/test_retrieval/test_atomic_tools.py \
              tests/test_retrieval/test_llm_semaphore.py -v
# Workflow integration (skips if Temporal port 7233 не доступен)
uv run pytest tests/test_workflow/test_search_workflow.py -v
```

**Известный flake**: совместный прогон MCP + SearchWorkflow в одной pytest-сессии может ловить `ImportError: cannot import name 'claw_state'` (beartype + fastmcp + Temporal sandbox import collision). Разделяй suites — обе passing'ят индивидуально.  Документация: [`tests/conftest.py`](../../tests/conftest.py).

---

## 9. Troubleshooting

| Симптом | Причина | Действие |
|---|---|---|
| MCP server падает на старте с `API_KEYS env is empty` | Strict auth (default) + нет ключей | Set `API_KEYS=dev-local-key` или `KB_MCP_REQUIRE_AUTH=false` |
| `kb_search` зависает > 30 минут | Temporal SearchWorkflow застрял (extract_kg upstream падает?) | Temporal UI → `mcp-search-<id>` → посмотреть failed activity. Cancel руками если нужно: `temporal workflow cancel -w mcp-search-...` |
| `graph_search` возвращает empty | Neo4j unreachable, fallback на `None` | Лог `MCP-2: graph_retriever disabled (Neo4j down?)`. `docker compose ps neo4j` — проверить статус |
| MCP-2 LLM-tool отвечает медленно (10+ сек на простой запрос) | LLM-семафор занят — много параллельных вызовов или ingest конкурирует | Grafana `kb-llamaindex/01-ingest-overview` дашборд — увидеть peak. Поднять `AGENT_LLM_MAX_CONCURRENT` если GPU справляется |
| `kb_search` возвращает пустой answer для нормального query | `simple` mode и vector retriever пустой (нет docs) или semafor пустой | Sanity: `curl /api/v1/search` напрямую с тем же query — должен дать тот же результат |
| OpenWebUI не видит MCP server в Settings | URL не правильный или transport mismatch | OpenWebUI ждёт `/sse` endpoint. `http://host:9001/sse` (не `http://host:9001` голый) |
| MCP-1 progress notifications не приходят клиенту | fastmcp version mismatch или транспорт не поддерживает | stdio: некоторые клиенты игнорят progress. HTTP/SSE: должно работать. Проверить `pip show fastmcp` ≥ 2.0 |

---

## 10. Cross-references

- **Search subsystem deep-dive**: [`search.md`](search.md)
- **Per-role LLM (search role)**: [`multimodel.md`](multimodel.md)
- **`atomic_tools.py` reference**: `src/retrieval/atomic_tools.py` (290 строк, 7 функций + dispatch)
- **`SearchWorkflow`**: `src/workflow/search_workflow.py`
- **`BoundedLLM`**: `src/retrieval/llm_semaphore.py`
- **MCP protocol spec**: https://spec.modelcontextprotocol.io/specification/
- **fastmcp docs**: https://gofastmcp.com/
