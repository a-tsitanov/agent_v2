# Runbook index

Operator-facing руководства по эксплуатации kb-llamaindex.  Каждый файл самодостаточен — читать линейно.

## Runbook'и

| Runbook | Тема | Когда читать |
|---|---|---|
| [`mcp.md`](mcp.md) | Два MCP-сервера: MCP-1 (`kb_search` через Temporal `SearchOrchestratorWorkflow`) и MCP-2 (atomic retrieval tools прямо в процессе). Stdio + HTTP/SSE транспорты, auth, tuning, troubleshooting | Подключаешь OpenWebUI / Claude Desktop / Cursor / Continue; настраиваешь concurrency cap'ы для GPU-защиты |
| [`search-usage.md`](search-usage.md) | Search: `/api/v1/search/{local,global,drift,auto}` — когда какой режим, параметры/тюнинг, примеры, диагностика зависшего синтеза | Чтобы понять как отвечает API на user-query + tuning (архитектура — в [`../SEARCH.md`](../SEARCH.md)) |
| [`multimodel.md`](multimodel.md) | Per-role LLM + GraphBuildWorkflow child + per-activity model в `ingest_metrics` | Перед первым сабмитом с разными моделями по ролям; при отладке `graph_status="vector_only"` |
| [`analytics.md`](analytics.md) | Grafana dashboards + Prometheus + Postgres `ingest_metrics` + version-tag механика | Чтобы понять что показывает каждый дашборд + retention правила |
| [`wikibase.md`](wikibase.md) | Самохостящийся Wikibase: bootstrap, push_wikibase activity, SPARQL/wdqs | Включение wikibase-фичи + смена `WIKIBASE_*` env |
| [`wiki-editor.md`](wiki-editor.md) | Непрерывный редактор Wiki-статей из графа: dirty-mark + Schedule sweep, бот-секция, анти-дрейф, sitelink, gotcha'и | Включаешь wiki-фичу / меняешь `WIKI_*` |
| [`er-native-vector-knn.md`](er-native-vector-knn.md) | Опциональный нативный векторный kNN для ER вместо окна 5000: backfill-скрипт → флаг `AGENT_ER_USE_NATIVE_VECTOR_KNN`, порядок включения, откат | Включаешь native-ER на большом графе (≫5000 сущностей) |

## Архитектурные документы (не runbook'и)

| Файл | Что |
|---|---|
| [`../ARCHITECTURE.md`](../ARCHITECTURE.md) | Top-level data flow + storage components + layer responsibilities |
| [`../MODELS.md`](../MODELS.md) | Model selection guidance, capability flags, escalation path |
| [`../DEPLOYMENT.md`](../DEPLOYMENT.md) | Deploy-сценарии |
| [`../SEARCH.md`](../SEARCH.md) | Search-подсистема: архитектура R7b (`/search/{local,global,drift,auto}`), workflows, очереди |
| [`../architecture.html`](../architecture.html) | Визуальная карта системы (открывается в браузере) |
| [`../architecture.d2`](../architecture.d2) / [`../architecture.svg`](../architecture.svg) | D2-источник + рендер |

## Плэны и specs

`docs/superpowers/plans/` — припаркованные/закрытые планы спринтов.
`docs/superpowers/specs/` — design specs.

## Что обновляется когда

- Добавляешь новую фичу → новый файл `docs/runbook/<feature>.md` + строка в этом index'е
- Меняешь архитектуру / data flow → обновить `ARCHITECTURE.md` + `architecture.html` + `architecture.d2`
- Меняешь модели / роли → `MODELS.md`
- Меняешь Grafana JSON → секция в `analytics.md` обновляется

## Recently shipped (хронологически)

| Sprint | Что добавилось | Runbook |
|---|---|---|
| `feature/wiki-editor` | Per-entity MediaWiki articles from the graph (dirty-mark + Schedule sweep, bot-section rewrite, anti-drift) | [`wiki-editor.md`](wiki-editor.md) |
| `feature/wikibase-population` | Wikibase push activity, base classes, identifier folding | [`wikibase.md`](wikibase.md) |
| `feature/analytics-grafana` | Prometheus + Grafana + `ingest_metrics` + version_tag | [`analytics.md`](analytics.md) |
| `feature/multimodel-and-child` | Per-role LLM (extraction/judge/search) + `GraphBuildWorkflow` child + per-activity model + LiteLLM model validator | [`multimodel.md`](multimodel.md) |
| `R7b cutover` | Legacy ReAct `/search`,`/agent`,`/selfrag` + `SearchWorkflow` УДАЛЕНЫ. Единственная поверхность — `/search/{local,global,drift,auto}` (plan-execute + GraphRAG global/drift/auto) | [`search-usage.md`](search-usage.md) · [`../SEARCH.md`](../SEARCH.md) |
| `feature/search-mcp` | Search → Temporal `SearchWorkflow` (`kb-search-llm` queue, configurable cap). `atomic_tools.py` (7 pure functions) + `BoundedLLM` (process-wide GPU semaphore). Два MCP-сервера: MCP-1 `kb_search` через workflow + progress streaming, MCP-2 atomic tools прямо in-process | [`mcp.md`](mcp.md) |
