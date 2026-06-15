# Индекс runbook'ов

Руководства по эксплуатации kb-llamaindex для операторов. Каждый файл самодостаточен — читать линейно.

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
| [`reresolve-graph.md`](reresolve-graph.md) | Пакетная консолидация: прогон ER по всему графу для слияния накопившихся дублей смысловых сущностей (`scripts/reresolve_graph.py`), сохранение типов связей, dry-run | Дедуп уже существующего графа без переингеста |
| [`classifier.md`](classifier.md) | Входной классификатор документов (opt-in `CLASSIFIER_*`): правила + LLM-гейт, `force`-обход, статус `skipped`, fail-soft | Включаешь фильтрацию мусора на входе ингеста |
| [`admission-control.md`](admission-control.md) | Допуск документов (opt-in `INGEST_ADMISSION_*`): синглтон `IngestSchedulerWorkflow`, ≤K в работе, FIFO, выбор K | Очередь забивается документами с первых стадий; хочешь «документ до конца, потом следующий» |
| [`leiden-diagnostics.md`](leiden-diagnostics.md) | Почему Leiden «не находит сообществ» на 50k: различение GDS-ошибки / пустого графа / синглтонов; Cypher-диагностика (WCC, leiden.stats); взвешенный Leiden | Перестройка сообществ даёт 0; тюнинг `COMMUNITY_*` |

## Архитектурные документы (не runbook'и)

| Файл | Что |
|---|---|
| [`../ARCHITECTURE.md`](../ARCHITECTURE.md) | Верхнеуровневый data flow + компоненты хранения + ответственность слоёв |
| [`../MODELS.md`](../MODELS.md) | Рекомендации по выбору моделей, флаги возможностей, путь эскалации |
| [`../DEPLOYMENT.md`](../DEPLOYMENT.md) | Сценарии деплоя |
| [`../SEARCH.md`](../SEARCH.md) | Подсистема поиска: архитектура R7b (`/search/{local,global,drift,auto}`), workflows, очереди |
| [`../diagrams/system_architecture.svg`](../diagrams/system_architecture.svg) / [`../diagrams/system_architecture.d2`](../diagrams/system_architecture.d2) | Визуальная карта системы (рендер + D2-источник) |

## Планы и спеки

`docs/superpowers/plans/` — припаркованные/закрытые планы спринтов.
`docs/superpowers/specs/` — design-спеки.

## Что обновляется когда

- Добавляешь новую фичу → новый файл `docs/runbook/<feature>.md` + строка в этом index'е
- Меняешь архитектуру / data flow → обновить `ARCHITECTURE.md` + `diagrams/system_architecture.d2` (перерендерить `.svg`)
- Меняешь модели / роли → `MODELS.md`
- Меняешь Grafana JSON → секция в `analytics.md` обновляется

## Недавно выпущенное (хронологически)

| Спринт | Что добавилось | Runbook |
|---|---|---|
| `seven-tracks (2026-06-15)` | Входной классификатор + `force`; admission control (`IngestSchedulerWorkflow`); шаблоны ответа (`answer_template`); GDS-анализ `/admin/graph/*`; взвешенные связи + теги + weighted Leiden; прод `Dockerfile`+`docker-compose.prod.yml`; фикс загрузки doc по id | [`classifier.md`](classifier.md) · [`admission-control.md`](admission-control.md) · [`leiden-diagnostics.md`](leiden-diagnostics.md) |
| `feature/wiki-editor` | MediaWiki-статьи на сущность из графа (dirty-mark + Schedule sweep, переписывание бот-секции, анти-дрейф) | [`wiki-editor.md`](wiki-editor.md) |
| `feature/wikibase-population` | Активность push в Wikibase, базовые классы, сворачивание идентификаторов | [`wikibase.md`](wikibase.md) |
| `feature/analytics-grafana` | Prometheus + Grafana + `ingest_metrics` + version_tag | [`analytics.md`](analytics.md) |
| `feature/multimodel-and-child` | Per-role LLM (extraction/judge/search) + child `GraphBuildWorkflow` + model по активностям + валидатор моделей LiteLLM | [`multimodel.md`](multimodel.md) |
| `R7b cutover` | Легаси ReAct `/search`,`/agent`,`/selfrag` + `SearchWorkflow` УДАЛЕНЫ. Единственная поверхность — `/search/{local,global,drift,auto}` (plan-execute + GraphRAG global/drift/auto) | [`search-usage.md`](search-usage.md) · [`../SEARCH.md`](../SEARCH.md) |
| `feature/search-mcp` | Search → Temporal `SearchWorkflow` (очередь `kb-search-llm`, настраиваемый cap). `atomic_tools.py` (7 чистых функций) + `BoundedLLM` (общепроцессный GPU-семафор). Два MCP-сервера: MCP-1 `kb_search` через workflow + стриминг прогресса, MCP-2 atomic-tools прямо in-process | [`mcp.md`](mcp.md) |
