# Runbook index

Operator-facing руководства по эксплуатации kb-llamaindex.  Каждый файл самодостаточен — читать линейно.

## Runbook'и

| Runbook | Тема | Когда читать |
|---|---|---|
| [`multimodel.md`](multimodel.md) | Per-role LLM + GraphBuildWorkflow child + per-activity model в `ingest_metrics` | Перед первым сабмитом с разными моделями по ролям; при отладке `graph_status="vector_only"` |
| [`analytics.md`](analytics.md) | Grafana dashboards + Prometheus + Postgres `ingest_metrics` + version-tag механика | Чтобы понять что показывает каждый дашборд + retention правила |
| [`wikibase.md`](wikibase.md) | Самохостящийся Wikibase: bootstrap, push_wikibase activity, SPARQL/wdqs | Включение wikibase-фичи + смена `WIKIBASE_*` env |

## Архитектурные документы (не runbook'и)

| Файл | Что |
|---|---|
| [`../ARCHITECTURE.md`](../ARCHITECTURE.md) | Top-level data flow + storage components + layer responsibilities |
| [`../MODELS.md`](../MODELS.md) | Model selection guidance, capability flags, escalation path |
| [`../DEPLOYMENT.md`](../DEPLOYMENT.md) | Deploy-сценарии |
| [`../QUERY.md`](../QUERY.md) | Подробности retrieval / search endpoints |
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
| `feature/wikibase-population` | Wikibase push activity, base classes, identifier folding | [`wikibase.md`](wikibase.md) |
| `feature/analytics-grafana` | Prometheus + Grafana + `ingest_metrics` + version_tag | [`analytics.md`](analytics.md) |
| `feature/multimodel-and-child` | Per-role LLM (extraction/judge/search) + `GraphBuildWorkflow` child + per-activity model | [`multimodel.md`](multimodel.md) |
