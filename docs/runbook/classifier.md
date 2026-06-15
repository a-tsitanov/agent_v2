# Runbook — входной классификатор документов

Опциональный фильтр, который **пропускает мусор до того**, как он потратит
работу на parse/embed/extract. Реализован как активность
`classify_document` (между `fetch_source` и `parse_and_chunk`).

- Код: `src/ingestion/classifier.py`, `src/workflow/activities/classify_document.py`,
  `mark_skipped` в `src/workflow/activities/finalize.py`.
- Конфиг: `CLASSIFIER_*` (см. `.env.example`).

## Как работает

1. **`force`** (Form-параметр `force=true` на `POST /ingest`, снапшот в
   `IngestParams.force`) — **обходит детерминированные правила** и сразу
   допускает документ.
2. **Детерминированные правила** (`apply_rules`): заблокированное расширение
   (`CLASSIFIER_SKIP_EXTENSIONS`), слишком маленький/пустой
   (`CLASSIFIER_MIN_SIZE_BYTES`), слишком большой (`CLASSIFIER_MAX_SIZE_MB`).
3. **LLM-гейт** (`classify_with_llm`, если `CLASSIFIER_LLM_ENABLED=true`):
   `astructured_predict` по превью первых `CLASSIFIER_PREVIEW_CHARS` символов →
   `{ingest, reason, doc_type}`.

На skip → активность `mark_skipped` пишет терминальный статус **`skipped`** +
reason в Postgres и чистит staging; parse/index/graph не выполняются.

**Fail-soft:** любая ошибка классификатора → **ingest** (ложный скип теряет
хороший документ — самая дорогая ошибка). Поэтому целься в высокий recall на
классе «keep».

## Включение

```bash
CLASSIFIER_ENABLED=true
# при необходимости — пороги:
CLASSIFIER_MAX_SIZE_MB=25.0
CLASSIFIER_SKIP_EXTENSIONS=["exe","zip","png","mp4"]
```
Флаг снапшотится в `IngestParams.classifier_enabled` на сабмите (детерминизм
Temporal) — воркфлоу ветвится по снапшоту, не читает settings в теле.

## Принудительный ингест (обход правил)

```bash
curl -F file=@doc.bin -F force=true -H "X-API-Key: $KEY" \
  http://localhost:8000/api/v1/ingest
```

## Диагностика

- «Документ не проиндексировался» → проверь статус в Postgres: `skipped` + reason
  в колонке error. В логах воркера: `classify  doc=…  SKIP (rules|llm): …`.
- Ложные скипы хороших документов → ослабь правила или `CLASSIFIER_LLM_ENABLED=false`
  (только правила), либо заливай с `force=true`.
- Бенчмарк качества: `tests/eval/classifier_scenarios.py` (guardrail на recall
  правил; расширяй реальными размеченными примерами перед тюнингом).

## Откат
`CLASSIFIER_ENABLED=false` → шаг классификации полностью пропускается, поведение
ингеста как раньше.
