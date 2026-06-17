# Runbook — допуск документов (admission control)

Опциональный контроль конкуренции на уровне документов: запущенный документ
**доводится до конца как приоритетная единица**, вместо того чтобы его хвост
(merge) голодал в очереди за extract'ами более новых документов.

- Код: `src/workflow/admission.py` (чистая `AdmissionState`),
  `src/workflow/ingest_scheduler.py` (`IngestSchedulerWorkflow`).
- Конфиг: `INGEST_ADMISSION_ENABLED`, `INGEST_ADMISSION_MAX_INFLIGHT`.

## Зачем

Стадии ингеста — раздельные FIFO-очереди Temporal с concurrency > 1
(`kb-ingest`=4, `kb-ingest-llm`=18, `kb-ingest-merge`=14). Без ограничения
десятки документов «в полёте» одновременно, и `merge_and_resolve` документа A
встаёт за `extract_kg` сорока более новых документов — очередь забивается
(см. [`worker_hang`](../../docs/QUEUES.md)). Допуск ограничивает число
документов «в работе» до **K**, и каждый проходит весь конвейер до завершения.

## Как работает

- При `INGEST_ADMISSION_ENABLED=true` `POST /ingest` делает **signal-with-start**
  синглтона `IngestSchedulerWorkflow` (фиксированный id `ingest-scheduler`,
  `WorkflowIDConflictPolicy.USE_EXISTING`, сигнал `submit`) вместо прямого старта
  `DocumentIngestWorkflow`.
- Scheduler допускает не более **K = `INGEST_ADMISSION_MAX_INFLIGHT`** документов
  одновременно, запускает каждый `DocumentIngestWorkflow` дочерним и ждёт его
  завершения перед допуском следующего (FIFO). Упавший документ всё равно
  освобождает слот.
- `continue_as_new` ограничивает историю (переезд только в покое, pending
  переносится).
- При выключенном флаге (по умолчанию) — прямой старт, поведение как раньше.

## Выбор K

| K | Поведение |
|---|---|
| **1** | Строго «начал → закончил, потом следующий». GPU простаивает на не-LLM стадиях документа. |
| **2–3** | Один документ на I/O-стадиях, другой грузит GPU; глубина очередей ограничена K документами. |

Рекомендация: старт с K=1 под требование «приоритет/до конца», затем поднимать
под утилизацию GPU, наблюдая глубину `kb-ingest-merge`.

## Включение

```bash
INGEST_ADMISSION_ENABLED=true
INGEST_ADMISSION_MAX_INFLIGHT=1
```
Воркер должен хостить `IngestSchedulerWorkflow` (зарегистрирован в main-пуле
рядом с `DocumentIngestWorkflow`).

## Диагностика

- В Temporal UI должен быть **один** синглтон `ingest-scheduler` + дочерние
  `ingest-{doc_id}`.
- Глубина `kb-ingest-merge` должна держаться в пределах работы K документов.
- Документы «висят pending» — это норма: они ждут допуска; статус обновит
  дочерний `DocumentIngestWorkflow` после старта.

## Оговорка

Чистая `AdmissionState` покрыта юнит-тестами (`tests/test_workflow/test_admission.py`).
Сама Temporal-оболочка **не прогонялась на live-Temporal** — перед продом
проверь на живом сервере: пробуждение `wait_condition` завершением дочернего,
`continue_as_new` в покое, сигнал в момент recycle.

## Изменение K в рантайме

`INGEST_ADMISSION_MAX_INFLIGHT` в `.env` применяется только к **новому** экземпляру
scheduler: поскольку `/ingest` использует `USE_EXISTING`, запущенный синглтон
сохраняет свой K. Перезапуск воркера **не обновляет** K живого workflow.

**Живое изменение** (без terminate):

```bash
uv run python -m scripts.set_admission <N>
```

Команда посылает сигнал `set_max_inflight` синглтону `ingest-scheduler`.
Значение сохраняется через `continue_as_new` (передаётся в `SchedulerParams.max_inflight`).
При повышении K `wait_condition` немедленно допускает ожидающие документы;
при понижении scheduler просто прекращает допуск, пока inflight не опустится до нового K.

**Полный сброс** (когда scheduler простаивает):
terminate `ingest-scheduler` в Temporal UI — следующий `/ingest` создаст
новый синглтон со значением из `.env`.

## Откат
`INGEST_ADMISSION_ENABLED=false` → прямой старт, scheduler не используется.
