# Runbook — backfill `doc_id` на legacy-чанки Milvus

Чанки, проиндексированные **до** того, как `index_vector` начал помечать
каждую ноду `doc_id`, не имеют этого поля в Milvus. Из-за этого агентские
инструменты `get_chunks_by_doc_id` / `read_full_document` не находят их.
Скрипт `scripts/backfill_doc_id.py` доустанавливает `doc_id`, не трогая
текст и эмбеддинг.

## Как работает

- Берёт карту `file_path → doc_id` из таблицы Postgres `documents`
  (`AsyncPostgres.list_id_path`).
- Стримит все строки коллекции Milvus батчами (`query_iterator`, чтобы
  обойти лимит окна offset-paging в 16 384).
- Для каждой строки **реконструирует ноду** из `_node_content`, возвращая
  на место **текст** (отдельное поле `text`) и **эмбеддинг** (поле
  `embedding`) — никакого реэмбеддинга/репарсинга.
- Чанкам без `doc_id` проставляет его по `file_path`; переиндексирует их
  обычным путём LlamaIndex (`vector_store.add`, `upsert_mode=True` →
  перезапись по `node_id`, без дублей).
- Чанки, у которых `file_path` нет в Postgres, остаются как есть и
  считаются `unresolved`.

## Запуск

```bash
# Сухой прогон — только счётчики (total / already / resolved / unresolved):
python -m scripts.backfill_doc_id

# Применить:
python -m scripts.backfill_doc_id --no-dry-run
python -m scripts.backfill_doc_id --no-dry-run --batch-size 2000
```

## Перед применением

- **Сделай бэкап Milvus** (или прогоняй на проверяемом датасете).
- Сначала **dry-run** и сверь счётчики: `resolved` — сколько будет
  исправлено, `unresolved` — сколько осиротевших (path drift / удалённые
  из Postgres документы).
- Идемпотентность: повторный запуск трогает только чанки, всё ещё без
  `doc_id`.

## Диагностика

- `unresolved > 0` — у чанков `file_path`, которого нет в `documents`.
  Причины: документ удалён из Postgres, или путь сменился между ингестами.
  Это не ошибка скрипта — такие чанки нельзя сопоставить с документом.
- Проверка после применения: `get_chunks_by_doc_id` для старого документа
  должен теперь возвращать его чанки (раньше — пусто).

## Оговорка

Чистое ядро (`src/storage/backfill.py`: планирование + реконструкция ноды
с восстановлением текста/вектора) покрыто юнит-тестами
(`tests/test_storage/test_backfill.py`). Сам Milvus-I/O пути (paging +
re-add) **не прогонялся против живого Milvus** в этой среде — проверь
dry-run-счётчики на реальной коллекции перед `--no-dry-run`.
