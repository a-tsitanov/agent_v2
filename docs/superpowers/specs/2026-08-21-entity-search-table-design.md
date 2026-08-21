# Таблица сущностей в Postgres для поиска

Дата: 2026-08-21. Связано: [`BACKLOG.md`](../../BACKLOG.md) («Поиск сущностей
по подстроке — через ключ-хранилище»), вход в граф (`_aretrieve_nebula`).

## Задача

Вход в граф сейчас **один** — векторный kNN по `entity_er_vec` (Milvus):
запрос эмбедится, берутся top-20 ближайших сущностей, каждая раскрывается
обходом. Это ловит смысл и синонимы, но:

- именованный запрос («что с Зеленским») вектор может не поднять первым;
- нет порога и фильтров — тащит 20 ближайших, включая шум;
- подстроку не умеет вовсе (`find_entity_by_name` даёт только префикс через
  Nebula `STARTS WITH`, потому что `CONTAINS` роняет граф по памяти).

Плоская таблица сущностей в Postgres даёт **лексический вход параллельно
вектору** и закрывает всё это одним индексируемым SQL — не трогая Nebula,
которая под нагрузкой падает на сканах.

Это НЕ замена вектору. Синонимы («настроения» → «социальное самочувствие»)
остаются за вектором. Таблица — второй путь: вектор для смысла, таблица для
имён, подстрок и фильтров.

## Где сущности живут сейчас

- Nebula: 163k вершин `Entity` (name, label, description, mention_count,
  first_doc_id, pagerank, betweenness, …).
- Milvus `entity_er_vec`: вектор на сущность.
- Postgres: сущностей НЕТ. `pg_trgm` установлен (обслуживает
  `stat_indicator_*_trgm_idx`).

## Схема

```sql
CREATE TABLE IF NOT EXISTS entity (
    vid           TEXT PRIMARY KEY,            -- entity_vid(name), тот же ключ что в графе
    name          TEXT NOT NULL,
    label         TEXT NOT NULL DEFAULT '',
    description   TEXT NOT NULL DEFAULT '',
    mention_count INTEGER NOT NULL DEFAULT 1,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS entity_name_trgm_idx
    ON entity USING GIN (name gin_trgm_ops);   -- подстрока + опечатки
CREATE INDEX IF NOT EXISTS entity_label_idx ON entity (label);
```

**Чего в таблице НЕТ намеренно:**

- `pagerank` / `betweenness` — считаются офлайн (`AnalyticsMaterialize`, раз в
  сутки), меняются пачкой, не при ингесте. Дублировать их сюда = держать
  два расходящихся источника центральности. Если ранжирование по центральности
  понадобится — читать из графа отдельным запросом, не из этой таблицы.
- Внешнего ключа/связи с графом. Это поисковый индекс-зеркало, не второй
  источник истины. Граф остаётся каноничным.

`vid = entity_vid(name)` — тот же детерминированный ключ, что и вершина в
Nebula (`nebula_store.entity_vid`). Значит upsert идемпотентен и совпадает с
графом по ключу.

## Запись при ингесте

Точка врезки — `NebulaGraphStore.upsert_nodes` (`nebula_store.py:126`), где
сущность уже пишется в граф: `INSERT VERTEX Entity (name, description,
mention_count, …)`, keyed by `entity_vid`. Тот же список узлов, тот же ключ —
добавить рядом upsert в Postgres.

Форма (следует `PostgresERVerdictCache` — синхронный pool, т.к. `upsert_nodes`
синхронный):

```sql
INSERT INTO entity (vid, name, label, description, mention_count)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (vid) DO UPDATE SET
    name = EXCLUDED.name, label = EXCLUDED.label,
    description = EXCLUDED.description,
    mention_count = EXCLUDED.mention_count, updated_at = now();
```

**FAIL-SOFT, как и весь ingest-путь:** ошибка Postgres логируется и глотается,
запись в граф не срывается. Таблица — ускоритель поиска, а не критичный путь;
рассинхрон лечится повторной заливкой, не падением ингеста.

**Слияние сущностей.** Когда ER схлопывает дубликат в каноникал
(`merge_loser_into_canonical`), проигравшая вершина удаляется из графа. Её
строка в `entity` останется сиротой. Это допустимо: имя проигравшего всё ещё
ведёт к реальной (теперь каноничной) сущности через тот же обход. Чистку
сирот отложить — она не ломает поиск, только слегка раздувает таблицу. Если
понадобится — отдельный проход по vid, которых больше нет в графе.

## Разовая заливка существующих

`scripts/backfill_entity_table.py` — по образцу `migrate_er_verdicts.py`
(курсорная выгрузка, батч-upsert, идемпотентно, возобновляемо).

Выгрузка из Nebula — по индексу, НЕ сканом (граф падает на полных проходах):

```
LOOKUP ON `Entity` WHERE `Entity`.name >= "<last>"
  YIELD id(vertex) AS vid, `Entity`.name AS name, `Entity`.label AS label,
        `Entity`.description AS description, `Entity`.mention_count AS mc
  | ORDER BY $-.name | LIMIT <page>
```

Пагинация по диапазону `name` (тот же приём, что в миграции вердиктов, где
offset-пагинация падала с `StorageMemoryExceeded`). Батч в Postgres —
`ON CONFLICT DO UPDATE`, повторный прогон безвреден.

163k сущностей, страница в тысячи → минуты, разово.

## Поиск

`src/storage/entity_search.py` — чистые билдеры запросов + тонкие методы, как
`src/storage/stats.py`. Один вход, четыре режима отбора:

- `exact`     — `WHERE name = %s`
- `prefix`    — `WHERE name ILIKE %s || '%'`
- `substring` — `WHERE name %% %s` (триграмма) `ORDER BY similarity(name,%s) DESC`
- всё с опциональным `label = %s`, лимитом, `ORDER BY mention_count DESC` как
  тай-брейком (частые сущности выше).

Вернуть `{vid, name, label, description, mention_count}`.

## Врезка во вход графа

`_aretrieve_nebula` (`retriever.py`) сейчас: только векторный kNN → обход.
Стало: **вектор ∪ таблица** → обход по объединению.

```
запрос → [ kNN(er_vec, top_k)  ,  entity_search.substring(query, limit) ]
       → объединить имена (dedup по vid) → awalk по каждому
```

Вектор даёт смысловые попадания, таблица — точные/подстрочные/фильтрованные.
Объединение имён, обход не меняется. Порог на векторной стороне (вариант из
разбора входа) — отдельная последующая правка, не в этой спеке.

## Побочные выгоды

Одна таблица закрывает три задачи:

1. **Вход в граф** — лексический путь рядом с вектором.
2. **`find_entity_by_name`** (MCP + `/entity` в боте) — переключить с Nebula
   `STARTS WITH` на Postgres: получает подстроку, фильтры, и не зависит от
   памяти Nebula. Закрывает бэклог-дыру «Ромаш → ООО Ромашка».
3. **Бэклог «substring entity search»** — снимается целиком.

## Проверка

- `entity` содержит ~163k строк после заливки, число сходится с
  `SHOW STATS` тегом Entity.
- `entity_search.substring("Ромаш")` находит «ООО Ромашка» — то, чего
  префикс не мог.
- Ингест-раунд после врезки: новые сущности появляются в таблице, ошибок
  Postgres в логе воркера нет, а сбой Postgres не роняет запись в граф.
- `_aretrieve_nebula` на именованном запросе поднимает точную сущность даже
  когда вектор её не в топе.

## Риски

- **Ещё одна нагрузка на Postgres** (делят Temporal, documents, er_verdict).
  Но это upsert по ключу и trigram-lookup, не сканы — нагрузка лёгкая.
- **Рассинхрон** таблицы и графа при сбоях. Смягчение: fail-soft + возможность
  переналить в любой момент (идемпотентно). Таблица — зеркало, не истина.
- **Сироты от слияния** — растут медленно, поиск не ломают, чистка отложена.
