# ADR-0015: Бэкенд детекции сообществ — in-worker leidenalg (вынос из Neo4j GDS)

- Статус: Принято
- Дата: 2026-06-22

## Контекст

Детекция сообществ (ADR-0009) запускала `gds.leiden.stream` внутри Neo4j: GDS
проецирует **весь** подграф `__Entity__` двунаправленно (каждое ребро дважды) и
с `includeIntermediateCommunities` держит всю дендрограмму в JVM-heap Neo4j. По
мере роста графа перестроение падает с `OutOfMemoryError: Java heap space` даже
при heap 32G (а `FAST_RETRY` ×3 добивает Neo4j повторными проекциями). Любой
подход, удерживающий весь граф в памяти за один проход, рано или поздно упирается
в потолок; вдобавок OOM роняет ту же БД, что обслуживает живой поиск.

## Решение

Выносим **вычисление** из GDS/JVM Neo4j в воркер `kb-graph-build` на
`leidenalg`/`python-igraph`, за **opt-in** флагом `community_backend`
(`"gds" | "leidenalg"`, дефолт `"gds"`). leidenalg-путь: стримит рёбра из Neo4j
keyset-пагинацией по **уникальному** `elementId(r)`, строит взвешенный igraph,
запускает иерархический Leiden **итеративной агрегацией** (кластеризуем → стампим
уровень на исходные ноды → агрегируем супер-граф → повторяем) и отдаёт **те же**
строки `{name, communityId, ids:[finest..coarsest]}`, что и GDS-стрим. Поэтому
группировка (`_coarsest_from_rows`/`_group_by_levels`), материализация
`:Community` и весь downstream (ADR-0009) **не меняются**. Память живёт в процессе
воркера (сайзится независимо), а не в heap Neo4j. Оркестрация упрочнена:
`DETECT_RETRY` делает `MemoryError` non-retryable, добавлены `heartbeat_timeout`
и heartbeat во время прогона. Флип дефолта на `leidenalg` — **только** после
прогона бенчмарка паритета (`tests/eval/bench_community_backends.py`) на реальном
Neo4j (сопоставимая модулярность/размеры + падение памяти Neo4j).

## Последствия

- Снимает OOM-потолок с Neo4j и **изолирует blast-radius** — краш кластеризации
  больше не дестабилизирует БД поиска. Глобальная иерархия (GraphRAG) сохранена.
- Swap **обратим** в любой момент через флаг (один и тот же output-контракт);
  дефолт остаётся `gds` до подтверждения паритета бенчмарком (политика проекта:
  benchmark before adopting).
- **Частично заменяет** GDS-вычисление из ADR-0009; его дизайн иерархии/отчётов,
  очередь `kb-graph-build` и идемпотентность остаются в силе.
- GDS-плагин Neo4j остаётся (его используют другие analysis-инструменты); с
  дефолтного пути уходит только `gds.leiden`.
- Потолок памяти теперь на отдельном воркере (далёкий, изолированный); дальнейшее
  масштабирование (партиционирование / распределённая кластеризация) — вне scope.

## Рассмотренные альтернативы

- **Тюнинг GDS-памяти / выключение `includeIntermediateCommunities`** — лишь
  отодвигает потолок по мере роста графа, не устраняет его.
- **Divide & conquer внутри GDS** (кластеризовать куски + верх иерархии) — даёт
  приближение на стыках и всё равно грузит JVM-heap Neo4j.
- **Отдельный clustering-сервис** — сильнее изоляция, но лишняя ops-сложность;
  существующий воркер `kb-graph-build` уже отдельный процесс/контейнер.

## Ссылки

- `src/graph/community_leiden.py` (`extract_entity_edges`, `build_graph`,
  `single_level_rows`, `hierarchy_rows`), `src/graph/communities.py`
  (ветка `community_backend`), `community_backend` в `src/config.py`,
  `src/workflow/search/_retry.py` (`DETECT_RETRY`),
  `src/workflow/search/activities/community.py`,
  `tests/eval/bench_community_backends.py`
- Спека/план: `docs/superpowers/specs/2026-06-22-community-detection-offload-design.md`,
  `docs/superpowers/plans/2026-06-22-community-detection-offload.md`
- ADR-0009 (иерархические сообщества — дизайн downstream, частично заменяемое
  GDS-вычисление)
