# Wiki-editor runbook

## 1. Что это

Непрерывный редактор Wiki-статей — превращает сущности графа Neo4j в per-entity
MediaWiki-статьи. Каждая сущность получает отдельную страницу MediaWiki; если у
сущности есть `wikibase_qid`, статья дополнительно привязывается к Wikibase Item
через sitelink. Единица работы — одна сущность; триггер — гибридный: ingest
помечает затронутые сущности флагом `wiki_dirty` (best-effort хук сразу после
записи в граф), а Temporal Schedule (или ручной запуск) дренирует грязную очередь
пачками.

Редактор переписывает только **бот-секцию** страницы — блок между маркерами
`<!-- KB-BOT:START -->` и `<!-- KB-BOT:END -->`. Текст, написанный человеком за
пределами этих маркеров, сохраняется нетронутым. Анти-дрейф достигается
архитектурно: в промпт подаётся только граф + цитаты, прошлая проза никогда не
подаётся обратно, поэтому галлюцинации из предыдущего прогона не накапливаются.
Неизменённые сущности пропускаются через hash-skip по подграфу сущности — если
граф не изменился, MediaWiki API вызов не происходит.

Связанные документы:
- Спека: `docs/superpowers/specs/2026-06-06-wiki-editor-design.md`
- План: `docs/superpowers/plans/2026-06-06-wiki-editor.md`
- Очереди: `docs/QUEUES.md` (секция `kb-wiki`)

## 2. Включение

```bash
# 1. Поднять MediaWiki/Wikibase (если ещё не запущены):
docker compose -p kb-llamaindex up -d wikibase wikibase-mysql
# Дождаться готовности (~90 сек на первый старт):
curl -fsS http://localhost:8181/wiki/Special:Version -o /dev/null && echo "wikibase ok"

# 2. Провижн бот-юзера (идемпотентно):
uv run python -m scripts.setup_wikibase

# 3. (Опц.) Зарегистрировать Temporal Schedule для автосвипа:
uv run python -m scripts.setup_wiki_schedule

# 4. Включить фичу и перезапустить воркер:
export WIKI_ENABLED=true
pkill -f "src.workflow.worker"
uv run python -m src.workflow.worker &
```

Без шага 3 свип запускается только вручную через `POST /admin/wiki/rebuild`.

## 3. Поток данных

```
Ingest
  └─► mark_entities_dirty()          # хук: помечает wiki_dirty=true на сущности
        |                             # и endpoint'ах её связей
        ▼
  WikiSweepWorkflow (kb-wiki)
        ├─ select_dirty_entities      # выбирает пачку dirty-сущностей из Neo4j
        └─ write_entity_article ×N   # для каждой: генерирует бот-секцию,
                                     # сравнивает hash, пишет в MediaWiki API
                                     # (sitelink → Wikibase Item, если есть qid)
                                     ▼
                              MediaWiki-страница
```

**Запуск вручную:**

```bash
# Дренировать текущую dirty-очередь:
curl -X POST http://localhost:8000/api/v1/admin/wiki/rebuild \
     -H "X-API-Key: $API_KEY"

# Полная пересборка — пометить ВСЕ сущности dirty, затем дренировать:
curl -X POST "http://localhost:8000/api/v1/admin/wiki/rebuild?all=true" \
     -H "X-API-Key: $API_KEY"
```

Параметр `?all=true` сначала выставляет `wiki_dirty=true` на все сущности в
Neo4j, потом запускает свип. Свип дренирует по `WIKI_SWEEP_BATCH` сущностей за
раз; при большом графе запускайте ещё раз пока Temporal показывает выполнение.

## 4. Конфигурация (`WIKI_*`)

| Переменная | По умолчанию | Описание |
|---|---|---|
| `WIKI_ENABLED` | `false` | Главный тумблер; `true` включает воркер на очередь `kb-wiki` |
| `WIKI_TASK_QUEUE` | `kb-wiki` | Temporal task-queue для wiki-воркера |
| `WIKI_ACTIVITY_CONCURRENCY` | `4` | Сколько `write_entity_article` параллельно |
| `WIKI_SWEEP_BATCH` | `50` | Сущностей за один прогон `select_dirty_entities` |
| `WIKI_SWEEP_INTERVAL_MINUTES` | `60` | Интервал Temporal Schedule (если настроен) |
| `WIKI_MAX_RELATIONS` | `30` | Сколько 1-hop связей (ранжированных по mention_count соседа, по убыванию) подаётся в промпт статьи. Ограничивает размер промпта для сущностей-хабов. |
| `WIKI_CITATIONS_TOP_K` | `8` | Сколько цитат-чанков подаётся на каждую сущность (один чанк на документ, дедуплицировано по doc_id) |
| `WIKI_DOCS_BASE_URL` | `http://localhost:8000/api/v1` | База для ссылок на скачивание исходных документов в секции «Источники» (эндпоинт `GET {base}/documents/{doc_id}`) |
| `WIKI_MEDIAWIKI_API_URL` | `` (пусто) | URL MediaWiki API; пусто → `{WIKIBASE_BASE_URL}/w/api.php` |
| `WIKI_SITE_GLOBAL_ID` | `kbwiki` | Site ID для sitelink-записи (см. gotcha ниже) |

Параметры `WIKIBASE_BOT_USER` / `WIKIBASE_BOT_PASSWORD` общие с Wikibase-populator
— wiki-editor логинится через те же учётные данные.

## 5. Operator gotchas (важно)

### 5.1 `WIKI_SITE_GLOBAL_ID` должен совпадать с реальным site id

Дефолтный контейнер `wikibase-docker` регистрирует вики под id `my_wiki`, а дефолт
конфига — `kbwiki`. Несовпадение не ломает запись страницы, но sitelink (Item ↔
страница) молча не создаётся.

Узнать реальный id:

```bash
curl -s 'http://localhost:8181/w/api.php?action=query&meta=siteinfo&siprop=general&format=json' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['query']['general']['wikiid'])"
```

Если вывод `my_wiki` — задайте в `.env`:

```bash
WIKI_SITE_GLOBAL_ID=my_wiki
```

### 5.2 `WIKIBASE_BOT_PASSWORD` должен быть ≥ 8 символов

MediaWiki отказывает при создании бот-аккаунта с паролем короче 8 символов.
Дефолт кода (`botpass`, 7 символов) слишком короткий. `scripts/make_env.py`
теперь выдаёт ERROR при валидации, если пароль короче 8 символов. Задайте более
длинный пароль в `.env` до первого запуска `setup_wikibase`.

### 5.3 Без `wikibase_qid` sitelink не создаётся, но статья пишется

Если Wikibase-push выключен (`WIKIBASE_ENABLED=false`) или у сущности нет
`wikibase_qid`, wiki-editor всё равно создаёт или обновляет страницу по имени
сущности — sitelink просто пропускается. Редактор работает независимо от
структурного пуша.

### 5.4 Ссылки на скачивание исходников требуют аутентификации

Эндпоинт `GET {WIKI_DOCS_BASE_URL}/documents/{doc_id}` из секции «Источники»
закрыт `require_api_key` — открытие ссылки напрямую из браузера вернёт 401,
поэтому для читателей проксируйте его через аутентифицированный шлюз.

## 6. Проверка

```bash
# Список всех созданных статей:
open http://localhost:8181/wiki/Special:AllPages

# Сырая разметка конкретной статьи (проверить бот-секцию):
curl -s 'http://localhost:8181/wiki/Special:Export/ИмяСущности?action=raw'
# или через API:
curl -s 'http://localhost:8181/w/api.php?action=query&titles=ИмяСущности&prop=revisions&rvprop=content&format=json' \
  | python3 -c "import sys,json; pages=json.load(sys.stdin)['query']['pages']; [print(list(p['revisions'])[0]['*']) for p in pages.values()]"
```

В бот-секции (между `<!-- KB-BOT:START -->` и `<!-- KB-BOT:END -->`):
- Человекочитаемая проза о сущности.
- `[[Вики-ссылки]]` на статьи соседних сущностей из графа.
- Цитаты вида `[doc_id]` к исходным документам.
- Секция `== Источники ==` — ссылки на скачивание исходников `[{WIKI_DOCS_BASE_URL}/documents/{doc_id} {doc_id}]`. Эндпоинт под `require_api_key` — для читателей из браузера проксируйте через аутентифицированный шлюз. Секция опускается, если у сущности нет источников.

## 7. Troubleshooting

| Симптом | Вероятная причина | Действие |
|---|---|---|
| Воркер не поднимает `kb-wiki` очередь | `WIKI_ENABLED=false` или перезапуск ещё не произошёл | `export WIKI_ENABLED=true` → рестарт воркера |
| Статьи не появляются после sweep | Wikibase/MediaWiki не поднят | `docker compose -p kb-llamaindex ps wikibase`; проверить `curl http://localhost:8181/wiki/Special:Version` |
| Sitelink не создаётся, страница есть | `WIKI_SITE_GLOBAL_ID` не совпадает с реальным id или `wikibase_qid` отсутствует | см. gotcha 5.1 и 5.3 |
| `setup_wikibase` падает: «password too short» | `WIKIBASE_BOT_PASSWORD` < 8 символов | Задать пароль ≥ 8 символов в `.env`; `uv run python -m scripts.make_env` выдаст ERROR при проверке |
| Hash-skip не срабатывает — статьи перезаписываются каждый раз | Граф менялся между прогонами | Нормальное поведение; если данные не менялись — проверить, что `write_entity_article` корректно вычисляет хэш |
| `write_entity_article` завершается с `LoginFailed` | Бот-аккаунт не создан или пароль изменился | Перезапустить `uv run python -m scripts.setup_wikibase` |
| Sweep не стартует по расписанию | Temporal Schedule не настроен или воркер не поллит `kb-wiki` | Проверить `uv run python -m scripts.setup_wiki_schedule`; рестартовать воркер |

## 8. Связанные runbook'и

- [`wikibase.md`](wikibase.md) — структурный пуш Item/Property; именно он создаёт `wikibase_qid`.
- [`docs/QUEUES.md`](../QUEUES.md) — описание всех Temporal-очередей, в том числе `kb-wiki`.
