# Bruno API-коллекция — kb-llamaindex

Коллекция [Bruno](https://www.usebruno.com/) для HTTP API
kb-llamaindex. Хранится как обычные текстовые `.bru`-файлы, чтобы
чисто диффилось в git.

## Открыть в Bruno

1. Установить Bruno: `brew install bruno` или скачать с
   <https://www.usebruno.com/downloads>.
2. **File → Open Collection** → выбрать `docs/bruno/`.
3. Селектор окружения справа сверху → выбрать `local` (по умолчанию
   указывает на `http://localhost:8000`).
4. Заменить секрет `apiKey` в окружении на один из ключей,
   сконфигурированных через env-переменную `API_KEYS` на сервере.

## Структура

```
docs/bruno/
├── bruno.json                       # collection metadata
├── environments/
│   ├── local.bru                    # baseUrl + apiKey for dev
│   └── docker.bru                   # same default but separate slot
│                                    # for any container-side overrides
├── Health/
│   └── Health Check.bru             # GET /health (no auth)
├── Ingestion/
│   ├── Upload Document.bru          # POST /api/v1/ingest (multipart)
│   └── Get Job Status.bru           # GET /api/v1/ingest/{job_id}
├── Search/
│   ├── Local Search.bru             # POST /api/v1/search/local
│   ├── Global Search.bru            # POST /api/v1/search/global
│   ├── Drift Search.bru             # POST /api/v1/search/drift
│   └── Auto Search.bru              # POST /api/v1/search/auto
├── Documents/
│   └── Download Source.bru          # GET /api/v1/documents/{doc_id}
├── Admin/
│   ├── Rebuild Communities.bru      # POST /api/v1/admin/communities/rebuild
│   └── Wiki Rebuild.bru             # POST /admin/wiki/rebuild (NO /api/v1, no auth)
└── README.md
```

> Легаси-эндпоинты `/api/v1/search`, `/agent`, `/selfrag`, `/legacy/agent`
> были удалены в R7b cutover. Единственная поверхность поиска
> теперь — `/api/v1/search/{local,global,drift,auto}`; памятка по
> использованию + тюнингу: `docs/runbook/search-usage.md`.

## Переменные окружения

| Переменная | По умолчанию            | Где задаётся                                    |
|----------|-------------------------|-------------------------------------------------|
| baseUrl  | `http://localhost:8000` | файл окружения                                  |
| apiKey   | `sk-litellm-stub`       | файл окружения (помечена как секрет — git-ignored при коммите) |

Дефолтный `apiKey` совпадает с `ApiSettings.api_key` из `src/config.py`
для раннего поднятия. В продакшене замените его на одно из значений,
перечисленных в `API_KEYS`.

## Аутентификация

Большинство эндпоинтов требуют заголовок

```
X-API-Key: {{apiKey}}
```

Bruno подставляет его автоматически, поскольку запросы берут значение
из `vars` активного окружения.

Исключения (в коде нет `X-API-Key`):
- `GET /health` — публичная liveness-проба.
- `POST /admin/wiki/rebuild` — не имеет зависимости `require_api_key`
  (и подключён без префикса `/api/v1`). Считайте его
  внутренним/операторским эндпоинтом.

## Типичный сценарий

1. **Health** → убедиться, что API поднят.
2. **Upload Document** → запостить небольшой файл, скопировать `job_id`
   из ответа.
3. **Get Job Status** → выставить request-переменную `jobId` в значение
   выше; поллить, пока статус не станет `completed` (или `vector_only`).
4. **Search → Local** → запрос по корпусу (режим по умолчанию).
   Используйте **Auto**, чтобы режим выбрал роутер.
5. **Documents → Download Source** → выставить request-переменную `docId`
   в `doc_id` из `sources[]` / `documents[]` ответа поиска и
   скачать оригинальный файл (Bruno: *Response → Save Response*).
6. *(опционально, для Global/Drift)* **Admin → Rebuild Communities** один
   раз, затем использовать **Global** / **Drift** для вопросов уровня
   корпуса.
7. *(опционально)* **Admin → Wiki Rebuild** для (пере)генерации
   MediaWiki-статей на сущность (`?all=true` пересобирает всё; нужен
   `WIKI_ENABLED`).

## Заметки

- `Upload Document` использует хелпер Bruno `@file(...)`. Положите
  пример-файл по пути `docs/bruno/samples/sample.txt` (или поменяйте
  путь прямо в запросе). Подходит всё, что принимает
  `SimpleDirectoryReader` из LlamaIndex — PDF, DOCX, PPTX, TXT, MD, EML.
- Синтезатор всегда форсит ответ на русском (корпус нормализован к
  русскому); вопросы на английском допустимы, но ответ будет на русском.
- **Global** и **Drift** делают map-reduce по community-summaries —
  сначала запустите **Admin → Rebuild Communities** (нужны Neo4j + GDS;
  сборка офлайн и идёт минуты).
- Все четыре режима поиска используют общую форму `SearchRequest`;
  потребляются только `query`, `top_k` и `history` (многоходовость —
  `[{role, content}]`) — режим выбирается эндпоинтом, а не полем тела.
  Остальные поля (`mode`, `department`, фильтры, …) принимаются, но
  игнорируются.
- `SearchResponse` несёт `documents[]` (`{doc_id, url}`) рядом с
  `sources[]`; каждый `url` — это относительная ссылка на скачивание
  `/api/v1/documents/{doc_id}`, которую обслуживает
  **Documents → Download Source**.

## Обновление коллекции

`.bru` построчный и дружелюбен к git. Когда меняете эндпоинт:

1. Поправьте маршрут в `src/api/routes/...`.
2. Обновите соответствующий `.bru` (тело запроса, секция docs).
3. Прогоните `uv run pytest tests/test_api -v` для проверки контракта.
4. Закоммитьте `src/` и `docs/bruno/` вместе.
