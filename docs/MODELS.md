# Модели

Проект гоняет LLM- и embedding-нагрузки через **прокси LiteLLM**
(сервис `docker compose` `litellm`, default
`LITELLM_BASE_URL=http://localhost:4000`).  Поставляемый
`docker/litellm_config.yaml` проксирует upstream **OpenAI**
(`gpt-4o-mini` / `gpt-4o` + `text-embedding-3-small`) и держит
закомментированный рецепт для возврата small-tier обратно на локальную
модель **Ollama** на `host.docker.internal:11434`.  Все ссылки на
модели конфигурируются через env-переменные `LITELLM_*` и
`model_list` в `docker/litellm_config.yaml`.

> Два слоя, две задачи — держите их раздельно:
> 1. **КАКАЯ модель** гоняет каждую роль → двухтировая конфигурация ниже
>    (`LITELLM_MODEL_SMALL/LARGE` + `LITELLM_ROLE_TIERS`).
> 2. **СКОЛЬКО конкурентных вызовов** может делать каждая роль/tier → 
>    пер-процессный **LLMPool** (`LLM_POOL_*`, см.
>    [LLMPool — гейтинг конкурентности](#llmpool--гейтинг-конкурентности)).
>    Пул теперь единственный владелец LLM-конкурентности; лимиты очередей
>    Temporal — только изоляция.

## Два физических tier'а — вы управляете ровно двумя именами моделей

Каждая логическая нагрузка («роль») маппится на один из **двух физических
tier'ов** моделей.  Операторы всегда задают только два имени моделей (`src/config.py`
`LiteLLMSettings`):

| Env var | Поле | Tier | Default | Характер |
|---|---|---|---|---|
| `LITELLM_MODEL_SMALL` | `model_small` | `small` | `gpt-4o-mini` | Высокообъёмные роли — extraction, judge, search, route, plan, retrieve.  Меняйте на локальную модель Ollama для on-prem. |
| `LITELLM_MODEL_LARGE` | `model_large` | `large` | `gpt-4o-mini` | Только финальный, обращённый к пользователю синтез ответа. |

> **Замечание про default:** оба tier'а по умолчанию `gpt-4o-mini` в
> `src/config.py` (поставляемый конфиг LiteLLM — OpenAI-first).  Чтобы
> гонять small-tier на дешёвой локальной модели, задайте
> `LITELLM_MODEL_SMALL=gemma4:e4b` (или вариант qwen3) **и**
> зарегистрируйте её в `docker/litellm_config.yaml` — см.
> [Поменять small-tier](#путь-эскалации).

| Прочее | Env var | Default | Зачем |
|---|---|---|---|
| Embedding | `LITELLM_EMBEDDING_MODEL` | `nomic-embed-text` (768-dim) | Default OpenAI в конфиге LiteLLM — `text-embedding-3-small` (1536-dim).  `LITELLM_EMBEDDING_DIM` ОБЯЗАН совпадать с `MILVUS_DIM`. |
| Reranker | `HF_RERANK_MODEL` | `BAAI/bge-reranker-v2-m3` | BGE cross-encoder; унифицированный graph+vector rerank перед синтезом (грузится из HF, не из LiteLLM). |

`LITELLM_EMBEDDING_MODEL` / `LITELLM_EMBEDDING_DIM` — НЕ роли —
embedding'и идут прямо через эндпоинт `embeddings` LiteLLM и
не гейтятся LLMPool.  Default конфига (`nomic-embed-text`,
768) и default поставляемого конфига LiteLLM (`text-embedding-3-small`,
1536) различаются — выберите один последовательно и задайте `MILVUS_DIM` под него.

## Роли и карта роль → tier

Существует **семь** логических ролей
(`src/config.py:LLMRole`).  Каждая декларативно маппится на tier в
`_DEFAULT_ROLE_TIERS`.  Резолвинг — это
`роль → tier → одна из двух физических моделей`
(`LiteLLMSettings.tier_for` → `model_for`).  Default: **всё
`small`, кроме `synthesis`, которая `large`.**

| Роль | Default tier | Получается через (call-site) | Используется для |
|---|---|---|---|
| `extraction` | small | `get_llm_pool().get("extraction")` — `extract_kg.py`, `parse_and_chunk.py`, CLI `ingestion/run.py` | извлечение KG-триплетов + пер-чанковый перевод |
| `judge` | small | `merge_and_resolve.py` | разрешение пограничных пар ER + summary межчанкового слияния |
| `search` | small | `di/providers.py`, `mcp/tools_server.py`, `_search_deps.py` | рассуждения на стороне поиска (graph synonym retrieval, MCP-инструменты) |
| `route` | small | (зарегистрирована; роутинг / контекстуализация запроса) | роутинг запроса |
| `plan` | small | `_search_plan_deps.py` | многошаговое планирование подвопросов |
| `retrieve` | small | `_search_deps.py` | LLM-вызовы на стороне ретрива |
| `synthesis` | **large** | `_search_deps.py`, `wiki/wiki_sweep.py` | финальный обращённый к пользователю синтез ответа + отчёты по сообществам + wiki-статьи |

> Старые роли `distill` / `coverage` ушли из `LLMRole`;
> проверка покрытия и работа с наблюдениями теперь гоняются под существующими ролями.

Call-site'ы больше не строят сырые LLM напрямую — они идут через
`get_llm_pool().get(role)` (`src/retrieval/llm_pool.py`), который возвращает
один общий, гейтящийся по конкурентности `BoundedLLM` на роль.  Пул
внутри зовёт `build_llm(role)` (`src/retrieval/llm.py`), который
резолвит через карту tier'ов.  `build_llm()` без роли использует
small-tier (или deprecated-алиас `LITELLM_LLM_MODEL`, если он
явно задан — см. ниже); он выживает только для diag-скриптов.

## LLMPool — гейтинг конкурентности

`src/retrieval/llm_pool.py` определяет `LLMPool`, один **на процесс**,
доступный через `get_llm_pool()` (ленивый process-singleton).  Это
текущий source of truth для LLM-конкурентности.  Два иерархических уровня
гейтов, захватываемых **сначала-полоса, потом tier-global** (согласованный
порядок ⇒ нет дедлоков):

1. **Потолок на tier** — глобальный семафор на физический tier:
   * `small` = реальная ёмкость GPU/backend по конкурентным запросам
     (`LLM_POOL_TIER_SMALL_TOTAL`, default **25**).
   * `large` = API-бюджет (`LLM_POOL_TIER_LARGE_TOTAL`, default **8**).
2. **Полоса на роль** — потолок на роль
   (`LLM_POOL_LANE_CAPS`, JSON-карта).  Полосы намеренно
   **подписаны с избытком** относительно tier-total (сумма потолков small-tier >
   `tier_small_total`), так что одна нагрузка может забить GPU, тогда как ни одна
   роль не может монополизировать его сверх собственного потолка.

Дефолтные потолки полос (`LLMPoolSettings.lane_caps`):

| Роль | Потолок полосы | Tier |
|---|---|---|
| `extraction` | 18 | small |
| `judge` | 14 | small |
| `search` | 14 | small |
| `plan` | 4 | small |
| `route` | 2 | small |
| `retrieve` | 4 | small |
| `synthesis` | 8 | large |

`LLM_POOL_JUDGE_FLOOR` (default **7**) — зарезервированный пол для
полосы merge/judge под потоком extraction; инвариант сайзинга
`extraction_ceiling ≤ tier_small_total − judge_floor` (18 ≤ 25 − 7)
гарантирует, что merge никогда не голодает.

Как это связано с Temporal: лимит очереди Temporal (например,
`TEMPORAL_LLM_ACTIVITY_CONCURRENCY=18`) управляет тем, сколько активностей
*планируется* конкурентно, но **пул** решает, сколько из них реально
делают LLM-вызов одновременно.  Так что 18 активностей `extract_kg`
могут быть в полёте, тогда как пул допускает столько конкурентных
GPU-вызовов, сколько позволяют полоса extraction + потолок small-tier.
Temporal-лимиты намеренно выставлены **≥** соответствующего потолка
полосы пула, чтобы пул связывал первым (см. комментарии
`TemporalSettings`).  Это только пер-процессно — настоящий межпроцессный
потолок GPU принадлежит прокси LiteLLM и вне области рассмотрения.

> **Deprecated:** `AGENT_LLM_MAX_CONCURRENT`
> (`AgentSettings.llm_max_concurrent`) был старым единственным регулятором
> конкурентности на стороне поиска.  Он **мёртв** — ни один production-путь
> его не читает; LLMPool заменил его.  Поле сохранено только чтобы env,
> которые всё ещё его задают, не падали.  Отдельностоящий путь
> `BoundedLLM(max_concurrent=...)` так же выживает только для diag-скриптов.

### Тюнинг конкурентности

```env
# Raise/lower the real backend capacity per tier:
LLM_POOL_TIER_SMALL_TOTAL=25     # GPU concurrent-request capacity
LLM_POOL_TIER_LARGE_TOTAL=8      # API budget

# Override the whole per-role lane map (JSON):
LLM_POOL_LANE_CAPS={"extraction":12,"judge":10,"search":10,"plan":4,"route":2,"retrieve":4,"synthesis":8}

LLM_POOL_JUDGE_FLOOR=7           # reserved floor so merge never starves
```

Смотрите живую занятость через `get_llm_pool().stats()` (per-lane +
per-tier `cap` / `in_use` / `available`).

## Эскалация одной роли

Чтобы перевести одну роль на большую модель, не трогая остальные, задайте
`LITELLM_ROLE_TIERS` как JSON-объект.  Он **мёржится** поверх
дефолтов, так что вы называете только ту роль (роли), которую хотите изменить:

```env
# Run planning on the large model too; everything else stays small.
LITELLM_ROLE_TIERS={"plan":"large"}
```

Мёрж сохраняет `synthesis: large` и каждый другой default — вам
никогда не нужно переобъявлять полную карту.  Неизвестные роли откатываются на
`small`.

### Deprecated-алиас `LITELLM_LLM_MODEL`

`LITELLM_LLM_MODEL` (`LiteLLMSettings.llm_model`) сохранён только как
deprecated-алиас, чтобы легаси `build_llm()` (без роли) всё ещё резолвил.
Оставьте его пустым; он дефолтится на `""`, в каковом случае путь без роли
использует `LITELLM_MODEL_SMALL` (через `effective_base`).  Если задан явно,
он выигрывает только для пути без роли — пер-ролевой резолвинг всегда использует
карту tier'ов.  Удалите его, как только все вызыватели передают роль.

> Историческая заметка: раньше были пер-ролевые env-переменные *модели*
> (`LITELLM_EXTRACTION_MODEL` / `LITELLM_JUDGE_MODEL` /
> `LITELLM_SEARCH_MODEL`).  Их **больше нет** — выбор роли теперь
> tier-based (`LITELLM_ROLE_TIERS`), и есть только два имени
> моделей.  Чтобы посадить одну роль на другую модель, поднимите её tier и
> задайте соответствующий `LITELLM_MODEL_*`.

### Snapshot мультимодели в момент /ingest

Пер-ролевой резолвинг модели **снимается в момент `POST /ingest`**
и протягивается через workflow (`IngestParams` →
`FinalizeIn` → `ingest_metrics`), так что каждая строка `ingest_metrics`
записывает точную модель, которая гоняла эту активность — даже если `LITELLM_MODEL_*`
меняется между сабмитами.  Активности резолвят через LLMPool во
время выполнения, но *записанная* модель приходит из snapshot'а на момент
сабмита.  Операционные детали (swap → restart → verify) — в
[`docs/runbook/multimodel.md`](runbook/multimodel.md).

### Smoke-верификация

```bash
# Submit batch A with default models
curl -F file=@doc.txt -H "X-Version-Tag: baseline" \
     -H "X-API-Key: $API_KEY" localhost:8000/api/v1/ingest

# Swap the small tier, restart worker + API
export LITELLM_MODEL_SMALL=qwen2.5:14b
# (restart processes)

# Submit batch B
curl -F file=@doc.txt -H "X-Version-Tag: small-14b" ... /api/v1/ingest

# Verify in Postgres
psql -c "SELECT activity_name, model, version_tag FROM ingest_metrics
         WHERE version_tag IN ('baseline','small-14b')
         ORDER BY activity_name, version_tag"
```

Все активности на стороне ingest гоняются на small-tier, так что смена
`LITELLM_MODEL_SMALL` сдвигает `model` каждой ingest-строки.  Пер-строчный
`ingest_metrics.model` по-прежнему отражает модель, **реально использованную**
для каждой активности (см. `docs/runbook/analytics.md`).

`MILVUS_DIM` ОБЯЗАН равняться выходной размерности embedding-модели (768 для
`nomic-embed-text`, 1024 для `bge-m3`).  Смена embed-модели
требует дропа и пересоздания коллекции Milvus.

## Подтягивание моделей в Ollama

```bash
ollama pull gemma4:e4b
ollama pull nomic-embed-text
# optional baseline for R9 comparative eval:
ollama pull llama3.1:8b
```

Прокси LiteLLM достаёт Ollama на хосте по
`host.docker.internal:11434`.  Это прошито в
`docker/litellm_config.yaml`.

## Оффлайн / air-gapped модели

LLM и embedding'и идут через **прокси LiteLLM**, так что air-gapped-хосту
нужен только достижимый прокси.  Но **две** модели подтягиваются напрямую
из **HuggingFace Hub** при первом использовании и должны быть
пред-кешированы для оффлайн-развёртываний:

| Модель | Конфиг | Default | Используется |
|---|---|---|---|
| GLiNER span-NER | `INGESTION_GLINER_MODEL` (`settings.ingestion.gliner_model`) | `urchade/gliner_multi-v2.1` | OPT-IN режимы экстрактора `gliner` / `gliner+llm` |
| BGE cross-encoder reranker | `HF_RERANK_MODEL` (`settings.hf.rerank_model`) | `BAAI/bge-reranker-v2-m3` | унифицированный graph+vector rerank перед синтезом |

### 1. Пред-загрузка онлайн (наполнение кеша)

На машине, которая достаёт Hub:

```bash
python -m scripts.download_models --cache-dir /data/hf
# or just one:  --models gliner   |   --models reranker
```

Это форсит процесс загрузки онлайн (даже если `HF_OFFLINE` /
`HF_HUB_OFFLINE` задан в env), наводит HF-кеш-переменные на
разрешённую директорию (CLI `--cache-dir` > `HF_CACHE_DIR` > HF default), тянет
обе модели и печатает кеш-директорию + следующие шаги.  Он выходит с ненулевым
кодом, если любая загрузка падает.

### 2. Скопируйте кеш на air-gapped-хост, затем гоняйте оффлайн

```env
HF_OFFLINE=true
HF_CACHE_DIR=/data/hf
HF_RERANK_MODEL=BAAI/bge-reranker-v2-m3   # only if non-default
```

`HF_OFFLINE=true` заставляет `src/retrieval/hf_offline.py:configure_hf()`
выставить `HF_HUB_OFFLINE=1` + `TRANSFORMERS_OFFLINE=1`, а `HF_CACHE_DIR`
наводит `HF_HOME` / `SENTENCE_TRANSFORMERS_HOME` / `TRANSFORMERS_CACHE`
на скопированный кеш.  `configure_hf()` гоняется в каждой точке загрузки HF-модели
(GLiNER `__init__` + `gliner_ner_callable`, и `build_reranker`)
ПЕРЕД тяжёлыми импортами библиотек, так что загрузчики читают только кеш.

Это СОБСТВЕННЫЕ env-переменные проекта (читаются через явные алиасы, чтобы они
никогда не затирали собственные `HF_HOME` / `HF_HUB_OFFLINE` HuggingFace — приложение
выводит и задаёт их само).  Заданный оператором `HF_HOME` остаётся
нетронутым, так что ручные override'ы выигрывают.

### Альтернатива: плоский `--local-dir` (без blob'ов/симлинков)

HF-кеш хранит файлы content-addressed: `models--org--name/blobs/<sha>`
(реальные файлы) + `snapshots/<rev>/file → ../../blobs/<sha>` (симлинки). Эта
раскладка стандартна (независима от версии `huggingface-hub` / `hf-xet`),
но **симлинки ломаются**, когда кеш копируется на air-gapped-хост
(`scp` / `docker COPY` / `tar` без dereference). Чтобы этого избежать, загружайте
в **плоскую** директорию реальных файлов вместо этого:

```bash
python -m scripts.download_models --local-dir /data/models
# → /data/models/gliner_multi-v2.1/  and  /data/models/bge-reranker-v2-m3/
```

`--local-dir` использует `huggingface_hub.snapshot_download(local_dir=...)` (hub
≥0.23 → реальные файлы, без blob'ов/симлинков; остаётся только маленькая
метаданных-поддиректория `.cache/huggingface/`, её безопасно удалить). Скопируйте
папку куда угодно, затем наведите конфиги моделей на локальные пути и грузите оффлайн:

```env
HF_OFFLINE=true
HF_RERANK_MODEL=/data/models/bge-reranker-v2-m3
INGESTION_GLINER_MODEL=/data/models/gliner_multi-v2.1
```

`SentenceTransformerRerank` (reranker) и `GLiNER.from_pretrained` оба
принимают путь к локальной директории; при `HF_OFFLINE=true` `configure_hf()` задаёт
`HF_HUB_OFFLINE=1`, так что загрузчики никогда не трогают сеть.

## Флаги возможностей

`src/retrieval/llm.py:build_llm` сверяется с env-переменной
`LITELLM_FUNCTION_CALLING` (default `true`).  Когда `true`,
клиенту `OpenAILike` говорят использовать function calls — требуется для:

* `LLMJudge.via_structured` — структурированный вывод через
  `llm.astructured_predict(JudgeOutput, ...)`.
* `SchemaLLMPathExtractor` (graph schema mode) — тот же механизм
  для извлечения триплетов.
* ReAct-агент (R7) — function calls = вызовы инструментов.

Задайте `LITELLM_FUNCTION_CALLING=false`, чтобы откатиться на парсинг
JSON на основе промпта.  Необходимо на меньших моделях, которые не надёжно
эмитят tool calls (llama3.1:8b, qwen2.5:3b).

## Путь эскалации

Если small-tier окажется недостаточным на корпусе проекта
(сигналы: надёжность tool-call < 80%, регулярные промахи маркеров
`[NEED]`/`[UNCERTAIN]`, растущий уровень галлюцинаций в eval R9) — есть два
рычага, в порядке стоимости:

1. **Эскалировать одну роль** на large-tier через `LITELLM_ROLE_TIERS`
   (например, толкнуть `plan` или `judge` на `large`) — хирургически, без
   инфра-изменений сверх env.
2. **Поменять саму small-модель** на больший локальный вариант:

| Модель | Оценка RAM | Когда рассматривать |
|---|---|---|
| `gemma4:e4b` | 4-6 GB | дефолтный small-tier |
| `qwen3:8b` | 6-8 GB | надёжный tool calling при скромной цене |
| `qwen3:14b` | 12-16 GB | лучший прирост цена/качество |
| `qwen3:32b` | 24-32 GB | устойчивые проблемы с точностью tool-call |

Чтобы поменять small-tier:

1. `ollama pull qwen3:14b`
2. Отредактируйте `.env`: `LITELLM_MODEL_SMALL=qwen3:14b`.
3. Добавьте запись `model_list` в `docker/litellm_config.yaml`
   (зеркальте запись `gemma4:e4b`, поменяйте путь).
4. `docker compose restart litellm`.

Локальный tier остаётся on-prem намеренно — переход на внешние API
(OpenAI/Anthropic) — это отдельное операционное решение, затрагивающее
компромиссы стоимость / приватность / vendor lock-in.  `large`-tier дефолтится на
хостируемую модель (`gpt-4o-mini`) именно потому, что финальный синтез — это
единственная низкообъёмная, критичная по качеству роль, где этот компромисс того стоит.

## Базлайн для сравнительного eval (R9)

`llama3.1:8b` держится зарегистрированной в LiteLLM как базлайн.  
Eval-скрипт (`tests/eval/answer_quality.py` начиная с R9)
промптит и qwen3:8b, и llama3.1:8b на одном и том же золотом наборе Q&A
и репортит дельты по моделям.  Так регрессии, вызванные
изменениями кода, отличаются от регрессий, вызванных
изменениями модели.

## Бюджет контекста перевода

`DocumentTranslateTransform` (в `src/ingestion/translate_transform.py`)
отправляет каждый документ — или его оконный срез — в LLM за
один вызов.  Кап размера окна —
`INGESTION_TRANSLATION_DOC_THRESHOLD_CHARS`.  Каждому вызову нужны:

* overhead промпта (~500 токенов на translate-промпт),
* окно документа (X токенов),
* бюджет вывода (~1.3 × X токенов на расширение EN→RU).

Итого ≈ 500 + 2.3 × X должно оставаться внутри контекстного окна модели.

| Модель | Контекст (токены) | Безопасный порог (символы) |
|---|---|---|
| **Ollama qwen3:8b / 14b / 32b** (native) | 32k | **30_000** (default) |
| Ollama qwen3 с расширением YaRN | 131k | 200_000 |
| **gpt-4o-mini / gpt-4o** | 128k | 200_000 – 400_000 |
| Anthropic claude-3.5-sonnet | 200k | 400_000 |

Поднимите порог для меньшего числа более крупных окон → лучше межпредложенческий
контекст, меньше LLM-вызовов.  Опустите его при переходе на модель с меньшим
контекстом.

Отношение символов к токенам — ~4 для английского, ~3 для русского, ~2 для
китайского; дефолты выше предполагают англо-тяжёлый корпус.  Корректируйте
вниз на 30%, если корпус русско-тяжёлый.

Когда документ превышает порог, переводчик дробит по
границам абзацев (затем по границам предложений для огромных
абзацев).  Каждое окно идёт в одном LLM-вызове; выводы
конкатенируются через `\n\n`.

## Переключение на другое семейство LLM

Тот же клиент `OpenAILike` совместим с:

* **OpenAI**:
  ```env
  LITELLM_BASE_URL=https://api.openai.com/v1
  LITELLM_API_KEY=sk-real-openai-key
  LITELLM_MODEL_SMALL=gpt-4o-mini
  LITELLM_MODEL_LARGE=gpt-4o
  LITELLM_EMBEDDING_MODEL=text-embedding-3-small
  LITELLM_EMBEDDING_DIM=1536        # MUST match MILVUS_DIM
  ```
  Пропустите контейнер LiteLLM в `docker compose`.

* **Anthropic**: аналогично, наводя на Anthropic-совместимый
  прокси (Anthropic не выставляет OpenAI-совместимый
  эндпоинт нативно — используйте прокси LiteLLM с подходящим
  upstream-конфигом).

## Быстрая smoke-проверка

После любой смены модели запустите:

```bash
uv run python -c "
import asyncio
from src.retrieval.llm import build_llm
from src.ingestion.embeddings import build_embedding_model

async def main():
    llm = build_llm()
    emb = build_embedding_model()
    print('LLM:', (await llm.acomplete('Reply with one word: OK')).text.strip())
    print('embed dim:', len(await emb.aget_text_embedding('test')))
asyncio.run(main())
"
```

Затем:

```bash
uv run python -m scripts.diag_kg
```

чтобы проверить, что KG-извлечение (Simple mode) всё ещё производит сущности +
отношения на встроенном тестовом абзаце.
