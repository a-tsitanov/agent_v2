# kb-llamaindex — Conference Deck Design

**Date:** 2026-05-14
**Project:** kb-llamaindex (production-bound RAG service)
**Author:** a.tsitanov

## 1. Цель

Подготовить **две версии** презентации по проекту `kb-llamaindex`,
которые делятся общим каркасом (~22 слайда), но различаются в 6
слайдах под две аудитории:

- **Версия A — Tech / ML-инженерная конференция**
  (DataFest / Highload / PyCon / ML-meetup).
  Глубокий технический доклад: архитектура, KG extraction, ER,
  ReAct / Self-RAG, реальные числа.
- **Версия D — Внутренняя защита проекта.**
  Те же подсистемы, но фокус на статус R1–R10, eval-как-договор,
  что НЕ в scope, бизнес-обоснование выбора стека.

Обе версии — **30–40 минут, ~22–23 слайда, RU, Marp Markdown**.

Нарратив: **N1 «Три endpoint-а — три ответа на quality vs latency
vs trust»** как центральная история, с одним сильным «war-story»
слайдом (элемент N3) и блоком из 6 слайдов про KG + ER как
«фундамент, на котором работают три режима» (элемент N2).

## 2. Core-темы доклада

Выбранные пользователем (вопрос 5 из брейншторма):

1. **Knowledge Graph extraction** — переход на LightRAG-style,
   `SimpleLLMPathExtractor` fallback, русский B2B-промпт,
   cross-chunk merge.
2. **Entity Resolution** — 12-step pipeline (embedding-blocked +
   LLM-confirmed), 12 identifier-типов skip, cross-script always
   LLM, conservative default (DIFFERENT on timeout).
3. **Три search-режима** — `/search`, `/agent` (ReAct, 8 tools),
   `/selfrag` (reflective synth с маркерами
   `[NEED]`/`[SUPPORTED]`/`[UNCERTAIN]`).

Не выбраны (упоминаются в фоне, без своих слайдов): MinIO upload,
identifiers / regex hardening, qwen3 migration, открытые race
conditions, RAGAS-eval.

## 3. Общая структура (shared spine, 22 слайда)

| # | Слайд | Содержание |
|---|---|---|
| 1 | **Cover** | Название, конференция/команда, дата, спикер |
| 2 | **Hook: один вопрос — три ответа** | Teaser — какой правильный и почему держим все три. **Без live-demo** (визуальный мокап «one query → three answers side-by-side» в самом слайде) |
| 3 | **Контекст: enterprise corpus** | Multilingual, договоры, медзаписи, e-mail треды, идентификаторы (ИНН/ОГРН/телефоны) |
| 4 | **Почему generic RAG ломается** | 4 pain-points: язык, KG extraction на small-LLM, ER, доверие к ответу |
| 5 | **Архитектура (one-pager)** | ASCII-схема из `docs/ARCHITECTURE.md` (API + worker + 4 store-а + LiteLLM gateway) |
| 6 | **Storage map** | Таблица Milvus / Neo4j / Postgres / RabbitMQ / FS / LiteLLM с ролью |
| 7 | **Ingestion pipeline** | 5 шагов: parse → chunk → identifiers (canon) → translate-to-RU → vector + KG (best-effort) |
| 8 | **KG extraction: war story** | Почему `SchemaLLMPathExtractor` сломался → `SimpleLLM` → LightRAG-style |
| 9 | **LightRAG-style extraction** | 1 LLM-call/chunk, tuple-format, RU B2B example вместо стокового Alice/Bob/Philz |
| 10 | **Cross-chunk merge** | Concat (<8 mentions, <12k chars) или summarize-LLM; relation pair-key |
| 11 | **ER: проблема** | «BCC ≡ Базальноклеточный Рак», cross-lang, abbrev, word-order, initialisms, cross-doc |
| 12 | **ER: 12-step pipeline** | embedding-block → deterministic prepass → cosine HIGH auto-merge → LLM-judge → union-find → hyper-hub clamp |
| 13 | **ER: трейдоффы** | Conservative default (DIFFERENT on timeout), 12 identifier-типов skip, cross-script → всегда LLM |
| 14 | **Три endpoint-а: обзор** | Таблица из `docs/QUERY.md`: outer loop / LLM calls / latency / когда что |
| 15 | **`/search` — baseline** | Top-k dense + один synth call. 5–20 s. Когда latency важнее качества |
| 16 | **`/agent` — ReAct** | 8 tools, anti-loop guard (3 идентичных вызова → exit), max_iterations=8 |
| 17 | **`/selfrag` — reflective** | Маркеры `[NEED]`/`[SUPPORTED]`/`[UNCERTAIN]`, re-retrieve loop, max_refinements=3 |
| 18 | **Russian-output guarantee** | 3 точки enforcement: ingest-translate, system-prompt hard-code, query-wrapper |
| 19 | **Cost / latency реально** | Re-ingest 1 MB / 514 chunks → ~1200 LLM calls / 15–25 min; per-endpoint calls |
| 20 | **Observability + Eval gate** | `trace_request` → loguru summary; 287 tests; identifier_recall (7 cases) + answer_quality |
| 21 | **Lessons learned** | 4 неочевидных вывода (содержание различается A vs D) |
| 22 | **Status / Roadmap / Q&A** | R1–R10 трек (содержание различается A vs D) |

**Логика:** проблема → архитектура → две сильные подсистемы
(KG / ER) → три режима как «потребители» → метрики/наблюдаемость
→ выводы. Тайминг ~1.5–2 мин/слайд = 33–44 минуты.

## 4. Дельты A vs D (6 слайдов)

Один дополнительный слайд только в D — итого **A = 22, D = 23**.

| # | Версия A (tech/ML) | Версия D (внутренняя защита) |
|---|---|---|
| **2** | Hook: визуальный мокап «один query → три ответа side-by-side» (статика, без live-demo) | Hook: «Что мы сдаём по итогам R1–R10 — три endpoint-а в одном API» |
| **3** | Контекст обобщённый — multilingual RAG для enterprise corpus | Контекст уточнённый: какие именно наши документы (отделы, типы), сколько в проде, объём |
| **6.5** *(только D)* | — | **Что НЕ в scope** — multi-tenant, streaming, BM25 wiring, periodic dedup, document-level summary (закрывает типичный вопрос «а вот ещё бы…») |
| **8** | War story жирно: код-сниппет `TypeError` от `SchemaLLMExtractor`, скрин 0 relations в Neo4j → fix | Та же история, но короче — фокус на том, как поймали (eval gate + diag-scripts) |
| **9** | LightRAG few-shot prompt — **показать сам RU-промпт** и output tuple-format | LightRAG без полного промпта — вместо этого слайд про cost ownership (1200 calls = во что обходится re-ingest) |
| **17** | `/selfrag` глубоко: regex маркеров, `parse_markers`, почему DIFFERENT-default | `/selfrag` короче, но добавлено «когда включать в проде» (compliance / medical / legal) |
| **21** | Lessons learned (tech): (1) tool-calling reliability — главный фильтр выбора модели; (2) ER conservative = лучше FN чем FP; (3) graph as augmentation, не blocking; (4) русский в промпте важнее качества модели на small-LLM | Lessons learned (process): (1) почему 9-stage build + R1–R10 refactor; (2) eval gate как договор с заказчиком; (3) что НЕ делаем; (4) on-prem-first как сознательное решение |
| **22** | Roadmap: open research — incremental ER race, phantom phone-chunks, claim-level citations. Без live-demo. | Status R1–R10 как табло: зелёное / в работе / парковано; risks + следующая итерация (slide-only, без demo) |

## 5. Marp / файловая структура

```
docs/presentation/
├── README.md                              # как собрать (marp CLI)
├── theme.css                              # shared Marp custom theme
├── kb-llamaindex-conf-A.md                # версия A, 22 слайда
├── kb-llamaindex-conf-D.md                # версия D, 23 слайда
├── shared/                                # общие фрагменты — дублируем
│   ├── architecture-diagram.md            #   (Marp не поддерживает include)
│   ├── three-endpoints-table.md
│   └── er-pipeline.md
└── assets/
    ├── architecture.svg                   # опционально, если перерисуем ASCII
    └── (screenshots/demo logs если будут)
```

### Front-matter каждого .md

```yaml
---
marp: true
theme: default
class: invert
paginate: true
size: 16:9
header: 'kb-llamaindex · <DataFest / internal>'
footer: '<speaker name> · 2026-05-14'
---
```

### Build команды

```bash
# PDF
npx @marp-team/marp-cli docs/presentation/kb-llamaindex-conf-A.md -o A.pdf

# PPTX (если просят отдать как .pptx)
npx @marp-team/marp-cli docs/presentation/kb-llamaindex-conf-A.md --pptx -o A.pptx

# HTML preview с auto-reload
npx @marp-team/marp-cli -w docs/presentation/kb-llamaindex-conf-A.md
```

### Принципы содержания слайдов

- Заголовок + 3–6 буллетов **или** 1 диаграмма / таблица /
  код-сниппет на слайд. Никаких walls-of-text.
- Code-блоки через highlight.js (Python / bash / YAML).
- ASCII-диаграммы — `<pre>` без подсветки, fit-content через CSS.
- Speaker notes (`<!-- ... -->`) в версии D — **тезисы по одной
  строке на каждый bullet слайда**, не полный текст. В версии A
  speaker-notes не пишем.

## 6. Источники фактов (что в репо ↔ что на слайдах)

| Слайд | Откуда тянем |
|---|---|
| 5 (архитектура) | `docs/ARCHITECTURE.md` §1, §2 |
| 6 (storage) | `docs/ARCHITECTURE.md` §2 (таблица) |
| 7 (ingestion) | `docs/ARCHITECTURE.md` §3 (ingestion data flow) |
| 8 (war story) | `CHANGELOG.md` `[post-Stage-9 fixes]`, `src/graph/lightrag_extract.py` docstring |
| 9 (LightRAG prompt) | `src/graph/lightrag_prompts.py` (`ENTITY_EXTRACTION_SYSTEM`, `EXAMPLES_DEFAULT`) |
| 10 (merge) | `src/graph/merge.py` (`merge_kg_extraction`, `_maybe_summarize_descriptions`) |
| 11–13 (ER) | `src/graph/entity_resolution.py` docstring (12-step), `ERConfig`, `_DETERMINISTIC_LABELS` |
| 14 (endpoint-таблица) | `docs/QUERY.md` Overview-table |
| 15–17 (per-endpoint) | `docs/QUERY.md` §`/search`, §`/agent`, §`/selfrag`; `src/retrieval/react_agent.py`, `src/retrieval/reflective_synth.py` |
| 18 (RU guarantee) | `docs/QUERY.md` Russian-output guarantee |
| 19 (cost) | `docs/ARCHITECTURE.md` Re-ingest cost table |
| 20 (eval) | `tests/eval/identifier_recall.py`, `tests/eval/answer_quality.py`, `pytest --collect-only` → 287 tests |
| 22 (R1–R10) | `README.md` Status section, `CHANGELOG.md` |

**Принцип:** все числа на слайдах подтверждены кодом или
существующей документацией. Если для слайда нужен факт, которого
нет в репо (например, конкретные latency-замеры на нашей машине),
он помечается `[verify]` в drafted Markdown и собирается в
implementation phase.

## 7. Что НЕ делаем (scope guard)

- Не делаем «универсальный шаблон» презентации — два конкретных
  дека, не фреймворк.
- Не пишем speaker-notes пословно для версии A. Только в D
  (защита читается с тезисами). В A — наброски ключевых тезисов.
- Не делаем live-demo инфраструктуру (готовить fixtures под
  3-endpoint демо — отдельная задача, опциональная).
- Не рисуем SVG-схемы вручную сейчас. Если ASCII-диаграммы
  читаются плохо в PDF — это вопрос implementation phase.
- Не делаем EN-версию.
- Не делаем reveal.js / PPTX как первичный формат. Marp Markdown
  → CLI экспорт в нужный формат по запросу.

## 8. Открытые вопросы — закрыты 2026-05-14

1. **Имена конференции и спикера** на слайдах 1/2 →
   **placeholder-ы** `<conference>`, `<speaker>`. Не блокирующее,
   подставится перед сдачей.
2. **Live-demo в слайде 22 (A)** → **не делаем**. Слайд 22 (A) =
   roadmap / open research, без demo-секции.
   Hook (слайд 2 в обеих версиях) — статический визуальный мокап
   «один query → три ответа side-by-side», тоже без демо.
3. **Speaker-notes в версии D** → **тезисы по одной строке на
   каждый bullet** слайда. В версии A — не пишем.
4. **Анонимизированные примеры реальных данных** на слайде 4
   (pain) → **не используем**. Pain-points формулируем
   обобщённо («multilingual», «small-LLM tool-calling reliability»
   и т.п.) без конкретных кейсов.

## 9. Спецификация принята когда

- [ ] Структура (22/23 слайда + дельты) утверждена.
- [ ] Marp-стек (Markdown + CLI экспорт) утверждён.
- [ ] Список источников фактов (§6) покрывает все слайды, для
      которых нужны конкретные числа.
- [ ] Открытые вопросы (§8) либо разрешены, либо явно отложены на
      implementation phase.

После approve этого spec — переход в `writing-plans` для
поштучного плана наполнения каждого слайда контентом.
