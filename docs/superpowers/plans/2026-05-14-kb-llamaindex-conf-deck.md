# kb-llamaindex Conference Deck Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Собрать две Marp Markdown презентации проекта `kb-llamaindex` — версии A (tech/ML конференция) и D (внутренняя защита) — согласно спецификации `docs/superpowers/specs/2026-05-14-kb-llamaindex-conf-deck-design.md`.

**Architecture:** Два независимых Markdown-файла с фронт-маттером Marp. 16 слайдов идентичны между A и D; 6 слайдов отличаются по содержанию; в D добавляется один дополнительный слайд (6.5 «Что НЕ в scope»). Опциональный `theme.css` для кастомных правок (стартуем на default+invert). Экспорт в PDF/PPTX через `@marp-team/marp-cli` (v4.4.0, доступен через `npx`).

**Tech Stack:** Marp Markdown, `@marp-team/marp-cli` через `npx`, Highlight.js (встроен в Marp) для кода. Без сборочной системы — экспорт через CLI команды.

---

## Источник правды по фактам

Все числа и факты на слайдах **должны** ссылаться на конкретные строки в репо. При расхождении plan vs репо — побеждает репо. Карта источников — §6 spec-а. Ключевые файлы для перепроверки:

- `docs/ARCHITECTURE.md` — диаграммы, storage map, ingestion flow, re-ingest cost.
- `docs/QUERY.md` — таблица трёх endpoint-ов, ReAct loop, reflective-маркеры.
- `docs/MODELS.md` — qwen3:8b, escalation path.
- `README.md` Status section — R1–R10 трек.
- `CHANGELOG.md` `[post-Stage-9 fixes]` — war story с SchemaLLMExtractor.
- `src/graph/lightrag_extract.py` docstring — LightRAG extractor описание.
- `src/graph/lightrag_prompts.py` — RU few-shot example для слайда 9 (A).
- `src/graph/entity_resolution.py` docstring + `_DETERMINISTIC_LABELS` — ER 12-шагов и список skip-типов.
- `src/graph/merge.py` `merge_kg_extraction` — concat / summarize правила.
- `src/retrieval/react_agent.py` — 8 tools, anti-loop guard.
- `src/retrieval/reflective_synth.py` — `[NEED]/[SUPPORTED]/[UNCERTAIN]` regex.

---

## File Structure

```
docs/presentation/
├── README.md                              # как собрать (marp CLI команды)
├── theme.css                              # опционально — пустой/коммент в Task 1
├── kb-llamaindex-conf-A.md                # версия A, 22 слайда
└── kb-llamaindex-conf-D.md                # версия D, 23 слайда
```

Промежуточные shared/* фрагменты, упомянутые в spec §5, **не создаём** — Marp всё равно не поддерживает include, дублирование между двумя файлами берём на себя руками. Это ~10 слайдов идентичного контента — терпимо.

Файлы `*.pdf` строятся локально и **не коммитятся** (output-only, размер большой). Добавляем `.gitignore` правило в Task 1.

---

## Соглашения по содержанию слайда

- Заголовок (`# Title` или `## Title` для подслайда) — обязателен.
- 3–6 буллетов **или** одна таблица / диаграмма / код-блок. Не смешивать.
- Никаких walls-of-text. Если контент не помещается — разделить на два слайда или сокращать.
- ASCII-диаграммы оборачивать в \`\`\`text … \`\`\` (Marp рендерит как monospace, без подсветки).
- Speaker notes (HTML-комментарии `<!-- ... -->`) — **только в версии D**, по одной строке-тезису на каждый bullet слайда.
- Разделитель слайдов в Marp — `---` на отдельной строке. **Внимание:** YAML front-matter сам ограничен `---`, поэтому первый разделитель слайдов идёт **после** закрывающего `---` front-matter-а.

---

## Tasks

### Task 1: Scaffolding — директория, README, front-matter обоих deck-ов

**Files:**
- Create: `docs/presentation/README.md`
- Create: `docs/presentation/theme.css`
- Create: `docs/presentation/kb-llamaindex-conf-A.md` (front-matter only)
- Create: `docs/presentation/kb-llamaindex-conf-D.md` (front-matter only)
- Modify: `.gitignore` (добавить `docs/presentation/*.pdf`, `docs/presentation/*.pptx`)

- [ ] **Step 1: Create directory**

Run: `mkdir -p docs/presentation`

- [ ] **Step 2: Write README.md**

Content of `docs/presentation/README.md`:

```markdown
# kb-llamaindex — Conference Decks

Two Marp Markdown decks for the project conference talks.

- `kb-llamaindex-conf-A.md` — Tech / ML conference (22 slides, ~35 min).
- `kb-llamaindex-conf-D.md` — Internal project defense (23 slides, ~35 min, w/ speaker notes).

Design spec: `../superpowers/specs/2026-05-14-kb-llamaindex-conf-deck-design.md`.

## Build

```bash
# PDF
npx -y @marp-team/marp-cli kb-llamaindex-conf-A.md -o A.pdf
npx -y @marp-team/marp-cli kb-llamaindex-conf-D.md -o D.pdf

# PPTX
npx -y @marp-team/marp-cli kb-llamaindex-conf-A.md --pptx -o A.pptx
npx -y @marp-team/marp-cli kb-llamaindex-conf-D.md --pptx -o D.pptx

# HTML preview with auto-reload
npx -y @marp-team/marp-cli -w kb-llamaindex-conf-A.md
```

Output files (`*.pdf`, `*.pptx`) are gitignored.
```

- [ ] **Step 3: Write empty theme.css placeholder**

Content of `docs/presentation/theme.css`:

```css
/* @theme kb-llamaindex */
/* @import "default"; */

/* Empty for now — default + class=invert front-matter works.
   Add custom styles here only if needed during content review. */
```

- [ ] **Step 4: Write front-matter to A.md**

Content of `docs/presentation/kb-llamaindex-conf-A.md`:

````markdown
---
marp: true
theme: default
class: invert
paginate: true
size: 16:9
header: 'kb-llamaindex · <conference>'
footer: '<speaker> · 2026-05-14'
---

<!-- Slide 1: Cover (placeholder, will be filled in Task 2) -->
# kb-llamaindex

Placeholder.
````

- [ ] **Step 5: Write front-matter to D.md**

Content of `docs/presentation/kb-llamaindex-conf-D.md`:

````markdown
---
marp: true
theme: default
class: invert
paginate: true
size: 16:9
header: 'kb-llamaindex · internal defense'
footer: '<speaker> · 2026-05-14'
---

<!-- Slide 1: Cover (placeholder, will be filled in Task 7) -->
# kb-llamaindex

Placeholder.
````

- [ ] **Step 6: Update .gitignore**

Append to `.gitignore`:

```
# Marp output
docs/presentation/*.pdf
docs/presentation/*.pptx
docs/presentation/*.html
```

- [ ] **Step 7: Smoke-test marp-cli builds both placeholders**

Run:
```bash
cd docs/presentation
npx -y @marp-team/marp-cli kb-llamaindex-conf-A.md -o A.pdf
npx -y @marp-team/marp-cli kb-llamaindex-conf-D.md -o D.pdf
ls -la A.pdf D.pdf
rm A.pdf D.pdf
```

Expected: both PDFs created, ≥10 KB each (1-page invert PDF). Then deleted.

- [ ] **Step 8: Commit**

```bash
git add docs/presentation/ .gitignore
git commit -m "docs(presentation): scaffold Marp deck files (A + D)"
```

---

### Task 2: Deck A slides 1–7 — Cover → Ingestion pipeline

**Files:**
- Modify: `docs/presentation/kb-llamaindex-conf-A.md` (replace placeholder with slides 1–7)

- [ ] **Step 1: Replace placeholder with slides 1–7**

Replace the placeholder cover with this content (preserves the front-matter):

````markdown
# kb-llamaindex

**Production-bound RAG service**

`<conference>` · `<speaker>` · 2026-05-14

---

# Один вопрос → три ответа

Какой правильный — и зачем держать все три?

```
                user query
                    │
       ┌────────────┼────────────┐
       ▼            ▼            ▼
   /search      /agent       /selfrag
   1 LLM call   3-8 calls    4-12 calls
   5-20s        20-90s       30-120s
   "fast"       "agentic"    "verifiable"
```

→ ответ зависит от того, что важнее: latency, рассуждение или проверяемость.

---

# Enterprise corpus — что мы реально индексируем

- **Multilingual** — RU + EN в одном корпусе (договоры, медзаписи, e-mail треды).
- **Документы со «структурой»** — ИНН/ОГРН/БИК, контрактные номера, даты, суммы.
- **Длинные thread-like единицы** — e-mail цепочки, главы договора.
- **Регулируемые домены** — нужна цитата, а не пересказ.
- **On-prem-only** — корпоративные данные не уходят в внешние API без явного решения.

---

# Почему generic RAG здесь ломается

1. **Язык.** Если индекс «как есть», запрос на RU не находит EN-чанки и наоборот.
2. **KG extraction на small-LLM.** Стоковые prompts эхо'ят `Alice/Bob/Philz`; `SchemaLLMPathExtractor` падает на малых моделях.
3. **Entity resolution.** «BCC» ≡ «Базальноклеточный Рак» — vector-уровня недостаточно.
4. **Доверие к ответу.** Plain RAG не различает «знаю», «не знаю», «уверен в этой части».

---

# Архитектура — one-pager

```text
   user upload                              user query
       │                                        │
       ▼                                        ▼
┌────────────┐    ┌──────────┐         ┌────────────────────┐
│ POST       │    │ RabbitMQ │         │ POST /search       │
│ /ingest    │───▶│ taskiq   │         │      /agent        │
└────────────┘    └────┬─────┘         │      /selfrag      │
                       ▼               └─────────┬──────────┘
                ┌──────────────┐                 │
                │ worker       │                 ▼
                │ process_doc  │       ┌─────────────────────┐
                └──┬──────┬────┘       │  retrieval stack    │
                   │      │            │  Milvus · Neo4j · FS│
                   ▼      ▼            └──────────┬──────────┘
              ┌─────────┐┌──────────┐             │
              │ Milvus  ││ Neo4j    │             ▼
              │ chunks  ││ KG nodes │     ┌────────────────┐
              │ vectors ││ + rels   │     │ LLM via        │
              └─────────┘└──────────┘     │ LiteLLM proxy  │
              ┌──────────────┐            └────────────────┘
              │ Postgres     │
              │ job state    │
              └──────────────┘
```

API + taskiq worker, 4 store-а, единый LLM/embed gateway.

→ источник: `docs/ARCHITECTURE.md` §1.

---

# Storage map

| Store | Роль | Wipe target |
|---|---|---|
| **Milvus** | vector index чанков, original-language text + embedding | drop collection |
| **Neo4j** | property graph: `:__Entity__:<Type>` + `:Chunk` + semantic relations | `MATCH (n) DETACH DELETE n` |
| **Postgres** | job-status table `documents` (uuid → status → path) | `TRUNCATE documents` |
| **RabbitMQ** | taskiq broker, queue `process_document` | recreate container |
| **Filesystem** | raw uploaded files под `API_UPLOAD_DIR` | `rm -rf` |
| **LiteLLM** | единый gateway к LLM + embeddings (Ollama / OpenAI) | restart (stateless) |

→ источник: `docs/ARCHITECTURE.md` §2.

---

# Ingestion pipeline — 5 шагов

```text
Document file
    │
    ▼
[1] SimpleDirectoryReader      → Document objects
[2] SentenceSplitter (512/50)  → TextNode[] (original language)
[3] IdentifierCanonicalization → metadata['canonical_identifiers']
                                 + augment-block в node.text
[4] TranslateToRussian         → metadata['translated_text']
                                 (skip если уже русский — без LLM-call)
[5] vector index (Milvus) + KG build (Neo4j, best-effort):
     a. inject canonical identifier nodes
     b. LightRAG extract (1 LLM call/chunk, RU input)
     c. cross-chunk merge
     d. PropertyGraphIndex writes :Chunk + :MENTIONS
     e. overwrite per-chunk descriptions с merged
```

→ источник: `docs/ARCHITECTURE.md` §3.

---
````

- [ ] **Step 2: Build PDF preview**

Run:
```bash
cd docs/presentation
npx -y @marp-team/marp-cli kb-llamaindex-conf-A.md -o A.pdf
```

Expected: 7-page PDF, no errors.

- [ ] **Step 3: Check slide count via headings**

Run: `grep -c "^# " docs/presentation/kb-llamaindex-conf-A.md`

Expected: 7 (slides 1–7 all start with `#`).

- [ ] **Step 4: Commit**

```bash
rm docs/presentation/A.pdf
git add docs/presentation/kb-llamaindex-conf-A.md
git commit -m "docs(presentation): A slides 1-7 (cover → ingestion)"
```

---

### Task 3: Deck A slides 8–13 — KG extraction + Entity Resolution

**Files:**
- Modify: `docs/presentation/kb-llamaindex-conf-A.md` (append slides 8–13)

- [ ] **Step 1: Append slides 8–13**

Append to `docs/presentation/kb-llamaindex-conf-A.md`:

````markdown
# KG extraction: war story

Что было:

- `SchemaLLMPathExtractor` падал с `TypeError` — его Pydantic-validator ловит только `KeyError/ValueError`, а small-LLM эмитят malformed JSON, который кидает `TypeError`.
- Даже с `is_function_calling_model=True` — llama3.1:8b часто **пропускал** tool-call.
- Результат: **0 relations в Neo4j**, поймали через `scripts/check_ingestion.py`.

Фикс:

1. Switch на `SimpleLLMPathExtractor` (regex-based, толерантен к шуму).
2. Замена English stock-prompt (`Alice/Bob/Philz`) на RU B2B example.
3. **18 entities + 9 typed relations** с 5 строк договора (vs 0 раньше).

→ источник: `CHANGELOG.md` `[post-Stage-9 fixes]`.

---

# LightRAG-style extraction

Один LLM-call на чанк → одновременно сущности **и** отношения.

```text
input chunk (RU after translate)
    │
    ▼
LLM prompt (system + few-shot RU example)
    │
    ▼
tuple-format output:
  entity<|#|>ООО Альфа<|#|>Organization<|#|>...
  entity<|#|>договор № 17-К<|#|>ContractNumber<|#|>...
  relation<|#|>ООО Альфа<|#|>ИП Иванов<|#|>контрагент<|#|>...
```

- ~1 call/chunk вместо N (per-entity enricher).
- Drop-in `TransformComponent` для `IngestionPipeline`.
- `gleaning_passes=0` (LightRAG default 1) — экономим, R9 eval решит.
- `num_workers=4` параллелизм при batch-ingest.

→ источник: `src/graph/lightrag_extract.py`, `src/graph/lightrag_prompts.py`.

---

# Cross-chunk merge

Один и тот же entity появляется в 50 чанках → нужно собрать одно описание.

```text
per-chunk descriptions for "ООО Альфа":
  chunk #3  "поставщик медоборудования..."
  chunk #17 "юр.лицо, ИНН 7707..."
  chunk #42 "договор от 2023-08..."
    │
    ▼
merge rule:
  • <8 mentions AND <12k chars  → CONCAT (no LLM)
  • else                         → summarize-LLM call
    │
    ▼
final entity description (RU)
```

Для relations — pair-key `(src, tgt)` undirected; те же правила.

→ источник: `src/graph/merge.py:merge_kg_extraction`.

---

# Entity Resolution — что vector сам не разрулит

Один и тот же концепт ≠ один и тот же string:

| Variant 1 | Variant 2 | Тип проблемы |
|---|---|---|
| `BCC` | `Базальноклеточный Рак` | cross-language |
| `DNA` | `deoxyribonucleic acid` | abbreviation |
| `Рак Кожи БК` | `Рак БК Кожи` | word-order / morphology |
| `Иванов И.И.` | `Иван Иванов` | initialism |
| canonical из doc 1 | новый вариант из doc 2 | cross-document |

→ только vector-уровня **не хватает**: `BCC` и `Базальноклеточный Рак` embed-аются в разные кластеры.

---

# ER: 12-step pipeline

```text
[1]  Filter eligible labels (skip 12 identifier types).
[2]  Load existing canonicals + embeddings from Neo4j.
[3]  Embed new entities (batched).
[4]  Deterministic prepass: initialism regex, exact-norm after diacritics.
[5]  Candidate pairs: same-label top-K cosine ≥ LOW (default 0.55).
[6]  Auto-merge: cosine ≥ HIGH AND same script (ASCII↔ASCII / Cyr↔Cyr).
[7]  LLM-judge borderline: batched 10 pairs, JSON YES/NO/UNSURE.
[8]  Union-find → connected components.
[9]  Verify large clusters (≥ max_cluster_size) via 1 LLM call.
[10] Hyper-hub clamp: clusters ≥ threshold → flag `er_review_needed`.
[11] Pick canonical, consolidate descriptions.
[12] Rewrite chunk-level KG_NODES_KEY metadata + merged_relations.
```

→ источник: `src/graph/entity_resolution.py` docstring.

---

# ER: трейдоффы

- **Conservative default.** На таймауте / parse failure → **DIFFERENT**. Лучше FN, чем FP — false merge порчит граф.
- **12 типов skip.** Email, PhoneNumber, INN, OGRN, BIC, BankAccount, ContractNumber, OrderNumber, InvoiceNumber, DocumentDate, Amount, PostalAddress — у них уже детерминированный canonical из `identifiers.py`. Эти ER не трогает.
- **Cross-script → всегда LLM.** Кириллица ↔ латиница никогда не auto-merge даже при cosine ≥ HIGH.
- **Hyper-hub clamp.** Если кластер вышел больше threshold (например, 30+) — auto-merge выключается, кластер маркируется на review.

Принцип: лучше пара дубликатов в графе, чем хороший entity, склеенный с плохим.

---
````

- [ ] **Step 2: Build PDF and verify slide count**

Run:
```bash
cd docs/presentation
npx -y @marp-team/marp-cli kb-llamaindex-conf-A.md -o A.pdf
grep -c "^# " kb-llamaindex-conf-A.md
```

Expected: 13 slides.

- [ ] **Step 3: Commit**

```bash
rm docs/presentation/A.pdf
git add docs/presentation/kb-llamaindex-conf-A.md
git commit -m "docs(presentation): A slides 8-13 (KG extraction + ER)"
```

---

### Task 4: Deck A slides 14–18 — Three endpoints + RU guarantee

**Files:**
- Modify: `docs/presentation/kb-llamaindex-conf-A.md` (append slides 14–18)

- [ ] **Step 1: Append slides 14–18**

Append to `docs/presentation/kb-llamaindex-conf-A.md`:

````markdown
# Три endpoint-а: обзор

| Endpoint | Outer loop | LLM calls | Latency (gpt-4o-mini) | Когда брать |
|---|---|---|---|---|
| `/search` | none | **1** | 5–20 s | latency важнее качества |
| `/agent` | ReAct (max 8) | 3–8 | 20–90 s | multi-hop / нужно «походить по графу» |
| `/selfrag` | ReAct + reflective | 4–12 | 30–120 s | regulated / нужны цитаты per claim |
| `/legacy/agent` | judge-loop (max 3) | 6–15 | 60–180 s | baseline для eval |

Все четыре делят один retrieval-стек: Milvus + опц. Neo4j + `ChunkRepository`.

→ источник: `docs/QUERY.md` Overview.

---

# `/search` — baseline

```text
POST /api/v1/search { "query": "..." }
    │
    ▼
retriever.aretrieve(query)
    → Milvus top-k=10 (cosine, original-language text + metadata)
    │
    ▼
synthesizer.asynthesize(ru_query, nodes)
    → COMPACT mode: stuff nodes в один prompt, 1 LLM call
    │
    ▼
SearchResponse(answer, sources=[full chunks], latency_ms)
```

- 1 LLM call. Никаких рефайнментов.
- RU-вывод гарантирован через query-wrapper «Ответь на русском …».
- Sources возвращаются **полным** chunk text — для UI цитаты.

---

# `/agent` — ReAct loop

8 tools:

```text
vector_search(query, top_k)            ← top-k Milvus, appends to accumulated_sources
graph_search(query, depth=2)           ← Neo4j entity+relation lookup
find_entity_by_id(name, entity_type)   ← exact match
find_neighbours(entity_name, hops=1)   ← 1-2 hop walk
filter_by_metadata(doc_id, dept, ...)  ← scope already-fetched context
get_chunks_by_doc_id(doc_id, lim, off) ← все чанки одного документа
read_full_document(doc_id, max_chars)  ← raw uploaded file pre-chunk
submit_answer(query_recap, src_ids)    ← триггерит синтезатор
```

Anti-loop guard: 3 идентичных `(tool, args)` подряд → exit. Защита от бесконечных циклов на «unanswerable» вопросах.

Типичные паттерны:

- `vector_search → submit_answer` — простой факт.
- `vector_search → graph_search → submit_answer` — multi-hop.
- `vector_search → get_chunks_by_doc_id → submit_answer` — «весь тред».
- `vector_search → read_full_document → submit_answer` — точная цитата.

---

# `/selfrag` — reflective synthesis

Тот же ReAct снаружи, но `submit_answer` → не plain synth, а reflective loop:

```text
for round_i in 0..max_refinements (default 3):
    draft = await llm.achat(prompt_with_marker_rules + context)
    needs, supports, uncertains = parse_markers(draft)
       [NEED:что не хватает]     ← regex
       [SUPPORTED:chunk_id]      ← regex
       [UNCERTAIN:причина]       ← regex

    if not needs OR round_i >= max_refinements:
        break

    for need in needs[:5]:
        extra = await retriever.aretrieve(need.topic)
        accumulated.extend(extra)
    # next round: redraft with expanded context

final = strip_markers(draft, keep_uncertain=True)
```

- `[NEED]` → re-retrieve и redraft.
- `[SUPPORTED:id]` → claim привязан к chunk_id (citation).
- `[UNCERTAIN:...]` → остаётся в финальном ответе, не галлюцинация.

Цена: 2× latency vs `/agent`. Берём только когда нужны цитаты per claim.

---

# Russian-output guarantee

3 независимых enforcement-точки — ответ всегда на русском, даже если корпус EN:

1. **Ingest**: `TranslateToRussianTransform` → `node.metadata['translated_text']`. LightRAG extractor читает оттуда → entity names + descriptions попадают в Neo4j на русском.
2. **System prompts** в `react_agent.py` и `reflective_synth.py` hard-code «WRITE YOUR ANSWER IN RUSSIAN».
3. **Plain /search** — query-wrapper «Ответь на следующий вопрос на русском, сохраняя имена собственные …» перед LlamaIndex synthesizer-ом.

Source chunks хранятся **в оригинальном языке** в Milvus и возвращаются в `sources[].content` для UI — ответ читается на RU, цитаты в source language.

---
````

- [ ] **Step 2: Build PDF and verify slide count**

Run:
```bash
cd docs/presentation
npx -y @marp-team/marp-cli kb-llamaindex-conf-A.md -o A.pdf
grep -c "^# " kb-llamaindex-conf-A.md
```

Expected: 18 slides.

- [ ] **Step 3: Commit**

```bash
rm docs/presentation/A.pdf
git add docs/presentation/kb-llamaindex-conf-A.md
git commit -m "docs(presentation): A slides 14-18 (3 endpoints + RU)"
```

---

### Task 5: Deck A slides 19–22 — Cost, observability, lessons, roadmap

**Files:**
- Modify: `docs/presentation/kb-llamaindex-conf-A.md` (append slides 19–22)

- [ ] **Step 1: Append slides 19–22**

Append to `docs/presentation/kb-llamaindex-conf-A.md`:

````markdown
# Cost / latency — реальные числа

Re-ingest **1 MB English corpus, ~514 chunks**:

| Шаг | LLM calls |
|---|---|
| Translate to Russian | ~514 |
| LightRAG extract | ~514 |
| Cross-chunk merge summary (≥8 occurrences) | 100–150 |
| Entity description embeddings | ~2 500 (embed, not LLM) |
| **Total LLM chat calls** | **~1 200** |
| Wall time on gpt-4o-mini | **15–25 min** |

Per-query stoимость:

- `/search` — 1 LLM call.
- `/agent` — 3 (reasoning) + 1 (synth) = **4** для multi-hop.
- `/selfrag` — 4–12 в зависимости от глубины refinements.

→ источник: `docs/ARCHITECTURE.md` Re-ingest cost table.

---

# Observability + Eval gate

**Trace per request** через `trace_request(endpoint, query)` context-manager (`src/observability/trace.py`):

```text
record_event("tool_call",   tool_name="vector_search")
record_event("llm_call",    kind="agent_reasoning")
record_event("refinement_round", round=0, needs=2)
record_timed(name, **payload)
   ↓
trace done  endpoint=agent  rid=ab12cd
  n_tool_calls=3  n_llm_calls=5  n_refinements=0
  total_ms=24300.2
  tool_breakdown={vector_search:1, graph_search:1, submit_answer:1}
```

ContextVar-scoped → concurrent requests не пересекаются.

**Eval gate:**

- **287 тестов** (`pytest --collect-only -q`).
- `tests/eval/identifier_recall.py` — 7 golden cases, thresholds: phone/email/INN/OGRN/BIC ≥ 0.95, contract/amount ≥ 0.85, address ≥ 0.75, precision ≥ 0.90.
- `tests/eval/answer_quality.py` (R9) — deterministic offline grader: fact recall, entity recall, citation precision, hallucination upper bound, uncertainty honesty.

---

# Lessons learned (technical)

1. **Tool-calling reliability — главный фильтр выбора модели.** Качество reasoning важно, но если модель пропускает tool-call в 20% случаев — весь agent ломается. qwen3:8b > llama3.1:8b ровно по этому критерию.

2. **ER должен быть consciously consérvative.** Дефолт на DIFFERENT при таймауте, cross-script всегда через LLM, hyper-hub clamp. Один false merge порочит весь граф; пара дубликатов — нет.

3. **Graph as augmentation, не blocking.** Neo4j падает → vector index всё ещё работает, `/search` отвечает. Ingestion graph-step wrap-нут в try/except, ошибка в `documents.error` поле.

4. **Русский в промпте важнее качества модели на small-LLM.** Замена стокового `Alice/Bob/Philz` на B2B-пример с `ООО Альфа → договор № 17-К → ИП Иванов` сдвинула llama3.1:8b с 0 relations на 18 entities + 9 typed relations.

---

# Roadmap & open research

**Что работает в проде:**
- 9-stage прототип сдан 2026-05-09, R1 (qwen3:8b migration) закрыт.

**В работе (R2–R10):**
- R2 — function calling + structured output cleanup.
- R3 — universal entity types + rich descriptions.
- R4 — DI hygiene + 3-endpoint split.
- R5–R6 — ≥115 тестов + ARCHITECTURE/MODELS docs.
- R7–R8 — ReAct + reflective synthesis (этот доклад).
- R9 — answer-quality eval over multi-domain golden Q&A.
- R10 — decommission legacy judge-based path.

**Open research / known unknowns:**
- **Incremental ER race** — параллельный ingest двух документов может «увидеть» друг друга на разных стадиях canonicalization.
- **Phantom phone-chunks** — `_consolidate_phone_entities` оставляет orphan chunks после merge.
- **Claim-level citations** — сейчас citation = chunk_id; span-внутри-chunk остаётся UI-задачей.
- **Multi-tenant isolation** — `department` flow-ит через metadata, но enforcement на retrieve-уровне ещё не сделан.

Спасибо. Вопросы?
````

- [ ] **Step 2: Build full A.pdf**

Run:
```bash
cd docs/presentation
npx -y @marp-team/marp-cli kb-llamaindex-conf-A.md -o A.pdf
grep -c "^# " kb-llamaindex-conf-A.md
```

Expected: 22 slides. PDF 22 pages.

- [ ] **Step 3: Eyeball A.pdf — open it**

Run: `open docs/presentation/A.pdf` (macOS) — глазами проверить:
- Все слайды читаются (не обрезано).
- Таблицы fit-в-страницу.
- Code-блоки не уезжают за край.

Если что-то обрезается — отметить какой слайд, фиксить в Task 11 (Final pass).

- [ ] **Step 4: Commit**

```bash
rm docs/presentation/A.pdf
git add docs/presentation/kb-llamaindex-conf-A.md
git commit -m "docs(presentation): A complete (22 slides) — cost, eval, lessons, roadmap"
```

---

### Task 6: Deck D — copy A, apply slides 1/2/3 deltas

**Files:**
- Modify: `docs/presentation/kb-llamaindex-conf-D.md` (replace placeholder)

- [ ] **Step 1: Copy A.md content into D.md (preserving D's front-matter header)**

Run:
```bash
# Extract A's body (everything after the closing front-matter '---')
awk 'BEGIN{n=0} /^---$/{n++; if(n==2){next} } n>=2' \
    docs/presentation/kb-llamaindex-conf-A.md \
    > /tmp/A-body.md

# Replace D body (everything after its front-matter)
awk 'BEGIN{n=0} /^---$/{n++; if(n<=2)print; next} n<2{print}' \
    docs/presentation/kb-llamaindex-conf-D.md \
    > /tmp/D-header.md

cat /tmp/D-header.md /tmp/A-body.md > docs/presentation/kb-llamaindex-conf-D.md
```

Verify front-matter still says `internal defense`:
```bash
head -10 docs/presentation/kb-llamaindex-conf-D.md
```

- [ ] **Step 2: Edit slide 2 (hook)**

In `docs/presentation/kb-llamaindex-conf-D.md`, replace slide 2 «Один вопрос → три ответа» content with:

````markdown
# Что мы сдаём по итогам R1–R10

Один API — **три endpoint-а** под разные требования к ответу:

```
                user query
                    │
       ┌────────────┼────────────┐
       ▼            ▼            ▼
   /search      /agent       /selfrag
   "fast"       "agentic"    "verifiable"
```

- Общая инфраструктура: ingestion + KG + vector index.
- Eval-gate как договор с заказчиком (287 тестов + golden Q&A).
- On-prem-only стек.

---
````

- [ ] **Step 3: Edit slide 3 (context — уточнённый)**

Replace slide 3 «Enterprise corpus — что мы реально индексируем» с:

````markdown
# Контекст — наши документы

- **Источники в проде:** `<отделы / типы — заполнить перед сдачей>`.
- **Объём:** `<кол-во документов / GB — заполнить>`.
- **Языки:** RU/EN (~`<X%>` EN после translate-to-RU).
- **Регулируемые домены:** медицина, юридические тексты — нужны цитаты per claim.
- **Чувствительные данные:** не уходят во внешние API. LiteLLM proxy → on-prem Ollama.

→ цифры собираются перед защитой; placeholder-ы помечены `<>`.

---
````

- [ ] **Step 4: Build PDF and verify**

Run:
```bash
cd docs/presentation
npx -y @marp-team/marp-cli kb-llamaindex-conf-D.md -o D.pdf
grep -c "^# " kb-llamaindex-conf-D.md
```

Expected: 22 slides (ещё не вставили 6.5 и не правили остальные дельты — будет в Task 7/8).

- [ ] **Step 5: Commit**

```bash
rm docs/presentation/D.pdf
git add docs/presentation/kb-llamaindex-conf-D.md
git commit -m "docs(presentation): D copy of A + slides 1-3 deltas"
```

---

### Task 7: Deck D — insert slide 6.5 «Что НЕ в scope»

**Files:**
- Modify: `docs/presentation/kb-llamaindex-conf-D.md` (insert one slide between slides 6 and 7)

- [ ] **Step 1: Insert slide between «Storage map» and «Ingestion pipeline»**

Найти в `docs/presentation/kb-llamaindex-conf-D.md` строку `# Ingestion pipeline — 5 шагов` и вставить **перед** ней (на отдельной строке) этот блок (включая закрывающий `---`):

````markdown
# Что НЕ в scope этой версии

Сознательные «нет» — закрывают типичные вопросы на защите:

- **Multi-tenant isolation** — `department` есть в metadata, но enforcement на retrieve не реализован. Следующая итерация.
- **Streaming responses (SSE)** — синхронный JSON. SSE требует переделки FastAPI слоя.
- **BM25 + RRF hybrid** — модуль `src.retrieval.hybrid` есть, но в DI не подключён. Нужно решение по docstore.
- **Periodic graph deduplication** — ER работает on-ingest, кросс-документный alias-merge раз в N часов — отложено.
- **Document-level summaries** — колонка `documents.summary` зарезервирована, но не используется.
- **Caching agent tool results** между запросами.

→ источник: `docs/ARCHITECTURE.md` §9.

---
````

- [ ] **Step 2: Build PDF and verify**

Run:
```bash
cd docs/presentation
npx -y @marp-team/marp-cli kb-llamaindex-conf-D.md -o D.pdf
grep -c "^# " kb-llamaindex-conf-D.md
```

Expected: 23 slides. Slide 7 в PDF теперь «Что НЕ в scope», slide 8 «Ingestion pipeline».

- [ ] **Step 3: Commit**

```bash
rm docs/presentation/D.pdf
git add docs/presentation/kb-llamaindex-conf-D.md
git commit -m "docs(presentation): D slide 6.5 (out of scope)"
```

---

### Task 8: Deck D — slides 8/9/17 deltas (war story, cost ownership, /selfrag short)

**Files:**
- Modify: `docs/presentation/kb-llamaindex-conf-D.md`

- [ ] **Step 1: Replace slide 8 «KG extraction: war story» (короче в D)**

Заменить содержимое слайда «KG extraction: war story» в D на:

````markdown
# KG extraction — как поймали скрытый баг

Проблема: KG-extraction работал «по логам», но **в Neo4j 0 relations**.

Как поймали:
- `scripts/check_ingestion.py` — diagnostic над PG + Milvus + Neo4j — показал 0 relations.
- `scripts/diag_kg.py` — hard-coded chunk → extractor вернул пустой результат.

Что было сломано:
- `SchemaLLMPathExtractor` не толерантен к шуму на small-LLM.
- Стоковый prompt с `Alice/Bob/Philz` мешал русским моделям.

Фикс: `SimpleLLMPathExtractor` + RU few-shot → **18 entities + 9 relations** на тестовом договоре.

**Урок:** end-to-end diagnostic важнее unit-теста. Compose-уровневые `diag_*` script-ы — стандарт.

---
````

- [ ] **Step 2: Replace slide 9 «LightRAG-style extraction» с слайдом про cost ownership**

Заменить содержимое слайда «LightRAG-style extraction» в D на:

````markdown
# Cost ownership — что стоит re-ingest

1 MB English corpus / 514 chunks:

| Шаг | LLM calls | Доля |
|---|---|---|
| Translate to Russian | ~514 | 43% |
| LightRAG extract | ~514 | 43% |
| Cross-chunk merge summary | 100–150 | 12% |
| Entity description (embed) | ~2 500 | (не LLM) |
| **Total chat calls** | **~1 200** | |

Wall time: **15–25 min** на gpt-4o-mini.

Контроль:
- `INGESTION_TRANSLATE_TO_RUSSIAN=false` → ~500 calls долой, граф остаётся в source language.
- Re-ingest корпуса повторно при смене embed-модели обязателен — `MILVUS_DIM` должен совпадать.

LightRAG-style extract = 1 call/chunk (детали в репо: `src/graph/lightrag_extract.py`).

---
````

- [ ] **Step 3: Replace slide 17 «`/selfrag` — reflective synthesis» (короче + when-to-use)**

Заменить содержимое в D на:

````markdown
# `/selfrag` — когда оно нужно в проде

Алгоритм: ReAct снаружи + reflective draft с маркерами `[NEED]/[SUPPORTED]/[UNCERTAIN]` внутри. Re-retrieve по `[NEED]`, max 3 refinements.

**Включаем в проде когда:**

- **Regulated / medical / legal** — ответ должен быть проверяемым по claim.
- **Audit trail** — нужны цитаты per утверждение, не общий список sources.
- **High-stakes decisions** — лучше «UNCERTAIN», чем галлюцинация.

**Не включаем когда:**

- Plain factoid Q&A — `/agent` достаточно за половину latency.
- High-throughput batch search — 30–120 s/запрос неприемлемо.

Цена: **2× latency vs `/agent`**, +30–50% LLM calls.

Детали алгоритма + regex маркеров — в репо: `src/retrieval/reflective_synth.py`.

---
````

- [ ] **Step 4: Build PDF and verify**

Run:
```bash
cd docs/presentation
npx -y @marp-team/marp-cli kb-llamaindex-conf-D.md -o D.pdf
grep -c "^# " kb-llamaindex-conf-D.md
```

Expected: 23 slides (без изменений в count — мы только заменили содержимое).

- [ ] **Step 5: Commit**

```bash
rm docs/presentation/D.pdf
git add docs/presentation/kb-llamaindex-conf-D.md
git commit -m "docs(presentation): D slides 8/9/17 deltas (war story, cost, selfrag)"
```

---

### Task 9: Deck D — slides 21/22 deltas (lessons process, status R1–R10)

**Files:**
- Modify: `docs/presentation/kb-llamaindex-conf-D.md`

- [ ] **Step 1: Replace slide 21 «Lessons learned (technical)» → process-фокус**

Заменить содержимое в D на:

````markdown
# Lessons learned (process)

1. **9-stage прототип + R1–R10 рефакторинг — не «два проекта».** Один трек: первые 9 стадий ставят функциональность, R1–R10 переводит в production-form (function calls, типы entities, DI hygiene, eval, decommission legacy). Резать на «прототип» и «продукт» дорого — теряется контекст.

2. **Eval gate — это договор с заказчиком.** Acceptance threshold-ы для identifier recall и answer quality зафиксированы в `tests/eval/`, считаются на каждый PR. Спор «а вот этот ответ хороший?» уходит в спор о метрике.

3. **Что НЕ делаем — такой же важный артефакт, как roadmap.** Multi-tenant, streaming, BM25, periodic dedup — все обоснованно отложены. См. слайд «Что НЕ в scope».

4. **On-prem-first — сознательное решение.** Стек проектируется под Ollama (даже когда дефолт gpt-4o-mini); LiteLLM как единый gateway даёт обратимый swap без изменения кода.

---
````

- [ ] **Step 2: Replace slide 22 «Roadmap & open research» → R1–R10 status board**

Заменить содержимое в D на:

````markdown
# Status R1–R10

| Стадия | Что | Статус |
|---|---|---|
| **R1** | qwen3:8b migration | done (2026-05-11) |
| **R2** | function calling + structured output | in progress |
| **R3** | universal entity types + descriptions | done (баклог) |
| **R4** | DI hygiene + 3-endpoint split | done |
| **R5** | ≥115 tests | **done (287)** |
| **R6** | `MODELS.md`, `ARCHITECTURE.md` | done |
| **R7** | ReAct agent `/agent` | done |
| **R8** | Reflective synthesis `/selfrag` | done |
| **R9** | Answer-quality eval, multi-domain golden | in progress |
| **R10** | Decommission legacy judge path | gated by R9 |

**Risks / открытые вопросы:**

- Incremental ER race на параллельном ingest — есть план, нет имплементации.
- Phantom phone-chunks после `_consolidate_phone_entities` merge.
- Claim-level citations требуют UI-стороны для span-в-chunk.

**Следующая итерация:** R9 завершён → A/B по multi-domain corpus → решение по R10.

Спасибо. Вопросы?
````

- [ ] **Step 3: Build PDF and verify**

Run:
```bash
cd docs/presentation
npx -y @marp-team/marp-cli kb-llamaindex-conf-D.md -o D.pdf
grep -c "^# " kb-llamaindex-conf-D.md
```

Expected: 23 slides.

- [ ] **Step 4: Commit**

```bash
rm docs/presentation/D.pdf
git add docs/presentation/kb-llamaindex-conf-D.md
git commit -m "docs(presentation): D slides 21-22 deltas (lessons process, R1-R10)"
```

---

### Task 10: Deck D — speaker notes на все 23 слайда

**Files:**
- Modify: `docs/presentation/kb-llamaindex-conf-D.md`

Speaker notes в Marp — HTML-комментарии `<!-- ... -->` сразу после блока слайда, до закрывающего `---`. По правилу: **одна строка-тезис на каждый bullet** слайда.

- [ ] **Step 1: Добавить speaker notes к слайду 1 (Cover)**

В конец слайда 1 «# kb-llamaindex» в D, перед `---`:

```markdown
<!--
- Представить себя + команду.
- Назвать заказчика проекта (если non-confidential).
- Время доклада: ~35 минут, вопросы в конце.
-->
```

- [ ] **Step 2: Добавить speaker notes к слайду 2 (Hook)**

```markdown
<!--
- /search — для UI-чата, latency ≤ 20s.
- /agent — для multi-hop вопросов оператора.
- /selfrag — для регулируемых доменов.
- Общая инфра — ingestion + KG + vector — одна для всех трёх.
- Eval-gate: каждый PR проверяется на 287 тестов + RAGAS-like.
-->
```

- [ ] **Step 3: Добавить speaker notes к слайдам 3–6 (контекст, pain, архитектура, storage)**

Slide 3 (Контекст):
```markdown
<!--
- Сначала заполнить плейсхолдеры реальными цифрами.
- Упомянуть отделы-источники без идентифицирующих деталей.
- На regulated пунктике остановиться — связь со слайдом /selfrag.
-->
```

Slide 4 (Pain):
```markdown
<!--
- Pain #1 язык: запрос на RU не находит EN-чанков — главный driver translate-to-RU.
- Pain #2 KG: stock prompts не работают на наших моделях (см. слайд 8).
- Pain #3 ER: vector сам не решает «BCC ≡ Базальноклеточный Рак».
- Pain #4 доверие: regulated домены требуют /selfrag.
-->
```

Slide 5 (Архитектура):
```markdown
<!--
- API + worker — два процесса, не монолит.
- 4 store-а: каждый со своей зоной ответственности.
- LiteLLM — единый gateway, обратимый swap LLM.
- На вопрос «зачем RabbitMQ» — async ingest, retries, backpressure.
-->
```

Slide 6 (Storage map):
```markdown
<!--
- Milvus — vector + original-language text для UI цитат.
- Neo4j — best-effort, падение не блокирует /search.
- Postgres — простая таблица jobs, никакого ORM.
- RabbitMQ — taskiq broker, одна очередь.
- FS — raw uploads для read_full_document tool (см. /agent).
- LiteLLM — stateless, restart-recoverable.
-->
```

- [ ] **Step 4: Добавить speaker notes к слайду 6.5 (Что НЕ в scope) и 7 (Ingestion)**

Slide 6.5 (Что НЕ в scope):
```markdown
<!--
- Multi-tenant: department есть в metadata, но retrieve-фильтр — следующая итерация.
- SSE: переделка FastAPI, отложили.
- BM25: модуль есть, не подключён, нет решения по docstore.
- Periodic dedup: ER on-ingest есть, cross-doc batch — нет.
- Document summaries: колонка зарезервирована, контент не пишется.
- Tool result caching: текущая нагрузка не требует.
-->
```

Slide 7 (Ingestion):
```markdown
<!--
- Стандартный LlamaIndex IngestionPipeline + наши 2 transform-а.
- Translate-to-RU включается condition'ом: skip если уже русский.
- KG step best-effort: Neo4j down → ingestion completes без графа.
- Augment-block (canonical identifiers) feeds LightRAG промпт in-band.
-->
```

- [ ] **Step 5: Добавить speaker notes к слайдам 8–13 (KG + ER)**

Slide 8 (war story в D):
```markdown
<!--
- Главная мысль: end-to-end diagnostic важнее unit-test.
- check_ingestion.py + diag_kg.py — два diagnostic-а, оба стоит показать.
- На вопрос «как стобы вы поймали без diag?» — сложно, отвечать честно.
-->
```

Slide 9 (cost ownership в D):
```markdown
<!--
- Главная цифра: ~1200 calls на 1 MB корпуса.
- Не путать LLM chat-calls с embedding-calls.
- INGESTION_TRANSLATE_TO_RUSSIAN=false — feature flag для cost control.
- MILVUS_DIM — частая ошибка на смене embed-модели.
-->
```

Slide 10 (Cross-chunk merge):
```markdown
<!--
- Правило 8 mentions / 12k chars — эмпирическое, не оптимальное.
- Relations: undirected pair key, иначе дубли в обе стороны.
- Если что-то идёт не так с описаниями — смотреть _maybe_summarize_descriptions.
-->
```

Slide 11 (ER problem):
```markdown
<!--
- BCC / Базальноклеточный — реальный кейс из медкорпуса.
- Cross-document — самый частый случай в проде.
- Инициалы (Иванов И.И. ≡ Иван Иванов) — часто в e-mail.
- Vector один не решает — embed-ы могут быть в разных кластерах.
-->
```

Slide 12 (ER 12-step):
```markdown
<!--
- Шаги 4-6 — детерминированные, дешёвые.
- Шаги 7-9 — LLM-judge, дорогие, batched 10.
- Шаг 10 (hyper-hub clamp) — против snowball merge.
- Шаг 12 — переписывает метаданные чанков, не только Neo4j.
-->
```

Slide 13 (ER trade-offs):
```markdown
<!--
- DIFFERENT on timeout: ключевое решение. Готов защищать.
- 12 identifier-типов skip — они уже канонизированы deterministic-ом.
- Cross-script всегда LLM: даже cosine=0.99 не гарантирует тождество.
- Hyper-hub: cluster >30 → not auto-merge, flag er_review_needed.
-->
```

- [ ] **Step 6: Добавить speaker notes к слайдам 14–18 (endpoints + RU)**

Slide 14 (overview):
```markdown
<!--
- 4 endpoint-а, legacy под флагом — не путать.
- Latency на gpt-4o-mini — на on-prem qwen3:8b цифры выше.
- Все четыре делят один retrieval-стек: важно для затрат на инфру.
-->
```

Slide 15 (`/search`):
```markdown
<!--
- Главный use case — UI chat, real-time запросы.
- Sources возвращаются полные, не truncated — UI делает «цитата + ссылка».
- RU-wrapper — необходимость, default prompt LlamaIndex англоязычен.
-->
```

Slide 16 (`/agent`):
```markdown
<!--
- 8 tools — фиксированный набор, новые добавляются через PR.
- Anti-loop guard — простая защита, но спасала в проде.
- read_full_document — самый «дорогой» tool, использовать sparingly.
- submit_answer — обязательный exit-tool, иначе уходим в max_iterations.
-->
```

Slide 17 (`/selfrag`):
```markdown
<!--
- Главное — flag по claim-уровню, не «вообще не знаю».
- Maркеры NEED триггерят дополнительный retrieve.
- max_refinements=3 — больше редко даёт улучшение, дороже линейно.
- Когда включать: regulated, audit trail, high-stakes.
-->
```

Slide 18 (RU guarantee):
```markdown
<!--
- 3 точки enforcement специально, чтобы одна сломалась — две защитят.
- Source chunks в Milvus остаются в исходном языке — для UI цитат.
- Если ответ внезапно на EN — смотреть translate_transform логи.
-->
```

- [ ] **Step 7: Добавить speaker notes к слайдам 19–23 (cost, observability, lessons, status)**

Slide 19 (cost / latency реальные числа):
```markdown
<!--
- 1 MB / 514 chunks — наш sample, не упрощённый benchmark.
- Wall time зависит от LLM provider — gpt-4o-mini средний.
- Per-query: важно зафиксировать «4 calls на multi-hop».
-->
```

Slide 20 (observability + eval):
```markdown
<!--
- Trace context-var — concurrent requests isolated.
- 287 тестов — реальный count из pytest --collect-only.
- Identifier recall thresholds — закреплены, спор → метрика.
- Answer quality — deterministic, не RAGAS-стиль (тот требует LLM-judge).
-->
```

Slide 21 (lessons process):
```markdown
<!--
- 9-stage + R1-R10: единый план, не два проекта.
- Eval gate как договор: ключевое для отношений с заказчиком.
- Что НЕ в scope: дать ссылку на слайд 6.5.
- On-prem-first: даже при дефолте gpt-4o-mini инфра готова к Ollama.
-->
```

Slide 22 (R1–R10 status):
```markdown
<!--
- Зеленые: R1, R3, R4, R5, R6, R7, R8.
- В работе: R2, R9.
- Gated: R10 (зависит от R9).
- Risks: 3 пункта — все известные, не сюрприз для команды.
- Следующая итерация: после R9 решаем по R10.
-->
```

- [ ] **Step 8: Build PDF, verify notes не появились на слайдах**

Run:
```bash
cd docs/presentation
npx -y @marp-team/marp-cli kb-llamaindex-conf-D.md -o D.pdf
```

Открыть `D.pdf`, проверить что HTML-комментарии **не** отрисованы на слайдах (Marp их прячет). Если видны — значит синтаксис комментариев нарушен.

- [ ] **Step 9: Commit**

```bash
rm docs/presentation/D.pdf
git add docs/presentation/kb-llamaindex-conf-D.md
git commit -m "docs(presentation): D speaker notes (single-line per bullet)"
```

---

### Task 11: Final pass — оба deck-а, fix layout issues, README review

**Files:**
- Modify: `docs/presentation/kb-llamaindex-conf-A.md` (если есть layout-issues)
- Modify: `docs/presentation/kb-llamaindex-conf-D.md` (если есть layout-issues)
- Modify: `docs/presentation/README.md` (sanity check команд)

- [ ] **Step 1: Build обе PDF и открыть глазами**

Run:
```bash
cd docs/presentation
npx -y @marp-team/marp-cli kb-llamaindex-conf-A.md -o A.pdf
npx -y @marp-team/marp-cli kb-llamaindex-conf-D.md -o D.pdf
open A.pdf D.pdf
```

- [ ] **Step 2: Чек-лист по каждому deck-у**

Пройтись по каждому слайду:

- [ ] Заголовок виден полностью (не обрезан справа).
- [ ] Таблицы помещаются на слайд (≤ 8 строк, ≤ 6 колонок).
- [ ] Code-блоки не выходят за правый край (≤ 70 chars/line).
- [ ] ASCII-диаграммы выровнены (monospace отображается корректно).
- [ ] Pagination ставит правильный номер.
- [ ] Header / footer не накладываются на контент.

Если слайд не помещается:
- Разделить на два слайда (новый `#` заголовок + `---` сверху).
- ИЛИ сократить bullets до 4–5.
- ИЛИ уменьшить таблицу.

- [ ] **Step 3: Финальный slide count check**

Run:
```bash
echo "A slides:"
grep -c "^# " docs/presentation/kb-llamaindex-conf-A.md
echo "D slides:"
grep -c "^# " docs/presentation/kb-llamaindex-conf-D.md
```

Expected:
- A: 22
- D: 23

Если расхождение — исправить.

- [ ] **Step 4: README sanity-check команд**

Run каждую команду из `docs/presentation/README.md` в отдельной shell, убедиться что она работает:
- `npx -y @marp-team/marp-cli kb-llamaindex-conf-A.md -o A.pdf`
- `npx -y @marp-team/marp-cli kb-llamaindex-conf-A.md --pptx -o A.pptx`
- `npx -y @marp-team/marp-cli -w kb-llamaindex-conf-A.md` (Ctrl+C сразу после старта)

Если что-то не работает — обновить README.

- [ ] **Step 5: Финальный commit**

Если были правки:
```bash
rm docs/presentation/A.pdf docs/presentation/D.pdf
git add docs/presentation/
git commit -m "docs(presentation): final layout pass + README sanity check"
```

Если правок не было — просто удалить PDF-ы:
```bash
rm docs/presentation/A.pdf docs/presentation/D.pdf
```

---

## Definition of Done

- [ ] `docs/presentation/kb-llamaindex-conf-A.md` собирается в 22-слайдовый PDF без ошибок.
- [ ] `docs/presentation/kb-llamaindex-conf-D.md` собирается в 23-слайдовый PDF без ошибок.
- [ ] Каждый слайд читается на 16:9 PDF без обрезаний.
- [ ] Версия D имеет speaker-notes (HTML-комментарии) на каждом слайде.
- [ ] Все числа на слайдах ссылаются на репо / документацию проекта (см. §6 spec).
- [ ] `docs/presentation/README.md` содержит работающие build-команды.
- [ ] `.gitignore` исключает `*.pdf` / `*.pptx` / `*.html` в `docs/presentation/`.
- [ ] Все промежуточные коммиты сделаны (по одному на task).
