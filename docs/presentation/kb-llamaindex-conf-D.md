---
marp: true
theme: default
class: invert
paginate: true
size: 16:9
header: 'kb-llamaindex · internal defense'
footer: '[speaker] · 2026-05-14'
---

# kb-llamaindex

**Production-bound RAG service**

[conference] · [speaker] · 2026-05-14

---

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

# Контекст — наши документы

- **Источники в проде:** [отделы / типы — заполнить перед сдачей].
- **Объём:** [кол-во документов / GB — заполнить].
- **Языки:** RU/EN (~[X%] EN после translate-to-RU).
- **Регулируемые домены:** медицина, юридические тексты — нужны цитаты per claim.
- **Чувствительные данные:** не уходят во внешние API. LiteLLM proxy → on-prem Ollama.

→ цифры собираются перед защитой; placeholder-ы помечены `[ ]`.

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

# Russian-output guarantee

3 независимых enforcement-точки — ответ всегда на русском, даже если корпус EN:

1. **Ingest**: `TranslateToRussianTransform` → `node.metadata['translated_text']`. LightRAG extractor читает оттуда → entity names + descriptions попадают в Neo4j на русском.
2. **System prompts** в `react_agent.py` и `reflective_synth.py` hard-code «WRITE YOUR ANSWER IN RUSSIAN».
3. **Plain /search** — query-wrapper «Ответь на следующий вопрос на русском, сохраняя имена собственные …» перед LlamaIndex synthesizer-ом.

Source chunks хранятся **в оригинальном языке** в Milvus и возвращаются в `sources[].content` для UI — ответ читается на RU, цитаты в source language.

---

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

Per-query стоимость:

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

2. **ER должен быть осознанно консервативным.** Дефолт на DIFFERENT при таймауте, cross-script всегда через LLM, hyper-hub clamp. Один false merge порочит весь граф; пара дубликатов — нет.

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
