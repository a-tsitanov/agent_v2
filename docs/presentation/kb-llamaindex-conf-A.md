---
marp: true
theme: default
class: invert
paginate: true
size: 16:9
header: 'kb-llamaindex · [conference]'
footer: '[speaker] · 2026-05-14'
---

# kb-llamaindex

**Production-bound RAG service**

[conference] · [speaker] · 2026-05-14

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
