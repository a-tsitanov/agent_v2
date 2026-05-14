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
