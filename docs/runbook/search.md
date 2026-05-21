# Search subsystem runbook

Self-contained гид по трём search-endpoint'ам kb-llamaindex: **`/api/v1/search`**, **`/api/v1/agent`**, **`/api/v1/selfrag`**. Linear reading guide — каждый раздел опирается на предыдущий. Со ссылками на конкретные файлы/строки и code-выдержками.

Связанные runbook'и: [`multimodel.md`](multimodel.md) (откуда берётся `search`-role модель), [`analytics.md`](analytics.md) (observability slot — но `ingest_metrics` к search'у НЕ относится; см. § 12). Если ищешь tuning knob — иди в [§ 10](#10-configuration-knobs) и [§ 13](#13-score-thresholds--tuning-matrix). Если что-то падает — [§ 15](#15-troubleshooting).

> **Что в скоупе:** `/search`, `/agent`, `/selfrag`. Legacy `/api/v1/legacy/agent` (judge-based loop) — упоминается one-line в § 11 для compare-context, не разворачивается.

---

## 1. Overview — три endpoint'а

```
┌──────────────────────────────────────────────────────────────────┐
│ Все три endpoint'а делят общий низ:                              │
│   hybrid_retrieve (BM25 + dense vector + RRF fusion)             │
│   → reranker (BGE-v2-m3 cross-encoder, top-N=5)                  │
│ Отличаются ТОЛЬКО тем, как пакуется retriever в loop:            │
└──────────────────────────────────────────────────────────────────┘

  /search      → single-shot: retrieve → synthesize → return
                  prompt: ResponseSynthesizer (default)

  /agent       → ReAct loop:  LLM выбирает tool из 8 → execute →
                              feedback → … → submit_answer → synthesize
                  prompt: ResponseSynthesizer (default)

  /selfrag     → ReAct loop (тот же что /agent) →
                  → reflective synthesis: draft → parse [NEED] markers →
                    re-retrieve gaps → redraft  (до max_refinements)
                  prompt: REFLECTIVE_SYSTEM_PROMPT + REFINE_PROMPT
```

| Endpoint | Когда выбирать | Latency tier | LLM calls (typ.) | Citation level |
|---|---|---|---|---|
| `/api/v1/search` | Простой fact-lookup, известная формулировка, no multi-hop | низкий (~1-3s) | 1 (synth) | chunks + score |
| `/api/v1/agent` | Multi-hop, нужен KG-walk, "найди всё про X через несколько прыжков" | средний (~5-30s) | 1 reasoning per loop iteration + 1 final synth | chunks + tool trace |
| `/api/v1/selfrag` | Вопросы где нужно явное "это есть в источниках, а это — неподтверждённо" | высокий (~15-60s) | reasoning × N + draft × M + retrieve per NEED | citations + uncertainties + refinement_rounds |

**Снизу-вверх:** [§ 5 hybrid retriever](#5-hybrid-retriever-deep-dive) → [§ 6 reranker](#6-reranker-deep-dive) → [§ 4 `/search`](#4-apiv1search-walkthrough) → [§ 7 `/agent`](#7-apiv1agent-react-walkthrough) → [§ 9 `/selfrag`](#9-apiv1selfrag-reflective-walkthrough).

---

## 2. Terminology cheat-sheet

| Термин | Что значит |
|---|---|
| **Node** | Единица LlamaIndex (chunk + metadata + optional embedding). В коде: `NodeWithScore` (node + similarity score). |
| **Chunk** | Текстовый фрагмент документа после semantic splitting'а (`chunk_size=512`, overlap=50). Один `TextNode` = один chunk. |
| **BM25** | Sparse retrieval, tf-idf поверх лемм. Хорош на exact-match (имена, артикулы). |
| **Dense vector** | Semantic retrieval через embedding cosine (BGE-M3 / text-embedding-3-small). Хорош на парафразах. |
| **RRF** | Reciprocal Rank Fusion. Объединение рангов из разных retrievers без калибровки скоров: `score(d) = Σ 1/(k + rank_i(d))`. |
| **Cross-encoder rerank** | Принимает (query, doc) парой, выдаёт relevance scalar. Дороже но точнее bi-encoder'а. |
| **Tool** | FunctionTool у агента: имя + описание + async callable. LLM выбирает имя в `tool_calls` ответе. |
| **submit_answer** | "Виртуальный" tool (не real callable) — флаг что агент готов завершить loop. См. § 7.2. |
| **Marker** | Inline-тэг внутри reflective draft'а: `[NEED:...]`, `[SUPPORTED:chunk_id]`, `[UNCERTAIN:...]`. См. § 9.2. |
| **Round** | Одна итерация reflective loop'а (draft → parse → maybe-retrieve → redraft). Лимит `max_refinements=3`. |
| **Synthesizer** | Финальная LLM-обёртка которая берёт (query, nodes) и пишет ответ. Default: `ResponseSynthesizer` из LlamaIndex. |

---

## 3. Architecture

Все три endpoint'а:

```
POST /api/v1/{search|agent|selfrag}
        │
        ▼ auth (X-API-Key, src/api/auth.py)
┌────────────────────────────────────────────────────────────────────┐
│ Route handler                                                      │
│  src/api/routes/{search,agent,selfrag}.py                          │
│                                                                    │
│  - DI: retriever, synthesizer, graph_retriever, llm,               │
│    chunk_repository                                                │
│  - trace_request(endpoint, query)  ← contextvar binding            │
│                                                                    │
│  /search:                                                          │
│     nodes = retriever.aretrieve(query)        ← hybrid (BM25+vec)  │
│     answer = synthesizer.asynthesize(query, nodes)                 │
│                                                                    │
│  /agent + /selfrag:                                                │
│     await agentic_react_search(                                    │
│         llm, retriever, graph_retriever,                           │
│         synthesize=<plain | reflective>,                           │
│         max_iterations=8,                                          │
│     )                                                              │
│         │                                                          │
│         ▼  inside react_agent.py                                   │
│     for step in 1..max_iterations:                                 │
│         llm.achat_with_tools(tools=[8 функций])                    │
│         if tool == submit_answer: break                            │
│         if repeat-call ≥ 3:       break                            │
│         execute tool → append observation to messages              │
│     answer = await synthesize(query, accumulated_sources)          │
│         │                                                          │
│         ▼ /selfrag substitutes synthesize=                         │
│     reflective_synthesize(...):                                    │
│         for round in 1..max_refinements+1:                         │
│             draft = llm.achat(messages + ctx)                      │
│             needs = parse_markers(draft)                           │
│             if not needs: break                                    │
│             for need in needs[:5]:                                 │
│                 extra = retriever.aretrieve(need)                  │
│                 accumulated.extend(extra)                          │
│         return ReflectiveAnswer(text, citations, uncertainties)    │
└────────────────────────────────────────────────────────────────────┘
        │
        ▼
SearchResponse → JSON
   { query, answer, mode, sources[], latency_ms, agentic_step_stats?,
     answer_detail? }
```

Визуально — [`docs/architecture.html`](../architecture.html) section 03 (pipeline) показывает место search в общей картине.

---

## 4. `/api/v1/search` walkthrough

**Самый простой** — фундамент остальных двух. Реализация целиком в [`src/api/routes/search.py:25-83`](../../src/api/routes/search.py):

```python
@router.post("/search", ...)
async def search(
    req: SearchRequest,
    retriever: FromDishka[RetrieverProtocol],
    synthesizer: FromDishka[SynthesizerProtocol],
) -> SearchResponse:
    with trace_request("search", req.query):
        with record_timed("tool_call", tool_name="vector_retrieve"):
            nodes = await retriever.aretrieve(req.query)
        ru_query = (
            "Ответь на следующий вопрос на русском языке, "
            "сохраняя имена собственные и идентификаторы дословно "
            f"из исходного языка контекста: {req.query}"
        )
        with record_timed("synthesize", n_sources=len(nodes)):
            response = await synthesizer.asynthesize(
                query=ru_query, nodes=nodes,
            )
    return SearchResponse(
        query=req.query,
        answer=getattr(response, "response", None) or str(response),
        mode=req.mode,
        sources=[
            SourceCitation(
                doc_id=str(n.node.metadata.get("doc_id") or "..."),
                chunk_id=n.node.node_id,
                content=n.node.get_content(),
                score=float(n.score or 0.0),
            )
            for n in nodes
        ],
        latency_ms=latency_ms,
    )
```

**Шаги:**
1. `retriever.aretrieve(query)` — это **HybridRetriever** (DI provides `RetrieverProtocol` → `QueryFusionRetriever` обёртка над BM25 + dense vector). Возвращает `list[NodeWithScore]`, обычно top-K=10 после RRF.
2. Префикс "Ответь … на русском языке …" — inline русско-output instruction. Без неё LlamaIndex'овский синтезатор иногда отвечает по-английски когда context chunks на английском. Граф у нас уже русифицирован при ingest'е, query всегда русский — ответ тоже должен быть русским.
3. `synthesizer.asynthesize` — стандартный `ResponseSynthesizer` LlamaIndex'а (default `TREE_SUMMARIZE` mode за кулисами).
4. Возвращаем answer + `SourceCitation[]` (chunk_id + content + score) — клиент сам решает что показывать.

**Что НЕ происходит:**
- ❌ Никакого многошагового retrieval'а — один `aretrieve` и всё
- ❌ Никакого rerank'а **в этом коде** (но **может** быть подключён внутри `RetrieverProtocol`-обёртки если DI собрал retriever с reranker — см. § 6)
- ❌ Никакого графа, никаких tool'ов, никакой reflection

**Когда использовать:** прямой fact-lookup ("какой ИНН у компании X в документе Y", "найди chunks про этот email"). Если у нас нет multi-hop или нужно прыгать через KG — берите `/agent`.

---

## 5. Hybrid retriever deep-dive

**File:** [`src/retrieval/hybrid.py`](../../src/retrieval/hybrid.py) (83 строки, влезает целиком).

Конструируется фабрикой `build_hybrid_retriever`:

```python
def build_hybrid_retriever(
    vector_index: VectorStoreIndex,
    bm25_nodes: list[BaseNode],
    *,
    similarity_top_k: int = 10,
    num_queries: int = 1,
    fusion_mode: str = "reciprocal_rerank",
    weights: list[float] | None = None,
    llm: LLM | None = None,
) -> BaseRetriever:
    vector_retriever = vector_index.as_retriever(
        similarity_top_k=similarity_top_k,
    )
    bm25_retriever = build_bm25_retriever(
        bm25_nodes, similarity_top_k=similarity_top_k,
    )
    return QueryFusionRetriever(
        retrievers=[vector_retriever, bm25_retriever],
        similarity_top_k=similarity_top_k,
        num_queries=num_queries,
        mode=fusion_mode,
        use_async=True,
        retriever_weights=weights,
        llm=llm,
    )
```

**Что под капотом:**

1. **`vector_retriever`** — `vector_index.as_retriever()` где `vector_index` — это LlamaIndex `VectorStoreIndex` поверх Milvus collection (`kb_llamaindex`). Делает k-NN cosine на embedding'ах. Embeddings от `BGE-M3` или `text-embedding-3-small` (зависит от prod-config'а), dim 768 или 1536.

2. **`bm25_retriever`** — `BM25Retriever.from_defaults(nodes=bm25_nodes, similarity_top_k=10)`. Pure-Python BM25 index в памяти. Узлы (`bm25_nodes`) приходят из `docstore` LlamaIndex'а — теже chunks что и в Milvus, просто другой index поверх них.

3. **`QueryFusionRetriever`** — фьюзит результаты двух retrievers. Default fusion mode — **`reciprocal_rerank`** (RRF). Альтернатива `"relative_score"` есть когда ты уверен в калибровке scores обоих retrievers (у нас не уверен, поэтому RRF).

4. **`num_queries=1`** — отключает встроенный LlamaIndex query expansion (он бы попросил LLM сгенерить альтернативные формулировки query). У нас **агент сам делает expansion** через repeat tool calls ("ничего по 'Иванов' — попробую 'И.И. Иванов'"), второй слой expansion — wasted tokens.

5. **`weights`** — если None, RRF treat retrievers как равные. Возможные значения: `[0.6, 0.4]` чтобы прижать BM25, `[0.3, 0.7]` чтобы прижать dense.

**RRF подробнее:** для каждого документа `d` встречающегося в обоих списках:

```
score(d) = Σ_i  1 / (k + rank_i(d))
```

где `rank_i(d)` — позиция `d` в i-том retriever'е (1-based), `k` — константа сглаживания (LlamaIndex default ~60). Документ, попавший на 1-е место в обоих списках, получает `≈ 1/61 + 1/61 = 0.0327`. На 1-е и 10-е: `≈ 1/61 + 1/70 = 0.0307`. RRF мягкий — небольшая разница в рангах не доминирует.

**Где используется:** все три endpoint'а получают **один и тот же** `HybridRetriever` через DI (`src/di/providers.py`). То есть base-retrieval идентичен; differ'ы — в loop'е поверх.

---

## 6. Reranker deep-dive

**File:** [`src/retrieval/reranker.py`](../../src/retrieval/reranker.py) (28 строк):

```python
def build_reranker(
    *,
    model_name: str = "BAAI/bge-reranker-v2-m3",
    top_n: int = 5,
) -> SentenceTransformerRerank:
    """First call downloads ``model_name`` (~1 GB for BGE-v2-m3) into
    the HuggingFace cache.  Air-gapped deploys should pre-cache."""
    return SentenceTransformerRerank(
        model=model_name,
        top_n=top_n,
    )
```

**Где подключается в pipeline:** опционально как `NodePostprocessor` у `RetrieverProtocol`. DI-провайдер может собрать chain `HybridRetriever → BGE rerank`, или оставить чистый hybrid. Решение — в `src/di/providers.py`.

**Логика:** retrieved top_k=10 nodes от hybrid'а → reranker считает cross-encoder score для каждой пары `(query, node.text)` → возвращает top_n=5 по этим scores. Семантически — гораздо точнее bi-encoder'ного RRF, потому что cross-encoder видит pair, а не каждый компонент отдельно.

**Когда отключать:**
- Latency-критично (cross-encoder работает ~50-200ms per node на CPU, на 10 nodes = 0.5-2s)
- Корпус маленький (< 1000 chunks) — RRF уже достаточно точен
- В тестах (тесты skip'ают reranker — модель тяжёлая, скачивание ~1 GB)

**Когда поднимать `top_n`:**
- Reflective synth (`/selfrag`) хочет больше контекста для draft'а → bump до 8-10
- Multi-document вопросы где надо собрать ответ из 3+ chunks → bump до 7

---

## 7. `/api/v1/agent` (ReAct) walkthrough

**Files:**
- Route: [`src/api/routes/agent.py`](../../src/api/routes/agent.py) — 67 строк
- Loop: [`src/retrieval/react_agent.py`](../../src/retrieval/react_agent.py) — основной (~450 строк)

### 7.1 System prompt

[`react_agent.py:67-92`](../../src/retrieval/react_agent.py):

```
You are a research agent answering questions over a corpus that
mixes analytical reports, email correspondence, and support-call
transcripts.  User questions arrive in Russian; the knowledge
graph (entities, descriptions, relations) is normalised to
Russian, while raw chunk text may be in any source language.

You have access to tools to look things up.  Your job:
1. Read the user's question.
2. Decide which tool to call to gather missing information.
3. Repeat until you have enough.
4. Call `submit_answer` to finalize.

Rules:
- Do NOT answer from prior knowledge — only from tool observations.
- Do NOT call `submit_answer` until you have at least one
  successful retrieval result.
- If you call the same tool with the same arguments twice and got
  the same result, stop retrying — submit what you have.
- Keep tool queries focused: one specific question per call.
- Tool queries may use Russian terms — the graph is Russian.  When
  querying for source-language strings (proper names, identifiers),
  preserve them verbatim.
- Final answer goes through a separate synthesizer that writes
  in Russian; you don't need to translate the tool outputs yourself.
```

### 7.2 Tools

В коде — 7 регистрируемых tools + один логический `submit_answer` (не FunctionTool — обрабатывается специально в loop'е).

| Tool | Args | Что делает | Файл:строка |
|---|---|---|---|
| `vector_search` | `query: str, top_k: int=10` | hybrid retrieve поверх Milvus + BM25 | `react_agent.py:112-132` |
| `graph_search` | `query: str, depth: int=2` | KG-walk через GraphRetriever; entities + relations + chunks | `:134-149` |
| `find_entity_by_id` | `name: str, entity_type: str=None` | exact lookup по canonical name (e.g. ИНН, E.164 phone) | `:151-167` |
| `find_neighbours` | `entity_name: str, hops: int=1` | 1-2 hop walk вокруг entity | `:168-180` |
| `get_chunks_by_doc_id` | `doc_id: str, limit=50, offset=0` | ВСЕ chunks одного документа в порядке появления | `:182-230` |
| `read_full_document` | `doc_id: str, max_chars=20000` | сырой raw text документа (pre-chunk, pre-translation) | `:232-258` |
| `filter_by_metadata` | `doc_id, department, doc_type` | фильтр уже собранных sources | `:260-281` |
| **`submit_answer`** | (no args) | **NOT a real tool** — флаг что готов финализировать | `:386-393` (special case в loop) |

**Description fields** (видимый LLM текст) — `:283-313`:

```python
return [
    FunctionTool.from_defaults(fn=vector_search, name="vector_search",
        description="Semantic search over text chunks. Use this for "
                    "questions where you don't know an exact entity "
                    "name yet."),
    FunctionTool.from_defaults(fn=graph_search, name="graph_search",
        description="Knowledge-graph traversal. Use when the question "
                    "involves relations between people/organizations/"
                    "topics/concepts."),
    # ... (полный список — см. react_agent.py:283-313)
]
```

### 7.3 Loop body

[`react_agent.py:319-448`](../../src/retrieval/react_agent.py) — ядро `agentic_react_search`:

```python
async def agentic_react_search(
    *, llm, retriever, graph_retriever, synthesize,
    query, max_iterations=8, mode="agent", chunk_repository=None,
):
    accumulated_sources: list[NodeWithScore] = []
    step_stats: list[AgenticStepStat] = []
    tools = _build_tools(retriever=retriever, ...,
                        accumulated_sources=accumulated_sources)
    tools_by_name = {t.metadata.name: t for t in tools}

    messages = [
        ChatMessage(role=SYSTEM, content=_SYSTEM_PROMPT),
        ChatMessage(role=USER, content=query),
    ]

    submit_requested = False
    last_call_signature = None
    repeat_count = 0

    for step_i in range(1, max_iterations + 1):
        with record_timed("llm_call", step=step_i, kind="agent_reasoning"):
            response = await llm.achat_with_tools(
                tools=tools, chat_history=messages,
            )
        tool_calls = llm.get_tool_calls_from_response(
            response, error_on_no_tool_call=False,
        )
        if not tool_calls:
            break    # модель сдалась — выходим, синтез поверх того что есть

        for tc in tool_calls:
            call_sig = f"{tc.tool_name}:{json.dumps(tc.tool_kwargs, sort_keys=True)}"
            if call_sig == last_call_signature:
                repeat_count += 1
            else:
                repeat_count = 0
            last_call_signature = call_sig

            if tc.tool_name == "submit_answer":
                submit_requested = True
                break

            tool = tools_by_name.get(tc.tool_name)
            with record_timed("tool_call", step=step_i,
                              tool_name=tc.tool_name, tool_args=tc.tool_kwargs):
                output = await tool.acall(**tc.tool_kwargs)
            obs = str(output)
            step_stats.append(AgenticStepStat(...))
            messages.append(ChatMessage(role=TOOL, content=obs, ...))

        if submit_requested:
            break
        if repeat_count >= 2:
            logger.info("agent loop  same call repeated 3× → exit")
            break

    # Synthesis
    with record_timed("synthesize", n_sources=len(accumulated_sources)):
        answer_response = await synthesize(query, accumulated_sources)
    return SearchResponse(query, answer_text, mode, ...)
```

### 7.4 Termination conditions (4 пути)

1. **`submit_answer` вызван** — happy path, агент сам решил что готов
2. **`max_iterations` исчерпан** (`=8` по умолчанию) — синтез поверх того что собрано
3. **Empty tool_calls** — модель в каком-то step'е не выбрала никакой tool (sometimes happens с qwen3-style models) → break
4. **Repeat-call guard** — одна и та же `(tool_name, kwargs)` сигнатура повторилась 3 раза → break. Защита от циклов "vector_search('X')" → "vector_search('X')" → … когда модель забыла что уже спрашивала

Все 4 пути сходятся в один `synthesize(query, accumulated_sources)` — `accumulated_sources` собирался по мере успешных retrieval'ов (closure-captured в `_build_tools`).

### 7.5 Чем `synthesize` подменяется в `/selfrag`

[`src/api/routes/selfrag.py:47-56`](../../src/api/routes/selfrag.py):

```python
last_reflective: dict = {"answer": None}
async def synth(query: str, nodes):
    answer = await reflective_synthesize(
        llm=llm, query=query, context_nodes=nodes,
        retriever=retriever,
        max_refinements=req.max_refinements,
    )
    last_reflective["answer"] = answer
    return answer

result = await agentic_react_search(..., synthesize=synth, mode="selfrag")
```

Тот же ReAct loop — но `submit_answer` → `reflective_synthesize`, который сам делает draft → markers → re-retrieve → redraft. См. § 9.

---

## 8. Graph retriever (`graph_search` tool detail)

**File:** [`src/graph/retriever.py`](../../src/graph/retriever.py) (77 строк):

```python
class GraphRetriever:
    def __init__(self, pg_index: PropertyGraphIndex, *,
                 similarity_top_k: int = 10, path_depth: int = 1,
                 include_text: bool = True):
        self._retriever = pg_index.as_retriever(
            similarity_top_k=similarity_top_k,
            path_depth=path_depth,
            include_text=include_text,
        )

    async def aretrieve(self, query: str) -> RoundGraphData:
        nodes = await self._retriever.aretrieve(query)
        out = RoundGraphData()
        for n in nodes:
            cls = type(n.node).__name__
            md = n.node.metadata or {}
            if cls in {"EntityNode", "ChunkNode"} and md.get("triplet_source_id"):
                out.relations.append({
                    "src_id": md.get("subj") or md.get("src") or "",
                    "tgt_id": md.get("obj") or md.get("tgt") or "",
                    "label": md.get("rel_type") or md.get("label") or "",
                    "description": text,
                })
            elif cls == "EntityNode":
                out.entities.append({
                    "entity_name": md.get("name") or text,
                    "entity_type": md.get("label") or "",
                    "description": text,
                })
            else:
                out.chunks.append(n)
        return out
```

**Стратегия:**
- `PropertyGraphIndex.as_retriever(...)` под капотом по умолчанию использует **`LLMSynonymRetriever`** который **через LLM** нормализует query terms (например "Ваня" → "Иван") **до** обхода графа. Это значит `graph_search` стоит дороже `vector_search` (один extra LLM call), но точнее на cross-language / multi-form lookup'ах.
- `path_depth=1` — 1-hop traversal от matched entity. Хочешь 2-hop — bump в DI providers.
- `include_text=True` — возвращает chunk-text узлов, привязанных к matched entity/relation.

**Возвращает `RoundGraphData`**:
```python
@dataclass
class RoundGraphData:
    entities: list[dict] = ...      # {entity_name, entity_type, description}
    relations: list[dict] = ...     # {src_id, tgt_id, label, description}
    chunks: list[NodeWithScore] = ... # для accumulated_sources синтеза
```

Когда `graph_search` или `find_*` tool успешно отработал — `chunks` отправляются в `accumulated_sources` (общий список с тем что vector_search принёс). Entities + relations — в JSON-наблюдение для агента (он их видит в следующей итерации).

---

## 9. `/api/v1/selfrag` (Reflective) walkthrough

**Files:**
- Route: [`src/api/routes/selfrag.py`](../../src/api/routes/selfrag.py) (86 строк)
- Loop: [`src/retrieval/reflective_synth.py`](../../src/retrieval/reflective_synth.py) (~310 строк)

Поверх ReAct loop'а (тот же `agentic_react_search`) — другой synthesizer вместо плоского.

### 9.1 System prompt

[`reflective_synth.py:80-108`](../../src/retrieval/reflective_synth.py):

```
You are a careful research-assistant LLM writing an answer over a
mixed corpus of analytical reports, emails, and support-call
transcripts.

WRITE YOUR ANSWER IN RUSSIAN.  Some context items may be in
English — translate the meaning into Russian in your prose, but
quote proper nouns, identifiers, drug names, and any text inside
backticks verbatim from the source.

Inline-annotate your draft with these three markers — verbatim:

- [NEED:что не хватает]   — write this when, mid-sentence, you
  realise you'd need more information to support a claim.  Be
  specific: [NEED:даты запуска онбординга в Q1 2024], NOT
  [NEED:больше деталей].
- [SUPPORTED:chunk_id]    — when a claim is grounded by a specific
  chunk you've seen in the context, append this marker right after
  the claim.  `chunk_id` is the literal id from the context items.
- [UNCERTAIN:причина]     — when you can't support a piece of the
  answer from context but the question explicitly asked about it,
  write [UNCERTAIN:причина] instead of guessing.  DO NOT hallucinate.

Rules:
- Do NOT answer from prior knowledge — only from the context items.
- Names, organizations, canonical identifiers (emails, phones,
  INN/OGRN/BIC, dates) — preserve verbatim from source language.
- Keep the draft tight — answer the question, don't pad.
```

### 9.2 Маркеры

| Маркер | Семантика | Парсится | Что происходит |
|---|---|---|---|
| `[NEED:текст]` | "Я бы здесь дописал X но context не покрывает" | `_NEED_RE = re.compile(r"\[NEED:([^\]]+)\]", re.IGNORECASE)` ([line 130](../../src/retrieval/reflective_synth.py)) | Driver — следующий round делает `retriever.aretrieve(текст)` для каждого NEED |
| `[SUPPORTED:chunk_id]` | "Это утверждение grounded на этом chunk'е" | `_SUPPORTED_RE` ([line 131](../../src/retrieval/reflective_synth.py)) | Маппится в final `citations[]` через `_build_citations(supports, accumulated)` |
| `[UNCERTAIN:причина]` | "Я не могу подтвердить, но вопрос требует ответа" | `_UNCERTAIN_RE` ([line 132](../../src/retrieval/reflective_synth.py)) | Остаётся в final text + попадает в `uncertainties[]` |

`strip_markers(draft, keep_uncertain=True)` ([line 145](../../src/retrieval/reflective_synth.py)) выводит финальный текст — `[NEED]` и `[SUPPORTED]` стираются, `[UNCERTAIN]` остаются видимыми (это user-relevant honesty signal).

### 9.3 Loop

[`reflective_synth.py:188-300`](../../src/retrieval/reflective_synth.py):

```python
async def reflective_synthesize(
    *, llm, query, context_nodes,
    retriever=None,
    max_refinements: int = 3,
) -> ReflectiveAnswer:
    accumulated = list(context_nodes)
    draft = ""

    for round_i in range(max_refinements + 1):
        if round_i == 0:
            user_msg = f"Question: {query}\n\nContext items:\n{_format_context(accumulated)}"
        else:
            needs, _, _ = parse_markers(draft)
            gaps_summary = "; ".join(needs) or "(parser found none)"
            user_msg = _REFINE_PROMPT.format(
                gaps_summary=gaps_summary, previous_draft=draft,
            ) + f"\n\nFull current context items:\n{_format_context(accumulated)}"

        messages = [
            ChatMessage(role=SYSTEM, content=_SYSTEM_PROMPT),
            ChatMessage(role=USER, content=user_msg),
        ]

        with record_timed("llm_call", round=round_i, kind="reflective_draft"):
            response = await llm.achat(messages)
        draft = strip_thinking(response.message.content or "").strip()
        needs, _, _ = parse_markers(draft)

        record_event("refinement_round", payload={
            "round": round_i,
            "needs": len(needs),
            "n_context_nodes": len(accumulated),
        })

        if not needs:
            break
        if round_i >= max_refinements:
            break
        if retriever is None:
            break    # no way to fix gaps — leave as UNCERTAIN

        for need in needs[:5]:    # protect budget
            with record_timed("tool_call", tool_name="retrieve_for_need", query=need):
                extra = await retriever.aretrieve(need)
            accumulated.extend(extra)
        accumulated = deduplicate_nodes(accumulated)

    needs, supports, uncertains = parse_markers(draft)
    final_text = strip_markers(draft, keep_uncertain=True)
    citations = _build_citations(supports, accumulated)
    uncertainties = [ReflectiveUncertainty(topic="", reason=u) for u in uncertains]
    return ReflectiveAnswer(text=final_text, citations=citations,
                            uncertainties=uncertainties,
                            refinement_rounds=round_i)
```

### 9.4 Refine prompt

Когда нашли NEEDs и идём на следующий round — добавляется специальная инструкция [`reflective_synth.py:111-124`](../../src/retrieval/reflective_synth.py):

```
Your previous draft below was missing information for these gaps:
{gaps_summary}

Additional context was retrieved.  Update the draft so that any
[NEED:...] markers are resolved — either replaced with a grounded
claim + [SUPPORTED:chunk_id] marker, or with [UNCERTAIN:reason]
if the additional context still doesn't cover the gap.

Previous draft:
"""
{previous_draft}
"""
```

### 9.5 Termination

| Условие | Что значит |
|---|---|
| **No NEEDs в draft'е** | Самый частый exit — модель не нашла ничего ненасыщенного |
| `round_i >= max_refinements` (3) | Защита от бесконечного цикла |
| `retriever is None` | Если caller не передал retriever — нечем закрывать NEEDs |
| `needs[:5]` capping | На один round максимум 5 retrieval'ов — если NEEDs больше, остальные игнорятся в этом round'е (могут вернуться в следующем) |

### 9.6 Response shape

`SearchResponse.answer_detail` ← `ReflectiveAnswerDetail` ([selfrag.py:72-76](../../src/api/routes/selfrag.py)):

```python
result.answer_detail = ReflectiveAnswerDetail(
    citations=ra.citations,                # list of (chunk_id, doc_id, score)
    uncertainties=ra.uncertainties,         # list of [UNCERTAIN] reasons
    refinement_rounds=ra.refinement_rounds, # int 0..max_refinements
)
```

Клиент может показать "ответ с 2 раундами уточнения, 3 источниками, 1 неопределённость".

---

## 10. Configuration knobs

[`src/config.py:281-310`](../../src/config.py) — class `AgentSettings`:

```python
class AgentSettings(BaseSettings):
    """Knobs for the agentic search endpoints (`/agent`, `/selfrag`)."""

    model_config = SettingsConfigDict(env_prefix="AGENT_", env_file=".env", extra="ignore")

    # Legacy judge-based loop (kept for R9 baseline eval).
    max_rounds: int = Field(default=3, ge=1, le=10)
    top_k: int = 10
    # ReAct loop (R7): how many tool-call iterations before forcing
    # `submit_answer`.
    max_iterations: int = Field(default=8, ge=1, le=20)
    # Entity Resolution (cross-language / multi-form dedup).
    er_enabled: bool = True
    # Pairs per LLM-judge call when ER routes borderline candidates.
    er_judge_batch_size: int = Field(default=10, ge=1, le=50)
    # Reflective synthesis (R8): how many draft → critique → retrieve
    # → redraft rounds the synthesizer attempts.
    max_refinements: int = Field(default=3, ge=0, le=10)
    # R10: legacy judge-based agentic_search remains in the codebase
    # as a comparative baseline for R9 eval.
    enable_legacy_agent: bool = False
```

**Полная сводная таблица tuning knobs:**

| Knob | Env var | Default | Range | Где влияет | Когда трогать |
|---|---|---|---|---|---|
| `max_iterations` | `AGENT_MAX_ITERATIONS` | 8 | 1-20 | ReAct loop в `/agent` и `/selfrag` | Поднять до 12-15 для multi-hop кейсов; опустить до 4-5 для быстрого ответа |
| `max_refinements` | `AGENT_MAX_REFINEMENTS` | 3 | 0-10 | Reflective loop в `/selfrag` | 0 = выключить refinement (draft без re-retrieve); 5+ для precision-critical |
| `top_k` (hybrid) | `AGENT_TOP_K` | 10 | int | `similarity_top_k` в hybrid retriever | Корпус малый (<1000 chunks) → 5; большой → 15-20 |
| `top_n` (reranker) | внутри `build_reranker` | 5 | int | После rerank финальное число chunks → synthesizer | Reflective хочет больше context → 8-10; latency-критично → 3 |
| LLM-модель | `LITELLM_SEARCH_MODEL` | fallback `LITELLM_LLM_MODEL` | string | LLM для `/agent` ReAct + `/selfrag` reflective | См. [`MODELS.md`](../MODELS.md), `multimodel.md` § 3 |
| Function calling | `LITELLM_FUNCTION_CALLING` | true | bool | `OpenAILike` `is_function_calling_model` | `false` для models которые не делают tool calls (llama3.1:8b) |
| Legacy agent gate | `AGENT_ENABLE_LEGACY_AGENT` | false | bool | Mount `/api/v1/legacy/agent` | true только для R9 baseline сравнений |

**Знaeте по факту в коде литералы которые НЕ в конфиге:**

| Литерал | Где | Что значит |
|---|---|---|
| `num_queries=1` | `hybrid.py:50` | Query expansion off (агент сам делает) |
| `fusion_mode="reciprocal_rerank"` | `hybrid.py:51` | RRF (vs "relative_score") |
| `path_depth=1` | `retriever.py:41` (graph) | 1-hop KG walk |
| `chunk_size=512`, `overlap=50` | `IngestionSettings` | Размер chunks при ingest'е |
| `needs[:5]` | `reflective_synth.py:265` | Максимум 5 re-retrieve'ов на один refinement round |
| `repeat_count >= 2` | `react_agent.py:425` | "Та же tool-call сигнатура 3× → break" |

---

## 11. Prompts catalog

| Prompt | File:line | Используется в |
|---|---|---|
| ReAct system prompt | [`react_agent.py:67-92`](../../src/retrieval/react_agent.py) | `/agent` + `/selfrag` (внешний ReAct loop) |
| Reflective system prompt | [`reflective_synth.py:80-108`](../../src/retrieval/reflective_synth.py) | `/selfrag` (внутренний synth) |
| Reflective refine prompt | [`reflective_synth.py:111-124`](../../src/retrieval/reflective_synth.py) | `/selfrag` (round >= 1) |
| `/search` inline RU-instruction | [`search.py:48-51`](../../src/api/routes/search.py) | `/search` |
| Tool descriptions | [`react_agent.py:283-313`](../../src/retrieval/react_agent.py) | Все три `/agent + /selfrag`-style endpoints |
| Legacy judge prompt | [`judge.py:37-61`](../../src/retrieval/judge.py) | `/legacy/agent` only (R10 baseline) |

**Что общее:** все промпты предписывают **русский output** + **verbatim** для proper nouns/identifiers/dates. Это потому что граф нормализован в русский при ingest'е, а user-queries тоже русские; chunk text может быть любого исходного языка.

---

## 12. Observability — что emit'ится per endpoint

[`src/observability/trace.py`](../../src/observability/trace.py) — структурированный per-request trace, **отдельный** от ingest-`ingest_metrics` (не путать!).

### 12.1 Trace data model

```python
@dataclass
class TraceEvent:
    name: str             # "llm_call" | "tool_call" | "synthesize" | "refinement_round"
    payload: dict         # tool_name, step, kind, query, ...
    duration_ms: float    # wall-time для этого event
    ts_offset_ms: float   # ms от начала trace

@dataclass
class Trace:
    request_id: str
    endpoint: str         # "search" | "agent" | "selfrag"
    query: str
    events: list[TraceEvent]
```

### 12.2 События per endpoint

| Endpoint | Что emit'ит | Span name | Payload |
|---|---|---|---|
| `/search` | 1 retrieve + 1 synth | `tool_call (vector_retrieve)`, `synthesize` | `n_sources` |
| `/agent` | N×reasoning + M×tool + 1 synth | `llm_call (kind=agent_reasoning, step=i)`, `tool_call (tool_name, step)`, `synthesize` | per-step args |
| `/selfrag` | Всё что у `/agent` + per-round draft + per-NEED retrieve | + `llm_call (kind=reflective_draft, round=i)`, `refinement_round (round, needs, n_context_nodes)`, `tool_call (tool_name=retrieve_for_need, query=need)` | round counter |

### 12.3 Trace.export() shape

```json
{
  "request_id": "abc123",
  "endpoint": "selfrag",
  "query": "...",
  "events": [
    {"name": "llm_call", "payload": {"step": 1, "kind": "agent_reasoning"}, "duration_ms": 432.1, "ts_offset_ms": 12.0},
    {"name": "tool_call", "payload": {"tool_name": "vector_search", ...}, "duration_ms": 78.3, "ts_offset_ms": 446.4},
    ...
    {"name": "refinement_round", "payload": {"round": 1, "needs": 2, ...}, "duration_ms": 0, "ts_offset_ms": 5821.0}
  ],
  "summary": {
    "n_tool_calls": 4,
    "n_llm_calls": 7,
    "n_refinements": 2,
    "total_ms": 8412.3,
    "tool_breakdown": {"vector_search": 2, "graph_search": 1, "retrieve_for_need": 1}
  }
}
```

### 12.4 Где это смотреть

- В **stdlib log** (loguru bind): `trace done endpoint=selfrag rid=abc123 summary={...}` — search/grep по request_id
- **LangFuse** (если включена) — `src/observability/trace.py` имеет hook для LangFuse-callback, видит `llm_call` события и связывает с LiteLLM-traces
- **TODO (Phase 2):** `/api/v1/search/trace/{request_id}` endpoint для retrieve полного trace (заготовка есть, не подключена)

---

## 13. Score thresholds & tuning matrix

**Текущие литералы** (full list):

| Значение | Где | Default | Когда менять |
|---|---|---|---|
| `similarity_top_k` (hybrid) | `hybrid.py:32, 49`, `vector_index.py:69` | 10 | Малый корпус → 5; рост precision-needs → 15-20 |
| `top_n` (reranker) | `reranker.py:17` | 5 | Reflective хочет шире контекст → 8-10 |
| `max_iterations` (ReAct) | `react_agent.py:326` + `AgentSettings:293` | 8 | Multi-hop сложные → 12; быстрый ответ → 4 |
| `max_refinements` (reflective) | `reflective_synth.py:194` + `AgentSettings:305` | 3 | 0 = выключить; 5+ для critical-precision |
| `num_queries` (RRF) | `hybrid.py:50` | 1 | Не трогать — agent expand'ит сам, второй слой = waste |
| `chunk_size` (ingest) | `IngestionSettings` | 512 | Не trogаем без re-ingest; короткие docs → 256 |
| `chunk_overlap` (ingest) | `IngestionSettings` | 50 | Поднять для тематически-плотных docs → 100 |
| `needs[:5]` cap | `reflective_synth.py:265` | 5 | Не trogат без обвязки тестами — может взорвать budget |
| `repeat_count >= 2` | `react_agent.py:425` | 2 | Loose → 3 (агент рискует зациклиться); strict → 1 |
| `path_depth` (graph) | `retriever.py:41` | 1 | 2 для multi-hop graph kein "найди всех соседей соседей" |

**`similarity_score_threshold` — отсутствует**. Score-based filter не подключён в pipeline'е: даже если cosine 0.2 — chunk попадает в context. Это намеренно: RRF не калиброван, threshold не имеет универсального смысла. Если нужно отрезать "слабые" — это работа reranker'а (он переоценит и оставит top_n).

---

## 14. Tests как живая документация

`tests/test_retrieval/`:

| Файл | Что покрывает |
|---|---|
| [`test_agent.py`](../../tests/test_retrieval/test_agent.py) | Legacy judge-based agentic_search loop (multi-round retrieval) |
| [`test_react.py`](../../tests/test_retrieval/test_react.py) | ReAct loop control: tool routing, submit_answer termination, max_iterations cap, repeat-call guard |
| [`test_reflective.py`](../../tests/test_retrieval/test_reflective.py) | Reflective: marker parsing, citation mapping, NEED-driven refinement, max_refinements budget |
| [`test_hybrid.py`](../../tests/test_retrieval/test_hybrid.py) | Hybrid RRF fusion, BM25/vector score combination |
| [`test_judge.py`](../../tests/test_retrieval/test_judge.py) | Legacy judge (via_structured + text fallback) |
| [`test_agent_graph.py`](../../tests/test_retrieval/test_agent_graph.py) | Graph retriever node classification, chunk accumulation |
| [`test_vector_index.py`](../../tests/test_retrieval/test_vector_index.py) | Milvus integration insertion/retrieval |
| [`test_common.py`](../../tests/test_retrieval/test_common.py) | `deduplicate_nodes`, `strip_thinking`, citation helpers |
| [`test_llm_factory.py`](../../tests/test_retrieval/test_llm_factory.py) | Per-role LLM instantiation (`build_search_llm` for this domain) |

**Gaps:**
- ❌ Integration tests на сами endpoints (`POST /search`, `/agent`, `/selfrag`) отсутствуют — тестируется только loop, не route handler
- ❌ Reranker не покрыт unit-тестами (модель тяжёлая, skip в тестах)
- ❌ Trace-export не имеет дедикейтного теста (есть `test_trace.py` в `test_observability/`, но search-spans не проверены)

---

## 15. Troubleshooting

| Симптом | Причина | Действие |
|---|---|---|
| `/agent` завершается без `submit_answer` | `max_iterations` исчерпан / repeat-call guard / empty tool_calls. Sintez поверх частичных sources | Посмотреть trace.summary `n_tool_calls` + `tool_breakdown`. Если `n_tool_calls=0` — модель не делает function calls (см. `LITELLM_FUNCTION_CALLING`). Если 8 — поднять `max_iterations` |
| Repeat-call guard срабатывает | Агент зациклился на одной формулировке: `vector_search('X')` → пусто → `vector_search('X')` → пусто | Это `feature, not bug` — guard правильно останавливает. Проверить почему `X` не находится: BM25 + vector обе пустые? → проверить ingest'нулся ли документ; canonical_identifiers индексированы? |
| Reflective крутит N раундов на тривиальном вопросе | Модель ставит `[NEED:...]` маркеры на caveat'ы которых не должно быть | (a) поднять контекст-window: `top_n=8` в reranker; (b) опустить `max_refinements` до 1; (c) inspect first draft — найти что именно модель считает gap'ом |
| `/selfrag` answer содержит [UNCERTAIN:...] | Это правильное поведение — модель честно признаётся что context не покрывает | Это в `result.answer_detail.uncertainties[]`. Если слишком много — дополнить документы или поднять `top_n` |
| Reranker увеличил latency × N | Cross-encoder работает 50-200ms/node на CPU; 10 nodes ≈ 1-2s | (a) GPU-инстанс LiteLLM; (b) bypass reranker для не-критичных endpoints (передать `RetrieverProtocol` без NodePostprocessor); (c) урезать input top_k до 5 |
| `hybrid` возвращает пусто | (a) BM25 index не построен — нужно `bm25_nodes` от docstore; (b) Milvus не reachable; (c) embeddings пустые в Milvus (ingest падал) | Проверить `psql -c "SELECT status, COUNT(*) FROM documents GROUP BY status"` — если все `failed` → ingest сломан; затем `pymilvus.list_collections()` показывает kb_llamaindex |
| `graph_search` всегда возвращает empty | `graph_retriever=None` в DI → Neo4j unreachable / `PropertyGraphIndex` пустой | Проверить `cypher-shell -p ... "MATCH (n:__Entity__) RETURN count(n)"` — если 0, KG не был построен. См. [`multimodel.md`](multimodel.md) § 4 (vector_only fallback может означать что graph half не запустился) |
| `read_full_document` возвращает Error | Файл удалён из MinIO / `chunk_repository` не настроен | Проверить `documents.status` для doc_id; если `vector_only` или `completed` но source-file удалён → `mc cp` восстановление либо ингест заново |
| `403`/`401` на endpoint | API-key не передан / X-API-Key неправильный | Header `X-API-Key: dev-local-key` (или то что в `.env`'s `API_KEYS`); см. [`src/api/auth.py`](../../src/api/auth.py) |
| Trace pусто после вызова | Activity не входил в `with trace_request(...)` блок (handler bug) | Все три route handler'а сейчас обёрнуты — если эта проблема возникла, проверить что route handler не ломанулся раньше before-trace |

---

## 16. Cross-references

- **[`docs/runbook/multimodel.md`](multimodel.md)** — где описано как `LITELLM_SEARCH_MODEL` тянется в DI и используется во всех трёх search-endpoint'ах. Сменить модель search'а — там.
- **[`docs/runbook/analytics.md`](analytics.md)** — Grafana / Postgres `ingest_metrics`. **К search'у не относится** — `ingest_metrics` это **ingest**-side analytics. Search-side observability — в [§ 12 выше](#12-observability--что-emitится-per-endpoint) (Trace events, не идут в Postgres).
- **[`docs/QUERY.md`](../QUERY.md)** — старый верхне-уровневый обзор search'а (до R7/R8). Этот runbook его заменяет в части деталей; QUERY.md оставлен как short-overview.
- **[`docs/MODELS.md`](../MODELS.md)** — выбор модели per role, capability flags (`LITELLM_FUNCTION_CALLING`).
- **[`docs/architecture.html`](../architecture.html)** — section 03 (pipeline) визуально показывает search в общей картине.
- **Specs:** `docs/superpowers/specs/2026-05-15-ingest-temporal-workflow-design.md` — ingest-сторона; search-side specs пока не оформлены.

---

## 17. Quick-reference: где что лежит

```
src/api/routes/
├── search.py       ← /api/v1/search        (single-shot)
├── agent.py        ← /api/v1/agent         (ReAct)
├── selfrag.py      ← /api/v1/selfrag       (ReAct + reflective)
└── legacy_agent.py ← /api/v1/legacy/agent  (R10 baseline, gated)

src/retrieval/
├── hybrid.py            ← QueryFusionRetriever(BM25, dense, RRF)
├── reranker.py          ← BGE-v2-m3 cross-encoder
├── vector_index.py      ← Milvus VectorStoreIndex builder
├── react_agent.py       ← ReAct loop + 7 tools + submit_answer
├── reflective_synth.py  ← marker-driven re-retrieve loop
├── agent.py             ← legacy judge-based loop  (R10)
├── judge.py             ← legacy LLM judge         (R10)
├── llm.py               ← build_search_llm + role factories (multimodel)
├── query_engine.py      ← ResponseSynthesizer wrapper
└── _common.py           ← deduplicate_nodes, strip_thinking, citations

src/graph/
└── retriever.py    ← GraphRetriever — PropertyGraphIndex обёртка

src/observability/
└── trace.py        ← TraceEvent / Trace / trace_request

src/config.py:281-310  ← AgentSettings (env_prefix=AGENT_)
```
