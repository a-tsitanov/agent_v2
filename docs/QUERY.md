# Query lifecycle

How a single user query becomes an answer across the three
endpoints.  Read after `docs/ARCHITECTURE.md` for the system map.

---

## Overview — three endpoints, two algorithms

| Endpoint | Outer loop | Inner generation | Typical LLM calls | Latency on gpt-4o-mini |
|---|---|---|---|---|
| `/api/v1/search` | none — single retrieve | LlamaIndex `ResponseSynthesizer` (compact) | **1** | 5-20 s |
| `/api/v1/agent` | ReAct agent (max 8 iterations) | plain ResponseSynthesizer over accumulated nodes | 3-8 | 20-90 s |
| `/api/v1/selfrag` | ReAct agent (max 8 iterations) | `reflective_synthesize` (markers + re-retrieve) | 4-12 | 30-120 s |
| `/api/v1/legacy/agent` | judge-based loop (max 3 rounds, gated) | ResponseSynthesizer | 6-15 | 60-180 s |

All four hit the same retrieval stack underneath: Milvus dense
retriever + optional Neo4j graph retriever + (NEW) `ChunkRepository`
for doc-id keyed access.

---

## Common preamble (all endpoints)

```
HTTP POST /api/v1/<endpoint>
  X-API-Key: <key>             ← require_api_key dep checks against API_KEYS allow-list
  Content-Type: application/json
  {
    "query": "русский вопрос",
    "max_iterations": 8,       ← /agent + /selfrag only
    "max_refinements": 3,      ← /selfrag only
  }

       │
       ▼
src/api/routes/<endpoint>.py:search_*  (Dishka @inject)
       │
       │  resolves from container:
       │   • llm: LLM             (LiteLLM-proxied gpt-4o-mini)
       │   • retriever            (VectorIndexRetriever, similarity_top_k=10)
       │   • synthesizer          (LlamaIndex compact ResponseSynthesizer)
       │   • graph_retriever      (Neo4j PropertyGraphIndex retriever, or None)
       │   • chunk_repository     (Milvus filter-query + Postgres lookup + FS read)
       │
       ▼
trace_request(endpoint, query)  ← context-var Trace begin
```

---

## `/api/v1/search` — plain retrieve + synthesize

```
src/api/routes/search.py:search

  1. retriever.aretrieve(query)
        → Milvus filter: ANN over `embedding` column,
          top_k=10 chunks by cosine similarity
        → list[NodeWithScore] (each node = TextNode with
          original-language `text` + metadata)

  2. ru_query = f"Ответь на следующий вопрос на русском "
                f"языке, сохраняя имена собственные ...: {query}"
        ← Russian-output instruction wrapper (the default
          LlamaIndex synthesizer prompt is English-leaning;
          this nudges it consistently into RU)

  3. synthesizer.asynthesize(query=ru_query, nodes=nodes)
        → COMPACT response mode: stuffs nodes into a single
          prompt, one LLM call
        → response.response (str)

  4. SearchResponse(
        query, answer, mode='hybrid',
        sources=[SourceCitation(doc_id, chunk_id, content, score)
                 for n in nodes],     ← FULL chunk text, no truncation
        latency_ms,
     )
```

One LLM call. No retrieval refinement. Fastest. Use when latency
matters more than reasoning quality.

---

## `/api/v1/agent` — ReAct loop

`src/api/routes/agent.py` → `agentic_react_search` in
`src/retrieval/react_agent.py`.

### State

```python
accumulated_sources: list[NodeWithScore] = []
step_stats:          list[AgenticStepStat] = []
messages = [SystemMessage(...), UserMessage(query)]
```

### Tools (8 total)

| Tool | Backed by | Effect |
|---|---|---|
| `vector_search(query, top_k=10)` | `retriever.aretrieve(...)` | Top-k dense Milvus, **appends to accumulated_sources** |
| `graph_search(query, depth=2)` | `graph_retriever.aretrieve(...)` | Neo4j entity + relation lookup. Returns entities, relations, chunks (if any) → accumulated |
| `find_entity_by_id(name, entity_type=None)` | `graph_retriever.aretrieve(name)` filtered by entity_type | Exact entity lookup |
| `find_neighbours(entity_name, hops=1)` | `graph_retriever.aretrieve(entity_name)` | 1-2 hop graph walk |
| `filter_by_metadata(doc_id, department, doc_type)` | filters `accumulated_sources` in-memory | Scopes already-fetched context |
| **`get_chunks_by_doc_id(doc_id, limit=50, offset=0)`** **NEW** | `chunk_repository.aget_chunks_by_doc_id(...)` → Milvus filter-query, ordered by `position` | Fetches every chunk of one document. **Appends to accumulated_sources**. Use for "everything from this email thread / chapter / file" intents. |
| **`read_full_document(doc_id, max_chars=20000)`** **NEW** | `chunk_repository.aread_document_text(...)` → Postgres lookup + FS read with cap | Reads the **raw uploaded source file** (pre-chunk, pre-translation). Use sparingly — context blowup. |
| `submit_answer(query_recap, gathered_source_ids)` | Triggers the synthesizer over `accumulated_sources` | Finalises the loop |

### Loop

```
for step_i in 1..max_iterations:
    response = await llm.achat_with_tools(tools, messages)
        ↑ ONE LLM call per iteration
    tool_calls = llm.get_tool_calls_from_response(response)
    if no tool_calls: break

    for tc in tool_calls:
        if tc.tool_name == "submit_answer":
            submit_requested = True
            break

        # Anti-loop guard: same tool + same args 3× in a row → exit
        if call_signature == last_signature: repeat_count++
        if repeat_count >= 2: break

        observation = await tool.acall(**tc.tool_kwargs)
        record_event("tool_call", tool_name=tc.tool_name, ...)
        messages.append(ToolMessage(observation, tool_call_id=tc.tool_id))

    if submit_requested: break

# Synthesis (1 more LLM call)
response = await synthesize(query, accumulated_sources)
return SearchResponse(answer=response.response, sources=[...], agentic_step_stats=...)
```

Typical pattern: `vector_search → submit_answer` (2 iterations) for
simple questions; `vector_search → graph_search → get_chunks_by_doc_id →
submit_answer` (4 iterations) for "give me the thread context"
multi-hop questions.

### Cost example

| Question type | Tools invoked | LLM calls |
|---|---|---|
| Single-fact ("what is BCC?") | `vector_search` → `submit_answer` | 1 reasoning + 1 synth = **2** |
| Multi-hop ("what risks are linked to UV?") | `vector_search` → `graph_search` → `submit_answer` | 3 reasoning + 1 synth = **4** |
| Document summary ("summarise the report") | `vector_search` → `get_chunks_by_doc_id` → `submit_answer` | 3 + 1 = **4** |
| Original document verification ("quote the contract clause 5") | `vector_search` → `read_full_document` → `submit_answer` | 3 + 1 = **4** |

---

## `/api/v1/selfrag` — ReAct + reflective synthesis

`src/api/routes/selfrag.py` reuses `agentic_react_search` from the
agent endpoint, but **wires a different synthesizer**: the inner
`submit_answer` triggers `reflective_synthesize` (in
`src/retrieval/reflective_synth.py`).

### Reflective synthesis algorithm

```
for round_i in 0..max_refinements:
    prompt = SYSTEM (RU output, marker rules) + context
    draft  = await llm.achat(prompt)
    needs, supports, uncertains = parse_markers(draft)
        ←  [NEED:topic]       extracted via regex
        ←  [SUPPORTED:chunk_id]
        ←  [UNCERTAIN:reason]

    if not needs or round_i >= max_refinements or retriever is None:
        break

    for need in needs[:5]:
        extra = await retriever.aretrieve(need.topic)
        accumulated.extend(extra)
    deduplicate by node_id
    # next round: redraft with the expanded context

final = strip_markers(draft, keep_uncertain=True)
return ReflectiveAnswer(
    text=final,                  ← Russian, no NEED/SUPPORTED markers,
                                   UNCERTAIN stays visible
    citations=[ReflectiveCitation(chunk_id) for c in valid_supports],
    uncertainties=[ReflectiveUncertainty(reason) for u in uncertains],
    refinement_rounds=round_i,
)
```

The `ReflectiveAnswer` is mapped into `SearchResponse.answer_detail`
(citations + uncertainties + refinement_rounds) on top of the usual
`answer` + `sources`.

### When `/selfrag` is worth the extra cost

Use it when:
* Answer needs verifiable citations per claim.
* Uncertain claims must be flagged explicitly (regulated /
  legal / medical context).
* The chunk distribution after initial retrieve is uneven and
  re-retrieve might surface better context.

Don't use it for plain factoid Q&A — pay 2× the latency for zero
benefit; `/agent` is enough.

---

## Side-channels: observability + trace

Every endpoint wraps its outer work in `trace_request(...)`:

```python
with trace_request("agent", req.query) as trace:
    result = await agentic_react_search(...)
```

Inside the agent loop / tools / synthesizer:

* `record_event("tool_call", payload={"tool_name": "vector_search"})`
* `record_event("llm_call",  payload={"kind": "agent_reasoning"})`
* `record_event("refinement_round", payload={"round": 0, "needs": 2})`
* `record_timed(name, **payload)` — timed variant.

At request end, the wrapper logs a summary via loguru:

```
trace done  endpoint=agent  rid=ab12cd  summary={
  'n_tool_calls': 3, 'n_llm_calls': 5, 'n_refinements': 0,
  'total_ms': 24300.2,
  'tool_breakdown': {'vector_search': 1, 'graph_search': 1, 'submit_answer': 1},
}
```

Concurrent requests stay isolated via ContextVar; async tasks
spawned inside the trace block inherit it.

---

## Response shape

All four endpoints return `SearchResponse`:

```python
{
  "query": "...",
  "answer": "Russian answer text",
  "mode": "hybrid" | "agent" | "selfrag" | "legacy",
  "sources": [
    {
      "doc_id": "...",
      "chunk_id": "...",
      "position": 0,
      "content": "FULL ORIGINAL-LANGUAGE chunk text",
      "score": 0.83,
      "department": "...",
      "doc_type": "..."
    },
    ...
  ],
  "latency_ms": 24300.2,

  # /agent + /selfrag only:
  "agentic_step_stats": [
    {
      "step": 1,
      "tool_name": "vector_search",
      "tool_args": {"query": "..."},
      "observation_summary": "[...truncated 300 chars...]",
      "reasoning_excerpt": ""   # reserved
    },
    ...
  ],

  # /selfrag only:
  "answer_detail": {
    "citations": [{"claim": "", "chunk_id": "c1"}, ...],
    "uncertainties": [{"topic": "", "reason": "..."}, ...],
    "refinement_rounds": 1
  },

  # /legacy/agent only (when AGENT_ENABLE_LEGACY_AGENT=true):
  "agentic_rounds": 3,
  "follow_up_queries": ["...", "..."],
  "agentic_round_stats": [...],
}
```

---

## Russian-output guarantee

Three independent enforcement points so the answer is **always Russian**
even when the source corpus is English:

1. **Ingest**: `TranslateToRussianTransform` populates
   `node.metadata["translated_text"]` with Russian text. The
   LightRAG extractor reads from there → entity names + descriptions
   land in Russian in Neo4j.
2. **Reflective synth + ReAct system prompts** hard-code
   "WRITE YOUR ANSWER IN RUSSIAN".
3. **Plain /search** wraps the user query with a Russian-output
   instruction before passing to LlamaIndex's default synthesizer.

The English source chunks are still preserved in Milvus + Neo4j
`:Chunk` nodes and returned verbatim in `sources[].content` — the
answer reads in Russian, citations stay in source language.

---

## When the agent reaches for the original document

Two scenarios with explicit tools:

**Scenario A — "give me everything from this thread"**
1. User: «что обсуждалось в треде про договор № 17-K?»
2. Agent: `vector_search("договор 17-K")` → finds 1 chunk from `doc_id=xyz`.
3. Agent: `get_chunks_by_doc_id(doc_id="xyz")` → all chunks of that
   email thread, ordered by position. All 12 chunks join
   `accumulated_sources`.
4. Agent: `submit_answer(...)` → synthesizer summarises 12 chunks
   into a Russian narrative; citations point at every chunk_id.

**Scenario B — "verify exact wording of clause 5"**
1. User: «какую формулировку имеет пункт 5 в договоре № 17-K?»
2. Agent: `vector_search("пункт 5 договор 17-K")` → 1 chunk
   matches, but cut off mid-clause.
3. Agent: `read_full_document(doc_id="xyz", max_chars=20000)` →
   reads the raw uploaded `.txt` from `${API_UPLOAD_DIR}` (pre-
   chunking, pre-translation, byte-for-byte).
4. Agent quotes the exact clause; reflective synth marks
   `[SUPPORTED:doc-xyz]`.

Both tools `record_event("tool_call", ...)` so they show up in the
trace summary alongside `vector_search` etc.

---

## Caveats

* **Graph retriever is optional** — if Neo4j is unreachable at
  startup, the DI provider returns `None`. Tools `graph_search`,
  `find_entity_by_id`, `find_neighbours` then return empty JSON
  payloads; the agent falls back to vector-only retrieval without
  crashing.
* **Anti-loop guard fires after 3 identical calls** — the agent
  exits the loop and proceeds to synthesis with whatever it has.
  This is a feature, not a bug: prevents infinite loops on
  unanswerable questions.
* **`/selfrag` may surface `[UNCERTAIN:...]` markers in the final
  answer text** — kept on purpose to make the model's gaps
  explicit. If you don't want them, post-process `answer_detail`
  in your client.
* **Citations are chunk-level, not claim-level** — `/selfrag`'s
  `answer_detail.citations` maps a claim to a chunk_id; resolving
  the exact span within the chunk is a frontend / UI concern.
