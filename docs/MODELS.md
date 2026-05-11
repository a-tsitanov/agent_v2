# Models

The project runs LLM and embedding workloads via a **LiteLLM
proxy** in front of a local **Ollama** instance.  All model
references are configured through `LITELLM_*` env variables and
the `model_list` in `docker/litellm_config.yaml`.

## Default stack

| Role | Model | Why |
|---|---|---|
| LLM | `qwen3:8b` | Reliable Hermes-style tool calling, structured output, multilingual. |
| Embedding | `nomic-embed-text` | 768-dim, fast, decent quality on English/Russian. |
| Reranker | not configured | Optional layer; would slot between retriever and synthesizer. |

`MILVUS_DIM` MUST equal the embedding model's output dim (768 for
`nomic-embed-text`, 1024 for `bge-m3`).  Changing the embed model
requires dropping and recreating the Milvus collection.

## Pulling models into Ollama

```bash
ollama pull qwen3:8b
ollama pull nomic-embed-text
# optional baseline for R9 comparative eval:
ollama pull llama3.1:8b
```

LiteLLM proxy reaches Ollama on the host at
`host.docker.internal:11434`.  This is wired in
`docker/litellm_config.yaml`.

## Capability flags

`src/retrieval/llm.py:build_llm` consults the env var
`LITELLM_FUNCTION_CALLING` (default `true`).  When `true`, the
`OpenAILike` client is told to use function calls — required by:

* `LLMJudge.via_structured` — structured output via
  `llm.astructured_predict(JudgeOutput, ...)`.
* `SchemaLLMPathExtractor` (graph schema mode) — same mechanism
  for triple extraction.
* ReAct agent (R7) — function calls = tool invocations.

Set `LITELLM_FUNCTION_CALLING=false` to fall back to prompt-based
JSON parsing.  Necessary on smaller models that don't reliably
emit tool calls (llama3.1:8b, qwen2.5:3b).

## Escalation path

If qwen3:8b proves insufficient on the project's corpus (signals:
tool-call reliability < 80%, regular `[NEED]`/`[UNCERTAIN]` marker
misses, rising hallucination rate in R9 eval), escalate to a
larger qwen3 variant:

| Model | RAM est. | When to consider |
|---|---|---|
| `qwen3:8b` | 6-8 GB | default |
| `qwen3:14b` | 12-16 GB | first escalation step — best price/quality bump |
| `qwen3:32b` | 24-32 GB | sustained tool-call accuracy issues |
| `qwen3:72b` | 48-64 GB | only with dedicated A100/H100 GPU |

To swap:

1. `ollama pull qwen3:14b`
2. Edit `.env`: `LITELLM_LLM_MODEL=qwen3:14b`.
3. Add a `model_list` entry in `docker/litellm_config.yaml`
   (mirror the qwen3:8b entry, change the path).
4. `docker compose restart litellm`.

The escalation path stays on-prem on purpose — moving to external
APIs (OpenAI/Anthropic) is a separate operational decision
involving cost / privacy / vendor lock-in trade-offs and is out
of scope for the in-prem-first design.

## Baseline for comparative eval (R9)

`llama3.1:8b` is kept registered in LiteLLM as a baseline.  The
eval script (`tests/eval/answer_quality.py` from R9 onward)
prompts both qwen3:8b and llama3.1:8b on the same golden Q&A
set and reports per-model deltas.  This way regressions caused
by code changes are distinguished from regressions caused by
model changes.

## Switching to a different LLM family

Same `OpenAILike` client is compatible with:

* **OpenAI**:
  ```env
  LITELLM_BASE_URL=https://api.openai.com/v1
  LITELLM_API_KEY=sk-real-openai-key
  LITELLM_LLM_MODEL=gpt-4o-mini
  LITELLM_EMBEDDING_MODEL=text-embedding-3-small
  LITELLM_EMBEDDING_DIM=1536        # MUST match MILVUS_DIM
  ```
  Skip the LiteLLM container in `docker compose`.

* **Anthropic**: similar, pointing at an Anthropic-compatible
  proxy (Anthropic doesn't expose the OpenAI-compatible
  endpoint natively — use LiteLLM proxy with appropriate
  upstream config).

## Quick smoke check

After any model swap, run:

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

Then:

```bash
uv run python -m scripts.diag_kg
```

to verify KG extraction (Simple mode) still produces entities +
relations on the bundled test paragraph.
