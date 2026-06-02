# Models

The project runs LLM and embedding workloads via a **LiteLLM
proxy** in front of a local **Ollama** instance.  All model
references are configured through `LITELLM_*` env variables and
the `model_list` in `docker/litellm_config.yaml`.

## Two physical tiers — you manage exactly two model names

Every logical workload ("role") maps to one of **two physical model
tiers**.  Operators only ever set two model names:

| Env var | Tier | Default | Character |
|---|---|---|---|
| `LITELLM_MODEL_SMALL` | `small` | `gemma4:e4b` | Local, cheap, fast.  Runs every high-volume role — extraction, judge, search, plan, route, retrieve, distill, coverage. |
| `LITELLM_MODEL_LARGE` | `large` | `gpt-4o-mini` | Final user-facing answer synthesis only. |

| Other | Model | Why |
|---|---|---|
| Embedding | `nomic-embed-text` (local) / `text-embedding-3-small` (OpenAI) | 768 / 1536-dim.  MUST match `MILVUS_DIM`. |
| Reranker | not configured | Optional layer; would slot between retriever and synthesizer. |

## Role → tier map

Roles are mapped declaratively to tiers in
`src/config.py:_DEFAULT_ROLE_TIERS`.  Resolution is
`role → tier → one of the two physical models`
(`LiteLLMSettings.model_for`).  Default: **everything is `small`
except `synthesis` which is `large`.**

| Role | Default tier | Used by |
|---|---|---|
| `extraction` | small | `extract_kg`, `parse_and_chunk` (translator), CLI ingest |
| `judge` | small | `merge_and_resolve` (cross-chunk merge + ER pair-wise yes/no) |
| `search` | small | search-side LLM (graph synonym retrieval, MCP-2 tools); shared BoundedLLM semaphore |
| `route` | small | query routing (search refactor) |
| `plan` | small | multi-step planning (search refactor) |
| `retrieve` | small | retrieval-side LLM calls (search refactor) |
| `distill` | small | observation distillation (R11) |
| `coverage` | small | pre-submit coverage check |
| `synthesis` | **large** | final user-facing answer synthesis |

The factories in `src/retrieval/llm.py` (`build_extraction_llm`,
`build_judge_llm`, `build_search_llm`, `build_synthesis_llm`) call
`build_llm(role)`, which resolves through this map.  `build_llm()`
with no role uses the small tier (or the deprecated `LITELLM_LLM_MODEL`
alias if it is explicitly set — see below).

## Escalating a single role

To move one role onto the large model without touching the rest, set
`LITELLM_ROLE_TIERS` to a JSON object.  It is **merged** onto the
defaults, so you only name the role(s) you want to change:

```env
# Run planning on the large model too; everything else stays small.
LITELLM_ROLE_TIERS={"plan":"large"}
```

The merge preserves `synthesis: large` and every other default — you
never have to re-declare the full map.  Unknown roles fall back to
`small`.

### Deprecated `LITELLM_LLM_MODEL` alias

`LITELLM_LLM_MODEL` is kept only as a deprecated alias so legacy
`build_llm()` (no role) still resolves.  Leave it empty; it defaults
to `""`, in which case the no-role path uses `LITELLM_MODEL_SMALL`.
If explicitly set, it wins for the no-role path only — per-role
resolution always uses the tier map.  Remove it once all callers pass
a role.

### Smoke verification

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

All ingest-side activities run on the small tier, so changing
`LITELLM_MODEL_SMALL` shifts every ingest row's `model`.  Per-row
`ingest_metrics.model` still reflects the model **actually used** for
each activity (see `docs/runbook/analytics.md`).

`MILVUS_DIM` MUST equal the embedding model's output dim (768 for
`nomic-embed-text`, 1024 for `bge-m3`).  Changing the embed model
requires dropping and recreating the Milvus collection.

## Pulling models into Ollama

```bash
ollama pull gemma4:e4b
ollama pull nomic-embed-text
# optional baseline for R9 comparative eval:
ollama pull llama3.1:8b
```

LiteLLM proxy reaches Ollama on the host at
`host.docker.internal:11434`.  This is wired in
`docker/litellm_config.yaml`.

## Offline / air-gapped models

LLMs and embeddings go through the **LiteLLM proxy**, so an air-gapped
host only needs the proxy reachable.  But **two** models are pulled
directly from the **HuggingFace Hub** on first use and must be
pre-cached for offline deploys:

| Model | Config | Default | Used by |
|---|---|---|---|
| GLiNER span-NER | `INGESTION_GLINER_MODEL` (`settings.ingestion.gliner_model`) | `urchade/gliner_multi-v2.1` | OPT-IN `gliner` / `gliner+llm` extractor modes |
| BGE cross-encoder reranker | `HF_RERANK_MODEL` (`settings.hf.rerank_model`) | `BAAI/bge-reranker-v2-m3` | unified graph+vector rerank before synthesis |

### 1. Pre-download online (populate the cache)

On a box that can reach the Hub:

```bash
python -m scripts.download_models --cache-dir /data/hf
# or just one:  --models gliner   |   --models reranker
```

This forces the download process online (even if `HF_OFFLINE` /
`HF_HUB_OFFLINE` is set in the env), points the HF cache vars at the
resolved dir (CLI `--cache-dir` > `HF_CACHE_DIR` > HF default), pulls
both models, and prints the cache dir + next steps.  It exits non-zero
if any download fails.

### 2. Copy the cache to the air-gapped host, then run offline

```env
HF_OFFLINE=true
HF_CACHE_DIR=/data/hf
HF_RERANK_MODEL=BAAI/bge-reranker-v2-m3   # only if non-default
```

`HF_OFFLINE=true` makes `src/retrieval/hf_offline.py:configure_hf()`
set `HF_HUB_OFFLINE=1` + `TRANSFORMERS_OFFLINE=1`, and `HF_CACHE_DIR`
points `HF_HOME` / `SENTENCE_TRANSFORMERS_HOME` / `TRANSFORMERS_CACHE`
at the copied cache.  `configure_hf()` runs at every HF model-load
point (GLiNER `__init__` + `gliner_ner_callable`, and `build_reranker`)
BEFORE the heavy library imports, so the loaders read the cache only.

These are the project's OWN env vars (read via explicit aliases so they
never clobber HuggingFace's own `HF_HOME` / `HF_HUB_OFFLINE` — the app
derives and sets those itself).  An operator-set `HF_HOME` is left
untouched, so manual overrides win.

### Alternative: flat `--local-dir` (no blobs/symlinks)

The HF cache stores files content-addressed: `models--org--name/blobs/<sha>`
(real files) + `snapshots/<rev>/file → ../../blobs/<sha>` (symlinks). That
layout is standard (independent of `huggingface-hub` / `hf-xet` version),
but the **symlinks break** when the cache is copied to an air-gapped host
(`scp` / `docker COPY` / `tar` without dereference). To avoid that, download
into a **flat** directory of real files instead:

```bash
python -m scripts.download_models --local-dir /data/models
# → /data/models/gliner_multi-v2.1/  and  /data/models/bge-reranker-v2-m3/
```

`--local-dir` uses `huggingface_hub.snapshot_download(local_dir=...)` (hub
≥0.23 → real files, no blobs/symlinks; only a small `.cache/huggingface/`
metadata subdir remains, safe to drop). Copy the folder anywhere, then point
the model configs at the local paths and load offline:

```env
HF_OFFLINE=true
HF_RERANK_MODEL=/data/models/bge-reranker-v2-m3
INGESTION_GLINER_MODEL=/data/models/gliner_multi-v2.1
```

`SentenceTransformerRerank` (reranker) and `GLiNER.from_pretrained` both
accept a local directory path; with `HF_OFFLINE=true`, `configure_hf()` sets
`HF_HUB_OFFLINE=1` so the loaders never touch the network.

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

If the small tier proves insufficient on the project's corpus
(signals: tool-call reliability < 80%, regular `[NEED]`/`[UNCERTAIN]`
marker misses, rising hallucination rate in R9 eval) there are two
levers, in order of cost:

1. **Escalate one role** to the large tier via `LITELLM_ROLE_TIERS`
   (e.g. push `plan` or `judge` to `large`) — surgical, no infra
   change beyond env.
2. **Swap the small model itself** to a larger local variant:

| Model | RAM est. | When to consider |
|---|---|---|
| `gemma4:e4b` | 4-6 GB | default small tier |
| `qwen3:8b` | 6-8 GB | reliable tool calling at modest cost |
| `qwen3:14b` | 12-16 GB | best price/quality bump |
| `qwen3:32b` | 24-32 GB | sustained tool-call accuracy issues |

To swap the small tier:

1. `ollama pull qwen3:14b`
2. Edit `.env`: `LITELLM_MODEL_SMALL=qwen3:14b`.
3. Add a `model_list` entry in `docker/litellm_config.yaml`
   (mirror the `gemma4:e4b` entry, change the path).
4. `docker compose restart litellm`.

The local tier stays on-prem on purpose — moving to external APIs
(OpenAI/Anthropic) is a separate operational decision involving cost /
privacy / vendor lock-in trade-offs.  The `large` tier defaults to a
hosted model (`gpt-4o-mini`) precisely because final synthesis is the
one low-volume, quality-critical role where that trade-off is worth it.

## Baseline for comparative eval (R9)

`llama3.1:8b` is kept registered in LiteLLM as a baseline.  The
eval script (`tests/eval/answer_quality.py` from R9 onward)
prompts both qwen3:8b and llama3.1:8b on the same golden Q&A
set and reports per-model deltas.  This way regressions caused
by code changes are distinguished from regressions caused by
model changes.

## Translation context budget

`DocumentTranslateTransform` (in `src/ingestion/translate_transform.py`)
sends each document — or a windowed slice of it — to the LLM in
one call.  The window size cap is
`INGESTION_TRANSLATION_DOC_THRESHOLD_CHARS`.  Each call needs:

* the prompt overhead (~500 tokens for the translate prompt),
* the document window (X tokens),
* output budget (~1.3 × X tokens for EN→RU expansion).

Total ≈ 500 + 2.3 × X must stay inside the model's context window.

| Model | Context (tokens) | Safe threshold (chars) |
|---|---|---|
| **Ollama qwen3:8b / 14b / 32b** (native) | 32k | **30_000** (default) |
| Ollama qwen3 with YaRN extension | 131k | 200_000 |
| **gpt-4o-mini / gpt-4o** | 128k | 200_000 – 400_000 |
| Anthropic claude-3.5-sonnet | 200k | 400_000 |

Raise the threshold to fewer, larger windows → better cross-sentence
context, fewer LLM calls.  Drop it when switching to a smaller-
context model.

Char-to-token ratio is ~4 for English, ~3 for Russian, ~2 for
Chinese; the defaults above assume English-heavy corpus.  Adjust
downward by 30% if the corpus is Russian-heavy.

When the document exceeds the threshold, the translator splits on
paragraph boundaries (then sentence boundaries for huge
paragraphs).  Each window goes in one LLM call; outputs are
concatenated with `\n\n`.

## Switching to a different LLM family

Same `OpenAILike` client is compatible with:

* **OpenAI**:
  ```env
  LITELLM_BASE_URL=https://api.openai.com/v1
  LITELLM_API_KEY=sk-real-openai-key
  LITELLM_MODEL_SMALL=gpt-4o-mini
  LITELLM_MODEL_LARGE=gpt-4o
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
