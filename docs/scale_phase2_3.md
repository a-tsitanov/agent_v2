# Phase 2–3 runbook — inference capacity + concurrency scale-out

Target: ~15k docs/hr (~4.2 docs/sec). Phase 0 (ER kNN default, think lever,
diagram) and Phase 1 (backfill + index verify) are done. This covers the
two phases that actually move throughput. Numbers are **starting points** —
validate and tune under load (Phase 6).

Hardware: 8× A16 GPU, model `gemma4:e4b-qat`.

---

## Phase 2 — Inference

CONSTRAINT: serve **`gemma4:e4b-qat`** (Google QAT int4 — near-BF16 quality).
Goal: batch concurrent requests across the 8 GPUs instead of one serial
endpoint. Don't re-quantize the GGUF to GPTQ/AWQ — loses the QAT provenance.

**Workload fit:** ingest extraction is SHORT-context (a ~512-token chunk +
instruction + short output). llama.cpp's parallel-slot batching splits the
context window across slots — but with short context that barely bites, so
Ollama's batching suits this workload well. → **Start with Ollama; reach for
vLLM only if it proves the bottleneck (decision rule below).**

### Path A (PRIMARY) — Ollama, replicated + parallel slots
Zero conversion; keeps the exact `gemma4:e4b-qat`. One Ollama per GPU,
fronted by the LiteLLM router.
```bash
# `nvidia-smi -L` to list GPUs; adjust the range to your GPU count.
for i in $(seq 0 7); do
  CUDA_VISIBLE_DEVICES=$i \
  OLLAMA_NUM_PARALLEL=10 \           # parallel slots (short ctx → can go high)
  OLLAMA_FLASH_ATTENTION=1 \         # faster attention, less KV memory
  OLLAMA_KV_CACHE_TYPE=q8_0 \        # quantize KV cache → more slots fit
  OLLAMA_MAX_LOADED_MODELS=1 \
  OLLAMA_HOST=127.0.0.1:$((11434+i)) \
  ollama serve &
done
# pull the model into each instance once:
for i in $(seq 0 7); do
  OLLAMA_HOST=127.0.0.1:$((11434+i)) ollama pull gemma4:e4b-qat &
done; wait
```
8 instances × `OLLAMA_NUM_PARALLEL=10` ≈ 80 concurrent slots. The q4_0 GGUF
is bit-packed int4 (smaller than the 10.73 GB w4a16-ct safetensors — check
the `…-qat-q4_0-gguf` repo for the exact size). `num_ctx` is divided across
slots, but short ingest context means you can push `OLLAMA_NUM_PARALLEL`
high while VRAM allows. Wire LiteLLM via `docker/litellm_config.scale.yaml`
(ready — 8 `ollama_chat/gemma4:e4b-qat` deployments, `least-busy` routing).

### Decision rule — stay on Ollama or move to vLLM?
Run the target load (Phase 6) and watch `nvidia-smi` (GPU util) vs measured
calls/sec:
- **GPUs ~90%+ util, throughput still short** → you're hardware-bound; vLLM
  won't help. Levers: E2B instead of E4B, fewer LLM calls/doc, or more GPUs.
- **GPUs idle, throughput short** → Ollama scheduling/overhead is the cap →
  move the hot path to vLLM (Path B).
- **Throughput meets 4.2/sec** → stop. Don't add vLLM complexity.

### Path B (fallback) — vLLM with NATIVE QAT (`w4a16-ct`)
If Path B is needed, prefer the **vLLM-native** QAT checkpoint over loading
GGUF into vLLM: Google ships `google/gemma-4-E4B-it-qat-w4a16-ct`
(compressed-tensors int4 → native Marlin int4 kernels, real paged-attention
batching, no experimental GGUF path).
```bash
for i in $(seq 0 7); do
  CUDA_VISIBLE_DEVICES=$i vllm serve google/gemma-4-E4B-it-qat-w4a16-ct \
    --served-model-name gemma4-e4b \
    --port $((8001+i)) --max-num-seqs 16 --gpu-memory-utilization 0.90 &
done
```
Caveats: weights ≈ **10.73 GB** → fits one per 16 GB A16 but only ~4–5 GB
left for KV cache → keep `--max-num-seqs` modest. Needs a vLLM build that
supports the **gemma-4** arch (multimodal — vision+audio towers load too).
LiteLLM: `docker/litellm_config.vllm.yaml`. (Loading the q4_0 GGUF in vLLM
instead is possible but experimental — `--tokenizer google/gemma-4-E4B-it`,
verify it even loads. The w4a16-ct route is strictly better.)

### Common: keep the role mapping
Keep `LITELLM_MODEL_SMALL=gemma4:e4b-qat` so every high-volume role resolves
to it. `routing_strategy: least-busy` keeps the endpoints evenly loaded.

### 2.3 ⚠ About `think=false` on gemma
The Phase-0 lever is wired, but the win depends on the model:
- **Qwen3 / hybrid-reasoning models** — big win (CoT is the majority of
  tokens, then stripped). Key differs by stack: Ollama → top-level
  `{"think": false}`; vLLM+Qwen → `extra_body={"chat_template_kwargs":
  {"enable_thinking": false}}`.
- **gemma-4 E4B** — no thinking mode, so `think=false` is likely a **no-op**
  (LiteLLM `drop_params: true` will just drop it). **Run the benchmark to
  confirm before assuming a win:**
  ```bash
  uv run python -m tests.eval.scale.bench_think --n 10
  ```
  If speedup ≈ 1.0x, leave `LITELLM_EXTRA_BODY` unset for gemma. The lever
  pays off if/when you put a Qwen3-class model on the extraction role.

**Exit:** aggregate ≥ ~40–90 LLM calls/sec sustained across the fleet.

---

## Phase 3 — Concurrency scale-out (coordinated)

Split the monolithic worker into per-pool services with replicas, and raise
the knobs **together** — raising any one alone just moves the queue.

### 3.1 Starting knob values
| Knob | env var | start | reasoning |
|------|---------|-------|-----------|
| Admission ceiling K | `INGEST_ADMISSION_MAX_INFLIGHT` | **200** | ≈ 4.2 × T; with batching+think T drops ~90s→~45s → K≈190. K only bounds in-flight; real limit is the GPU fleet. |
| LLM pool / process | `LLM_POOL_N` | **16** | per-process semaphore; set on `llm` AND `merge` workers. |
| `llm` replicas | (compose) | **4** | 4 × 16 = 64 concurrent extract calls. |
| `merge` replicas | (compose) | **2** | 2 × 16 = 32 concurrent merge/ER LLM. |
| `main` replicas | (compose) | **2** | non-LLM IO/embed stages. |
| main activity cap | `TEMPORAL_ACTIVITY_CONCURRENCY` | **16** | was 4 — too low for 7 per-doc activities. |
| llm Temporal cap | `TEMPORAL_LLM_ACTIVITY_CONCURRENCY` | **20** | MUST be ≥ LLM_POOL_N (preflight-enforced). |
| merge Temporal cap | `TEMPORAL_MERGE_ACTIVITY_CONCURRENCY` | **20** | MUST be ≥ LLM_POOL_N. |
| vLLM batch / replica | `--max-num-seqs` | **24** | 8 × 24 = 192 decode slots » 64 concurrent calls → not slot-bound; tune by decode speed. |

Capacity sanity: 64 concurrent extract calls ÷ ~1.5s/call (batched) ≈ ~43
calls/sec from extract + merge on top → in the 40–90 target band. Confirm
in Phase 6.

### 3.2 Deploy the split
`docker-compose.scale.yml` (ready, in this repo) defines `worker-llm`,
`worker-merge`, `worker-main`, `worker-misc` via `extends` and neutralizes
the monolithic `worker`:
```bash
docker compose -f docker-compose.prod.yml -f docker-compose.scale.yml up -d
```

### 3.3 Caveats
- **Metrics ports**: each pool binds Prometheus on base+index; with replicas
  host-port scraping collides. The override sets `METRICS_ENABLED=false` on
  replicated services (bind is fail-soft anyway). Re-enable via compose
  DNS/service-discovery if you need per-replica metrics.
- **K too high** stresses the singleton scheduler (signal-storm + drain
  pauses) and Temporal history — that's exactly what Phase 4 (isolate the
  scheduler, separate Temporal Postgres) hardens. If you push K past a few
  hundred, do Phase 4 first.
- **Postgres connection churn** (~K-proportional connect/sec, no pool) and
  **Neo4j hub-node contention** climb with K — Phase 4 addresses both.

**Exit:** at a test load, K documents truly run in parallel, GPUs are the
binding resource, backlog doesn't grow.

---

## Order from here
Phase 2 → Phase 3 → (Phase 4 hardening if K is large) → Phase 6 load-test.
Phase 4/5 are pure code, independent of this infra — can run in parallel.
