# kb-llamaindex

Greenfield reference RAG built on **LlamaIndex 0.13+** to compare
against the production LightRAG stack in `enterprise-kb`. Focus of
the prototype is **agentic search** — the multi-hop loop with an
LLM judge, accumulated context, and per-round telemetry — which
becomes the first end-to-end working feature (Stage 4) before
hybrid retrieval, KG, canonicalisation, API, and eval are layered
on top.

Plan: `~/.claude/plans/hashed-rolling-llama.md`.

## Status

All 9 stages of the original plan are committed (one commit per
stage, each with a section in `CHANGELOG.md`):

- ✅ **Stage 0** — Bootstrap (skeleton, deps, settings, smoke tests)
- ✅ **Stage 1** — Minimal infra (Milvus + Postgres + LiteLLM)
- ✅ **Stage 2** — IngestionPipeline (parser + chunker + cache)
- ✅ **Stage 3** — Vector index + basic query engine
- ✅ **Stage 4** — Agentic loop (priority milestone)
- ✅ **Stage 5** — Hybrid retrieval (BM25 + vector + RRF)
- ✅ **Stage 6** — Knowledge graph (PropertyGraphIndex + graph-aware agent)
- ✅ **Stage 7** — Identifier canonicalization (ported + LlamaIndex transform)
- ✅ **Stage 8** — FastAPI + Taskiq worker + dishka DI
- ✅ **Stage 9** — Eval gate + ops scripts

**Tests:** 107 green (unit + eval gate). Live-stack integration
(Milvus / Neo4j / RabbitMQ / LiteLLM via Docker) is exercised
manually — see `scripts/start.sh`, `scripts/setup_db.py`,
`scripts/smoke.sh`.

## Quick start

```bash
cd kb-llamaindex
cp .env.example .env

# uv (recommended) — installs into .venv automatically
uv sync --all-extras --dev

# or pip
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

pytest                           # smoke tests on Stage-0 scaffold
```

## Directory layout

```
kb-llamaindex/
├── pyproject.toml
├── .env.example
├── src/
│   ├── config.py                # nested pydantic-settings
│   ├── utils/logging.py         # loguru bootstrap
│   ├── api/                     # FastAPI app (Stage 8)
│   ├── ingestion/               # IngestionPipeline (Stages 2, 7)
│   ├── retrieval/               # vector + hybrid + agent (Stages 3-5)
│   ├── graph/                   # PropertyGraphIndex (Stage 6)
│   └── models/                  # Pydantic response shapes
├── tests/
│   └── test_smoke.py            # Stage-0 smoke tests
├── scripts/                     # populated from Stage 1 onward
└── docs/
```

## Reference

Logic is ported from `enterprise-kb/` where the LightRAG version
already encodes lessons paid for. Concrete pointers are in
`~/.claude/projects/-Users-a-tsitanov-projects-enterprise-kb/memory/project_kb_llamaindex_prototype.md`.
