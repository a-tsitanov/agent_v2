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

- **Stage 0 — Bootstrap** ✅ done
- Stages 1-9 — pending; each will be unlocked individually after
  the previous one is verified end-to-end.

## Quick start (Stage 0)

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
