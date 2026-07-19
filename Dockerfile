# Single image for every app role (api / worker / mcp).  The role is
# chosen by the compose `command` override; the default is the API server.
# Multi-stage: the builder compiles C-extension wheels (e.g. PyStemmer,
# pulled by the bm25 retriever) with a toolchain; the runtime stays slim
# (no compiler) and just carries the resolved virtualenv.

# ── builder: resolve + compile deps ────────────────────────────────
FROM python:3.12-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# build-essential for source-only wheels (PyStemmer cythonizes libstemmer).
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Optional extras to fold into the image (space/comma-separated group names
# from [project.optional-dependencies], e.g. `tg` for the Telegram harness).
# Empty by default → the api/worker/mcp images stay slim.  Only the
# tg-ingest compose service builds with APP_EXTRAS=tg (own image), so
# telethon never bloats the core services.  ${APP_EXTRAS:+…} expands to
# nothing when unset, keeping the sync command byte-identical (cache-safe).
ARG APP_EXTRAS=

# Dependency layer — cached unless pyproject/uv.lock change.  README is
# referenced by [project].readme so it must be present for the project
# install below.  (`postal`/libpostal is an OPTIONAL extra — not synced
# here, so no system libpostal is needed.)
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project --no-dev ${APP_EXTRAS:+--extra $APP_EXTRAS}

# Application source + the project install.
COPY src ./src
COPY prompts ./prompts
COPY scripts ./scripts
RUN uv sync --frozen --no-dev ${APP_EXTRAS:+--extra $APP_EXTRAS}

# ── runtime: slim, no toolchain ────────────────────────────────────
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

# curl for container healthchecks.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
# The venv lives at /app/.venv with absolute paths; copying the whole
# /app to the same path keeps it valid (same python base in both stages).
COPY --from=builder /app /app

# Pre-cache the BGE cross-encoder reranker into the image's HF cache when
# built with the `rerank` extra (the search worker). Done in the RUNTIME
# stage on purpose — the builder's ~/.cache is NOT copied over, and this
# avoids a ~1.1 GB first-request download (there is no persistent HF volume,
# so a first-use download would be re-fetched on every restart). Downloaded
# directly via huggingface_hub so the build never imports src.config (no .env
# at build time). No-op for the slim api/ingest images (APP_EXTRAS unset).
ARG APP_EXTRAS=
RUN if echo "$APP_EXTRAS" | grep -qw rerank; then \
        python -c "from huggingface_hub import snapshot_download; snapshot_download('BAAI/bge-reranker-v2-m3')"; \
    fi

EXPOSE 8000
# Default role: API.  worker / mcp services override `command` in compose.
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
