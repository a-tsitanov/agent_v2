# Single image for every app role (api / worker / mcp).  The role is
# chosen by the compose `command` override; the default is the API server.
# uv-based, multi-stage-ish: deps layer is cached separately from source.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

# uv (pinned-by-tag upstream image)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# curl for container healthchecks
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 1) Dependency layer — cached unless pyproject/uv.lock change.  README is
#    referenced by [project].readme so it must be present for the project
#    install step below.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project --no-dev

# 2) Application source + the project install.
COPY src ./src
COPY prompts ./prompts
COPY scripts ./scripts
RUN uv sync --frozen --no-dev

EXPOSE 8000
# Default role: API.  worker / mcp services override `command` in compose.
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
