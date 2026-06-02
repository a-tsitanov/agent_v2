"""FastAPI app entry point.

Wires routes, CORS, and the dishka DI container.  Ingest hands off
to Temporal directly from the route handler — no broker lifecycle
needed here.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from src.api.routes import documents, health, ingest, search_v2
from src.config import settings
from src.di.providers import build_api_container
from src.utils.logging import configure_logging

configure_logging(level=settings.api.log_level, json_output=settings.api.log_json)
_container = build_api_container()


@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info(
        "kb-llamaindex API starting  env={env}  log_level={lvl}",
        env=settings.api.env, lvl=settings.api.log_level,
    )
    # Probe LiteLLM /v1/models so a misconfigured per-role env
    # surfaces here as a WARNING (or, with LITELLM_VALIDATE_MODELS_STRICT,
    # blocks startup) — instead of an opaque 500 mid-ingest.
    from src.observability.litellm_models import validate_litellm_models
    validate_litellm_models(source="api")
    try:
        yield
    finally:
        await _container.close()


app = FastAPI(
    title="kb-llamaindex",
    version="0.0.1",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.api.cors_origins_list or ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dishka adds middleware → must run before the app starts processing
# requests, hence before `include_router` and outside `lifespan`.
setup_dishka(_container, app)

# Search R7b cutover: the legacy ReAct routes (/search, /agent,
# /selfrag) and the judge-based /legacy/agent baseline were removed.
# The sole search surface is now search_v2 → /api/v1/search/{local,
# global,drift,auto} (+ /admin/communities/rebuild).
app.include_router(health.router)
app.include_router(search_v2.router, prefix="/api/v1")
app.include_router(ingest.router, prefix="/api/v1")
app.include_router(documents.router, prefix="/api/v1")
