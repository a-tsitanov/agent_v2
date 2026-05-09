"""FastAPI app entry point.

Wires routes, CORS, dishka DI container, and (optionally) the
taskiq broker lifecycle.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from src.api.routes import health, ingest, search
from src.config import settings
from src.di.providers import build_api_container
from src.utils.logging import configure_logging


@asynccontextmanager
async def lifespan(_: FastAPI):
    configure_logging(level=settings.api.log_level, json_output=settings.api.log_json)
    logger.info(
        "kb-llamaindex API starting  env={env}  log_level={lvl}",
        env=settings.api.env, lvl=settings.api.log_level,
    )
    container = build_api_container()
    setup_dishka(container, app)
    try:
        yield
    finally:
        await container.close()


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

app.include_router(health.router)
app.include_router(search.router, prefix="/api/v1")
app.include_router(ingest.router, prefix="/api/v1")
