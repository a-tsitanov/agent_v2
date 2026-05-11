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

from src.api.routes import agent, health, ingest, search, selfrag
from src.config import settings
from src.di.providers import build_api_container
from src.ingestion.tasks import broker as taskiq_broker
from src.utils.logging import configure_logging


configure_logging(level=settings.api.log_level, json_output=settings.api.log_json)
_container = build_api_container()


@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info(
        "kb-llamaindex API starting  env={env}  log_level={lvl}",
        env=settings.api.env, lvl=settings.api.log_level,
    )
    # Taskiq client-side init — without this `.kiq(...)` calls from
    # routes raise "broker is not started".  Worker processes
    # start the broker themselves on launch, so this is needed only
    # in the API.
    await taskiq_broker.startup()
    try:
        yield
    finally:
        await taskiq_broker.shutdown()
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

app.include_router(health.router)
app.include_router(search.router, prefix="/api/v1")
app.include_router(agent.router, prefix="/api/v1")
app.include_router(selfrag.router, prefix="/api/v1")
app.include_router(ingest.router, prefix="/api/v1")
