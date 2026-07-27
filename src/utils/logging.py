"""Loguru bootstrap.

Call :func:`configure_logging` once at app/worker startup.  The
function is idempotent — repeated calls just replace the existing
sinks (useful in tests / hot-reload).
"""

from __future__ import annotations

import logging
import sys

from loguru import logger


class _InterceptHandler(logging.Handler):
    """Forward stdlib ``logging`` records into loguru.

    ``temporalio``'s ``activity.logger`` is a STDLIB logger, so without this the
    project's loguru setup does not cover activity logs at all: the root logger
    keeps its default (level WARNING, no handler), every ``activity.logger.info``
    is dropped, and warnings fall through to ``logging.lastResort``.  Whether
    activity logs appeared was then accidental and per-process — the worker forks
    one process per pool, so a third-party import that calls ``basicConfig`` at
    runtime configured only the pool it ran in (in production ``kb-ingest``
    emitted 18654 activity INFO lines while ``kb-graph-build`` emitted none).
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        # Walk out of the logging machinery so the reported name/line is the
        # real call site rather than this handler.
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage(),
        )


def configure_logging(level: str = "info", json_output: bool = False) -> None:
    """Replace loguru's default handler with a project-tuned one.

    ``json_output=True`` emits one JSON object per line — preferred in
    container deployments where downstream collectors (Loki, ELK)
    expect structured logs.  Locally a coloured human-readable format
    is friendlier.
    """
    logger.remove()
    if json_output:
        logger.add(
            sys.stderr,
            level=level.upper(),
            serialize=True,
            backtrace=False,
            diagnose=False,
        )
    else:
        logger.add(
            sys.stderr,
            level=level.upper(),
            colorize=True,
            backtrace=False,
            diagnose=False,
            format=(
                "<green>{time:HH:mm:ss.SSS}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{line}</cyan> | "
                "<level>{message}</level>"
            ),
        )
    # Bridge stdlib logging (temporalio activity/workflow loggers, third-party
    # libraries) into the same sink.  force=True drops any handler a library
    # installed via basicConfig, so the stream stays single-format.
    logging.basicConfig(
        handlers=[_InterceptHandler()], level=level.upper(), force=True,
    )
    logger.info(
        "logging configured  level={level}  json={json}",
        level=level, json=json_output,
    )
