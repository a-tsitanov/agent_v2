"""Loguru bootstrap.

Call :func:`configure_logging` once at app/worker startup.  The
function is idempotent — repeated calls just replace the existing
sinks (useful in tests / hot-reload).
"""

from __future__ import annotations

import sys

from loguru import logger


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
    logger.info(
        "logging configured  level={level}  json={json}",
        level=level, json=json_output,
    )
