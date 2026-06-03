"""Shared helpers for the two MCP servers (auth + DI bootstrap)."""

from __future__ import annotations

import os
from typing import Any

from loguru import logger

from src.config import settings


def parse_args() -> dict[str, Any]:
    """Tiny argparse — fastmcp's runtime takes `transport`, `host`,
    `port` via kwargs.  We support `--transport stdio|sse` plus the
    standard host/port for sse.
    """
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--transport", choices=["stdio", "sse"], default="stdio")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=9001)
    args, _ = p.parse_known_args()
    return {
        "transport": args.transport,
        "host": args.host,
        "port": args.port,
    }


def assert_api_key_env_set() -> None:
    """Best-effort startup check.

    When ``KB_MCP_REQUIRE_AUTH=true``, exits with a clear message if
    no API_KEYS are configured — protects ops from accidentally
    exposing an open MCP server.
    """
    require = os.environ.get(
        "KB_MCP_REQUIRE_AUTH", "true",
    ).lower() not in {"0", "false", "no"}
    if not require:
        logger.warning(
            "MCP auth DISABLED via KB_MCP_REQUIRE_AUTH=false — "
            "anyone reaching this port can call tools",
        )
        return
    keys = settings.api.keys_list
    if not keys:
        raise SystemExit(
            "MCP startup: API_KEYS env is empty — refusing to expose "
            "tools without auth.  Set API_KEYS=... or "
            "KB_MCP_REQUIRE_AUTH=false to opt out.",
        )


def is_valid_key(provided: str) -> bool:
    """Match against the configured API key list."""
    require = os.environ.get(
        "KB_MCP_REQUIRE_AUTH", "true",
    ).lower() not in {"0", "false", "no"}
    if not require:
        return True
    if not provided:
        return False
    return provided in settings.api.keys_list


def build_sse_auth() -> Any:
    """Build a FastMCP auth provider for HTTP/SSE transports.

    Returns a ``StaticTokenVerifier`` seeded with the configured API
    keys when ``KB_MCP_REQUIRE_AUTH`` is on and keys exist; otherwise
    ``None`` (auth disabled — stdio/desktop usage).  The verifier
    validates the incoming ``Authorization: Bearer <key>`` header
    against the configured key set.  Enforced only on HTTP/SSE
    transports; stdio is unaffected.
    """
    require = os.environ.get(
        "KB_MCP_REQUIRE_AUTH", "true",
    ).lower() not in {"0", "false", "no"}
    if not require:
        return None
    keys = settings.api.keys_list
    if not keys:
        return None
    from fastmcp.server.auth import StaticTokenVerifier
    return StaticTokenVerifier(
        tokens={
            k: {"sub": "kb-mcp-client", "client_id": "kb"}
            for k in keys
        },
    )


def log_banner(server_name: str, transport: str, host: str, port: int) -> None:
    if transport == "stdio":
        logger.info("MCP server '{n}' starting  transport=stdio", n=server_name)
    else:
        logger.info(
            "MCP server '{n}' starting  transport={t}  host={h}  port={p}",
            n=server_name, t=transport, h=host, p=port,
        )
