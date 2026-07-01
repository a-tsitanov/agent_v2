# src/analytics/catalog.py
"""Primitive registry: the single source of truth the planner sees."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel


@dataclass
class PrimitiveResult:
    cypher: str
    params: dict[str, Any]
    rows: list[dict[str, Any]]
    source_chunks: list[str] = field(default_factory=list)
    truncated: bool = False


@dataclass(frozen=True)
class Primitive:
    name: str
    fn: Callable[..., Awaitable[PrimitiveResult]]
    param_model: type[BaseModel]
    description: str
    tier: str = "online"  # "online" | "offline-mat"


CATALOG: dict[str, Primitive] = {}


def register(primitive: Primitive) -> Primitive:
    CATALOG[primitive.name] = primitive
    return primitive


def render_catalog_for_planner() -> str:
    """Human-readable catalog (name + description + params) for the planner prompt."""
    lines: list[str] = []
    for name in sorted(CATALOG):
        p = CATALOG[name]
        fields = ", ".join(p.param_model.model_fields) or "(none)"
        lines.append(f"- {name}({fields}): {p.description}")
    return "\n".join(lines)
