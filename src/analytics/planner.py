"""NL → AnalysisPlan. Plain achat + tolerant parse + strict pydantic validation.

Mirrors src/retrieval/query_planner.py: no structured/function-calling; defensive
parsing; fail-open. The number-producing work happens later in the executor.
"""

from __future__ import annotations

import json
import re
from typing import Any

from llama_index.core.base.llms.types import ChatMessage, MessageRole
from loguru import logger

from src.analytics.catalog import CATALOG, render_catalog_for_planner
from src.analytics.contracts import AnalysisPlan, PrimitiveCall

_SYSTEM = (
    "You are the planner for an analytical layer over a knowledge graph. "
    "Map the user's question to 1-3 calls from the CATALOG below. "
    "Reply with ONLY a JSON object: "
    '{"route":"catalog","steps":[{"primitive":"<name>","params":{...}}],"reason":"<why>"}. '
    "Use only catalog primitive names and only their listed params. If nothing fits, "
    'reply {"route":"catalog","steps":[],"reason":"no matching primitive"}.\n\nCATALOG:\n'
)

_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def parse_plan(raw: str, *, max_steps: int) -> AnalysisPlan:
    """Pure, tolerant parse + per-step validation. Never raises."""
    try:
        obj: dict[str, Any] | None = None
        raw = (raw or "").strip()
        if raw:
            try:
                obj = json.loads(raw)
            except Exception:
                m = _JSON_RE.search(raw)
                obj = json.loads(m.group(0)) if m else None
        if not isinstance(obj, dict):
            return AnalysisPlan(steps=[], reason="planner output not JSON")

        route = obj.get("route", "catalog")
        if route not in ("catalog", "cypher"):
            route = "catalog"
        reason = str(obj.get("reason", ""))

        validated: list[PrimitiveCall] = []
        for step in obj.get("steps", []) or []:
            if not isinstance(step, dict):
                continue
            name = step.get("primitive")
            prim = CATALOG.get(name)
            if prim is None:
                continue  # unknown primitive → drop
            params = step.get("params", {}) or {}
            if not isinstance(params, dict):
                continue
            try:
                model = prim.param_model(**params)  # validates required + types
            except Exception:
                continue  # bad params → drop
            validated.append(PrimitiveCall(primitive=name, params=model.model_dump()))
            if len(validated) >= max_steps:
                break

        return AnalysisPlan(route=route, steps=validated, reason=reason)
    except Exception as exc:
        logger.warning("parse_plan failed: {e}", e=exc)
        return AnalysisPlan(steps=[], reason="parse error")


async def plan_query(question: str, llm: Any, *, max_steps: int) -> AnalysisPlan:
    """Call LLM and parse the result into an AnalysisPlan. Fail-open on LLM error."""
    messages = [
        ChatMessage(role=MessageRole.SYSTEM, content=_SYSTEM + render_catalog_for_planner()),
        ChatMessage(role=MessageRole.USER, content=question),
    ]
    try:
        resp = await llm.achat(messages)
        raw = resp.message.content or ""
    except Exception as exc:
        logger.warning("plan_query LLM failed: {e}", e=exc)
        return AnalysisPlan(steps=[], reason="llm error — could not plan")
    return parse_plan(raw, max_steps=max_steps)
