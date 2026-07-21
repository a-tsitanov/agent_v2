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
from src.analytics.ids import coerce_entity_type

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
                # Fallback: grab the first {...} span and try again. Guard this
                # json.loads too — a matched-but-invalid span must degrade to the
                # not-JSON path, never crash out to reason="parse error".
                m = _JSON_RE.search(raw)
                obj = None
                if m:
                    try:
                        obj = json.loads(m.group(0))
                    except Exception:
                        obj = None
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
            dumped = model.model_dump()
            # The planner LLM tends to fill a `type` param with a generic word
            # from the question ("entity"/"сущности"); coerce anything that
            # isn't a real EntityType to None (= all types) so type-filtered
            # primitives don't silently match zero rows.
            if dumped.get("type"):
                dumped["type"] = coerce_entity_type(dumped["type"])
            validated.append(PrimitiveCall(primitive=name, params=dumped))
            if len(validated) >= max_steps:
                break

        return AnalysisPlan(route=route, steps=validated, reason=reason)
    except Exception as exc:
        logger.warning("parse_plan failed: {e}", e=exc)
        return AnalysisPlan(steps=[], reason="parse error")


# Trend / popularity intent → deterministic fallback. The dedicated burst
# primitive (trending_events) is empty under the nebula backend — event→participant
# edges aren't populated there — so route trend questions to primitives that DO
# work on the current graph: most-mentioned entities + newest events. This keeps
# "что в трендах" answerable even when the planner LLM maps nothing (its jargon
# catalog descriptions make it emit "no matching primitive" for trend phrasings).
_TREND_KEYWORDS = (
    "тренд", "в тренде", "трендах", "trending", "популярн", "часто упомин",
    "самое обсуждаемое", "обсуждаем", "на слуху", "хайп", "burst", "surge",
    "most mentioned", "most talked", "most discussed", "hot topic", "hottest",
)
_TREND_FALLBACK_PRIMITIVES = ("top_entities_by_mentions", "new_events")


def trend_fallback_steps(question: str) -> list[PrimitiveCall]:
    """Deterministic fallback for trend/popularity questions the LLM couldn't plan.

    Returns steps for backend-working primitives when the question reads as a
    trend/popularity ask; an empty list otherwise (or if those primitives are
    not registered). Pure — safe to unit-test.
    """
    q = (question or "").lower()
    if not any(k in q for k in _TREND_KEYWORDS):
        return []
    steps: list[PrimitiveCall] = []
    for name in _TREND_FALLBACK_PRIMITIVES:
        prim = CATALOG.get(name)
        if prim is None:
            continue
        try:
            params = prim.param_model().model_dump()
        except Exception:
            continue
        steps.append(PrimitiveCall(primitive=name, params=params))
    return steps


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
    plan = parse_plan(raw, max_steps=max_steps)
    # Trend/popularity intent → guarantee the backend-working primitives run.
    # Covers BOTH failure modes: the LLM plans nothing ("no matching primitive")
    # AND the LLM picks the trend primitives that are empty under nebula
    # (topic_trend / trending_events). Prepend the working ones, keep any
    # distinct LLM steps behind them, cap at max_steps.
    fb = trend_fallback_steps(question)
    if fb:
        seen = {s.primitive for s in fb}
        steps = fb + [s for s in plan.steps if s.primitive not in seen]
        return AnalysisPlan(route="catalog", steps=steps[:max_steps], reason="trend-intent fallback")
    return plan
