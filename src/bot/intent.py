"""Route a question to the analytical layer (graph aggregation via /analyze) or
the retrieval layer (/search). A cheap keyword/stem heuristic — no LLM call on
the hot path; an LLM classifier can replace ``classify_intent`` later behind the
same signature. Defaults to SEARCH (retrieval is the safe fallback)."""
from __future__ import annotations

import re

ANALYTICAL = "analytical"
SEARCH = "search"

# Stems that signal a structural/aggregation question over the graph.
_ANALYTICAL_RE = re.compile(
    r"скольк|распределени|\bтоп|больше всего|всплеск|тренд|динамик|"
    r"статистик|рейтинг|сравн|противоречи|центральн|\bграф",
    re.IGNORECASE,
)


def classify_intent(question: str) -> str:
    """ANALYTICAL for aggregation/statistics/contradiction questions, else SEARCH."""
    q = (question or "").strip()
    if not q:
        return SEARCH
    return ANALYTICAL if _ANALYTICAL_RE.search(q) else SEARCH
