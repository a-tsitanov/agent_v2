"""Synthesis prompt + numeric-faithfulness checker.

The LLM verbalizes; it must not introduce numbers absent from the rows. The
faithfulness checker is a pure function used by the eval (Task 18) and can be
used as a runtime guard later.
"""

from __future__ import annotations

import json
import re

from llama_index.core.base.llms.types import ChatMessage, MessageRole

from src.analytics.contracts import StepResult

_NUM_RE = re.compile(r"-?\d+(?:[.,]\d+)?")


def _norm(tok: str) -> str:
    return tok.replace(",", ".").rstrip("0").rstrip(".") if "." in tok or "," in tok else tok


def extract_numbers(text: str) -> set[str]:
    return {_norm(m.group(0)) for m in _NUM_RE.finditer(text or "")}


def numbers_in_rows(steps: list[StepResult]) -> set[str]:
    nums: set[str] = set()
    for s in steps:
        for row in s.rows:
            nums |= extract_numbers(json.dumps(row, ensure_ascii=False, default=str))
    return nums


def faithfulness_score(answer: str, steps: list[StepResult]) -> float:
    ans = extract_numbers(answer)
    if not ans:
        return 1.0
    allowed = numbers_in_rows(steps)
    hits = sum(1 for n in ans if n in allowed)
    return hits / len(ans)


def build_synthesis_prompt(query: str, steps: list[StepResult]) -> list[ChatMessage]:
    blocks = []
    for s in steps:
        params_json = json.dumps(s.params, ensure_ascii=False, default=str)
        if s.error:
            # Structural backend limitation, not a computed zero — must not
            # read like an empty-but-successful result.
            blocks.append(
                f"primitive: {s.primitive}\nparams: {params_json}\n"
                f"result: не удалось вычислить: {s.error}"
            )
        else:
            blocks.append(
                f"primitive: {s.primitive}\nparams: {params_json}\n"
                f"rows: {json.dumps(s.rows, ensure_ascii=False, default=str)}"
            )
    evidence = "\n\n".join(blocks) or "(no results)"
    system = (
        "You answer analytical questions about a knowledge graph. You are given the "
        "exact computed RESULTS. Use ONLY the numbers and values present in those rows. "
        "Do NOT invent or estimate any number that is not in the rows. If the rows are "
        "empty, say the question could not be computed. Some steps may be marked "
        "'не удалось вычислить' (could not be computed) instead of having rows — treat "
        "those as missing, not as a computed zero, and say explicitly that they could not "
        "be answered. Answer concisely in the user's language."
    )
    user = f"Question: {query}\n\nRESULTS:\n{evidence}"
    return [
        ChatMessage(role=MessageRole.SYSTEM, content=system),
        ChatMessage(role=MessageRole.USER, content=user),
    ]
