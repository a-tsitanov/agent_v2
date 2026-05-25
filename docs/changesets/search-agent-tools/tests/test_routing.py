"""Routing discrimination check — does the LLM pick the RIGHT tool per
question type (not just always-graph)?  Through litellm → ollama."""

import asyncio
import os

os.environ["LITELLM_BASE_URL"] = "http://localhost:4000"
os.environ["LITELLM_LLM_MODEL"] = "gemma4:e4b"
os.environ["LITELLM_API_KEY"] = "sk-litellm-stub"
os.environ["LITELLM_FUNCTION_CALLING"] = "true"

from src.retrieval.llm import build_search_llm  # noqa: E402
from src.workflow._search_serde import serialized_to_message  # noqa: E402
from src.workflow.activities.agent_reasoning import _stub_tool  # noqa: E402
from src.workflow.contracts import SerializedMessage  # noqa: E402
from src.workflow.search_workflow import (  # noqa: E402
    _SYSTEM_PROMPT,
    _build_tool_specs,
)

CASES = [
    ("relation",   "Кто связан с компанией ООО Ромашка и какие у них отношения?"),
    ("identifier", "Найди информацию по телефону +79161234567"),
    ("everything", "Расскажи всё про Иванова Ивана Ивановича"),
    ("topical",    "Какие основные выводы по рынку логистики за 2025 год?"),
]


async def main():
    llm = build_search_llm()
    tools = [_stub_tool(s.name, s.description) for s in _build_tool_specs()]
    for kind, q in CASES:
        msgs = [
            SerializedMessage(role="system", content=_SYSTEM_PROMPT),
            SerializedMessage(role="user", content=q),
        ]
        resp = await llm.achat_with_tools(
            tools=tools,
            chat_history=[serialized_to_message(m) for m in msgs],
        )
        calls = llm.get_tool_calls_from_response(
            resp, error_on_no_tool_call=False,
        )
        if calls:
            print(f"[{kind:10}] -> {calls[0].tool_name:18} {calls[0].tool_kwargs}")
        else:
            print(f"[{kind:10}] -> NO CALL: {(resp.message.content or '')[:120]}")


if __name__ == "__main__":
    asyncio.run(main())
