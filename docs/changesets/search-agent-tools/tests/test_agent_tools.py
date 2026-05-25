"""Real function-calling test against local ollama.

Exercises the two fixes:
  1. tool schemas + prompt/descriptions → does the LLM pick a GRAPH
     tool for a relation question (not vector_search)?
  2. assistant-tool_call history → does a 2nd turn (after a tool
     observation) get accepted and keep calling tools?

Reuses the real code paths; only the LLM endpoint is pointed at ollama.
"""

import asyncio
import json
import os

# Point the search LLM at local ollama's OpenAI-compatible endpoint
# BEFORE importing project modules (config reads env at import).
os.environ["LITELLM_BASE_URL"] = "http://localhost:4000"
os.environ["LITELLM_LLM_MODEL"] = "gemma4:e4b"
os.environ["LITELLM_API_KEY"] = "sk-litellm-stub"
os.environ["LITELLM_FUNCTION_CALLING"] = "true"

from src.retrieval.llm import build_search_llm  # noqa: E402
from src.workflow._search_serde import serialized_to_message  # noqa: E402
from src.workflow.activities.agent_reasoning import _stub_tool  # noqa: E402
from src.workflow.contracts import (  # noqa: E402
    SerializedMessage,
    SerializedToolCall,
)
from src.workflow.search_workflow import (  # noqa: E402
    _SYSTEM_PROMPT,
    _build_tool_specs,
)


async def main():
    llm = build_search_llm()
    specs = _build_tool_specs()
    tools = [_stub_tool(s.name, s.description) for s in specs]
    print("model:", llm.model, " tools:", [s.name for s in specs])

    query = "Кто связан с компанией ООО Ромашка и какие у них отношения?"
    messages = [
        SerializedMessage(role="system", content=_SYSTEM_PROMPT),
        SerializedMessage(role="user", content=query),
    ]

    # ── turn 1 ──────────────────────────────────────────────────────
    print("\n=== TURN 1 ===  query:", query)
    resp = await llm.achat_with_tools(
        tools=tools,
        chat_history=[serialized_to_message(m) for m in messages],
    )
    calls = llm.get_tool_calls_from_response(resp, error_on_no_tool_call=False)
    if not calls:
        print("NO TOOL CALL. raw:", (resp.message.content or "")[:300])
        return
    tc = calls[0]
    print("tool chosen:", tc.tool_name)
    print("kwargs:", tc.tool_kwargs)
    call_id = str(getattr(tc, "tool_id", "") or "") or "call_1"

    # ── build the assistant tool_call turn + a fake observation ──────
    messages.append(SerializedMessage(
        role="assistant",
        content=resp.message.content or "",
        tool_calls=[SerializedToolCall(
            id=call_id, name=tc.tool_name,
            arguments=json.dumps(tc.tool_kwargs, ensure_ascii=False),
        )],
    ))
    fake_obs = json.dumps({
        "entities": [
            {"name": "ООО Ромашка", "entity_type": "organization"},
            {"name": "Иванов И.И.", "entity_type": "person"},
        ],
        "relations": [
            {"source": "Иванов И.И.", "target": "ООО Ромашка",
             "label": "директор"},
        ],
    }, ensure_ascii=False)
    messages.append(SerializedMessage(
        role="tool", content=fake_obs,
        tool_call_id=call_id, name=tc.tool_name,
    ))

    # ── turn 2: does the malformed-history fix let it continue? ──────
    print("\n=== TURN 2 ===  (after tool observation)")
    resp2 = await llm.achat_with_tools(
        tools=tools,
        chat_history=[serialized_to_message(m) for m in messages],
    )
    calls2 = llm.get_tool_calls_from_response(
        resp2, error_on_no_tool_call=False,
    )
    if calls2:
        print("turn-2 tool chosen:", calls2[0].tool_name,
              " kwargs:", calls2[0].tool_kwargs)
    else:
        print("turn-2 no tool call (model wants to answer). text:",
              (resp2.message.content or "")[:300])
    print("\nOK — history with assistant tool_calls was accepted.")


if __name__ == "__main__":
    asyncio.run(main())
