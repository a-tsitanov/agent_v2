"""``agent_reasoning_step`` activity — one LLM call deciding next tool.

Inputs: chat history (system + user + accumulated tool messages) and
the list of tool specs the LLM should pick from.

Output: ``AgentDecision`` — tool name + kwargs + call id (so the
workflow can build the matching TOOL message for the next round).

We construct stub FunctionTool objects from the spec list because
``llm.achat_with_tools`` expects ``BaseTool``s — but **their bodies
are never invoked** here.  Workflow dispatches via the separate
``tool_execution`` activity, so the stub just exists to satisfy the
function-calling prompt shape.
"""

from __future__ import annotations

import time

from llama_index.core.tools import FunctionTool
from temporalio import activity

from src.workflow._search_deps import get_search_llm
from src.workflow._search_serde import serialized_to_message
from src.workflow.contracts import AgentDecision, ReasoningParams


def _stub_tool(name: str, description: str) -> FunctionTool:
    async def _never_called(**kw):  # noqa: ARG001
        raise NotImplementedError(
            f"stub tool {name!r} must not be executed in-process — "
            "workflow dispatches via tool_execution activity",
        )
    return FunctionTool.from_defaults(
        fn=_never_called, name=name, description=description,
    )


@activity.defn
async def agent_reasoning_step(params: ReasoningParams) -> AgentDecision:
    """One ReAct decision step."""
    t0 = time.monotonic()
    llm = await get_search_llm()
    tools = [_stub_tool(t.name, t.description) for t in params.tools]
    messages = [serialized_to_message(m) for m in params.messages]
    activity.heartbeat({"stage": "llm_call", "n_tools": len(tools)})

    response = await llm.achat_with_tools(
        tools=tools, chat_history=messages,
    )
    tool_calls = llm.get_tool_calls_from_response(
        response, error_on_no_tool_call=False,
    )

    raw_text = ""
    try:
        raw_text = response.message.content or ""
    except Exception:  # noqa: BLE001
        pass

    if not tool_calls:
        activity.logger.info(
            "agent_reasoning_step  no tool call  raw_text_len=%d",
            len(raw_text),
        )
        return AgentDecision(
            tool_name="",
            tool_kwargs={},
            raw_text=raw_text[:1000],
            finished_no_call=True,
        )

    tc = tool_calls[0]   # take the first call; workflow can handle
                         # multi-tool decisions by re-prompting next loop
    duration_ms = int((time.monotonic() - t0) * 1000)
    activity.logger.info(
        "agent_reasoning_step  tool=%s  ms=%d", tc.tool_name, duration_ms,
    )
    return AgentDecision(
        tool_name=tc.tool_name,
        tool_kwargs=dict(tc.tool_kwargs or {}),
        tool_call_id=str(getattr(tc, "tool_id", "") or ""),
        raw_text=raw_text[:1000],
        finished_no_call=False,
    )
