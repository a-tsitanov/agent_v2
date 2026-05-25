"""Tiny serde helpers between LlamaIndex live objects and the
wire-friendly contracts in ``src/workflow/contracts.py``.

Used by the three search activities (agent_reasoning, tool_execution,
synthesize_answer) and by the route handlers that need to lift
``SerializedNode`` back into ``NodeWithScore`` for citation building.

Pure / synchronous / no side effects.
"""

from __future__ import annotations

from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.schema import NodeWithScore, TextNode

from src.workflow.contracts import (
    SerializedMessage,
    SerializedNode,
    SerializedToolCall,
)


def node_to_serialized(n: NodeWithScore) -> SerializedNode:
    return SerializedNode(
        chunk_id=n.node.node_id,
        text=n.node.get_content() or "",
        score=float(n.score or 0.0),
        metadata=dict(n.node.metadata or {}),
    )


def serialized_to_node(s: SerializedNode) -> NodeWithScore:
    return NodeWithScore(
        node=TextNode(
            id_=s.chunk_id,
            text=s.text,
            metadata=dict(s.metadata or {}),
        ),
        score=s.score,
    )


_ROLE_FORWARD = {
    "system":    MessageRole.SYSTEM,
    "user":      MessageRole.USER,
    "assistant": MessageRole.ASSISTANT,
    "tool":      MessageRole.TOOL,
}


def serialized_to_message(s: SerializedMessage) -> ChatMessage:
    role = _ROLE_FORWARD[s.role]
    kw: dict = {}
    if s.tool_call_id:
        kw["tool_call_id"] = s.tool_call_id
    if s.name:
        kw["name"] = s.name
    if s.tool_calls:
        # OpenAI-shaped tool_calls — to_openai_message_dict spreads
        # additional_kwargs straight onto the wire message, so this is
        # exactly what a real assistant tool-call turn looks like.
        kw["tool_calls"] = [
            {
                "id": tc.id,
                "type": "function",
                "function": {"name": tc.name, "arguments": tc.arguments},
            }
            for tc in s.tool_calls
        ]
    return ChatMessage(role=role, content=s.content, additional_kwargs=kw)


def message_to_serialized(m: ChatMessage) -> SerializedMessage:
    role_value = m.role.value if hasattr(m.role, "value") else str(m.role)
    add = m.additional_kwargs or {}
    tool_calls = []
    for tc in add.get("tool_calls") or []:
        fn = (tc.get("function") if isinstance(tc, dict) else None) or {}
        tool_calls.append(SerializedToolCall(
            id=str((tc.get("id") if isinstance(tc, dict) else "") or ""),
            name=str(fn.get("name") or ""),
            arguments=str(fn.get("arguments") or "{}"),
        ))
    return SerializedMessage(
        role=role_value,  # type: ignore[arg-type]
        content=m.content or "",
        tool_call_id=str(add.get("tool_call_id") or ""),
        name=str(add.get("name") or ""),
        tool_calls=tool_calls,
    )
