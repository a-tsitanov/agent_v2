"""Tiny serde helpers between LlamaIndex live objects and the
wire-friendly contracts in ``src/workflow/contracts.py``.

Used by the search activities (retrieve, rerank, synthesize_answer) and
by the route handlers that need to lift ``SerializedNode`` back into
``NodeWithScore`` for citation building.

The R7b cutover removed the legacy ReAct ``ChatMessage`` <-> wire
helpers (``serialized_to_message`` / ``message_to_serialized``) together
with the ReAct ``SearchWorkflow`` + its ``agent_reasoning_step`` /
``tool_execution`` activities — the plan-execute path carries no chat
history, only nodes.

Pure / synchronous / no side effects.
"""

from __future__ import annotations

from llama_index.core.schema import NodeWithScore, TextNode

from src.workflow.contracts import SerializedNode


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
