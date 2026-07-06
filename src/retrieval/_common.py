"""Shared helpers used by both legacy `agentic_search` and the new
ReAct / reflective implementations.

Kept here so the three retrieval entry points don't duplicate
node-to-citation conversion, dedup logic, thinking-block stripping,
etc.
"""

from __future__ import annotations

import re

from llama_index.core.schema import NodeWithScore

from src.models.search import SourceCitation

# Matches both <think>...</think> and <thinking>...</thinking>,
# case-insensitive, DOTALL so multi-line thinking blocks are caught.
# qwen3 emits these in normal completions; the parser of
# SimpleLLMPathExtractor and our enricher / reflective synth all
# assume plain text, so we strip the block before downstream
# processing.
_THINK_RE = re.compile(
    r"<(think|thinking)\b[^>]*>.*?</\1>",
    re.IGNORECASE | re.DOTALL,
)
# Some qwen3 builds emit an open <think> without a matching </think>
# when generation truncates; drop everything from <think> to end.
_THINK_OPEN_RE = re.compile(
    r"<(think|thinking)\b[^>]*>.*\Z",
    re.IGNORECASE | re.DOTALL,
)


def strip_thinking(text: str) -> str:
    """Remove `<think>...</think>` / `<thinking>...</thinking>` blocks.

    Qwen3 (and a handful of other open-weights models) emit a
    chain-of-thought block before their actual response.  Downstream
    parsers (`SimpleLLMPathExtractor`, the reflective marker regex,
    entity descriptions) treat the whole response as plain text and
    pull garbage out of the thinking block — example: the few-shot
    triplets we showed the model get re-emitted inside its `<think>`
    block and the path parser pulls them as if they were extracted.
    """
    if not text:
        return text
    out = _THINK_RE.sub("", text)
    out = _THINK_OPEN_RE.sub("", out)
    return out.strip()


def deduplicate_nodes(
    nodes: list[NodeWithScore],
) -> list[NodeWithScore]:
    """Keep first occurrence per `node.node_id`."""
    seen: set[str] = set()
    out: list[NodeWithScore] = []
    for n in nodes:
        if n.node.node_id in seen:
            continue
        seen.add(n.node.node_id)
        out.append(n)
    return out


def node_to_citation(n: NodeWithScore) -> SourceCitation:
    md = n.node.metadata or {}
    return SourceCitation(
        doc_id=str(md.get("doc_id") or md.get("file_path") or ""),
        chunk_id=n.node.node_id,
        position=int(md.get("position", 0) or 0),
        content=n.node.get_content(),
        score=float(n.score or 0.0),
        department=str(md.get("department", "") or ""),
        doc_type=str(md.get("doc_type", "") or md.get("file_type", "") or ""),
    )
