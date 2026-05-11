"""Second-pass entity description enrichment.

`SimpleLLMPathExtractor` returns entities with `name` only — no
semantic `description`.  This module runs a follow-up LLM call per
chunk to generate 1-2 sentence descriptions for each unique entity
that was extracted from that chunk's text.  The result writes into
`EntityNode.properties["description"]`, matching what
`SchemaLLMPathExtractor` would have emitted natively if it worked
reliably.

Why a second pass instead of folding into the extract prompt:

- The first pass uses `SimpleLLMPathExtractor`'s regex parser
  which expects `(subject, predicate, object)` lines only —
  adding a description field would break the parser.
- A custom prompt + parser was considered but it's fragile across
  Russian / English / German inputs.
- A second pass costs N additional LLM calls per chunk where N =
  unique entities, but each call is short (~50 tokens of output)
  so the overall cost is bounded and the descriptions are useful
  enough for both query-time citation and agent reasoning.
"""

from __future__ import annotations

from typing import Any

from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.graph_stores.types import (
    KG_NODES_KEY,
    EntityNode,
)
from llama_index.core.llms import LLM
from llama_index.core.schema import BaseNode, TransformComponent
from loguru import logger
from pydantic import ConfigDict


_DESCRIBE_SYSTEM_PROMPT = (
    "You write concise factual descriptions of entities based ONLY "
    "on a given text excerpt.  Output a single sentence of 10-30 "
    "words.  Keep entity names and quoted text in their ORIGINAL "
    "language.  Do not invent facts.  If the entity is not "
    "described in the excerpt, reply with exactly: NO_INFO"
)


async def _describe_entity(
    llm: LLM, entity_name: str, source_text: str,
) -> str:
    """Single LLM call → description string (or '' on failure)."""
    user = (
        f"Entity: {entity_name}\n\n"
        f"Source excerpt:\n{source_text[:1500]}"
    )
    try:
        resp = await llm.achat(
            messages=[
                ChatMessage(role=MessageRole.SYSTEM, content=_DESCRIBE_SYSTEM_PROMPT),
                ChatMessage(role=MessageRole.USER, content=user),
            ]
        )
        text = (resp.message.content or "").strip()
        if not text or text.upper().startswith("NO_INFO"):
            return ""
        # Clip pathological outputs (one line, reasonable length).
        return text.split("\n", 1)[0].strip()[:500]
    except Exception as exc:  # noqa: BLE001 — best-effort enrichment
        logger.warning(
            "describe-entity failed  entity={e}  err={err}",
            e=entity_name, err=exc,
        )
        return ""


class EntityDescriptionEnricher(TransformComponent):
    """Pipeline transform that fills `description` on each
    `EntityNode` emitted by a preceding extractor.

    Insert AFTER the extractor in the IngestionPipeline:

        IngestionPipeline(transformations=[
            splitter,
            kg_extractor,                # → KG_NODES_KEY populated
            EntityDescriptionEnricher(llm=llm),
        ])
    """

    # Tests pass duck-typed `_ScriptedLLM` stubs that only implement
    # `achat`.  `arbitrary_types_allowed` skips Pydantic's LLM
    # subclass check so we don't have to drag stubs through the LLM
    # hierarchy.
    model_config = ConfigDict(arbitrary_types_allowed=True)

    llm: Any

    def __call__(self, nodes, **kwargs):  # type: ignore[override]
        """Sync entry point — defers to asyncio.run."""
        import asyncio
        return asyncio.run(self.acall(nodes, **kwargs))

    async def acall(self, nodes, **kwargs):  # type: ignore[override]
        for node in nodes:
            ent_list = node.metadata.get(KG_NODES_KEY) or []
            if not ent_list:
                continue
            source_text = (
                node.get_content() if hasattr(node, "get_content") else ""
            )
            seen: set[str] = set()
            for ent in ent_list:
                if not isinstance(ent, EntityNode):
                    continue
                if ent.name in seen:
                    continue
                seen.add(ent.name)
                if (ent.properties or {}).get("description"):
                    continue
                desc = await _describe_entity(
                    self.llm, ent.name, source_text,
                )
                if desc:
                    if ent.properties is None:
                        ent.properties = {}
                    ent.properties["description"] = desc
        return nodes


__all__ = ["EntityDescriptionEnricher"]
