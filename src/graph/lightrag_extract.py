"""LightRAG-style KG extractor as a LlamaIndex `TransformComponent`.

One LLM call per chunk emits entities (name + type + description)
and relations (src + tgt + keywords + description) in a single
structured response — LightRAG's algorithm, our taxonomy.

A drop-in replacement for `SimpleLLMPathExtractor`: populates
`KG_NODES_KEY` and `KG_RELATIONS_KEY` on each input node so
`PropertyGraphIndex` (with a `NoOpKGExtractor`) can consume the
output identically.

Cross-chunk consolidation (concatenating / summarising descriptions
of the same entity across many chunks) is a *separate* step —
`src/graph/merge.py:merge_kg_extraction`.  The extractor only
populates per-chunk metadata; the merger reads it back and produces
the final entity/relation set for storage.

Behaviour knobs (constructor args):

  * `gleaning_passes` (default 0): how many follow-up LLM calls
    per chunk to ask "what did you miss?".  LightRAG default is 1;
    we keep it off out of the box and let R9 eval decide.
  * `num_workers` (default 4): parallel chunks.  Keep low when
    upstream is single-stream (Ollama on CPU).
  * `examples` (default `EXAMPLES_DEFAULT`): few-shot examples
    rendered into `{examples}` in the system prompt.
  * `entity_types`: list of strings.  Defaults to the project's
    universal `EntityType` Literal so callers don't have to pass it.
"""

from __future__ import annotations

from typing import Any

from llama_index.core.async_utils import run_jobs
from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.graph_stores.types import (
    KG_NODES_KEY,
    KG_RELATIONS_KEY,
)
from llama_index.core.llms import LLM
from llama_index.core.schema import BaseNode, MetadataMode, TransformComponent
from loguru import logger
from pydantic import ConfigDict, Field

from src.graph.lightrag_parse import (
    ensure_orphan_entities,
    parse_lightrag_output,
    parsed_relations_to_relations,
    _normalize_entity_name,
)
from src.graph.lightrag_prompts import (
    COMPLETE_DELIM,
    ENTITY_CONTINUE_EXTRACTION_USER,
    ENTITY_EXTRACTION_SYSTEM,
    ENTITY_EXTRACTION_USER,
    EXAMPLES_DEFAULT,
    TUPLE_DELIM,
    render_examples,
)


def _default_entity_types() -> list[str]:
    """Pull entity-type strings out of `src.graph.schema.EntityType`."""
    from typing import get_args

    from src.graph.schema import EntityType

    return list(get_args(EntityType))


class LightRAGExtractor(TransformComponent):
    """Per-chunk extractor with optional gleaning, LightRAG-style.

    Output:
      * `node.metadata[KG_NODES_KEY]`: list[EntityNode] — entities
        the LLM identified in this chunk, each carrying
        `properties["description"]`.
      * `node.metadata[KG_RELATIONS_KEY]`: list[Relation] —
        binary relations the LLM identified, with semantic relation
        labels derived from `relationship_keywords`.

    Stub-test friendly: accepts any object with `.achat(messages)`
    in `llm`; we duck-type it instead of forcing the full LlamaIndex
    `LLM` hierarchy.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    llm: Any
    entity_types: list[str] = Field(default_factory=_default_entity_types)
    examples: list[str] = Field(default_factory=lambda: list(EXAMPLES_DEFAULT))
    num_workers: int = 4
    gleaning_passes: int = 0
    language: str = "the source-text language"

    # ── TransformComponent contract ─────────────────────────────────

    def __call__(
        self, nodes: list[BaseNode], *, show_progress: bool = False, **kwargs: Any,
    ) -> list[BaseNode]:
        """Sync entry point.  Forwards to async via asyncio.run.

        IMPORTANT: do NOT call this from within a running event loop
        (PropertyGraphIndex.run inside taskiq worker).  Use `acall`
        directly instead, or wrap this call in `asyncio.to_thread`.
        """
        import asyncio

        return asyncio.run(self.acall(nodes, show_progress=show_progress, **kwargs))

    async def acall(
        self, nodes: list[BaseNode], *, show_progress: bool = False, **kwargs: Any,
    ) -> list[BaseNode]:
        jobs = [self._aextract(n) for n in nodes]
        return await run_jobs(
            jobs,
            workers=self.num_workers,
            show_progress=show_progress,
            desc="LightRAG extract",
        )

    # ── per-chunk extract + gleaning ─────────────────────────────────

    async def _aextract(self, node: BaseNode) -> BaseNode:
        chunk_text = node.get_content(metadata_mode=MetadataMode.LLM)
        entity_types_str = ", ".join(self.entity_types)
        examples_rendered = render_examples(
            self.examples,
            entity_types=entity_types_str,
        )

        system_msg = ENTITY_EXTRACTION_SYSTEM.format(
            entity_types=entity_types_str,
            language=self.language,
            examples=examples_rendered,
            tuple_delimiter=TUPLE_DELIM,
            completion_delimiter=COMPLETE_DELIM,
        )
        user_msg = ENTITY_EXTRACTION_USER.format(
            entity_types=entity_types_str,
            input_text=chunk_text,
            language=self.language,
            tuple_delimiter=TUPLE_DELIM,
            completion_delimiter=COMPLETE_DELIM,
        )

        chunk_id = node.node_id
        file_path = (node.metadata or {}).get("file_path", "")

        # 1. Initial extraction call.
        try:
            initial_text = await self._chat(system_msg, user_msg)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "lightrag-extract chunk={c} initial failed: {err}",
                c=chunk_id, err=exc,
            )
            # Preserve the contract — downstream readers expect the
            # keys to be present (possibly empty).
            node.metadata[KG_NODES_KEY] = []
            node.metadata[KG_RELATIONS_KEY] = []
            return node

        parsed = parse_lightrag_output(
            initial_text,
            source_chunk_id=chunk_id,
            file_path=file_path,
        )

        # 2. Optional gleaning — same conversation, "did you miss any".
        history: list[ChatMessage] = [
            ChatMessage(role=MessageRole.SYSTEM, content=system_msg),
            ChatMessage(role=MessageRole.USER, content=user_msg),
            ChatMessage(role=MessageRole.ASSISTANT, content=initial_text),
        ]
        for _ in range(self.gleaning_passes):
            glean_user = ENTITY_CONTINUE_EXTRACTION_USER.format(
                tuple_delimiter=TUPLE_DELIM,
                completion_delimiter=COMPLETE_DELIM,
                language=self.language,
            )
            try:
                glean_text = await self._chat_history(history, glean_user)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "lightrag-extract chunk={c} gleaning failed: {err}",
                    c=chunk_id, err=exc,
                )
                break
            glean = parse_lightrag_output(
                glean_text,
                source_chunk_id=chunk_id,
                file_path=file_path,
            )
            # Merge with dedup on already-known names.
            known = {_normalize_entity_name(e.name) for e in parsed.entities}
            for ent in glean.entities:
                if _normalize_entity_name(ent.name) not in known:
                    parsed.entities.append(ent)
                    known.add(_normalize_entity_name(ent.name))
            parsed.relations.extend(glean.relations)
            history.append(ChatMessage(role=MessageRole.USER, content=glean_user))
            history.append(ChatMessage(role=MessageRole.ASSISTANT, content=glean_text))

        # 3. Resolve relation src/tgt names → entity ids; synthesise
        #    orphan entities for any unreferenced endpoint so the
        #    relation can still be stored.
        id_by_name: dict[str, str] = {
            _normalize_entity_name(e.name): e.id for e in parsed.entities
        }
        orphans = ensure_orphan_entities(
            parsed.relations,
            id_by_name,
            source_chunk_id=chunk_id,
        )
        parsed.entities.extend(orphans)
        relations = parsed_relations_to_relations(
            parsed.relations,
            id_by_name,
            source_chunk_id=chunk_id,
        )

        node.metadata[KG_NODES_KEY] = parsed.entities
        node.metadata[KG_RELATIONS_KEY] = relations
        return node

    # ── LLM call helpers (stub-friendly) ─────────────────────────────

    async def _chat(self, system_msg: str, user_msg: str) -> str:
        messages = [
            ChatMessage(role=MessageRole.SYSTEM, content=system_msg),
            ChatMessage(role=MessageRole.USER, content=user_msg),
        ]
        resp = await self.llm.achat(messages)
        return resp.message.content or ""

    async def _chat_history(
        self, history: list[ChatMessage], next_user_msg: str,
    ) -> str:
        messages = list(history) + [
            ChatMessage(role=MessageRole.USER, content=next_user_msg),
        ]
        resp = await self.llm.achat(messages)
        return resp.message.content or ""


__all__ = ["LightRAGExtractor"]
