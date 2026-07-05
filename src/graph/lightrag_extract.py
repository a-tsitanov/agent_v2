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
from llama_index.core.schema import BaseNode, MetadataMode, TransformComponent
from loguru import logger
from pydantic import ConfigDict, Field

from src.graph.event_extract import events_to_graph
from src.graph.lightrag_parse import (
    _normalize_entity_name,
    drop_unsupported_dates,
    ensure_orphan_entities,
    parse_lightrag_output,
    parsed_relations_to_relations,
)
from src.graph.lightrag_prompts import (
    COMPLETE_DELIM,
    ENTITY_CONTINUE_EXTRACTION_USER,
    ENTITY_EXTRACTION_SYSTEM,
    ENTITY_EXTRACTION_USER,
    EVENT_INSTRUCTION,
    EXAMPLES_DEFAULT,
    TUPLE_DELIM,
    render_examples,
)
from src.ingestion.identifier_transform import _AUGMENT_METADATA_KEY


def _extraction_text(node: BaseNode) -> str:
    """Select the text to feed the KG-extraction LLM for *node*.

    Priority: ``translated_text`` metadata (Russian, set by
    ``TranslateToRussianTransform``) → ``get_content(MetadataMode.LLM)``
    (raw chunk text + LLM-visible metadata, including the augment block).

    When ``translated_text`` is used, the canonical-identifier augment
    block stored in ``_AUGMENT_METADATA_KEY`` is appended if present so
    the KG extractor receives the canonical nudge on the translated path
    as well.  A substring guard prevents double-inclusion on the
    ``MetadataMode.LLM`` path (which already carries the augment).
    """
    chunk_text = (node.metadata or {}).get("translated_text") or node.get_content(
        metadata_mode=MetadataMode.LLM
    )
    augment = (node.metadata or {}).get(_AUGMENT_METADATA_KEY) or ""
    if augment and augment not in chunk_text:
        chunk_text = chunk_text + "\n\n" + augment
    return chunk_text


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
    # Default to Russian — the ingest pipeline normalises chunk
    # text to Russian via `TranslateToRussianTransform` so the
    # entire knowledge graph is uniformly Russian.  Override only
    # for tests / benchmarks that intentionally extract from
    # source-language text.
    language: str = "Russian"

    # ── TransformComponent contract ─────────────────────────────────

    def __call__(
        self,
        nodes: list[BaseNode],
        *,
        show_progress: bool = False,
        **kwargs: Any,
    ) -> list[BaseNode]:
        """Sync entry point.  Forwards to async via asyncio.run.

        IMPORTANT: do NOT call this from within a running event loop
        (e.g. PropertyGraphIndex.run inside a Temporal activity).  Use
        `acall` directly instead, or wrap this call in `asyncio.to_thread`.
        """
        import asyncio

        return asyncio.run(self.acall(nodes, show_progress=show_progress, **kwargs))

    async def acall(
        self,
        nodes: list[BaseNode],
        *,
        show_progress: bool = False,
        **kwargs: Any,
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
        from src.config import settings as _settings

        events_enabled: bool = _settings.events.extraction_enabled

        chunk_text = _extraction_text(node)
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
        if events_enabled:
            system_msg = system_msg + EVENT_INSTRUCTION.format(
                tuple_delimiter=TUPLE_DELIM,
                completion_delimiter=COMPLETE_DELIM,
                taxonomy=", ".join(_settings.events.taxonomy) + ", other",
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
        except Exception as exc:  # broad catch — LLM call can fail in many ways
            logger.warning(
                "lightrag-extract chunk={c} initial failed: {err}",
                c=chunk_id,
                err=exc,
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
            except Exception as exc:  # broad catch — LLM call can fail in many ways
                logger.warning(
                    "lightrag-extract chunk={c} gleaning failed: {err}",
                    c=chunk_id,
                    err=exc,
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

        # 2b. Anti-fabrication: a validity date whose year is absent from
        #     the chunk text was copied from the prompt, not extracted.
        dropped_dates = drop_unsupported_dates(
            parsed.relations, node.get_content() or "",
        )
        if dropped_dates:
            logger.debug(
                "lightrag-extract chunk={c}: dropped {n} unsupported date bounds",
                c=chunk_id, n=dropped_dates,
            )

        # 3. Resolve relation src/tgt names → entity ids; synthesise
        #    orphan entities for any unreferenced endpoint so the
        #    relation can still be stored.
        id_by_name: dict[str, str] = {_normalize_entity_name(e.name): e.id for e in parsed.entities}
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

        if events_enabled and parsed.events:
            _md = node.metadata or {}
            ev_nodes, ev_rels = events_to_graph(
                parsed.events,
                id_by_name=id_by_name,
                # anchor: document date; fallback ingest date (spec §4.3)
                doc_date_epoch_days=_md.get("doc_date_epoch", _md.get("inserted_at_epoch")),
            )
            parsed.entities.extend(ev_nodes)
            relations.extend(ev_rels)
            n_raw = sum(1 for e in parsed.events if e.event_ts)
            n_resolved = sum(
                1 for n_ in ev_nodes
                if n_.label == "EventOrAction" and "event_start_epoch" in (n_.properties or {})
            )
            logger.info(
                "event-ts chunk={c} events={n} ts_raw={r} ts_resolved={s}",
                c=chunk_id, n=len(parsed.events), r=n_raw, s=n_resolved,
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
        self,
        history: list[ChatMessage],
        next_user_msg: str,
    ) -> str:
        messages = [*history, ChatMessage(role=MessageRole.USER, content=next_user_msg)]
        resp = await self.llm.achat(messages)
        return resp.message.content or ""


__all__ = ["LightRAGExtractor"]
