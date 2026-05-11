"""Per-chunk translation step that normalises ingest input to Russian
without losing the original text.

Inserted between `IdentifierCanonicalizationTransform` and the KG
extractor in `IngestionPipeline`.  Translates each chunk via the
project LLM and stores the result on `node.metadata["translated_text"]`
— `node.text` itself stays verbatim so:

  * Milvus stores the original-language chunk text (citation fidelity).
  * Neo4j `:Chunk` nodes store the original-language text.
  * `LightRAGExtractor` reads `translated_text` and produces
    Russian entity names + descriptions → cross-lingual graph
    dedup works ("Basal cell carcinoma" + "базальноклеточный рак"
    map to the same Russian entity name).

Identifier preservation is enforced via prompt — proper nouns,
drug / gene names, emails / phones / INN / dates / amounts /
addresses, URLs and inline-code spans MUST stay verbatim.  We
also skip translation entirely when the chunk is already Russian
(cheap regex heuristic — no LLM call wasted).
"""

from __future__ import annotations

import re
from typing import Any

from llama_index.core.async_utils import run_jobs
from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.schema import BaseNode, MetadataMode, TransformComponent
from loguru import logger
from pydantic import ConfigDict, Field

from src.retrieval._common import strip_thinking


TRANSLATED_TEXT_KEY = "translated_text"


# ── prompt ──────────────────────────────────────────────────────────


TRANSLATE_PROMPT = """\
Translate the following text to Russian.

Rules:
1. Preserve VERBATIM and DO NOT translate:
   - Proper nouns (people, organizations, places, brand names).
   - Drug names, gene names, protein names, scientific identifiers.
   - Identifiers: emails, phone numbers, INN/OGRN/BIC, ISO dates,
     contract numbers, monetary amounts with currency codes,
     postal codes, URLs.
   - Any text wrapped in backticks (`code`) or fenced code blocks.
2. Use standard Russian medical / scientific / legal vocabulary
   for terminology; do not invent calques when an established
   Russian term exists.
3. Keep the paragraph structure — same paragraph count and
   approximate line breaks.
4. If the input is already in Russian, output it UNCHANGED.
5. Output ONLY the translated text.  No prefixes, no commentary,
   no "Translation:" header.

Text:
---
{text}
---

Russian translation:
"""


# ── heuristics ──────────────────────────────────────────────────────


# Roughly "is more than half the alpha chars Cyrillic?" — quick
# language signal that avoids an LLM call when input is already
# Russian.  Won't be 100% accurate on short snippets but does the
# job on chunk-sized input.
_CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")
_LATIN_RE = re.compile(r"[A-Za-z]")


def _looks_russian(text: str, threshold: float = 0.6) -> bool:
    cyr = len(_CYRILLIC_RE.findall(text))
    lat = len(_LATIN_RE.findall(text))
    total = cyr + lat
    if total == 0:
        # Numbers / punctuation only — nothing to translate.
        return True
    return (cyr / total) >= threshold


# ── transform ───────────────────────────────────────────────────────


class TranslateToRussianTransform(TransformComponent):
    """Pipeline step that fills `node.metadata["translated_text"]`
    with a Russian rendering of `node.text`.

    Stub-friendly: `llm` is duck-typed (only `.achat(messages)`
    is required), so tests can pass a `_ScriptedLLM`.

    `__call__` exists for the sync pipeline path but it relies on
    `asyncio.run` — keep it out of any already-running event loop;
    the worker awaits `acall` via `pipeline.arun(...)` instead.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    llm: Any
    num_workers: int = 4
    skip_if_russian: bool = True

    # ── TransformComponent contract ─────────────────────────────────

    def __call__(
        self, nodes: list[BaseNode], *, show_progress: bool = False, **kwargs: Any,
    ) -> list[BaseNode]:
        import asyncio

        return asyncio.run(
            self.acall(nodes, show_progress=show_progress, **kwargs)
        )

    async def acall(
        self, nodes: list[BaseNode], *, show_progress: bool = False, **kwargs: Any,
    ) -> list[BaseNode]:
        jobs = [self._atranslate(n) for n in nodes]
        return await run_jobs(
            jobs,
            workers=self.num_workers,
            show_progress=show_progress,
            desc="translate→ru",
        )

    # ── per-chunk ───────────────────────────────────────────────────

    async def _atranslate(self, node: BaseNode) -> BaseNode:
        text = node.get_content(metadata_mode=MetadataMode.NONE)
        if not text.strip():
            return node

        if self.skip_if_russian and _looks_russian(text):
            # Already Russian — store as-is so downstream code can
            # uniformly read translated_text without a fallback.
            node.metadata[TRANSLATED_TEXT_KEY] = text
            return node

        try:
            resp = await self.llm.achat([
                ChatMessage(
                    role=MessageRole.USER,
                    content=TRANSLATE_PROMPT.format(text=text),
                ),
            ])
            translated = strip_thinking(resp.message.content or "").strip()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "translate chunk={c} failed: {err}",
                c=node.node_id, err=exc,
            )
            return node  # leave translated_text absent; extractor falls back

        if not translated:
            logger.warning(
                "translate chunk={c} produced empty output — skip",
                c=node.node_id,
            )
            return node

        node.metadata[TRANSLATED_TEXT_KEY] = translated
        return node


__all__ = ["TRANSLATED_TEXT_KEY", "TranslateToRussianTransform"]
