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

# Metadata keys used to ferry a document-level translation through
# the SentenceSplitter so each resulting chunk can carve out its own
# proportional span.  Cleaned up after alignment so they don't end
# up in Milvus / Neo4j.
FULL_TRANSLATED_TEXT_KEY = "full_translated_text"
ORIGINAL_DOC_LENGTH_KEY = "orig_doc_length"


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
            _drop_doc_translation_metadata(node)
            return node

        full_trans = (node.metadata or {}).get(FULL_TRANSLATED_TEXT_KEY)
        orig_len = (node.metadata or {}).get(ORIGINAL_DOC_LENGTH_KEY)
        start = getattr(node, "start_char_idx", None)
        end = getattr(node, "end_char_idx", None)

        try:
            # ── Fast path: doc-level translation already present ────
            # `DocumentTranslateTransform` (running before the splitter)
            # may have left a whole-document translation on the
            # parent document; each chunk inherits it via metadata.
            # If so, slice the proportional span instead of burning
            # another LLM call.
            if (full_trans and orig_len
                    and start is not None and end is not None):
                sliced = _slice_proportional(
                    full_trans=full_trans,
                    full_orig_len=int(orig_len),
                    start_char_idx=int(start),
                    end_char_idx=int(end),
                )
                if sliced:
                    node.metadata[TRANSLATED_TEXT_KEY] = sliced
                return node

            # ── Skip when already Russian ───────────────────────────
            if self.skip_if_russian and _looks_russian(text):
                node.metadata[TRANSLATED_TEXT_KEY] = text
                return node

            # ── Per-chunk fallback LLM call ─────────────────────────
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
                return node

            if not translated:
                logger.warning(
                    "translate chunk={c} produced empty output — skip",
                    c=node.node_id,
                )
                return node

            node.metadata[TRANSLATED_TEXT_KEY] = translated
            return node
        finally:
            # Unconditionally drop the doc-level scaffolding so
            # Milvus / Neo4j don't get a 100kB blob on every chunk's
            # metadata (Milvus dynamic-field cap is 65k chars).
            _drop_doc_translation_metadata(node)


def _drop_doc_translation_metadata(node: BaseNode) -> None:
    md = getattr(node, "metadata", None)
    if not md:
        return
    md.pop(FULL_TRANSLATED_TEXT_KEY, None)
    md.pop(ORIGINAL_DOC_LENGTH_KEY, None)


# ── alignment helpers ────────────────────────────────────────────────


_SENTENCE_END_RE = re.compile(r"[.!?][\s\n]")


def _slice_proportional(
    *,
    full_trans: str,
    full_orig_len: int,
    start_char_idx: int,
    end_char_idx: int,
) -> str:
    """Cut the translation span corresponding to one original chunk.

    Strategy:
      1. Compute scale ratio `len(translation) / len(original)`.
      2. Multiply chunk's char offsets by the ratio → tentative span
         in the translated text.
      3. Snap the start backwards and the end forwards to the nearest
         sentence boundary so we don't chop a phrase mid-word.

    Off-by-a-sentence is acceptable here — KG extraction is robust
    to small slop, and the chunk text in Milvus / :Chunk is still
    the original verbatim.
    """
    if full_orig_len <= 0 or not full_trans:
        return full_trans
    ratio = len(full_trans) / full_orig_len
    raw_start = max(0, min(int(start_char_idx * ratio), len(full_trans)))
    raw_end = max(raw_start, min(int(end_char_idx * ratio), len(full_trans)))

    # Snap start back to a sentence boundary (or chunk start).
    snap_start = raw_start
    for m in _SENTENCE_END_RE.finditer(full_trans, 0, raw_start):
        snap_start = m.end()
    # Snap end forwards.
    snap_end = raw_end
    m = _SENTENCE_END_RE.search(full_trans, raw_end)
    if m:
        snap_end = m.end()
    else:
        snap_end = len(full_trans)
    return full_trans[snap_start:snap_end].strip()


# ── document-level translator ───────────────────────────────────────


# Paragraph splitter used to fit a giant document into translation
# windows without breaking mid-paragraph.  Falls back to sentence
# splits when a single paragraph itself exceeds the threshold.
_PARAGRAPH_RE = re.compile(r"\n\s*\n")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def _split_for_translation(text: str, threshold: int) -> list[str]:
    """Slice `text` into ≤ `threshold`-char windows on paragraph
    boundaries; recurse to sentence splits for huge paragraphs.

    Preserves the relative order of windows — concatenation with
    `"\n\n".join(...)` reproduces (approximately) the same paragraph
    structure as the input."""
    if len(text) <= threshold:
        return [text]
    paragraphs = _PARAGRAPH_RE.split(text)
    windows: list[str] = []
    current = ""
    for para in paragraphs:
        candidate = (current + "\n\n" + para) if current else para
        if len(candidate) <= threshold:
            current = candidate
            continue
        if current:
            windows.append(current)
            current = ""
        if len(para) <= threshold:
            current = para
            continue
        # Single paragraph too big — fall back to sentence splits.
        sentences = _SENTENCE_SPLIT_RE.split(para)
        for sent in sentences:
            cand = (current + " " + sent) if current else sent
            if len(cand) <= threshold:
                current = cand
            else:
                if current:
                    windows.append(current)
                current = sent
    if current:
        windows.append(current)
    return windows


class DocumentTranslateTransform(TransformComponent):
    """Pre-splitter step that translates each input Document to
    Russian in one (or a few windowed) LLM calls.

    Stores the full translation on `document.metadata[FULL_TRANSLATED_TEXT_KEY]`;
    the SentenceSplitter copies metadata onto every resulting chunk,
    so each chunk can slice its proportional span via
    `TranslateToRussianTransform`.

    When this transform doesn't run (or fails), the per-chunk
    translator falls back to a per-chunk LLM call — both code paths
    coexist by design.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    llm: Any
    threshold_chars: int = 400_000
    num_workers: int = 2  # docs are heavier than chunks; less concurrency
    skip_if_russian: bool = True

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
        jobs = [self._atranslate_doc(n) for n in nodes]
        return await run_jobs(
            jobs,
            workers=self.num_workers,
            show_progress=show_progress,
            desc="translate doc→ru",
        )

    async def _atranslate_doc(self, doc: BaseNode) -> BaseNode:
        text = doc.get_content(metadata_mode=MetadataMode.NONE)
        if not text.strip():
            return doc

        # Already-Russian fast path: store text as-is so chunks can
        # still pick it up via the alignment path.
        if self.skip_if_russian and _looks_russian(text):
            doc.metadata[FULL_TRANSLATED_TEXT_KEY] = text
            doc.metadata[ORIGINAL_DOC_LENGTH_KEY] = len(text)
            _exclude_doc_translation_metadata(doc)
            return doc

        windows = _split_for_translation(text, self.threshold_chars)
        translated_windows: list[str] = []
        for win in windows:
            try:
                resp = await self.llm.achat([
                    ChatMessage(
                        role=MessageRole.USER,
                        content=TRANSLATE_PROMPT.format(text=win),
                    ),
                ])
                translated = strip_thinking(resp.message.content or "").strip()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "doc-translate window len={l} failed: {err}",
                    l=len(win), err=exc,
                )
                # Per-chunk fallback will handle this document's chunks.
                return doc
            if not translated:
                logger.warning(
                    "doc-translate window produced empty output — abort doc",
                )
                return doc
            translated_windows.append(translated)

        full = "\n\n".join(translated_windows)
        doc.metadata[FULL_TRANSLATED_TEXT_KEY] = full
        doc.metadata[ORIGINAL_DOC_LENGTH_KEY] = len(text)
        _exclude_doc_translation_metadata(doc)
        return doc


def _exclude_doc_translation_metadata(doc: BaseNode) -> None:
    """Mark the doc-level translation fields as excluded from
    LLM / embed metadata views.

    Without this, `SentenceSplitter` counts the (~megabyte-sized)
    `full_translated_text` against its `chunk_size` budget and
    raises `Metadata length is longer than chunk size`.  These
    keys are scaffolding for the alignment step — they MUST NOT
    appear in chunk-level rendering for LLM or embeddings.
    """
    for key in (FULL_TRANSLATED_TEXT_KEY, ORIGINAL_DOC_LENGTH_KEY):
        if hasattr(doc, "excluded_embed_metadata_keys"):
            if key not in doc.excluded_embed_metadata_keys:
                doc.excluded_embed_metadata_keys.append(key)
        if hasattr(doc, "excluded_llm_metadata_keys"):
            if key not in doc.excluded_llm_metadata_keys:
                doc.excluded_llm_metadata_keys.append(key)


__all__ = [
    "FULL_TRANSLATED_TEXT_KEY",
    "ORIGINAL_DOC_LENGTH_KEY",
    "TRANSLATED_TEXT_KEY",
    "DocumentTranslateTransform",
    "TranslateToRussianTransform",
]
