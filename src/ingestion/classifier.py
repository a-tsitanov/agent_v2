"""Input document classifier — decides whether a freshly-fetched
document is worth ingesting, so junk is skipped before it costs any
parse / embed / extract work.

Two layers:
  1. ``apply_rules`` — cheap deterministic gate (extension / size).
     Pure + fully tested.  Bypassed by the ``force`` flag (in the
     activity, not here).
  2. ``classify_with_llm`` — an LLM gate over a bounded preview for the
     semantic "is this junk?" call the rules can't make.

Fail-soft is the rule: a false-skip loses a good document, which is worse
than ingesting some junk, so every uncertain / error path defaults to
INGEST.  See ``ClassifierSettings``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from llama_index.core.prompts import PromptTemplate
from loguru import logger
from pydantic import BaseModel


@dataclass(frozen=True)
class RuleVerdict:
    skip: bool
    reason: str = ""


def _ext(filename: str) -> str:
    return Path(filename).suffix.lower().lstrip(".")


def apply_rules(
    filename: str,
    size_bytes: int,
    *,
    max_size_mb: float,
    min_size_bytes: int,
    skip_extensions: list[str],
) -> RuleVerdict:
    """Deterministic skip rules.  Returns ``skip=True`` + a human reason
    for blocked extensions, empty/too-small files, and oversized files;
    otherwise ``skip=False`` (the LLM gate decides next)."""
    ext = _ext(filename)
    blocked = {e.lower().lstrip(".") for e in skip_extensions}
    if ext and ext in blocked:
        return RuleVerdict(True, f"blocked extension .{ext}")
    if size_bytes < max(min_size_bytes, 1):
        return RuleVerdict(True, f"file too small ({size_bytes} bytes)")
    if size_bytes > max_size_mb * 1024 * 1024:
        return RuleVerdict(
            True, f"file too large ({size_bytes} bytes > {max_size_mb} MB)",
        )
    return RuleVerdict(False, "")


class LLMVerdict(BaseModel):
    ingest: bool = True
    reason: str = ""
    doc_type: str = ""


_CLASSIFY_PROMPT = PromptTemplate(
    "Ты — фильтр корпоративной базы знаний. По началу документа реши, "
    "стоит ли его индексировать. Пропусти (ingest=false) только явный "
    "мусор: пустые/служебные файлы, бинарные дампы, спам, заведомо "
    "нерелевантный контент. Любой содержательный документ — оставь "
    "(ingest=true). Если сомневаешься — ОСТАВЛЯЙ (ingest=true). "
    "doc_type — короткий ярлык типа документа (договор/отчёт/письмо/…). "
    "Ответь строго JSON: {{\"ingest\": bool, \"reason\": str, "
    "\"doc_type\": str}}.\n\nНачало документа:\n{preview}"
)


async def classify_with_llm(preview: str, *, llm) -> LLMVerdict:
    """Run the LLM gate over a document preview.  Fail-soft: any error or
    unparseable reply defaults to INGEST (never lose a document to a
    classifier hiccup)."""
    try:
        return await llm.astructured_predict(
            LLMVerdict, _CLASSIFY_PROMPT, preview=preview,
        )
    except Exception as exc:  # noqa: BLE001 — fail-soft to ingest
        logger.warning("classifier LLM failed, defaulting to ingest: {e}", e=exc)
        return LLMVerdict(ingest=True, reason="classifier-error-default-ingest")


def read_preview(path: Path, max_chars: int) -> str:
    """Read up to ``max_chars`` of a file for the LLM gate.  utf-8 with a
    latin-1 fallback for arbitrary byte streams; "" if unreadable."""
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            return fh.read(max_chars)
    except Exception as exc:  # noqa: BLE001
        logger.warning("classifier preview read failed: {e}", e=exc)
        return ""


__all__ = [
    "RuleVerdict",
    "LLMVerdict",
    "apply_rules",
    "classify_with_llm",
    "read_preview",
]
