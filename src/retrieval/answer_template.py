"""Server-side answer templates (Track 6, variant a).

Lets a caller shape the SHAPE of the synthesised answer by supplying a
text template — either a NAMED template stored under
``prompts/answer_templates/<name>.md`` or an INLINE template string.  The
template is framed into the synthesis instruction; absent a template the
default Russian-output preamble is used (today's behaviour, unchanged).

Guards: a name with path separators / unknown names are treated as inline
(never reads an arbitrary file), and templates are length-capped.
"""

from __future__ import annotations

import re
from pathlib import Path

_TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "prompts" / "answer_templates"
_MAX_TEMPLATE_CHARS = 8000
_MAX_NAME_LEN = 64
_SAFE_NAME = re.compile(r"^[A-Za-z0-9_-]+$")


def ru_query(query: str) -> str:
    """Russian-output instruction (the default when no template is set)."""
    return (
        "Ответь на следующий вопрос на русском языке, "
        "сохраняя имена собственные и идентификаторы дословно "
        f"из исходного языка контекста: {query}"
    )


def load_template(name_or_inline: str) -> str:
    """Resolve a template.  A bare safe name that matches a file under
    ``prompts/answer_templates/`` loads that file; anything else is taken
    as an inline template verbatim.  Result is length-capped."""
    if not name_or_inline:
        return ""
    if len(name_or_inline) <= _MAX_NAME_LEN and _SAFE_NAME.match(name_or_inline):
        candidate = _TEMPLATES_DIR / f"{name_or_inline}.md"
        if candidate.is_file():
            return candidate.read_text(encoding="utf-8")[:_MAX_TEMPLATE_CHARS]
    return name_or_inline[:_MAX_TEMPLATE_CHARS]


def build_query(query: str, template_ref: str) -> str:
    """Compose the synthesis instruction.  No template → the RU preamble;
    otherwise frame the resolved template around the question."""
    template = load_template(template_ref)
    if not template:
        return ru_query(query)
    return (
        "Сформируй ответ на вопрос строго в следующем формате/по образцу. "
        "Отвечай на русском языке, сохраняя имена собственные и "
        "идентификаторы дословно из исходного языка контекста.\n\n"
        f"=== ФОРМАТ ОТВЕТА ===\n{template}\n=== КОНЕЦ ФОРМАТА ===\n\n"
        f"Вопрос: {query}"
    )


__all__ = ["build_query", "load_template", "ru_query"]
