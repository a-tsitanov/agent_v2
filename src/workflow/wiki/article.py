"""Bot-section splice + LLM render for entity wiki articles.

The bot owns ONLY the text between BOT_START/BOT_END markers; everything
outside is human-owned and preserved verbatim. The article is rewritten
from the graph each time (no prior prose fed to the LLM) — see the spec's
anti-drift rationale."""
from __future__ import annotations

import re

BOT_START = "<!-- KB-BOT:START -->"
BOT_END = "<!-- KB-BOT:END -->"

_SECTION_RE = re.compile(
    re.escape(BOT_START) + r".*?" + re.escape(BOT_END), re.DOTALL)


def splice_bot_section(existing_wikitext: str, bot_md: str) -> str:
    """Replace the marked bot section with `bot_md` (wrapped in markers).
    If no markers exist, prepend the bot section, keeping human text below."""
    block = f"{BOT_START}\n{bot_md}\n{BOT_END}"
    if BOT_START in existing_wikitext and BOT_END in existing_wikitext:
        return _SECTION_RE.sub(lambda _m: block, existing_wikitext, count=1)
    if not existing_wikitext.strip():
        return block + "\n"
    return block + "\n\n" + existing_wikitext


_PROMPT = """\
/no_think
Write a factual encyclopedia section in MediaWiki markup about the entity \
"{name}" ({label}). Use ONLY the facts and source snippets below. Cite every \
statement inline as [doc_id]. Do NOT invent anything. Keep entity names in \
their original language. Link related entities with [[wiki links]].

Entity description: {description}

Facts (relations):
{relations}

Source snippets (for citation):
{citations}

Output ONLY the article section body (no page title heading).
"""


def _fmt_relations(ctx) -> str:
    if not ctx.relations:
        return "(none)"
    lines = []
    for rl, d, nn, nl, rd in ctx.relations:
        arrow = "→" if d == "out" else "←"
        extra = f" — {rd}" if rd else ""
        lines.append(f"- {arrow} {rl}: [[{nn}]] ({nl}){extra}")
    return "\n".join(lines)


def _fmt_citations(cites) -> str:
    if not cites:
        return "(none)"
    return "\n".join(f"[{doc_id}] {text[:300]}" for text, doc_id in cites)


def _fmt_sources(doc_ids, base_url: str) -> str:
    """Deterministic '== Источники ==' section with download links to the
    original files. Empty string when there are no docs or no base URL
    (section omitted entirely). Link text is the bare doc_id (UUID)."""
    if not doc_ids or not base_url:
        return ""
    base = base_url.rstrip("/")
    lines = ["== Источники ==", ""]
    for d in doc_ids:
        lines.append(f"* [{base}/documents/{d} {d}] — скачать исходник")
    return "\n".join(lines)


async def render_bot_section(ctx, citations, llm,
                             source_doc_ids=(), docs_base_url: str = "") -> str:
    """LLM-render the bot section grounded ONLY in `ctx` (graph facts) and
    `citations`. No prior article prose is passed — this is the anti-drift
    guarantee (see spec §5). A deterministic '== Источники ==' section with
    download links is appended after the prose (not LLM-generated)."""
    prompt = _PROMPT.format(
        name=ctx.name, label=ctx.label or "entity",
        description=ctx.description or "(none)",
        relations=_fmt_relations(ctx), citations=_fmt_citations(citations),
    )
    resp = await llm.acomplete(prompt)
    prose = str(resp).strip()
    sources = _fmt_sources(source_doc_ids, docs_base_url)
    return f"{prose}\n\n{sources}" if sources else prose
