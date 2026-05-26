"""Canonical entity linker — resolve a mention to an existing Wikibase QID.

Turns the local self-hosted Wikibase from a write-only sink into a
canonical entity-linking *anchor*.  Given a surface form (mention) and
its embedding, :class:`CanonicalLinker` resolves it to an existing
Wikibase item's QID, or returns ``None`` (meaning "mint a brand-new
item").

Resolution cascade (cheapest → most expensive):

  1. **Exact-alias lookup** — the surface form is already recorded as an
     alias on some item (see ``AsyncWikibase.set_aliases`` /
     ``push_entities`` alias storage).  Deterministic, no LLM.
  2. **Embedding kNN** — nearest stored items by cosine over the
     mention embedding.  The top candidate auto-links when its score
     clears ``auto_link_threshold`` AND both surfaces share a single
     non-mixed script (cross-script always routes to the LLM, mirroring
     the same-script auto-merge gate in ``entity_resolution.py``).
  3. **LLM verify** — borderline candidates (sub-threshold or
     cross-script) are confirmed by a YES/NO LLM call.  When no LLM is
     wired, borderline candidates fall through to ``None``
     (conservative — prefer minting a new item over a wrong link).

This is a building block: it is FLAGGED OFF
(``AgentSettings.canonical_linker_enabled``, default False) and NOT yet
wired into the ingest path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from llama_index.core.base.llms.types import ChatMessage, MessageRole

from src.graph.entity_resolution import _script_of


@dataclass
class CanonicalCandidate:
    """One linking candidate — an existing Wikibase item.

    ``score`` is an alias-match certainty (1.0 for exact alias) or a
    cosine similarity (kNN candidates).
    """

    qid: str
    name: str
    score: float


_VERIFY_SYSTEM = (
    "You decide if a mention refers to the SAME real-world entity "
    "as a candidate from a knowledge base. Answer strictly YES or NO."
)


class CanonicalLinker:
    """Resolve a mention → existing Wikibase QID, else ``None``.

    ``index`` exposes two read methods:
      * ``alias_lookup(surface, label) -> CanonicalCandidate | None``
      * ``knn(embedding, label, k) -> list[CanonicalCandidate]``

    ``llm`` is an optional LlamaIndex-style chat model (``achat``); when
    ``None``, borderline candidates are not verified and the linker
    returns ``None`` for them.  ``embed`` is accepted for symmetry with
    callers that compute the mention embedding here (unused directly —
    the caller passes the ready embedding into :meth:`link`).
    """

    def __init__(
        self,
        index: Any,
        llm: Any,
        embed: Any,
        auto_link_threshold: float = 0.9,
        knn_k: int = 5,
    ) -> None:
        self.index = index
        self.llm = llm
        self.embed = embed
        self.auto_link_threshold = auto_link_threshold
        self.knn_k = knn_k

    async def link(self, surface: str, label: str, embedding: list[float]) -> str | None:
        """Return the QID this mention links to, or ``None`` to mint new."""
        # 1. Exact alias — deterministic, cheapest.
        hit = self.index.alias_lookup(surface, label)
        if hit is not None:
            return hit.qid

        # 2. Embedding kNN — nearest stored items.
        candidates = self.index.knn(embedding, label, self.knn_k)
        if not candidates:
            return None
        top = max(candidates, key=lambda c: c.score)

        # Auto-link only when confident AND same single (non-mixed)
        # script — cross-script always routes to the LLM, matching the
        # same-script auto-merge gate in entity_resolution.py.
        same_script = _script_of(surface) == _script_of(top.name) != "mixed"
        if top.score >= self.auto_link_threshold and same_script:
            return top.qid

        # 3. LLM verify borderline.  No LLM → conservative None.
        if self.llm is None:
            return None
        return top.qid if await self._verify(surface, label, top) else None

    async def _verify(self, surface: str, label: str, cand: CanonicalCandidate) -> bool:
        body = (
            f"Mention: {surface!r} (type={label})\n"
            f"Candidate: {cand.name!r} (qid={cand.qid})\nSame entity? Answer YES or NO."
        )
        resp = await self.llm.achat([
            ChatMessage(role=MessageRole.SYSTEM, content=_VERIFY_SYSTEM),
            ChatMessage(role=MessageRole.USER, content=body),
        ])
        return "YES" in (resp.message.content or "").upper()


__all__ = [
    "CanonicalCandidate",
    "CanonicalLinker",
]
