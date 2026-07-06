"""Entity Resolution — cross-language / multi-form deduplication.

After `merge_kg_extraction` collapses entities by normalised name,
ER finds **semantically equivalent** duplicates that survive
orthographic dedup:

  * cross-language ("BCC" ≡ "Базальноклеточный Рак"),
  * abbreviations ("DNA" ≡ "deoxyribonucleic acid"),
  * word-order / morphology ("Рак Кожи БК" ≡ "Рак БК Кожи"),
  * initialisms ("Иванов И.И." ≡ "Иван Иванов"),
  * cross-document (already-stored canonical from doc 1 vs new
    variant from doc 2 — handled via `_load_existing_canonicals`).

Algorithm — embedding-blocked, LLM-confirmed:

  1. Filter eligible labels (drop deterministic identifiers —
     Phone/Email/INN/etc. already canonicalised).
  2. (incremental) Load canonical entities + embeddings from Neo4j.
  3. Compute embeddings for new entities (single batched call).
  4. Deterministic pre-pass — initialism regex, exact-normalised
     after stripping diacritics / punctuation.
  5. Candidate pairs — same-label top-K cosine neighbors above LOW.
  6. Auto-merge — cosine ≥ HIGH AND same script (both ASCII or
     both Cyrillic).  Cross-script always routes to LLM.
  7. LLM-judge borderline pairs (batched 10, JSON YES/NO/UNSURE).
  8. Union-find → connected components.
  9. Verify large clusters (≥ `max_cluster_size`) via one LLM call;
     drop low-confidence members.
  10. Hyper-hub clamp — clusters ≥ `hyper_hub_threshold` not
      auto-merged; flagged `er_review_needed` instead.
  11. Pick canonical per cluster, consolidate descriptions via
      `_maybe_summarize_descriptions` from `merge.py`.
  12. Build name_map, rewrite chunk-level KG_NODES_KEY metadata
      and merged_relations, drop self-loops, re-aggregate.

All LLM-side decisions default to DIFFERENT on timeout / failure —
conservative ER avoids false-positive merges that pollute the graph.
"""

from __future__ import annotations

import asyncio
import json
import math
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any

from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.graph_stores.types import (
    KG_NODES_KEY,
    KG_RELATIONS_KEY,
    EntityNode,
    Relation,
)
from llama_index.core.schema import BaseNode
from loguru import logger

from src.graph.lightrag_parse import (
    _normalize_entity_name,
)
from src.graph.merge import _maybe_summarize_descriptions
from src.retrieval._common import strip_thinking

# ── identifier labels excluded from ER ──────────────────────────────


# These types already have a deterministic canonical form produced
# by `src/ingestion/identifiers.py` and injected into Neo4j by
# `inject_canonical_entities`.  Re-running ER on them risks
# collapsing two genuinely different identifiers that happen to
# embed close (e.g. two different phone numbers in the same area).
_DETERMINISTIC_LABELS: frozenset[str] = frozenset({
    "Email",
    "PhoneNumber",
    "PostalAddress",
    "DocumentDate",
    "Amount",
    "ContractNumber",
    "OrderNumber",
    "InvoiceNumber",
    "INN",
    "OGRN",
    "BIC",
    "BankAccount",
    "KPP",
    "IBAN",
    "CreditCard",
})


@dataclass
class ERConfig:
    """Tuning knobs for entity resolution."""

    low: float = 0.55
    """Cosine floor for a pair to even be considered.  Below this
    they're treated as unrelated."""

    high: float = 0.85
    """Cosine ceiling for auto-merge (same-script only).  Pairs at
    `[low, high)` AND any cross-script pair go to LLM judge."""

    knn_k: int = 10
    """Top-K cosine neighbors considered per entity."""

    judge_batch: int = 10
    """Pairs per LLM judge call."""

    verify_cluster_size: int = 3
    """Clusters with this many members or more get re-verified via
    a single LLM consolidation call that returns explicit groups.
    Protects against transitive over-merge (A=B + B=C confirmed
    pairwise, but A and C are actually different)."""

    hyper_hub_threshold: int = 12
    """Clusters bigger than this are NOT auto-merged.  Members
    keep their original names; cluster info is recorded in
    `properties["er_review_needed"] = True` for manual review."""

    language: str = "Russian"
    """Drives the canonical-name tiebreak (prefer Cyrillic when
    language is Russian) and the LLM-judge prompt's expected
    output language."""

    eligible_labels: frozenset[str] = field(default_factory=lambda: frozenset())
    """When non-empty, only entities with these labels go through
    ER.  Empty = all labels except `_DETERMINISTIC_LABELS`."""

    empty_description_floor: float = 0.70
    """When an entity has no description, embed `name` only; this
    is less reliable, so raise the candidate floor."""

    skip_generic_names: bool = True
    """Skip ER entirely for entities that are likely too generic to
    safely participate in cross-document matching: single short
    token (e.g. 'Анна', 'Договор'), mention_count == 1, no
    description.  They stay as singletons but are NOT registered
    as canonicals for future incremental ER (would cause false-
    positive matches in next ingest)."""

    name_token_min_overlap: float = 0.0
    """Minimum Jaccard overlap between content-tokens of the two
    names for a pair to enter the candidate pool.  Defaults to
    0 (no extra filter).  Set higher (e.g. 0.1) to reject pairs
    whose names share no meaningful tokens — protects against
    description-context contamination where two semantically
    different entities co-occur in similar contexts and embed
    close.  Special case: when EITHER name normalises to the same
    transliteration as the other (cross-script "Romashka" vs
    "Ромашка"), the overlap check is skipped."""

    name_overlap_floor_bypass: float = 0.5
    """Minimum Jaccard token overlap that bypasses the cosine LOW
    floor and lets the pair reach the LLM judge anyway.  Catches
    cases where the same real-world entity is mentioned in very
    different contexts across documents — embeddings drift apart
    (cos < 0.55) but the names themselves clearly overlap (e.g.
    "СтройИнвест" ⊂ "АО «СтройИнвест", overlap=1.0).  Set to a
    value > 1.0 to disable this bypass."""

    incremental_window: int = 5000
    """How many already-stored canonical entities to load per ingest for
    cross-document matching.  The load is now ``ORDER BY mention_count
    DESC`` so the most-mentioned (hub) canonicals are always in the
    window — without that ordering the LIMIT picked an arbitrary slice
    and, once the graph exceeds this many canonicals, new mentions of a
    frequent entity could silently fail to match (→ duplicates).  Raise
    for larger graphs (memory ≈ window × embedding-dim × 4 bytes: 5k×768
    ≈ 15 MB, 25k ≈ 75 MB); candidate-generation cost grows with it too.

    Ignored when ``use_native_vector_knn`` is on."""

    use_native_vector_knn: bool = False
    """Opt-in: replace the bounded ``incremental_window`` load + Python
    brute-force candidate gen with a native Neo4j vector-index kNN per
    new entity (``er_vec`` list property + ``er_embedding_vec`` index).
    Removes the window ceiling entirely — measured on a synthetic 200k
    graph the mention_count window reaches only ~2 % of true nearest
    canonicals, native kNN ~96 % at ~6 ms/query (see
    ``tests/eval/scale/bench_er_native``).  Requires the backfill
    (``scripts/backfill_er_vector.py``) to have populated ``er_vec`` on
    existing entities and built the index.  Default off — flip only after
    backfill."""

    vector_knn_k: int = 20
    """Neighbours fetched per new entity from the ER vector index when
    ``use_native_vector_knn`` is on (the per-entity candidate fan-out)."""

    verdict_cache_enabled: bool = True
    """When True AND an `er_store` is passed to `resolve_entities`,
    borderline LLM-judge verdicts are cached in Neo4j (label
    `:ERVerdict`, keyed on the order-insensitive
    `(label:norm)|(label:norm)` pair) so recurring name-pairs across
    re-ingests / hub-heavy docs skip the LLM.  The cache is OPTIONAL
    and FAIL-SAFE: with no store (or any Neo4j error) ER falls back
    to pure LLM judging with byte-for-byte identical behaviour."""


# ── small utilities ────────────────────────────────────────────────


# Form A: "Surname F.M." or "Surname F." (dots indicate initials)
_INITIALISM_FULL_RE = re.compile(
    r"^\s*([A-ZА-ЯЁ][a-zа-яё]+)\s+"
    r"([A-ZА-ЯЁ])\.?\s*(?:([A-ZА-ЯЁ])\.?)?\s*$"
)
# Form B: "First Surname" — two capitalised words, no dots
_INITIALISM_RAW_RE = re.compile(
    r"^\s*([A-ZА-ЯЁ][a-zа-яё]+)\s+([A-ZА-ЯЁ][a-zа-яё]+)\s*$"
)


def _strip_diacritics(text: str) -> str:
    """Drop combining marks (é → e, ё → е).  Preserves case."""
    return "".join(
        ch for ch in unicodedata.normalize("NFD", text)
        if not unicodedata.combining(ch)
    )


def _deep_normalize(name: str) -> str:
    """Aggressive normalisation for the deterministic pre-pass —
    casefold + drop diacritics + collapse non-alphanumeric to
    single space."""
    if not name:
        return ""
    txt = _strip_diacritics(name).casefold()
    txt = re.sub(r"[^a-zа-яё0-9]+", " ", txt)
    return " ".join(txt.split())


_CONTENT_STOPWORDS: frozenset[str] = frozenset({
    # Org legal forms / honorifics — too generic to anchor a match.
    "ооо", "оао", "ао", "зао", "пао", "ип", "ldd", "llc", "ltd",
    "inc", "co", "corp", "group", "groupp", "jsc", "plc", "gmbh",
    "company", "компания", "группа", "холдинг", "корпорация",
    # Common filler punctuation traces.
    "the", "a", "an", "и", "of", "in",
})


def _name_tokens(name: str) -> set[str]:
    """Content-bearing tokens of a name after deep-normalise minus
    legal forms / generic stopwords."""
    deep = _deep_normalize(name)
    return {
        tok for tok in deep.split()
        if len(tok) > 1 and tok not in _CONTENT_STOPWORDS
    }


def _name_token_overlap(a: str, b: str) -> float:
    """Jaccard similarity on the content-token sets of two names."""
    ta, tb = _name_tokens(a), _name_tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _initials_signature(name: str) -> tuple[str, str] | None:
    """Extract `(surname_lower, first_initial_lower)` from either
    "Surname F.M." (dots) or "First Surname" (two capitalised words).

    Both forms collapse to the SAME signature so that
    "Иванов И.И." and "Иван Иванов" match without an LLM call.
    """
    name = (name or "").strip()
    if not name:
        return None
    m = _INITIALISM_FULL_RE.match(name)
    if m:
        surname = m.group(1).lower()
        first_initial = m.group(2).lower()
        return (surname, first_initial)
    m = _INITIALISM_RAW_RE.match(name)
    if m:
        # "First Surname" — assume the LAST word is the surname.
        first = m.group(1).lower()
        last = m.group(2).lower()
        return (last, first[0])
    return None


def _is_ascii_name(name: str) -> bool:
    return bool(name) and name.isascii()


def _is_cyrillic_name(name: str) -> bool:
    return bool(re.search(r"[А-Яа-яЁё]", name)) and not re.search(r"[A-Za-z]", name)


def _script_of(name: str) -> str:
    if _is_ascii_name(name):
        return "ascii"
    if _is_cyrillic_name(name):
        return "cyrillic"
    return "mixed"


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (math.sqrt(na) * math.sqrt(nb))


# ── data shapes ─────────────────────────────────────────────────────


@dataclass
class _Item:
    """One ER-eligible entity (new or already-stored)."""

    name: str          # display name
    norm: str          # _normalize_entity_name(name)
    label: str
    description: str
    mention_count: int
    source: str        # "new" or "stored"
    embedding: list[float] = field(default_factory=list)
    entity: EntityNode | None = None  # for new entities


@dataclass
class _UnionFind:
    parent: dict[str, str] = field(default_factory=dict)

    def find(self, x: str) -> str:
        while self.parent.get(x, x) != x:
            self.parent[x] = self.parent.get(self.parent.get(x, x), self.parent.get(x, x))
            x = self.parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb

    def add(self, x: str) -> None:
        self.parent.setdefault(x, x)

    def groups(self) -> list[set[str]]:
        clusters: dict[str, set[str]] = defaultdict(set)
        for x in list(self.parent):
            clusters[self.find(x)].add(x)
        return list(clusters.values())


# ── embeddings ──────────────────────────────────────────────────────


async def _embed_entities(
    items: list[_Item], embed_model: Any,
) -> bool:
    """Fill `_Item.embedding` for items lacking one.  Returns True
    when everyone has an embedding, False if the API failed —
    caller should pass through without ER in that case."""
    pending = [it for it in items if not it.embedding]
    if not pending:
        return True
    texts = [
        f"{it.name}: {it.description}" if it.description else it.name
        for it in pending
    ]
    try:
        batch_fn = getattr(embed_model, "aget_text_embedding_batch", None)
        if batch_fn is not None:
            vectors = await batch_fn(texts)
        else:
            vectors = await asyncio.gather(*[
                embed_model.aget_text_embedding(t) for t in texts
            ])
    except Exception as exc:
        logger.warning("ER embed batch failed: {err}", err=exc)
        return False
    for it, vec in zip(pending, vectors):
        it.embedding = list(vec)
    return True


# ── candidate generation ───────────────────────────────────────────


def _normalized_matrix(group: list[_Item]):
    """Row-normalised float64 matrix of the group's embeddings, or None.

    Lets ``_candidate_pairs`` get every pairwise cosine in one BLAS
    matrix-vector product per row instead of an O(N²) pure-Python
    ``_cosine`` loop (the candidate-gen cost cliff at scale).  Returns
    None — keeping the slow but identical pure-Python path — when numpy
    is unavailable or the embeddings are ragged, so behaviour never
    silently changes.  Zero vectors keep cosine 0 (norm clamped to 1),
    matching ``_cosine``.
    """
    try:
        import numpy as np

        m = np.asarray([it.embedding for it in group], dtype="float64")
        if m.ndim != 2 or m.shape[0] != len(group):
            return None
        norms = np.linalg.norm(m, axis=1, keepdims=True)
        norms[norms == 0.0] = 1.0
        return m / norms
    except Exception:  # broad by design — fall back to pure-Python cosine
        return None


def _candidate_pairs(
    items: list[_Item], cfg: ERConfig,
) -> tuple[list[tuple[str, str, float]], list[tuple[str, str, float]]]:
    """Compute candidate pairs grouped by label.

    Returns `(auto_merges, borderline)`:
      * `auto_merges` — pairs with cosine ≥ HIGH AND same script.
      * `borderline`  — pairs with cosine in `[LOW, HIGH)` OR
                        cosine ≥ HIGH but cross-script.

    Pairs are reported by NORMALISED name (the union-find key).

    Cosines come from a vectorised matrix product when numpy is
    available (``_normalized_matrix``); otherwise an identical
    pure-Python ``_cosine`` fallback runs.  The surrounding filter /
    top-k / classification logic is unchanged either way.
    """
    by_label: dict[str, list[_Item]] = defaultdict(list)
    for it in items:
        if it.embedding:
            by_label[it.label].append(it)

    auto: list[tuple[str, str, float]] = []
    borderline: list[tuple[str, str, float]] = []
    seen: set[tuple[str, str]] = set()

    for label, group in by_label.items():
        mat = _normalized_matrix(group)
        # Tokenise each name once (O(N)) instead of re-tokenising both
        # sides of every pair inside the O(N²) loop below.
        toks = [_name_tokens(it.name) for it in group]
        for i, a in enumerate(group):
            # All cosines from `a` to the group in one shot (or None →
            # per-pair pure-Python below).
            cos_row = (mat @ mat[i]) if mat is not None else None
            ta = toks[i]
            # Top-K nearest neighbors above LOW threshold.
            sims = []
            for j, b in enumerate(group):
                if i == j or a.norm == b.norm:
                    continue
                floor = (
                    cfg.empty_description_floor
                    if not a.description or not b.description
                    else cfg.low
                )
                cos = (
                    float(cos_row[j]) if cos_row is not None
                    else _cosine(a.embedding, b.embedding)
                )
                # High name-token overlap bypasses the cosine floor.
                # `_embed_entities` keys on `name + description`, so two
                # entities sharing the same Cyrillic surface but with
                # divergent descriptions (e.g. "СтройИнвест" mentioned
                # as future partner in doc1, then as former employer
                # in doc2) can fall below LOW=0.55 despite being the
                # same real-world entity.  When the surface names
                # themselves substantially overlap (one is a substring
                # of the other, or shares ≥ half its content tokens),
                # the cosine signal becomes secondary to the
                # orthographic signal — let the LLM judge decide.
                tb = toks[j]
                overlap = (len(ta & tb) / len(ta | tb)) if ta and tb else 0.0
                if cos < floor and overlap < cfg.name_overlap_floor_bypass:
                    continue
                # Name-token guard: when both names share at least
                # one content-token (or transliteration of each
                # other across scripts), accept.  Pure embedding-
                # similarity matches with zero token overlap are
                # almost always description-context contamination
                # (e.g. "Romashka" embeds close to "TechnoStroy"
                # because both describe a partnership).
                if cfg.name_token_min_overlap > 0:
                    if overlap < cfg.name_token_min_overlap:
                        cross_script = _script_of(a.name) != _script_of(b.name)
                        # Cross-script pairs can have zero overlap
                        # legitimately ("Romashka" vs "Ромашка") —
                        # let those through to LLM judge.
                        if not cross_script:
                            continue
                sims.append((b, cos))
            sims.sort(key=lambda p: -p[1])
            for b, cos in sims[: cfg.knn_k]:
                key = tuple(sorted((a.norm, b.norm)))
                if key in seen:
                    continue
                seen.add(key)
                same_script = _script_of(a.name) == _script_of(b.name) != "mixed"
                if cos >= cfg.high and same_script:
                    auto.append((a.norm, b.norm, cos))
                else:
                    borderline.append((a.norm, b.norm, cos))
    return auto, borderline


# ── deterministic pre-pass ─────────────────────────────────────────


def _deterministic_pairs(items: list[_Item]) -> list[tuple[str, str]]:
    """Find pairs that match without any LLM / embedding signal:

    * Same `_deep_normalize` form (case-/diacritic-/punct-insensitive).
    * `_initials_signature` collision (Surname + 2 initials matching
      a Given Surname full-form entry).
    """
    out: list[tuple[str, str]] = []

    # By deep-normalised text.
    by_deep: dict[str, list[_Item]] = defaultdict(list)
    for it in items:
        deep = _deep_normalize(it.name)
        if deep:
            by_deep[deep].append(it)
    for group in by_deep.values():
        if len(group) >= 2:
            anchor = group[0].norm
            for other in group[1:]:
                if other.norm != anchor:
                    out.append((anchor, other.norm))

    # By initials signature — only within Person label, when present.
    by_sig: dict[tuple[str, ...], list[_Item]] = defaultdict(list)
    for it in items:
        if it.label != "Person":
            continue
        sig = _initials_signature(it.name)
        if sig is not None:
            by_sig[sig].append(it)
    for group in by_sig.values():
        if len(group) >= 2:
            anchor = group[0].norm
            for other in group[1:]:
                if other.norm != anchor:
                    out.append((anchor, other.norm))

    return out


# ── LLM judge ──────────────────────────────────────────────────────


_JUDGE_SYSTEM = """\
You are an entity-resolution adjudicator.  Decide whether each pair
of entity names refers to the SAME real-world thing.

Same-entity examples (output SAME):
- abbreviation ↔ full name: "BCC" ↔ "Basal Cell Carcinoma"
- cross-language: "UV Radiation" ↔ "Ультрафиолетовое излучение"
- initials: "J. Smith" ↔ "John Smith"
- word-order: "Рак Кожи БК" ↔ "Рак БК Кожи"

Different-entity traps (output DIFFERENT):
- type vs instance: "Customer" ≠ "Customer #4521"
- general vs specific: "Tariff" ≠ "Tariff Premium"
- related but distinct: "Skin Cancer" ≠ "Melanoma"
- different individuals sharing a surname: "Ivanov I.I." ≠ "Ivanov P.S."

If you genuinely cannot tell, output UNSURE — it will be treated as
DIFFERENT (conservative).

Output format — STRICTLY a JSON array, one entry per input pair,
in the same order.  No prose before or after:

[
  {"pair": 1, "verdict": "SAME"},
  {"pair": 2, "verdict": "DIFFERENT"},
  {"pair": 3, "verdict": "UNSURE"}
]
"""


def _format_pair_prompt(
    pairs: list[tuple[_Item, _Item]],
) -> str:
    lines = []
    for i, (a, b) in enumerate(pairs, 1):
        lines.append(
            f"Pair {i}:\n"
            f"  A: {a.name!r}  (type={a.label}, desc={a.description[:200]!r})\n"
            f"  B: {b.name!r}  (type={b.label}, desc={b.description[:200]!r})\n"
        )
    return "\n".join(lines)


async def _llm_judge_pairs(
    pairs: list[tuple[_Item, _Item]], llm: Any, cfg: ERConfig,
) -> list[bool]:
    """For each input pair, return True when LLM judges SAME, else
    False (DIFFERENT, UNSURE, or any failure).

    Batches are dispatched concurrently; the process-wide BoundedLLM
    semaphore (src/retrieval/llm_semaphore.py) caps real parallelism.
    """
    if not pairs:
        return []
    verdicts: list[bool] = [False] * len(pairs)
    batch_offsets = list(range(0, len(pairs), cfg.judge_batch))

    async def _judge_one(batch_start: int) -> tuple[int, list[bool]]:
        batch = pairs[batch_start: batch_start + cfg.judge_batch]
        body = _format_pair_prompt(batch)
        messages = [
            ChatMessage(role=MessageRole.SYSTEM, content=_JUDGE_SYSTEM),
            ChatMessage(role=MessageRole.USER, content=body),
        ]
        try:
            resp = await llm.achat(messages)
            text = strip_thinking(resp.message.content or "")
        except Exception as exc:
            logger.warning(
                "ER judge batch failed at offset={o}: {err}",
                o=batch_start, err=exc,
            )
            return batch_start, [False] * len(batch)
        return batch_start, list(_parse_judge_response(text, len(batch)))

    results = await asyncio.gather(*[_judge_one(o) for o in batch_offsets])
    for batch_start, oks in results:
        for verdict_pos, ok in enumerate(oks):
            if ok:
                verdicts[batch_start + verdict_pos] = True
    return verdicts


_JUDGE_JSON_RE = re.compile(r"\[.*?\]", re.DOTALL)


# ── cluster verification (consolidation) ────────────────────────────


_CONSOLIDATE_SYSTEM = """\
You are an entity-resolution consolidator.  A cluster of N candidate
entities below was tentatively grouped together by pairwise similarity.
Some pairwise verdicts may have linked entities transitively even
though they are NOT all the same real-world entity.

Your task: split the cluster into groups of TRULY equivalent
entities.  Different real-world entities (e.g. different companies,
different people sharing a surname, different sub-concepts) MUST
end up in separate groups even when they appear in the same context.

Output STRICTLY a JSON array of arrays — each inner array is a
group of entity NAMES from the input that refer to the same
real-world entity.  Every input entity name must appear in exactly
one group.  Example for input [A, B, C, D] where A=B but C and D
are different from both: `[["A", "B"], ["C"], ["D"]]`.
"""


def _format_cluster_prompt(items: list[_Item]) -> str:
    lines = ["Entities in the cluster:"]
    for i, it in enumerate(items, 1):
        d = (it.description or "").replace("\n", " ")[:200]
        lines.append(
            f"{i}. {it.name!r}  (type={it.label}, desc={d!r})"
        )
    lines.append(
        "\nReturn JSON array of arrays of entity names, "
        "each inner array = one group of same-entity items."
    )
    return "\n".join(lines)


async def _verify_cluster(
    cluster_items: list[_Item], llm: Any,
) -> list[list[_Item]]:
    """Re-partition a tentative cluster via one consolidating LLM
    call.  Returns a list of groups; each group is a list of
    `_Item`s that the LLM judged equivalent.

    On any failure (timeout, parse error) the cluster is split into
    singletons — conservative behaviour matching the rest of ER.
    """
    if len(cluster_items) <= 1:
        return [cluster_items]
    name_to_item = {it.name: it for it in cluster_items}
    try:
        resp = await llm.achat([
            ChatMessage(role=MessageRole.SYSTEM, content=_CONSOLIDATE_SYSTEM),
            ChatMessage(role=MessageRole.USER, content=_format_cluster_prompt(cluster_items)),
        ])
        raw = strip_thinking(resp.message.content or "")
    except Exception as exc:
        logger.warning(
            "ER cluster verify failed (size={s}): {err}",
            s=len(cluster_items), err=exc,
        )
        return [[it] for it in cluster_items]

    # Find a top-level JSON array of arrays.
    match = re.search(r"\[\s*\[.*?\]\s*\]", raw, re.DOTALL)
    if match is None:
        logger.warning(
            "ER cluster verify: no JSON found in '{raw}' — splitting",
            raw=raw[:200],
        )
        return [[it] for it in cluster_items]
    try:
        groups_raw = json.loads(match.group(0))
    except json.JSONDecodeError:
        return [[it] for it in cluster_items]
    if not isinstance(groups_raw, list):
        return [[it] for it in cluster_items]

    groups: list[list[_Item]] = []
    used: set[str] = set()
    for grp in groups_raw:
        if not isinstance(grp, list):
            continue
        members = []
        for raw_name in grp:
            if not isinstance(raw_name, str):
                continue
            it = name_to_item.get(raw_name)
            if it is not None and raw_name not in used:
                members.append(it)
                used.add(raw_name)
        if members:
            groups.append(members)

    # Any items the LLM forgot become their own singleton groups —
    # safe-by-default.
    for it in cluster_items:
        if it.name not in used:
            groups.append([it])
            used.add(it.name)
    logger.info(
        "ER verify  in_size={i}  out_groups={o}  shapes={s}",
        i=len(cluster_items), o=len(groups),
        s=[len(g) for g in groups],
    )
    return groups


def _parse_judge_response(raw: str, expected: int) -> list[bool]:
    """Pull a JSON array out of `raw`, return `expected` booleans.

    Missing / malformed entries → False.
    """
    match = _JUDGE_JSON_RE.search(raw or "")
    if match is None:
        return [False] * expected
    try:
        items = json.loads(match.group(0))
    except json.JSONDecodeError:
        return [False] * expected
    if not isinstance(items, list):
        return [False] * expected
    out = [False] * expected
    for entry in items:
        if not isinstance(entry, dict):
            continue
        idx = entry.get("pair")
        verdict = str(entry.get("verdict", "")).upper().strip()
        if not isinstance(idx, int) or idx < 1 or idx > expected:
            continue
        out[idx - 1] = verdict == "SAME"
    return out


# ── persistent verdict cache ───────────────────────────────────────
#
# Borderline pairs are judged by an LLM every run; the same
# `(name, label)` pairs recur across re-ingests and within hub-heavy
# documents.  Caching `(norm_a, label_a, norm_b, label_b) -> SAME/
# DIFFERENT` in Neo4j (`:ERVerdict {key, same}`) lets ER skip
# already-judged pairs.  Everything here is OPTIONAL and FAIL-SAFE:
# with `store is None` (or any Neo4j error) the functions degrade to
# no-ops and ER behaves exactly as pure LLM judging.


def _verdict_key(a, b) -> str:
    """Order-insensitive cache key for a candidate pair.

    Keyed on `(norm, label)` of both items, sorted so that
    `_verdict_key(a, b) == _verdict_key(b, a)`.
    """
    left = (a.norm, a.label)
    right = (b.norm, b.label)
    lo, hi = sorted([left, right])
    # JSON join (not an f-string with ':'/'|' separators) so a norm/label
    # containing a delimiter char cannot collide with a different pair.
    return json.dumps([lo, hi], ensure_ascii=False, sort_keys=True)


def _partition_cached(pairs, cache):
    """Split `pairs` into `(cached, uncached)`.

    `cached` is a list of `(pair, verdict)` for pairs whose key is in
    `cache`; `uncached` is the list of pairs that still need judging.
    Original order is preserved within each partition.
    """
    cached, uncached = [], []
    for pair in pairs:
        key = _verdict_key(pair[0], pair[1])
        if key in cache:
            cached.append((pair, cache[key]))
        else:
            uncached.append(pair)
    return cached, uncached


def _load_verdict_cache(store, keys) -> dict[str, bool]:
    """Fetch cached verdicts for `keys` from Neo4j.

    Returns `{key -> same}`.  Empty (no-op) when `store is None`,
    `keys` is empty, or any query error — ER then judges everything.
    """
    if store is None or not keys:
        return {}
    try:
        rows = store.structured_query(
            "MATCH (v:ERVerdict) WHERE v.key IN $keys "
            "RETURN v.key AS key, v.same AS same",
            param_map={"keys": keys},
        )
    except Exception as exc:
        logger.warning("ER verdict cache load failed: {e}", e=exc)
        return {}
    return {r["key"]: bool(r["same"]) for r in (rows or []) if isinstance(r, dict)}


def _store_verdicts(store, entries: dict[str, bool]) -> None:
    """Persist freshly-judged verdicts to Neo4j (`MERGE` by key).

    No-op when `store is None` or `entries` is empty.  Any query
    error is logged and swallowed — caching is best-effort and must
    never break ER.
    """
    if store is None or not entries:
        return
    try:
        # Idempotent: backs the MERGE and prevents duplicate :ERVerdict
        # nodes under concurrent writes; also indexes the IN-list load.
        store.structured_query(
            "CREATE CONSTRAINT er_verdict_key IF NOT EXISTS "
            "FOR (v:ERVerdict) REQUIRE v.key IS UNIQUE"
        )
        store.structured_query(
            "UNWIND $rows AS row MERGE (v:ERVerdict {key: row.key}) "
            "SET v.same = row.same, v.updated = datetime()",
            param_map={"rows": [{"key": k, "same": s} for k, s in entries.items()]},
        )
    except Exception as exc:
        logger.warning("ER verdict cache store failed: {e}", e=exc)


# ── consolidation ──────────────────────────────────────────────────


def _pick_canonical(cluster_items: list[_Item], cfg: ERConfig) -> _Item:
    """Pick the canonical entity for a cluster.

    Priority order:
      1. `source == "stored"` — entities already in Neo4j from
         previous ingests ALWAYS win.  This prevents orphan nodes:
         if a new entity has higher mention_count than the stored
         one, picking it as canonical would create a brand-new
         Neo4j node and leave the old one as a dangling alias.
      2. Within the same source class: max mention_count.
      3. Tiebreak: longest name (more specific).
      4. Tiebreak: Cyrillic surface form preferred when language=Russian.
      5. Alphabetical (deterministic).
    """

    def key(it: _Item) -> tuple[int, int, int, int, str]:
        stored_pref = 1 if it.source == "stored" else 0
        cyr_pref = 0
        if cfg.language.lower().startswith("rus"):
            cyr_pref = 1 if _is_cyrillic_name(it.name) else 0
        return (stored_pref, it.mention_count, len(it.name), cyr_pref, it.name)

    return max(cluster_items, key=key)


async def _consolidate_cluster(
    cluster_items: list[_Item],
    canonical: _Item,
    *,
    llm: Any,
    cfg: ERConfig,
    er_review_needed: bool = False,
) -> EntityNode:
    """Build the canonical EntityNode for a cluster."""
    descriptions = [it.description for it in cluster_items if it.description]
    mention_counts = sum(max(it.mention_count, 1) for it in cluster_items)
    labels = Counter(it.label for it in cluster_items)
    canonical_label = labels.most_common(1)[0][0] if labels else canonical.label
    aliases = sorted({
        it.name for it in cluster_items if it.name != canonical.name
    })

    merged_desc = await _maybe_summarize_descriptions(
        llm=llm,
        description_name=canonical.name,
        description_type="Entity",
        descriptions=descriptions,
        force_count=8,
        force_chars=12_000,
        summary_max_tokens=1200,
        language=cfg.language,
    )

    source_chunks: list[str] = []
    file_paths: list[str] = []
    for it in cluster_items:
        if it.entity is None:
            continue
        props = it.entity.properties or {}
        for cid in (props.get("source_chunks") or []):
            if cid not in source_chunks:
                source_chunks.append(cid)
        for fp in (props.get("file_paths") or []):
            if fp not in file_paths:
                file_paths.append(fp)

    properties: dict[str, Any] = {
        "description": merged_desc,
        "source_chunks": source_chunks,
        "file_paths": file_paths,
        "mention_count": mention_counts,
        "aliases": aliases,
        "er_canonical_name": canonical.name,
        "er_embedding": json.dumps(canonical.embedding),
    }
    if er_review_needed:
        properties["er_review_needed"] = True
    # Native vector kNN (opt-in): also store the embedding as a native
    # list property so the ER vector index can find this canonical.
    if cfg.use_native_vector_knn and canonical.embedding:
        properties["er_vec"] = list(canonical.embedding)

    return EntityNode(
        name=canonical.name,
        label=canonical_label,
        properties=properties,
    )


# ── name-map application ───────────────────────────────────────────


def _apply_name_map(
    name_map: dict[str, str],
    relations: list[Relation],
    nodes: list[BaseNode],
) -> list[Relation]:
    """Rewrite chunk-level KG_NODES_KEY entity names AND merged
    relations to use canonical names.  Drops self-loops, re-
    aggregates pairs that collide post-rewrite.

    Mutation: chunks are mutated in place.  `relations` is rebuilt
    and returned (cannot mutate in place because we may collapse
    multiple inputs into one output).
    """
    if not name_map:
        return relations

    # 1. Rewrite chunk metadata.
    #
    # Both KG_NODES_KEY (EntityNode.name) AND KG_RELATIONS_KEY
    # (Relation.source_id / target_id) reference entities by NAME.
    # PropertyGraphIndex MERGEs entities by id (= name) and resolves
    # relations the same way — if a per-chunk Relation still points
    # at a pre-canonical name, Neo4j creates a phantom :Chunk node
    # with that name as id.  Rewrite both so the chunk-level KG state
    # is consistent with the merged-relations output.
    for node in nodes:
        meta = node.metadata or {}
        ents = meta.get(KG_NODES_KEY) or []
        for ent in ents:
            if not isinstance(ent, EntityNode):
                continue
            normalized = _normalize_entity_name(ent.name)
            canonical = name_map.get(normalized)
            if canonical and canonical != ent.name:
                ent.name = canonical
        rels = meta.get(KG_RELATIONS_KEY) or []
        for rel in rels:
            if not isinstance(rel, Relation):
                continue
            src_norm = _normalize_entity_name(str(rel.source_id))
            tgt_norm = _normalize_entity_name(str(rel.target_id))
            src_can = name_map.get(src_norm)
            tgt_can = name_map.get(tgt_norm)
            if src_can and src_can != rel.source_id:
                rel.source_id = src_can
            if tgt_can and tgt_can != rel.target_id:
                rel.target_id = tgt_can

    # 2. Rewrite + dedup merged relations.
    #
    # Two-step dedup:
    #   a) drop self-loops (same canonical at both endpoints — happens
    #      when two clustered entities had a relation between them);
    #   b) merge by undirected (source_id, target_id) pair regardless
    #      of relation label.  When the same pair has multiple
    #      relations with different labels (LightRAG often emits
    #      both "LEADERSHIP" and "COMPANY" for "person heads org"),
    #      keep the one with the longest description and append the
    #      others' keywords/labels to its keywords list.
    keyed: dict[tuple[str, str], Relation] = {}
    for rel in relations:
        # Rewrite endpoints to canonical names BEFORE dedup-key
        # construction.  Without this the merged-relations list that
        # gets `graph_store.upsert_relations`'d still points at the
        # pre-canonical names, which makes Neo4j create phantom
        # :Chunk nodes for those missing entity ids.
        src_norm = _normalize_entity_name(str(rel.source_id))
        tgt_norm = _normalize_entity_name(str(rel.target_id))
        src_can = name_map.get(src_norm)
        tgt_can = name_map.get(tgt_norm)
        if src_can:
            rel.source_id = src_can
        if tgt_can:
            rel.target_id = tgt_can
        src_id = str(rel.source_id)
        tgt_id = str(rel.target_id)
        if src_id == tgt_id:
            continue
        key = tuple(sorted((src_id, tgt_id)))
        existing = keyed.get(key)
        if existing is None:
            keyed[key] = rel
            continue
        # Pick the relation with the longer description as the
        # survivor; merge the other's metadata in.
        ex_desc = str((existing.properties or {}).get("description") or "")
        new_desc = str((rel.properties or {}).get("description") or "")
        survivor = existing if len(ex_desc) >= len(new_desc) else rel
        loser = rel if survivor is existing else existing
        sp = survivor.properties or {}
        lp = loser.properties or {}
        # Merge keywords and append loser's label as a keyword for
        # traceability — saves "LEADERSHIP" and "COMPANY" info.
        sp_kw = [k.strip() for k in str(sp.get("keywords") or "").split(",") if k.strip()]
        lp_kw = [k.strip() for k in str(lp.get("keywords") or "").split(",") if k.strip()]
        loser_label = (loser.label or "").lower().replace("_", " ")
        if loser_label and loser_label not in sp_kw:
            sp_kw.append(loser_label)
        for kw in lp_kw:
            if kw not in sp_kw:
                sp_kw.append(kw)
        sp["keywords"] = ", ".join(sp_kw)
        # Sum mention_count.
        sp["mention_count"] = int(sp.get("mention_count") or 1) + int(
            lp.get("mention_count") or 1,
        )
        # Union source_chunks.
        sc = list(sp.get("source_chunks") or [])
        for cid in (lp.get("source_chunks") or []):
            if cid not in sc:
                sc.append(cid)
        sp["source_chunks"] = sc
        survivor.properties = sp
        keyed[key] = survivor
    return list(keyed.values())


# ── existing canonicals (incremental ER) ───────────────────────────


async def _cleanup_stored_losers(
    graph_store: Any,
    pairs: list[tuple[str, str]],
) -> None:
    """Repoint relations from each loser stored node onto its
    canonical sibling, then detach-delete the loser.

    Uses ``apoc.merge.relationship`` to copy each loser's incoming and
    outgoing edges onto the canonical with the original (dynamic)
    relationship type and dedup, then ``DETACH DELETE`` the loser.
    APOC is already a project-wide dependency (e.g. ``graph_walk`` uses
    ``apoc.coll.flatten``).

    Failure is SAFE-BY-INACTION: if the repoint+delete throws (APOC
    missing, transient error), the loser node is LEFT INTACT — it stays
    an un-merged duplicate (recoverable on a later run) rather than
    losing its relationships.  The old fallback ``DETACH DELETE``d the
    loser *without* moving its edges, silently dropping them.
    """
    for loser_name, canon_name in pairs:
        try:
            await asyncio.to_thread(
                graph_store.structured_query,
                """
                MATCH (loser:__Entity__ {name: $loser})
                MATCH (canon:__Entity__ {name: $canon})
                WHERE elementId(loser) <> elementId(canon)
                // Copy outgoing edges loser→X to canon→X
                CALL {
                    WITH loser, canon
                    MATCH (loser)-[r]->(t)
                    WHERE elementId(t) <> elementId(canon)
                    WITH canon, t, type(r) AS rt, properties(r) AS rp
                    CALL apoc.merge.relationship(canon, rt, {}, rp, t, {})
                        YIELD rel
                    RETURN count(*) AS _o
                }
                // Copy incoming edges X→loser to X→canon
                CALL {
                    WITH loser, canon
                    MATCH (s)-[r]->(loser)
                    WHERE elementId(s) <> elementId(canon)
                    WITH canon, s, type(r) AS rt, properties(r) AS rp
                    CALL apoc.merge.relationship(s, rt, {}, rp, canon, {})
                        YIELD rel
                    RETURN count(*) AS _i
                }
                DETACH DELETE loser
                """,
                {"loser": loser_name, "canon": canon_name},
            )
        except Exception as exc:
            # Safe-by-inaction: leave the loser node INTACT (with its
            # edges) rather than deleting it without repointing.  Worst
            # case is a lingering duplicate, which a later ER run can
            # still merge — never silent relationship loss.
            logger.warning(
                "ER stored-loser cleanup failed for {l}→{c}; leaving "
                "loser intact (un-merged duplicate, recoverable): {e}",
                l=loser_name, c=canon_name, e=exc,
            )


async def _load_existing_canonicals(
    graph_store: Any | None,
    *,
    limit: int = 5000,
) -> list[_Item]:
    """Read Neo4j entities with `er_canonical_name` and their stored
    embedding.  Returns empty when graph_store is None or any error
    occurs (incremental ER is best-effort — without it we still do
    within-batch ER).

    The result is ordered by ``mention_count DESC`` so that, when the
    graph has more canonicals than ``limit``, the window always contains
    the most-mentioned (hub) entities rather than an arbitrary Neo4j
    slice — the difference between a new mention reliably matching a
    frequent entity and silently fragmenting into a duplicate.
    """
    if graph_store is None:
        return []
    try:
        # `structured_query` is a generic Cypher entry on the
        # PropertyGraphStore base.  Available on Neo4jPGStore.
        rows = await asyncio.to_thread(
            graph_store.structured_query,
            """
            MATCH (n:__Entity__)
            WHERE n.er_canonical_name IS NOT NULL
            RETURN n.name AS name,
                   labels(n) AS labels,
                   n.er_embedding AS er_embedding,
                   coalesce(n.mention_count, 1) AS mention_count,
                   coalesce(n.description, '') AS description
            ORDER BY mention_count DESC
            LIMIT $limit
            """,
            param_map={"limit": int(limit)},
        )
    except Exception as exc:
        logger.warning("ER load existing canonicals failed: {err}", err=exc)
        return []

    out: list[_Item] = []
    for row in rows or []:
        name = row.get("name") or ""
        if not name:
            continue
        raw_emb = row.get("er_embedding") or "[]"
        try:
            emb = json.loads(raw_emb) if isinstance(raw_emb, str) else list(raw_emb)
        except json.JSONDecodeError:
            emb = []
        if not emb:
            continue
        labels = [lab for lab in (row.get("labels") or [])
                  if lab not in ("__Entity__", "__Node__")]
        label = labels[0] if labels else "Other"
        out.append(_Item(
            name=name,
            norm=_normalize_entity_name(name),
            label=label,
            description=row.get("description") or "",
            mention_count=int(row.get("mention_count") or 1),
            source="stored",
            embedding=emb,
        ))
    return out


async def _load_candidates_native(
    graph_store: Any | None,
    new_items: list[_Item],
    *,
    k: int,
    dim: int,
) -> list[_Item]:
    """Native-vector-index alternative to ``_load_existing_canonicals``.

    For each new entity, queries the ER vector index
    (``er_embedding_vec`` over ``__Entity__.er_vec``) for its ``k``
    nearest stored canonicals — across the WHOLE graph, no window — and
    returns the deduplicated union as stored ``_Item``s.  Best-effort:
    returns ``[]`` (→ within-batch ER only) when the store is missing or
    every query errors (e.g. index not built yet — run the backfill).
    """
    if graph_store is None:
        return []
    # Idempotently ensure the index exists (no-op once built).
    try:
        from src.graph.index import ensure_er_vector_index

        await asyncio.to_thread(ensure_er_vector_index, graph_store, dim)
    except Exception as exc:
        logger.warning("ensure ER vector index failed: {e}", e=exc)

    seen: dict[str, _Item] = {}
    for it in new_items:
        if not it.embedding:
            continue
        try:
            rows = await asyncio.to_thread(
                graph_store.structured_query,
                """
                CALL db.index.vector.queryNodes('er_embedding_vec', $k, $vec)
                YIELD node
                WHERE node.er_canonical_name IS NOT NULL
                RETURN node.name AS name,
                       labels(node) AS labels,
                       node.er_vec AS er_vec,
                       node.er_embedding AS er_embedding,
                       coalesce(node.mention_count, 1) AS mention_count,
                       coalesce(node.description, '') AS description
                """,
                param_map={"k": int(k), "vec": list(it.embedding)},
            )
        except Exception as exc:
            logger.warning("ER native kNN query failed: {e}", e=exc)
            continue
        for row in rows or []:
            name = row.get("name") or ""
            if not name or name in seen:
                continue
            emb = row.get("er_vec")
            if not emb:  # fall back to the legacy JSON column
                raw = row.get("er_embedding") or "[]"
                try:
                    emb = json.loads(raw) if isinstance(raw, str) else list(raw)
                except json.JSONDecodeError:
                    emb = []
            if not emb:
                continue
            labels = [lab for lab in (row.get("labels") or [])
                      if lab not in ("__Entity__", "__Node__")]
            seen[name] = _Item(
                name=name,
                norm=_normalize_entity_name(name),
                label=labels[0] if labels else "Other",
                description=row.get("description") or "",
                mention_count=int(row.get("mention_count") or 1),
                source="stored",
                embedding=list(emb),
            )
    return list(seen.values())


# ── public entry point ─────────────────────────────────────────────


async def resolve_entities(
    entities: list[EntityNode],
    relations: list[Relation],
    nodes: list[BaseNode],
    *,
    llm: Any,
    embed_model: Any,
    graph_store: Any | None = None,
    config: ERConfig | None = None,
    er_store: Any | None = None,
) -> tuple[list[EntityNode], list[Relation], dict[str, str]]:
    """Run ER over already-merged entities.

    Returns:
      * `resolved_entities`: list of canonical EntityNodes.  Each
        carries the consolidated description plus `aliases`,
        `er_canonical_name`, `er_embedding` properties.
      * `resolved_relations`: relations with self-loops dropped;
        endpoint identities remain UUIDs (Neo4j merges by name
        downstream via the rewritten chunk metadata).
      * `name_map`: `{old_normalised_name → canonical_name}` for
        each entity that got merged into another canonical.

    Behaviour notes:
      * Entities with deterministic-identifier labels (Phone, INN,
        Email, ...) are excluded — those have their own canon
        from `inject_canonical_entities`.
      * `graph_store=None` skips the incremental cross-ingest
        matching (only within-batch ER).
      * Any embed-model or LLM failure falls back to a no-op pass:
        return `(entities, relations, {})` so the ingest path
        keeps working.
    """
    cfg = config or ERConfig()
    eligible = cfg.eligible_labels or None  # empty → all (minus deterministic)

    # 1. Filter eligible new entities.
    new_items: list[_Item] = []
    skipped: list[EntityNode] = []
    for ent in entities:
        label = ent.label or "Other"
        if label in _DETERMINISTIC_LABELS:
            skipped.append(ent)
            continue
        if eligible and label not in eligible:
            skipped.append(ent)
            continue
        props = ent.properties or {}
        # P5 — generic-name guard: short single-token names with
        # mention_count == 1 and empty description are too generic.
        # They pass through unchanged AND don't get `er_canonical_name`,
        # so the next ingest won't try to match against them.
        if cfg.skip_generic_names:
            tokens = (ent.name or "").split()
            mc = int(props.get("mention_count") or 1)
            desc = str(props.get("description") or "")
            if (len(tokens) == 1 and len(tokens[0]) < 5
                    and mc <= 1 and not desc):
                skipped.append(ent)
                continue
        new_items.append(_Item(
            name=ent.name,
            norm=_normalize_entity_name(ent.name),
            label=label,
            description=str(props.get("description") or ""),
            mention_count=int(props.get("mention_count") or 1),
            source="new",
            entity=ent,
        ))

    if not new_items:
        return entities, relations, {}

    # 2. Embed new entities first — needed for candidate cosines and,
    #    when native kNN is on, to query the ER vector index.  Stored
    #    items already carry embeddings.
    if not await _embed_entities(new_items, embed_model):
        logger.warning("ER skipped: embed model failed")
        return entities, relations, {}

    # 3. Load the stored canonicals to compare against.  Native vector
    #    kNN (opt-in) queries the ER vector index per new entity over the
    #    WHOLE graph (no window ceiling); otherwise the bounded
    #    mention_count window.
    if cfg.use_native_vector_knn:
        from src.config import settings as _settings

        stored_items = await _load_candidates_native(
            graph_store, new_items, k=cfg.vector_knn_k, dim=_settings.milvus.dim,
        )
    else:
        stored_items = await _load_existing_canonicals(
            graph_store, limit=cfg.incremental_window,
        )

    all_items = new_items + stored_items
    items_by_norm: dict[str, _Item] = {it.norm: it for it in all_items}

    # 4. Deterministic pre-pass.
    confirmed_pairs: set[tuple[str, str]] = set()
    for a, b in _deterministic_pairs(all_items):
        confirmed_pairs.add(tuple(sorted((a, b))))

    # 5. Candidate generation.
    auto_pairs, borderline_pairs = _candidate_pairs(all_items, cfg)
    for a, b, cos in auto_pairs:
        confirmed_pairs.add(tuple(sorted((a, b))))
        logger.debug(
            "ER auto-merge  '{a}' ≡ '{b}'  cosine={c:.3f}",
            a=items_by_norm[a].name, b=items_by_norm[b].name, c=cos,
        )

    # 6. LLM judge borderline.
    # Skip pairs already confirmed by the deterministic pre-pass —
    # spending an LLM call on an already-certain merge is waste and
    # risks the judge over-ruling a high-precision rule (e.g.
    # initialism match).
    borderline_pairs = [
        (a, b, c) for (a, b, c) in borderline_pairs
        if tuple(sorted((a, b))) not in confirmed_pairs
    ]
    if borderline_pairs:
        judge_input = [
            (items_by_norm[a], items_by_norm[b])
            for a, b, _ in borderline_pairs
            if a in items_by_norm and b in items_by_norm
        ]
        for (it_a, it_b), (_, _, cos) in zip(judge_input, borderline_pairs):
            logger.debug(
                "ER judge-pair  '{a}' (label={la}) ↔ '{b}' (label={lb})  cosine={c:.3f}",
                a=it_a.name, la=it_a.label,
                b=it_b.name, lb=it_b.label, c=cos,
            )
        # Persistent verdict cache: skip re-judging pairs we've already
        # judged in a prior run / earlier hub-heavy doc.  OPTIONAL and
        # FAIL-SAFE — when disabled or no store is available, this
        # reduces to the original `verdicts = _llm_judge_pairs(...)`
        # path (cache empty, nothing cached, nothing uncached-only,
        # nothing persisted).  Order/length of `verdicts` is preserved
        # to match `judge_input` exactly for the downstream zip.
        cache: dict[str, bool] = {}
        if cfg.verdict_cache_enabled and er_store is not None:
            keys = [_verdict_key(a, b) for a, b in judge_input]
            # sync Neo4j read — off the loop.
            cache = await asyncio.to_thread(_load_verdict_cache, er_store, keys)
        cached, uncached = _partition_cached(judge_input, cache)
        fresh = await _llm_judge_pairs(uncached, llm, cfg)
        vmap = {id(p): v for (p, v) in cached}
        for p, v in zip(uncached, fresh):
            vmap[id(p)] = v
        verdicts = [vmap[id(p)] for p in judge_input]
        if cfg.verdict_cache_enabled and er_store is not None:
            # sync Neo4j write — off the loop.
            await asyncio.to_thread(
                _store_verdicts,
                er_store,
                {_verdict_key(a, b): v for (a, b), v in zip(uncached, fresh)},
            )
        for ok, (it_a, it_b) in zip(verdicts, judge_input):
            logger.info(
                "ER judge-verdict  '{a}' vs '{b}' = {v}",
                a=it_a.name, b=it_b.name,
                v="SAME" if ok else "DIFFERENT",
            )
            if ok:
                confirmed_pairs.add(tuple(sorted((
                    items_by_norm[it_a.norm].norm,
                    items_by_norm[it_b.norm].norm,
                ))))

    # 7. Union-find clustering.
    uf = _UnionFind()
    for it in all_items:
        uf.add(it.norm)
    for a, b in confirmed_pairs:
        uf.union(a, b)
    components = [c for c in uf.groups() if len(c) > 1]

    # 8. Hyper-hub clamp.
    review_clusters: list[set[str]] = []
    final_clusters: list[set[str]] = []
    for c in components:
        if len(c) >= cfg.hyper_hub_threshold:
            review_clusters.append(c)
        else:
            final_clusters.append(c)

    # 9. Verify clusters with ≥ verify_cluster_size members via a
    #    single consolidation LLM call per cluster.  LightRAG-style
    #    protection against transitive over-merge — pairwise judge
    #    confirmed A↔B and B↔C, but A might not actually be C.
    verified_clusters: list[set[str]] = []
    for cluster in final_clusters:
        if len(cluster) < cfg.verify_cluster_size:
            verified_clusters.append(cluster)
            continue
        cluster_items = [items_by_norm[n] for n in cluster if n in items_by_norm]
        groups = await _verify_cluster(cluster_items, llm)
        for group in groups:
            if len(group) >= 2:
                verified_clusters.append({it.norm for it in group})
            # Singletons drop out of the cluster pool — they're
            # added back via the normal singleton path later.
    final_clusters = verified_clusters

    # 10. Pick canonical and build name_map.
    name_map: dict[str, str] = {}
    new_canonical_entities: list[EntityNode] = []
    cluster_canonicals: dict[str, str] = {}  # norm → canonical norm

    for cluster in final_clusters:
        cluster_items = [items_by_norm[n] for n in cluster if n in items_by_norm]
        if not cluster_items:
            continue
        canonical = _pick_canonical(cluster_items, cfg)
        cluster_canonicals.update({it.norm: canonical.norm for it in cluster_items})
        contains_new = any(it.source == "new" for it in cluster_items)
        logger.info(
            "ER cluster  canonical='{c}'  size={s}  members={m}  has_new={n}",
            c=canonical.name, s=len(cluster_items),
            m=[it.name + (" (stored)" if it.source == "stored" else "")
               for it in cluster_items],
            n=contains_new,
        )
        if not contains_new:
            # Pure stored-only cluster — nothing to upsert.
            continue
        merged = await _consolidate_cluster(
            cluster_items, canonical, llm=llm, cfg=cfg,
        )
        new_canonical_entities.append(merged)
        for it in cluster_items:
            if it.name != canonical.name:
                name_map[_normalize_entity_name(it.name)] = canonical.name

    # Hyper-hub clusters: pass through unchanged but mark for review.
    for cluster in review_clusters:
        cluster_items = [items_by_norm[n] for n in cluster if n in items_by_norm]
        new_ones = [it for it in cluster_items if it.source == "new"]
        for it in new_ones:
            if it.entity is None:
                continue
            (it.entity.properties or {})["er_review_needed"] = True

    # 11. Build the final entity list:
    #     * Canonical-merged entities for clusters with at least one
    #       new member.
    #     * Singletons (entities not in any cluster) pass through
    #       unchanged with `er_canonical_name` + `er_embedding`
    #       added so the next ingest's incremental ER can use them.
    singleton_norms = {
        it.norm for it in new_items
        if it.norm not in cluster_canonicals
    }
    for it in new_items:
        if it.norm not in singleton_norms:
            continue
        ent = it.entity
        if ent is None:
            continue
        if ent.properties is None:
            ent.properties = {}
        ent.properties.setdefault("er_canonical_name", ent.name)
        ent.properties["er_embedding"] = json.dumps(it.embedding)
        if cfg.use_native_vector_knn and it.embedding:
            ent.properties["er_vec"] = list(it.embedding)

    # Build output entity list.
    by_canonical_norm = {
        _normalize_entity_name(e.name): e for e in new_canonical_entities
    }
    out_entities: list[EntityNode] = []
    seen: set[str] = set()
    for it in new_items:
        canonical_norm = cluster_canonicals.get(it.norm)
        if canonical_norm:
            # Cluster member — replace with the canonical version.
            canonical_ent = by_canonical_norm.get(canonical_norm)
            if canonical_ent is None or canonical_norm in seen:
                continue
            out_entities.append(canonical_ent)
            seen.add(canonical_norm)
        else:
            if it.entity and it.norm not in seen:
                out_entities.append(it.entity)
                seen.add(it.norm)
    out_entities.extend(skipped)

    # 12. Apply name_map to chunk metadata + clean self-loop relations.
    resolved_relations = _apply_name_map(name_map, relations, nodes)

    # 13. Clean up stored-loser entities.  When a cluster contains
    # two `(stored)` items + one new (e.g. doc1 stored 'СтройИнвест',
    # doc2 stored 'АО «СтройИнвест', doc3 EN 'Stroyinvest Jsc' lands
    # and the LLM agrees all three are the same), the canonical
    # upsert only refreshes the surviving canonical node — the
    # non-canonical stored node remains in Neo4j as an orphan with
    # its historical relations.  Repoint those edges onto the
    # canonical node and detach-delete the loser.
    if graph_store is not None and name_map:
        stored_losers: list[tuple[str, str]] = []
        for it in stored_items:
            canon_norm = cluster_canonicals.get(it.norm)
            if canon_norm and canon_norm != it.norm:
                canon_item = items_by_norm.get(canon_norm)
                if canon_item and canon_item.name != it.name:
                    stored_losers.append((it.name, canon_item.name))
        if stored_losers:
            await _cleanup_stored_losers(graph_store, stored_losers)
            logger.info(
                "ER stored-loser cleanup  pairs={p}",
                p=stored_losers,
            )

    logger.info(
        "ER done  new_entities={n}  canonical_clusters={c}  "
        "merged={m}  review={r}",
        n=len(new_items), c=len(final_clusters),
        m=len(name_map), r=sum(len(c) for c in review_clusters),
    )
    return out_entities, resolved_relations, name_map


__all__ = [
    "ERConfig",
    "resolve_entities",
]
