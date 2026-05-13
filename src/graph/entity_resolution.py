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
    EntityNode,
    Relation,
)
from llama_index.core.schema import BaseNode
from loguru import logger

from src.graph.lightrag_parse import (
    _cypher_safe_label,
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
    "the", "a", "an", "и", "the", "of", "in",
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
    except Exception as exc:  # noqa: BLE001
        logger.warning("ER embed batch failed: {err}", err=exc)
        return False
    for it, vec in zip(pending, vectors):
        it.embedding = list(vec)
    return True


# ── candidate generation ───────────────────────────────────────────


def _candidate_pairs(
    items: list[_Item], cfg: ERConfig,
) -> tuple[list[tuple[str, str, float]], list[tuple[str, str, float]]]:
    """Compute candidate pairs grouped by label.

    Returns `(auto_merges, borderline)`:
      * `auto_merges` — pairs with cosine ≥ HIGH AND same script.
      * `borderline`  — pairs with cosine in `[LOW, HIGH)` OR
                        cosine ≥ HIGH but cross-script.

    Pairs are reported by NORMALISED name (the union-find key).
    """
    by_label: dict[str, list[_Item]] = defaultdict(list)
    for it in items:
        if it.embedding:
            by_label[it.label].append(it)

    auto: list[tuple[str, str, float]] = []
    borderline: list[tuple[str, str, float]] = []
    seen: set[tuple[str, str]] = set()

    for label, group in by_label.items():
        for i, a in enumerate(group):
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
                cos = _cosine(a.embedding, b.embedding)
                if cos < floor:
                    continue
                # Name-token guard: when both names share at least
                # one content-token (or transliteration of each
                # other across scripts), accept.  Pure embedding-
                # similarity matches with zero token overlap are
                # almost always description-context contamination
                # (e.g. "Romashka" embeds close to "TechnoStroy"
                # because both describe a partnership).
                if cfg.name_token_min_overlap > 0:
                    overlap = _name_token_overlap(a.name, b.name)
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
    False (DIFFERENT, UNSURE, or any failure)."""
    if not pairs:
        return []
    verdicts: list[bool] = [False] * len(pairs)
    for batch_start in range(0, len(pairs), cfg.judge_batch):
        batch = pairs[batch_start: batch_start + cfg.judge_batch]
        body = _format_pair_prompt(batch)
        messages = [
            ChatMessage(role=MessageRole.SYSTEM, content=_JUDGE_SYSTEM),
            ChatMessage(role=MessageRole.USER, content=body),
        ]
        try:
            resp = await llm.achat(messages)
            text = strip_thinking(resp.message.content or "")
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "ER judge batch failed at offset={o}: {err}",
                o=batch_start, err=exc,
            )
            continue
        for verdict_pos, ok in enumerate(_parse_judge_response(text, len(batch))):
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
    except Exception as exc:  # noqa: BLE001
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
    for node in nodes:
        ents = (node.metadata or {}).get(KG_NODES_KEY) or []
        for ent in ents:
            if not isinstance(ent, EntityNode):
                continue
            normalized = _normalize_entity_name(ent.name)
            canonical = name_map.get(normalized)
            if canonical and canonical != ent.name:
                ent.name = canonical

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


async def _load_existing_canonicals(
    graph_store: Any | None,
) -> list[_Item]:
    """Read Neo4j entities with `er_canonical_name` and their stored
    embedding.  Returns empty when graph_store is None or any error
    occurs (incremental ER is best-effort — without it we still do
    within-batch ER).
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
            LIMIT 5000
            """,
        )
    except Exception as exc:  # noqa: BLE001
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

    # 2. Load existing canonicals (incremental ER).
    stored_items = await _load_existing_canonicals(graph_store)

    # 3. Embed new entities.  Stored ones already have embeddings.
    if not await _embed_entities(new_items, embed_model):
        logger.warning("ER skipped: embed model failed")
        return entities, relations, {}

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
        verdicts = await _llm_judge_pairs(judge_input, llm, cfg)
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
