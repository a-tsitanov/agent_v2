"""Whole-graph batch consolidation for SEMANTIC entities.

Re-runs Entity Resolution (Person / Organization / Concept / …) over the
ENTIRE existing Neo4j graph to merge fragmented duplicate nodes that
accumulated across many independent ingests.  Where the per-ingest ER
only ever compares a NEW batch against stored canonicals, this job
sweeps every eligible entity already in the graph and consolidates the
duplicates that slipped through.

Unlike ``scripts/merge_identifier_duplicates`` — which covers only
deterministic identifier nodes (Phone / Email / INN / …) and flattens
every redirected edge to ``RELATED_TO`` — THIS job operates on semantic
entities and PRESERVES the original relationship types: it reuses
``entity_resolution._cleanup_stored_losers`` (``apoc.merge.relationship``
with the source edge's ``type(r)``), so a ``WORKS_AT`` stays a
``WORKS_AT`` after a merge.

How it works
------------
The real ER engine (``resolve_entities``) is reused as a pure DECISION
function.  We wrap the graph store in a READ-ONLY proxy
(``_ReadOnlyGraphStore``) that passes candidate-loading reads through but
NO-OPS every write, and pass it as BOTH ``graph_store`` and ``er_store``.
``resolve_entities`` therefore never mutates the graph; its returned
``name_map`` (``old_normalised_name -> canonical_name``) is a clean merge
decision.  We accumulate that across all batches, collapse transitive
chains, then — only with ``--no-dry-run`` — apply the merges with the
real store via ``_cleanup_stored_losers``.

PREREQUISITES (read before running ``--no-dry-run``)
----------------------------------------------------
1. Take a Neo4j BACKUP first.  Merges DETACH DELETE the loser nodes.
2. For ``--candidate-source native`` (the default), the ER vector index
   (``er_embedding_vec`` over ``__Entity__.er_vec``) and ``er_vec`` must
   already exist — the read-only proxy NO-OPs the ``CREATE VECTOR INDEX``
   write, so this job cannot build it.  Run::

       python -m scripts.backfill_er_vector --no-dry-run

   first.  Otherwise use ``--candidate-source window`` (bounded
   mention_count window, no index needed).

Usage::

    python -m scripts.reresolve_graph                       # dry-run (default)
    python -m scripts.reresolve_graph --candidate-source window
    python -m scripts.reresolve_graph --types Person Organization --limit 500
    python -m scripts.reresolve_graph --no-dry-run --yes     # APPLY merges

Apply only AFTER a Neo4j backup.  The apply step is idempotent and
safe-by-inaction (a missing loser is a no-op; a failed repoint leaves the
loser intact as a recoverable duplicate).
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger

from src.config import settings
from src.graph.entity_resolution import (
    _DETERMINISTIC_LABELS,
    ERConfig,
    _cleanup_stored_losers,
    resolve_entities,
)
from src.graph.lightrag_parse import _normalize_entity_name
from src.utils.logging import configure_logging

# Fallback for the per-ingest stored-canonical window: there is no
# dedicated setting, and the spec fixes 5000 (matches ERConfig's own
# default).  Used only for ``--candidate-source window``.
_DEFAULT_INCREMENTAL_WINDOW = 5000


# ── pure helpers ─────────────────────────────────────────────────────


# Substrings (uppercased) that mark a Cypher statement as a WRITE.  The
# read-only proxy no-ops any query containing one of these; everything
# else (MATCH ... RETURN, the native-kNN db.index.vector.queryNodes read)
# passes through to the real store.  The leading/trailing spaces on
# ``DELETE ``/`` MERGE ``/`` SET ``/``CREATE `` keep them from matching
# substrings of unrelated tokens (e.g. ``RETURN`` does not contain
# ``CREATE``; ``queryNodes`` does not contain `` MERGE ``).
_WRITE_MARKERS: tuple[str, ...] = (
    "DETACH DELETE",
    "DELETE ",
    " MERGE ",
    " SET ",
    "CREATE ",
    "APOC.MERGE",
)


def _is_write_cypher(cypher: str) -> bool:
    """True when ``cypher`` mutates the graph.

    Pure substring heuristic over the uppercased query.  Deliberately
    conservative: any statement containing a write marker is treated as a
    write so the proxy never lets a mutation slip through during dry-run.
    """
    upper = (cypher or "").upper()
    return any(marker in upper for marker in _WRITE_MARKERS)


def _loader_cypher() -> str:
    """Cypher that streams ALL eligible semantic entities in a stable
    (name-ordered) batched window.

    Picks the entity's domain label (the first label that is not the
    structural ``__Entity__`` / ``__Node__``) and returns the fields
    needed to rebuild an ``EntityNode`` for ER.  Ordering by ``name`` +
    SKIP/LIMIT gives deterministic, resumable batching.
    """
    return """
    MATCH (n:__Entity__)
    WITH n, head([l IN labels(n) WHERE l <> '__Entity__' AND l <> '__Node__']) AS etype
    WHERE etype IS NOT NULL
    RETURN n.name AS name, etype AS label,
           coalesce(n.description,'') AS description,
           coalesce(n.mention_count,1) AS mention_count
    ORDER BY name SKIP $skip LIMIT $limit
    """


def _resolve_chains(merges: dict[str, str]) -> dict[str, str]:
    """Collapse transitive merge chains to their final canonical.

    Given ``{loser -> canon}`` where a canon may itself be a loser
    elsewhere (``{A->B, B->C}``), rewrite every loser to point at the
    terminal canonical (``{A->C, B->C}``).  Self-maps (``loser == canon``)
    are dropped — they are no-op pairs.

    Cycle-safe: a degenerate cycle (``{A->B, B->A}``) terminates by
    bailing out once a node is revisited, rather than looping forever.
    """
    out: dict[str, str] = {}
    for loser in merges:
        seen: set[str] = {loser}
        canon = merges[loser]
        # Follow the chain until we hit a terminal canon (one that is not
        # itself a loser) or detect a cycle.
        while canon in merges and canon not in seen:
            seen.add(canon)
            canon = merges[canon]
        if canon != loser:
            out[loser] = canon
    return out


# ── read-only graph-store proxy ──────────────────────────────────────


class _ReadOnlyGraphStore:
    """Wrap a real graph store so writes become no-ops.

    ``structured_query`` passes READS through to the inner store (so
    candidate loading / native-kNN works for ``resolve_entities``) but
    returns ``[]`` for any WRITE (``_is_write_cypher``) without touching
    the inner store.  Every other attribute access is delegated to the
    inner store via ``__getattr__``.

    Passed as BOTH ``graph_store`` and ``er_store`` to
    ``resolve_entities`` so that the engine's only graph mutations
    (``_cleanup_stored_losers`` and the ``:ERVerdict`` cache writes) are
    suppressed — leaving ``name_map`` as a pure merge decision.
    """

    def __init__(self, inner: Any) -> None:
        # Bypass __setattr__/__getattr__ recursion by writing to __dict__.
        object.__setattr__(self, "_inner", inner)

    def structured_query(self, cypher: str, param_map: dict | None = None):
        if _is_write_cypher(cypher):
            logger.debug("read-only proxy: no-op write {c}", c=cypher[:80])
            return []
        return self._inner.structured_query(cypher, param_map=param_map)

    def __getattr__(self, name: str) -> Any:
        # Only reached for attributes NOT found on the proxy itself.
        return getattr(object.__getattribute__(self, "_inner"), name)


# ── batched loading ──────────────────────────────────────────────────


async def _load_all_entities(
    graph_store: Any,
    *,
    batch_size: int,
    limit: int | None,
    types: frozenset[str] | None,
) -> list[dict[str, Any]]:
    """Page through every eligible semantic entity.

    Deterministic-identifier labels are excluded up front (belt-and-
    suspenders — ``resolve_entities`` also skips them).  When ``types`` is
    given, only those labels are kept.  ``limit`` caps the TOTAL number of
    entities loaded (testing convenience).
    """
    loader = _loader_cypher()
    rows: list[dict[str, Any]] = []
    skip = 0
    while True:
        page_limit = batch_size
        if limit is not None:
            remaining = limit - len(rows)
            if remaining <= 0:
                break
            page_limit = min(batch_size, remaining)
        page = await asyncio.to_thread(
            graph_store.structured_query,
            loader,
            param_map={"skip": skip, "limit": page_limit},
        )
        page = page or []
        for row in page:
            label = row.get("label")
            if not label or label in _DETERMINISTIC_LABELS:
                continue
            if types and label not in types:
                continue
            rows.append(row)
        if len(page) < page_limit:
            break
        skip += page_limit
    return rows


# ── core consolidation ───────────────────────────────────────────────


async def _plan_merges(
    graph_store: Any,
    rows: list[dict[str, Any]],
    *,
    batch_size: int,
    candidate_source: str,
    types: frozenset[str] | None,
) -> tuple[dict[str, str], int]:
    """Run ER over the loaded rows in batches and collect the merge plan.

    Returns ``(merges, batches)`` where ``merges`` is the chain-resolved
    ``{loser_name -> canon_name}`` decision (NO graph mutation — the proxy
    suppresses every write).
    """
    from llama_index.core.graph_stores.types import EntityNode

    from src.ingestion.embeddings import build_embedding_model
    from src.retrieval.llm_pool import get_llm_pool

    judge_llm = get_llm_pool().get("judge")
    embed_model = build_embedding_model()

    proxy = _ReadOnlyGraphStore(graph_store)
    cfg = ERConfig(
        use_native_vector_knn=(candidate_source == "native"),
        vector_knn_k=settings.agent.er_vector_knn_k,
        incremental_window=_DEFAULT_INCREMENTAL_WINDOW,
        eligible_labels=frozenset(types) if types else frozenset(),
    )

    raw_merges: dict[str, str] = {}
    batches = 0
    for start in range(0, len(rows), batch_size):
        chunk = rows[start : start + batch_size]
        batch = [
            EntityNode(
                name=row["name"],
                label=row["label"],
                properties={
                    "description": row.get("description") or "",
                    "mention_count": int(row.get("mention_count") or 1),
                },
            )
            for row in chunk
            if row.get("name")
        ]
        if not batch:
            continue
        batches += 1
        _, _, name_map = await resolve_entities(
            batch,
            [],
            [],
            llm=judge_llm,
            embed_model=embed_model,
            graph_store=proxy,
            config=cfg,
            er_store=proxy,
        )
        # name_map keys are NORMALISED loser names; values are canonical
        # display names.  We key the plan on the loser DISPLAY name (what
        # _cleanup_stored_losers MATCHes on), so map norm->display here.
        norm_to_name = {_normalize_entity_name(b.name): b.name for b in batch}
        for loser_norm, canon_name in name_map.items():
            loser_name = norm_to_name.get(loser_norm)
            if loser_name and loser_name != canon_name:
                raw_merges[loser_name] = canon_name

    return _resolve_chains(raw_merges), batches


async def _apply_merges(
    graph_store: Any,
    merges: dict[str, str],
) -> dict[str, int]:
    """Apply the merge plan with the REAL store (rel-type preserving).

    Skips self-pairs, delegates to ``_cleanup_stored_losers`` (idempotent,
    safe-by-inaction).  Returns a small summary.
    """
    pairs = [(loser, canon) for loser, canon in merges.items() if loser != canon]
    if not pairs:
        return {"applied": 0}
    await _cleanup_stored_losers(graph_store, pairs)
    logger.info("applied {n} merge pairs (rel-type preserving)", n=len(pairs))
    return {"applied": len(pairs)}


# ── entry ────────────────────────────────────────────────────────────


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Plan merges without mutating the graph (default: True). "
        "Pass --no-dry-run to apply.",
    )
    p.add_argument(
        "--candidate-source",
        choices=["native", "window"],
        default="native",
        help="How to load stored candidates: 'native' uses the ER vector "
        "index (run backfill_er_vector first), 'window' uses the bounded "
        "mention_count window (default: native).",
    )
    p.add_argument(
        "--batch-size", type=int, default=200,
        help="Entities per ER batch (default: 200).",
    )
    p.add_argument(
        "--types", nargs="*", default=None,
        help="Restrict to these semantic labels (default: all eligible).",
    )
    p.add_argument(
        "--limit", type=int, default=None,
        help="Cap total entities scanned (testing).",
    )
    p.add_argument(
        "--yes", action="store_true",
        help="Skip the confirmation prompt when applying (--no-dry-run).",
    )
    return p.parse_args(argv)


def _print_plan(merges: dict[str, str]) -> None:
    """Group the plan by canonical and log ``loser -> canon`` lines."""
    by_canon: dict[str, list[str]] = defaultdict(list)
    for loser, canon in merges.items():
        by_canon[canon].append(loser)
    for canon in sorted(by_canon):
        for loser in sorted(by_canon[canon]):
            logger.info("[dry-run] {l!r} -> {c!r}", l=loser, c=canon)


async def _amain(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    configure_logging(level=settings.api.log_level)

    from src.graph.store import build_graph_store

    types = frozenset(args.types) if args.types else None
    graph_store = build_graph_store()

    logger.info(
        "reresolve_graph: candidate_source={s} batch_size={b} types={t} "
        "limit={l} dry_run={d}",
        s=args.candidate_source, b=args.batch_size,
        t=sorted(types) if types else "ALL", l=args.limit, d=args.dry_run,
    )

    rows = await _load_all_entities(
        graph_store,
        batch_size=args.batch_size,
        limit=args.limit,
        types=types,
    )
    if not rows:
        logger.info("no eligible semantic entities — nothing to do")
        return 0

    merges, batches = await _plan_merges(
        graph_store,
        rows,
        batch_size=args.batch_size,
        candidate_source=args.candidate_source,
        types=types,
    )

    summary = {
        "entities_scanned": len(rows),
        "planned_merges": len(merges),
        "batches": batches,
    }

    if args.dry_run:
        _print_plan(merges)
        logger.info("DRY-RUN summary={s} — re-run with --no-dry-run to apply", s=summary)
        return 0

    if not merges:
        logger.info("no merges to apply  summary={s}", s=summary)
        return 0

    if not args.yes:
        prompt = (
            f"About to apply {len(merges)} merges to Neo4j "
            f"(DETACH DELETE losers, rel-types preserved). "
            f"Did you take a backup? Type 'yes' to proceed: "
        )
        if input(prompt).strip().lower() != "yes":
            logger.info("aborted by user")
            return 1

    apply_summary = await _apply_merges(graph_store, merges)
    summary.update(apply_summary)
    logger.info("APPLY done  summary={s}", s=summary)
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_amain(argv))


if __name__ == "__main__":
    raise SystemExit(main())
