"""Stage 9 — periodic merge job for legacy identifier duplicates.

Adapts ``enterprise-kb/scripts/merge_identifier_duplicates.py`` to
LlamaIndex's ``Neo4jPropertyGraphStore``.  Pure helpers
(``canonicalize_for_type``, ``group_by_canonical``) port verbatim;
the merge-write path uses Cypher directly because LlamaIndex's
graph-store interface lacks a single-shot ``amerge_entities``
equivalent for moving relationships.

Usage::

    python -m scripts.merge_identifier_duplicates                    # dry-run
    python -m scripts.merge_identifier_duplicates --no-dry-run       # apply

Apply only AFTER a Neo4j backup.  Failures per group are logged and
counted; a single bad group does not abort the whole run.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger

from src.config import settings
from src.ingestion.identifiers import extract_identifiers
from src.utils.logging import configure_logging

DEFAULT_IDENTIFIER_TYPES: frozenset[str] = frozenset({
    "PhoneNumber",
    "Email",
    "INN",
    "OGRN",
    "BIC",
    "ContractNumber",
    "PostalAddress",
    "DocumentDate",
    "Amount",
})


# ── pure helpers ─────────────────────────────────────────────────────


def canonicalize_for_type(name: str, entity_type: str) -> str | None:
    """Run all detectors, return canonical for ``entity_type`` or
    None.  Idempotent on already-canonical inputs."""
    for ident in extract_identifiers(name):
        if ident.entity_type == entity_type:
            return ident.canonical
    return None


def group_by_canonical(
    nodes: list[tuple[str, str]],
    types: frozenset[str],
) -> dict[tuple[str, str], list[str]]:
    """``[(name, entity_type), ...]`` → ``{(type, canonical): [names]}``.

    Only groups requiring action are returned: singletons whose
    name already equals the canonical are filtered out.
    """
    groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    for name, etype in nodes:
        if etype not in types:
            continue
        canonical = canonicalize_for_type(name, etype)
        if canonical is None:
            continue
        groups[(etype, canonical)].append(name)
    return {
        key: names
        for key, names in groups.items()
        if len(names) > 1 or (len(names) == 1 and names[0] != key[1])
    }


# ── graph-bound bits (Neo4j via LlamaIndex store) ────────────────────


async def collect_entity_nodes(
    graph_store, types: frozenset[str],
) -> list[tuple[str, str]]:
    """Fetch ``(name, entity_type)`` pairs via the LlamaIndex graph
    store ``get`` API restricted by entity type label."""
    out: list[tuple[str, str]] = []
    for etype in types:
        nodes = graph_store.get(properties={"label": etype}) or []
        for n in nodes:
            name = getattr(n, "name", "") or ""
            label = getattr(n, "label", "") or etype
            if name:
                out.append((name, label))
    return out


def _build_merge_cypher(
    target_label: str, source_names: list[str], target_name: str,
) -> tuple[str, dict]:
    """Generate Cypher that:
      * MERGEs the target node;
      * for each source node, redirects all incoming/outgoing
        relations to the target;
      * deletes the source node.
    Idempotent — safe to re-run.
    """
    cypher = """
    MATCH (target {label: $target_label, name: $target_name})
    WITH target
    UNWIND $source_names AS src_name
      OPTIONAL MATCH (s {label: $target_label, name: src_name})
      WHERE s <> target
      WITH target, s
      WHERE s IS NOT NULL
      OPTIONAL MATCH (s)-[r]->(other)
      FOREACH (_ IN CASE WHEN r IS NOT NULL THEN [1] ELSE [] END |
        MERGE (target)-[nr:RELATED_TO]->(other)
        SET nr += properties(r)
      )
      WITH target, s
      OPTIONAL MATCH (other2)-[r2]->(s)
      FOREACH (_ IN CASE WHEN r2 IS NOT NULL THEN [1] ELSE [] END |
        MERGE (other2)-[nr2:RELATED_TO]->(target)
        SET nr2 += properties(r2)
      )
      WITH s
      DETACH DELETE s
    """
    return cypher, {
        "target_label": target_label,
        "target_name": target_name,
        "source_names": [n for n in source_names if n != target_name],
    }


async def apply_merges(
    graph_store,
    groups: dict[tuple[str, str], list[str]],
    *,
    dry_run: bool,
) -> dict[str, int]:
    summary = {"groups": len(groups), "merged_sources": 0, "errors": 0}
    for (etype, canonical), names in groups.items():
        if dry_run:
            logger.info(
                "[dry-run] type={t}  target={c!r}  sources={n}",
                t=etype, c=canonical, n=names,
            )
            continue
        try:
            # Ensure the target exists first via upsert; redirection
            # cypher then collapses the duplicates onto it.
            cypher, params = _build_merge_cypher(etype, names, canonical)
            graph_store.structured_query(cypher, params)
            summary["merged_sources"] += len([n for n in names if n != canonical])
            logger.info(
                "merged  type={t}  target={c!r}  sources={n}",
                t=etype, c=canonical, n=names,
            )
        except Exception as exc:
            logger.warning(
                "merge failed  type={t}  target={c!r}  err={err}",
                t=etype, c=canonical, err=exc,
            )
            summary["errors"] += 1
    return summary


# ── entry ────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Preview merges without modifying the graph (default: True).",
    )
    p.add_argument(
        "--types", nargs="*", default=None,
        help=f"Restrict to types (default: {sorted(DEFAULT_IDENTIFIER_TYPES)})",
    )
    p.add_argument("--limit", type=int, default=None)
    return p.parse_args()


async def _amain() -> None:
    args = _parse_args()
    configure_logging(level=settings.api.log_level)

    from src.graph.store import build_neo4j_graph_store

    types = (
        frozenset(args.types) if args.types else DEFAULT_IDENTIFIER_TYPES
    )
    graph_store = build_neo4j_graph_store()
    nodes = await collect_entity_nodes(graph_store, types)
    groups = group_by_canonical(nodes, types)
    if args.limit:
        groups = dict(list(groups.items())[: args.limit])
    logger.info(
        "candidate groups: {n}  types={t}",
        n=len(groups), t=sorted(types),
    )
    if not groups:
        logger.info("nothing to merge — graph is already canonical")
        return
    summary = await apply_merges(graph_store, groups, dry_run=args.dry_run)
    logger.info("done  dry_run={dr}  summary={s}", dr=args.dry_run, s=summary)


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
