"""E2 event de-duplication: deterministic (type, participants, ts-bucket) match-key."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import UTC, datetime

from llama_index.core.graph_stores.types import EntityNode, Relation

from src.graph.lightrag_parse import _normalize_entity_name

_NO_TS = "∅"
_TS_PROPS = ("event_ts_raw", "event_start_epoch", "event_end_epoch", "event_ts_precision")


def _ts_bucket(event_start_epoch: int | None, bucket_days: int) -> str:
    if event_start_epoch is None:
        return _NO_TS
    d = datetime.fromtimestamp(int(event_start_epoch), tz=UTC).date()
    if bucket_days == 7:
        # Use ISO year-week so calendar weeks are never split at bucket boundaries.
        year, week, _ = d.isocalendar()
        return f"{year}-W{week:02d}"
    return str(d.toordinal() // max(bucket_days, 1))


def event_key(
    event_type: str,
    participants: list[str],
    event_start_epoch: int | None,
    *,
    bucket_days: int = 7,
) -> tuple:
    """Deterministic, order-insensitive match key for an event.

    Returns:
        ``(event_type_lower, frozenset(normalised_participant_names), ts_bucket)``
    """
    parts = frozenset(_normalize_entity_name(p) for p in participants if p)
    return ((event_type or "event").strip().lower(), parts, _ts_bucket(event_start_epoch, bucket_days))


def merge_events(
    event_nodes: list[EntityNode],
    event_rels: list[Relation],
    *,
    bucket_days: int = 7,
) -> tuple[list[EntityNode], list[Relation]]:
    """Collapse event nodes sharing an ``event_key`` into one canonical node.

    The canonical node retains:
    * majority ``event_type`` (Counter.most_common);
    * union of ``source_chunks`` (dedup-ordered, earliest-first);
    * interval of the earliest-starting member.

    Argument edges (``source_id`` / ``target_id``) are rewritten to point
    at the canonical node's ``name`` (the first member in insertion order).

    This is the anti-re-report invariant: the same real-world event
    re-reported in a later document merges to ONE node, so its
    ``first_seen`` / ``created_at`` stays old and it is not re-flagged.
    """
    groups: dict[tuple, list[EntityNode]] = defaultdict(list)
    for n in event_nodes:
        p = n.properties or {}
        k = event_key(
            p.get("event_type", ""),
            p.get("participants", []) or [],
            p.get("event_start_epoch"),
            bucket_days=bucket_days,
        )
        groups[k].append(n)

    canonical_id: dict[str, str] = {}  # old node name -> canonical name
    merged: list[EntityNode] = []

    for _k, members in groups.items():
        first = members[0]
        chunks: list[str] = []
        type_votes: Counter = Counter()

        for m in members:
            mp = m.properties or {}
            chunks += list(mp.get("source_chunks", []) or [])
            type_votes[mp.get("event_type", "event")] += 1
            canonical_id[m.name] = first.name

        earliest = min(
            (m for m in members if (m.properties or {}).get("event_start_epoch") is not None),
            key=lambda m: m.properties["event_start_epoch"],
            default=None,
        )
        props = dict(first.properties or {})
        props["event_type"] = type_votes.most_common(1)[0][0]
        props["source_chunks"] = list(dict.fromkeys(chunks))
        if earliest is not None:
            for k in _TS_PROPS:
                if k in (earliest.properties or {}):
                    props[k] = earliest.properties[k]
        merged.append(EntityNode(name=first.name, label="EventOrAction", properties=props))

    # rewrite argument edges to the canonical event node
    out_rels: list[Relation] = []
    for r in event_rels:
        src = canonical_id.get(r.source_id, r.source_id)
        tgt = canonical_id.get(r.target_id, r.target_id)
        out_rels.append(
            Relation(
                label=r.label,
                source_id=src,
                target_id=tgt,
                properties=dict(r.properties or {}),
            )
        )
    return merged, out_rels


# ── cross-channel event dedup (п.4) ─────────────────────────────────
#
# Every event-heavy chunk is double-extracted: the ENTITY channel emits a
# nominal EventOrAction ("Уничтожение диверсантов") and the EVENT channel a
# verbal one ("уничтожены три диверсанта"). Different phrasings → different
# name-VIDs → two nodes for one happening that never merge. We fold the
# entity-channel node into the nearest EVENT-channel node *in the same chunk*
# by embedding cosine; below threshold it is kept (a real event the event
# channel missed must survive — recall over aggression). Cross-chunk pairs
# never merge (a different chunk is a different context).


def _cos(a: list[float] | None, b: list[float] | None) -> float:
    if not a or not b:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(y * y for y in b) ** 0.5
    return dot / (na * nb) if na and nb else 0.0


def dedup_cross_channel_events(
    entity_events: list[EntityNode],
    pipeline_events: list[EntityNode],
    embeddings: dict[str, list[float]],
    *,
    threshold: float = 0.88,
) -> tuple[list[EntityNode], dict[str, str]]:
    """Fold entity-channel EventOrAction nodes into same-chunk event-channel
    events by name cosine.

    Parameters
    ----------
    entity_events:
        Entity-channel EventOrAction nodes (label EventOrAction, no ``trigger``
        pipeline prop); each carries ``source_chunk_id``.
    pipeline_events:
        Event-channel nodes (from ``events_to_graph``, carry ``source_chunks``);
        these are the survivors (richer: event_type / participants / ts).
    embeddings:
        ``{node.name: vector}`` for every node in both lists.
    threshold:
        Minimum cosine for a merge.

    Returns
    -------
    (kept_entity_events, alias)
        ``kept_entity_events`` are the entity-channel nodes with no confident
        same-chunk event match (recall-preserving).  ``alias`` maps a folded
        entity node's ``name`` -> the surviving event node's ``name`` so the
        caller can rewrite that node's relation endpoints.
    """
    kept: list[EntityNode] = []
    alias: dict[str, str] = {}
    for ent in entity_events:
        ep = ent.properties or {}
        chunk = ep.get("source_chunk_id")
        ent_vec = embeddings.get(ent.name)
        best_name: str | None = None
        best_cos = threshold
        if chunk and ent_vec:
            for pe in pipeline_events:
                if chunk not in ((pe.properties or {}).get("source_chunks") or []):
                    continue
                c = _cos(ent_vec, embeddings.get(pe.name))
                if c >= best_cos:
                    best_cos, best_name = c, pe.name
        if best_name is not None:
            alias[ent.name] = best_name
        else:
            kept.append(ent)
    return kept, alias


__all__ = ["dedup_cross_channel_events", "event_key", "merge_events"]
