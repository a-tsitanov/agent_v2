"""E2 event de-duplication: deterministic (type, participants, ts-bucket) match-key."""

from __future__ import annotations

from collections import Counter, defaultdict

from llama_index.core.graph_stores.types import EntityNode, Relation

from src.graph.lightrag_parse import _normalize_entity_name

_NO_TS = "∅"


def _ts_bucket(event_ts: str | None, bucket_days: int) -> str:
    if not event_ts:
        return _NO_TS
    from datetime import date

    try:
        d = date.fromisoformat(event_ts[:10])
    except ValueError:
        return event_ts[:7]  # fall back to month string
    if bucket_days == 7:
        # Use ISO year-week so calendar weeks are never split at bucket boundaries.
        year, week, _ = d.isocalendar()
        return f"{year}-W{week:02d}"
    return str(d.toordinal() // max(bucket_days, 1))


def event_key(
    event_type: str,
    participants: list[str],
    event_ts: str | None,
    *,
    bucket_days: int = 7,
) -> tuple:
    """Deterministic, order-insensitive match key for an event.

    Returns:
        ``(event_type_lower, frozenset(normalised_participant_names), ts_bucket)``
    """
    parts = frozenset(_normalize_entity_name(p) for p in participants if p)
    return ((event_type or "event").strip().lower(), parts, _ts_bucket(event_ts, bucket_days))


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
    * earliest ``event_ts`` across members.

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
            p.get("event_ts"),
            bucket_days=bucket_days,
        )
        groups[k].append(n)

    canonical_id: dict[str, str] = {}  # old node name -> canonical name
    merged: list[EntityNode] = []

    for _k, members in groups.items():
        first = members[0]
        chunks: list[str] = []
        type_votes: Counter = Counter()
        ts_vals: list[str] = []

        for m in members:
            mp = m.properties or {}
            chunks += list(mp.get("source_chunks", []) or [])
            type_votes[mp.get("event_type", "event")] += 1
            if mp.get("event_ts"):
                ts_vals.append(mp["event_ts"])
            canonical_id[m.name] = first.name

        props = dict(first.properties or {})
        props["event_type"] = type_votes.most_common(1)[0][0]
        props["source_chunks"] = list(dict.fromkeys(chunks))
        props["event_ts"] = min(ts_vals) if ts_vals else props.get("event_ts")
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


__all__ = ["event_key", "merge_events"]
