"""Convert `ParsedEvent` objects → graph nodes + edges.

Called from `LightRAGExtractor._aextract` when event extraction is
enabled (gated by `settings.events.extraction_enabled`).

API
---
``events_to_graph(events, *, id_by_name, doc_date_epoch_days=None) -> (nodes, relations)``

  * One ``EntityNode(label="EventOrAction")`` per event.
  * ``PARTICIPATED_IN`` relations from event→participant for each participant.
  * Unknown participant names get a synthesised orphan
    ``EntityNode(label="Other")`` mirroring ``ensure_orphan_entities`` in
    ``lightrag_parse.py``.
  * ``id_by_name`` is mutated in-place so that newly synthesised orphan ids
    are visible to subsequent processing in the same chunk.

DATED edge: omitted — the resolved timeframe (``event_ts_raw`` / ``event_start_epoch``
/ ``event_end_epoch`` / ``event_ts_precision``) is stored as properties on the
event node itself and is available for query without an extra relationship hop.
"""

from __future__ import annotations

from llama_index.core.graph_stores.types import EntityNode, Relation

from src.graph.event_ts_resolver import resolve
from src.graph.lightrag_parse import ParsedEvent, _normalize_entity_name


def events_to_graph(
    events: list[ParsedEvent],
    *,
    id_by_name: dict[str, str],
    doc_date_epoch_days: int | None = None,
) -> tuple[list[EntityNode], list[Relation]]:
    """Convert a list of ``ParsedEvent`` objects to graph nodes + relations.

    Parameters
    ----------
    events:
        Parsed event tuples produced by ``parse_lightrag_output``.
    id_by_name:
        Mutable mapping ``{normalised_name: entity_id}`` built from the
        entities extracted in the same chunk.  Unknown participant names are
        added here (with orphan ids) so downstream code sees them.
    doc_date_epoch_days:
        Anchor date (epoch days) used to resolve relative/partial time
        phrases (e.g. "вчера") into an absolute interval.  ``None`` disables
        anchor-relative resolution — absolute phrases still resolve.

    Returns
    -------
    tuple[list[EntityNode], list[Relation]]
        - nodes: event nodes first, then any newly synthesised orphan nodes.
        - relations: one ``PARTICIPATED_IN`` edge per (event, participant) pair.
    """
    from src.config import settings as _settings

    taxonomy = {t.strip().lower() for t in _settings.events.taxonomy} | {"other"}

    event_nodes: list[EntityNode] = []
    relations: list[Relation] = []
    # Orphans keyed by normalised participant name to avoid duplicates across
    # multiple events in the same chunk.
    orphan_by_norm: dict[str, EntityNode] = {}

    for ev in events:
        # The trigger phrase IS the event's name; the category lives in the
        # `event_type` property, so don't smear it into the display name
        # ("other: провела операцию" → "провела операцию"). Fall back to the
        # type only when there's no trigger, so the node is never unnamed.
        trigger = (ev.trigger or "").strip()
        event_name = (trigger or (ev.event_type or "event").strip())[:120]
        etype = (ev.event_type or "event").strip().lower()

        props: dict = {
            "event_type": etype if etype in taxonomy else "other",
            "trigger": ev.trigger,
            "polarity": ev.polarity,
            "participants": list(ev.participants),
            "source_chunks": [ev.source_chunk_id] if ev.source_chunk_id else [],
            "file_paths": [ev.file_path] if ev.file_path else [],
        }
        if etype not in taxonomy:
            props["event_type_raw"] = ev.event_type
        if ev.event_ts:
            props["event_ts_raw"] = ev.event_ts
            resolved = resolve(ev.event_ts, doc_date_epoch_days)
            if resolved:
                props["event_start_epoch"], props["event_end_epoch"], props["event_ts_precision"] = resolved

        event_node = EntityNode(name=event_name, label="EventOrAction", properties=props)
        event_nodes.append(event_node)

        for participant in ev.participants:
            norm = _normalize_entity_name(participant)
            if not norm:
                continue

            participant_id = id_by_name.get(norm)

            if participant_id is None:
                # Synthesise an orphan entity so the edge can be stored.
                if norm not in orphan_by_norm:
                    orphan = EntityNode(
                        name=norm,
                        label="Other",
                        properties={
                            "description": "",
                            "source_chunk_id": ev.source_chunk_id,
                            "orphan": True,
                        },
                    )
                    orphan_by_norm[norm] = orphan
                    id_by_name[norm] = orphan.id
                participant_id = id_by_name[norm]

            relations.append(
                Relation(
                    label="PARTICIPATED_IN",
                    source_id=event_node.id,
                    target_id=participant_id,
                    properties={},
                )
            )

    nodes: list[EntityNode] = event_nodes + list(orphan_by_norm.values())
    return nodes, relations


__all__ = ["events_to_graph"]
