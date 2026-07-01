"""Convert `ParsedEvent` objects → graph nodes + edges.

Called from `LightRAGExtractor._aextract` when event extraction is
enabled (gated by `settings.events.extraction_enabled`).

API
---
``events_to_graph(events, *, id_by_name) -> (nodes, relations)``

  * One ``EntityNode(label="EventOrAction")`` per event.
  * ``PARTICIPATED_IN`` relations from event→participant for each participant.
  * Unknown participant names get a synthesised orphan
    ``EntityNode(label="Other")`` mirroring ``ensure_orphan_entities`` in
    ``lightrag_parse.py``.
  * ``id_by_name`` is mutated in-place so that newly synthesised orphan ids
    are visible to subsequent processing in the same chunk.

DATED edge: omitted — the ``event_ts`` field is stored as a property on the
event node itself and is available for query without an extra relationship hop.
"""

from __future__ import annotations

from llama_index.core.graph_stores.types import EntityNode, Relation

from src.graph.lightrag_parse import ParsedEvent, _normalize_entity_name


def events_to_graph(
    events: list[ParsedEvent],
    *,
    id_by_name: dict[str, str],
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

    Returns
    -------
    tuple[list[EntityNode], list[Relation]]
        - nodes: event nodes first, then any newly synthesised orphan nodes.
        - relations: one ``PARTICIPATED_IN`` edge per (event, participant) pair.
    """
    event_nodes: list[EntityNode] = []
    relations: list[Relation] = []
    # Orphans keyed by normalised participant name to avoid duplicates across
    # multiple events in the same chunk.
    orphan_by_norm: dict[str, EntityNode] = {}

    for ev in events:
        event_name = f"{ev.event_type}: {ev.trigger}"[:120]

        event_node = EntityNode(
            name=event_name,
            label="EventOrAction",
            properties={
                "event_type": ev.event_type,
                "trigger": ev.trigger,
                "event_ts": ev.event_ts,
                "polarity": ev.polarity,
                "participants": list(ev.participants),
                "source_chunks": [ev.source_chunk_id] if ev.source_chunk_id else [],
                "file_paths": [ev.file_path] if ev.file_path else [],
            },
        )
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
