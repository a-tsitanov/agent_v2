"""mark_entities_dirty — flag an ingest's entities (and relation endpoints)
for wiki re-write. Best-effort: never raises out (caller ignores failures)."""
from __future__ import annotations

from temporalio import activity

from src.config import settings
from src.graph.store import build_neo4j_graph_store
from src.graph.wiki_dirty import mark_dirty
from src.workflow.contracts import MarkDirtyIn


def _dirty_names(payload: MarkDirtyIn) -> set[str]:
    return set(payload.entity_names) | set(payload.relation_endpoints)


@activity.defn
async def mark_entities_dirty(payload: MarkDirtyIn) -> int:
    if not settings.wiki.enabled:
        return 0
    names = sorted(_dirty_names(payload))
    if not names:
        return 0
    store = build_neo4j_graph_store()
    mark_dirty(store, names)
    activity.logger.info("mark_entities_dirty  count=%d", len(names))
    return len(names)
