"""push_wikibase activity.

Reads the merged-entities staging blob, loads the bootstrap caches
(``:WikibaseBaseClass`` and ``:WikibaseProperty`` Neo4j nodes), then
calls ``src/storage/wikibase.py:push_entities`` to project the
batch into the local Wikibase instance.

Best-effort:
  * ``WIKIBASE_ENABLED=false`` -> status="skipped".
  * Any exception below -> status="failed"; workflow does NOT fail --
    the next activity (`finalize`) still runs and ingest completes.
"""

from __future__ import annotations

from temporalio import activity

from src.config import settings
from src.graph.store import build_neo4j_graph_store
from src.storage.wikibase import AsyncWikibase, push_entities
from src.workflow.contracts import Merged, WikibasePushed
from src.workflow.staging import build_staging_store


@activity.defn
async def push_wikibase(merged: Merged) -> WikibasePushed:
    activity.logger.info(
        "push_wikibase start  doc=%s  enabled=%s",
        merged.kg.parsed.ctx.doc_id, settings.wikibase.enabled,
    )
    activity.heartbeat({"stage": "init", "enabled": settings.wikibase.enabled})

    if not settings.wikibase.enabled:
        return WikibasePushed(status="skipped")

    try:
        staging = build_staging_store()
        entities, relations, _nodes = staging.read_pickle(
            merged.merged_entities_uri,
        )
        graph_store = build_neo4j_graph_store()

        base_class_qids = _load_base_classes(graph_store)
        property_pids = _load_properties(graph_store)
        activity.heartbeat({
            "stage": "caches_loaded",
            "base_classes": len(base_class_qids),
            "properties": len(property_pids),
        })

        # AsyncWikibase.from_settings is `async def` (it logs into the
        # Wikibase REST API to obtain a session), so we MUST await it.
        # Forgetting the `await` produced a coroutine object that then
        # surfaced inside push_entities as `'coroutine' object has no
        # attribute 'create_item'` for every owner — push_entities's
        # per-owner try/except swallowed each, leaving counters at 0
        # and returning status="ok" misleadingly.  See test
        # `test_push_wikibase_zero_counters_marked_failed` below.
        wb_client = await AsyncWikibase.from_settings(settings.wikibase)
        activity.heartbeat({"stage": "pushing", "entities": len(entities)})

        counts = await push_entities(
            entities=entities, relations=relations,
            neo4j_store=graph_store, wb_client=wb_client,
            base_class_qids=base_class_qids, property_pids=property_pids,
        )
        activity.heartbeat({"stage": "pushed", **counts})

        # Detect the silent-no-op case: there were owner entities to
        # push but nothing landed.  Mark failed so the operator can
        # see it in the workflow result instead of a misleading ok.
        had_work = bool(entities)
        nothing_done = (
            counts["created_items"] == 0
            and counts["updated_items"] == 0
        )
        if had_work and nothing_done:
            activity.logger.warning(
                "push_wikibase: %d entities in, 0 items created/updated -- "
                "treating as failed", len(entities),
            )
            return WikibasePushed(status="failed", **counts)
        return WikibasePushed(status="ok", **counts)
    except Exception as exc:  # noqa: BLE001
        activity.logger.warning(
            "push_wikibase failed (best-effort): %s", exc,
        )
        return WikibasePushed(status="failed")


def _load_base_classes(graph_store) -> dict[str, str]:
    rows = graph_store.structured_query(
        "MATCH (b:WikibaseBaseClass) RETURN b.label AS label, b.qid AS qid"
    )
    return {row["label"]: row["qid"] for row in rows}


def _load_properties(graph_store) -> dict[str, str]:
    rows = graph_store.structured_query(
        "MATCH (p:WikibaseProperty) RETURN p.label AS label, p.pid AS pid"
    )
    return {row["label"]: row["pid"] for row in rows}
