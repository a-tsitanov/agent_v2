# Wikibase populator runbook

## 1. Overview

The Wikibase populator pushes canonical entities and their relations from the ingest pipeline into a self-hosted Wikibase instance. Owner-class entities (Person, Organization, etc.) become Wikibase **Items**; identifier-typed entities (phone, email, INN, ...) become **external-id statements** attached to their owner; relation labels between two owners are written as object-property statements between the two Items. Properties that don't yet exist in the instance are lazy-created on first use. Wikibase is maintained separately from Neo4j because consumers outside the RAG team want a queryable, schema-rich source of truth (Wikibase REST API + WDQS SPARQL) without being granted Neo4j access. QIDs are stored back on `:__Entity__` nodes (`wikibase_qid` property) so the next ingest knows whether to **create** or **update**.

Source plan: `docs/superpowers/plans/2026-05-18-wikibase-population.md`.

## 2. Bring-up

Three docker services were added at plan task T1: `wikibase-mysql` (MariaDB-based metadata store), `wikibase` (MediaWiki + Wikibase extension), and `wdqs` (Blazegraph SPARQL endpoint, optional for runtime). The Wikibase container needs ~30-90 seconds on first run to install MediaWiki tables, so start the DB first and give it a head start:

```bash
docker compose -p kb-llamaindex up -d wikibase-mysql
sleep 15
docker compose -p kb-llamaindex up -d wikibase
sleep 90

docker compose -p kb-llamaindex ps wikibase-mysql wikibase
# Both should be Up (healthy).

curl -fsS http://localhost:8181/wiki/Special:Version -o /dev/null && echo "wikibase ok"
```

If `wikibase` flaps health (occasional MediaWiki LocalSettings race when the DB is slow to settle), the simple recovery is:

```bash
docker compose -p kb-llamaindex restart wikibase
```

The healthcheck for `wikibase-mysql` uses `mariadb-admin ping` rather than `mysqladmin` — the MariaDB image in the bundle dropped the legacy alias. Don't replace it.

## 3. Bootstrap base classes + properties

One-time per Wikibase instance. The script is **idempotent** — safe to re-run; existing Items/Properties are reused via `wbsearchentities`.

```bash
# Dry-run first if you want to see what would be created:
uv run python -m scripts.setup_wikibase --dry-run

# Actual run (10 base-class Items + 27 Properties):
uv run python -m scripts.setup_wikibase

# Expected output:
# wikibase bootstrap done  base=10  common=3  identifier=24
```

If the source taxonomy changes and you want to force a re-read of remote labels, add `--refresh-cache`:

```bash
uv run python -m scripts.setup_wikibase --refresh-cache
```

Verify cache nodes were written to Neo4j (the populator queries these on every push):

```bash
uv run python -c "
from src.graph.store import build_neo4j_graph_store
gs = build_neo4j_graph_store()
rows = gs.structured_query('MATCH (b:WikibaseBaseClass) RETURN count(b) AS n')
print('base classes cached:', rows[0]['n'])  # expect 10
rows = gs.structured_query('MATCH (p:WikibaseProperty) RETURN count(p) AS n')
print('properties cached:', rows[0]['n'])    # expect 27 (3 common + 24 identifier)
"
```

The bootstrap logs in as `WikibaseAdmin` using the password from `WIKIBASE_ADMIN_PASS` (default `ChangeMe-Wb-Admin-2026`). Match what is actually set in the compose env — if the project `.env` does not define `WIKIBASE_ADMIN_PASS`, both the container and the script will fall back to the default; if you change one, change both. MediaWiki refuses to start with a weaker admin password than its policy demands, hence the long default.

The bootstrap script also provisions the **runtime bot account** before doing API work. It auto-discovers the Wikibase container (compose label `com.docker.compose.service=wikibase`; override via `WIKIBASE_DOCKER_CONTAINER` env) and runs:

```bash
docker exec <wikibase-container> php /var/www/html/maintenance/run.php \
    createAndPromote --bot --force "$WIKIBASE_BOT_USER" "$WIKIBASE_BOT_PASSWORD"
```

`--force` makes this idempotent. The bot account is what `push_wikibase` logs in as at ingest time — the bootstrap admin creds are only used for the bootstrap itself. The "Main-account login deprecated" warning in MediaWiki logs comes from the admin-side login; cosmetic.

If you need to mint the bot user by hand (e.g. CI without docker daemon access), the equivalent one-liner is the command above; the password must be ≥8 characters or MediaWiki rejects it.

## 4. Enable the push activity

Default is **OFF**. The activity always runs in the workflow, but with `WIKIBASE_ENABLED=false` it returns `status="skipped"` instantly. To turn on:

```bash
# In .env (or shell):
export WIKIBASE_ENABLED=true
# Restart the worker so the new env propagates:
pkill -f "src.workflow.worker"
uv run python -m src.workflow.worker &
```

Verify in Temporal UI: a new workflow run shows `push_wikibase` activity between `build_property_graph` and `finalize`. Activity heartbeat includes `enabled=True` and `counts` (created_items, updated_items, statements_added) after the push completes.

## 5. Smoke verify end-to-end

After bootstrap + enable, run a small ingest and confirm the round-trip.

```bash
# Ingest a small document.
curl -X POST http://localhost:8000/api/v1/ingest \
     -F "file=@tests/test_ingestion/fixtures/sample.txt" \
     -H "X-API-Key: $API_KEY"

# Wait for the workflow to finish in Temporal UI.
# Look at the workflow's final result — `wikibase_status` should be "ok".
```

For a tighter feedback loop without driving the API, the dedicated smoke script pushes a synthetic Person + PhoneNumber pair directly through the activity:

```bash
uv run python -m scripts.smoke_wikibase_push
# Last successful run created Q11 in the live instance.
```

Verify Neo4j has QIDs on every canonical entity from this run:

```bash
uv run python -c "
from src.graph.store import build_neo4j_graph_store
gs = build_neo4j_graph_store()
rows = gs.structured_query('''
    MATCH (e:__Entity__) WHERE e.wikibase_qid IS NOT NULL
    RETURN count(e) AS n
''')
print('entities with qid:', rows[0]['n'])
"
```

Spot-check one entity in the Wikibase UI (replace the QID with a real one from the previous query):

```
http://localhost:8181/wiki/Item:Q11
```

You should see:

- Label = entity name.
- `instance_of` = a Q-id matching the base class (Person/Organization/...).
- `er_canonical_name` = the canonical name string.
- `mention_count` = number.
- External-id statements for any phone/email/INN/etc. tied to this entity.

For owner-owner relations: re-ingest a document with clear relationships (e.g. "Иван работает в Ромашке") and confirm the Person Item gains a `WORKS_AT` (or whichever relation label the extractor produced) statement pointing at the Organization Item's Q-id.

## 6. Querying via SPARQL (wdqs)

WDQS (Blazegraph) is exposed at `http://localhost:8989`. Its query UI lives at `http://localhost:8989/`. Useful when you want graph-style traversal across the whole instance rather than per-Item lookups.

**Caveat — sync lag.** WDQS pulls from the Wikibase change stream in batches; freshly-pushed Items typically appear in SPARQL within seconds, but under heavy ingest load that gap can stretch to a minute or two. The MediaWiki REST API (`wbgetentities`) is always authoritative; treat WDQS as eventually-consistent.

**Common queries** (PIDs are stable for a given instance — confirm yours via `wbgetentities` if these are off):

```sparql
# 1. All Items of class Person (instance_of → Q1)
SELECT ?item ?itemLabel WHERE {
  ?item wdt:P2 wd:Q1 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ru,en". }
}
LIMIT 100
```

```sparql
# 2. Find everything that points at a given entity (incoming relations)
SELECT ?subject ?subjectLabel ?prop ?propLabel WHERE {
  ?subject ?prop wd:Q14 .            # Q14 = the entity you care about
  FILTER(STRSTARTS(STR(?prop), STR(wdt:)))
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ru,en". }
}
```

```sparql
# 3. Top-mentioned entities (sort by mention_count = P3)
SELECT ?item ?itemLabel ?mentions WHERE {
  ?item wdt:P3 ?mentions ;
        wdt:P2 ?class .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ru,en". }
}
ORDER BY DESC(?mentions)
LIMIT 20
```

```sparql
# 4. Entities with a specific external-id type (e.g. INN = P6)
SELECT ?item ?itemLabel ?inn WHERE {
  ?item wdt:P6 ?inn .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ru,en". }
}
```

If a query returns empty unexpectedly, hit the same data via the REST API to confirm the data is there but wdqs hasn't picked it up yet — `curl "http://localhost:8181/w/api.php?action=wbgetentities&ids=Q14&format=json&props=claims"`.

## 7. Re-ingest idempotency

Re-ingest the **same content** with a fresh `job_id`. Wikibase already knows the entities → push goes to the **update** path (statements added/refreshed, no new Items):

```bash
# Re-ingest:
curl -X POST http://localhost:8000/api/v1/ingest \
     -F "file=@tests/test_ingestion/fixtures/sample.txt" \
     -H "X-API-Key: $API_KEY"

# In Temporal UI, push_wikibase heartbeat for this run should show
# `updated_items > 0` and `created_items == 0`.
```

If `created_items > 0` on a re-ingest, the QID writeback in Neo4j didn't persist, or the lookup is broken. Quick check:

```cypher
MATCH (e:__Entity__) RETURN e.name, e.wikibase_qid LIMIT 10
```

Every canonical entity present in the previous run should have a non-null `wikibase_qid`. If they don't, see the troubleshooting row on QID writeback below.

## 8. Disable temporarily

```bash
export WIKIBASE_ENABLED=false
pkill -f "src.workflow.worker"
uv run python -m src.workflow.worker &
```

`push_wikibase` activity returns `status="skipped"` instantly without touching Wikibase or Neo4j caches. Ingest behaves exactly as it did before this feature.

## 9. Full reset

**WARNING — destructive.** The steps below delete every Item and Property in the local Wikibase instance, drop the MySQL volume, and remove the QID writeback from every Neo4j entity. Only do this on a dev/test instance.

```bash
# Drop the Wikibase data volumes.  WARNING — deletes ALL Items / Properties.
docker compose -p kb-llamaindex down wikibase wikibase-mysql wdqs
docker volume rm \
  kb-llamaindex_wikibase_mysql_data \
  kb-llamaindex_wikibase_images \
  kb-llamaindex_wikibase_wdqs_data 2>/dev/null || true

# Clear the Neo4j cache nodes and entity QIDs:
uv run python -c "
from src.graph.store import build_neo4j_graph_store
gs = build_neo4j_graph_store()
gs.structured_query('MATCH (n:WikibaseBaseClass) DELETE n')
gs.structured_query('MATCH (n:WikibaseProperty) DELETE n')
gs.structured_query('MATCH (e:__Entity__) REMOVE e.wikibase_qid')
"

# Bring back up + bootstrap:
docker compose -p kb-llamaindex up -d wikibase-mysql wikibase
sleep 90
uv run python -m scripts.setup_wikibase
```

After this, the next ingest gets fresh QIDs (Q1, Q2, ...) — they won't match the old ones. Any external system that pinned to specific Q-ids needs to be reconciled.

## 10. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `push_wikibase` heartbeat shows `enabled=False` despite env var | Worker started before env was set | `pkill -f src.workflow.worker; uv run python -m src.workflow.worker` after `export WIKIBASE_ENABLED=true` |
| `push_wikibase` status="failed" in result | Wikibase container down / Neo4j cache empty / bootstrap not run | `docker compose -p kb-llamaindex ps wikibase` + verify cache counts (section 3) |
| `Item not found` errors in Temporal logs | Bootstrap missed a step OR Neo4j cache was wiped | Re-run `uv run python -m scripts.setup_wikibase` |
| `created_items > 0` on every re-ingest | QID writeback to Neo4j failing | Check `:__Entity__` nodes actually exist for these names (Neo4j-side bug); enable debug logs in `_persist_qid_for_entity` |
| MediaWiki "Main-account login deprecated" warning in logs | Bootstrap (admin path) + runtime bot login are both username/password style | Cosmetic; full migration to `Special:BotPasswords` token-auth is a future improvement (see section 11) |
| `push_wikibase` keeps failing with `Login failed: Incorrect username or password` | KbBot user never provisioned, or its password drifted from `WIKIBASE_BOT_PASSWORD` | Re-run `uv run python -m scripts.setup_wikibase` — it idempotently reprovisions the bot via `createAndPromote --bot --force` |
| `wbsearchentities` returning weird results | Label exists in wrong language slot | Confirm `WIKIBASE_LANGUAGE` matches what bootstrap used (default `ru`) |
| Disk full on Docker Desktop | wdqs + Wikibase + MySQL + Milvus add up fast | `docker system df` + `docker builder prune -af` + `docker image prune -f` |
| Wikibase UI shows raw labels instead of localized | Default skin / language config missing | Bootstrap sets `MW_WG_DEFAULT_SKIN=vector`; confirm via `Special:Version` |
| `wikibaseintegrator` `KeyError: 'label'` in our scripts | Known 0.12.15 bug with local Wikibase API responses | Use direct `mediawiki_api_call_helper` (already done in `scripts/setup_wikibase.py`) |
| `wikibase-mysql` healthcheck failing | Old healthcheck used `mysqladmin`, MariaDB image only ships `mariadb-admin` | Keep the `mariadb-admin ping` form in compose; do not "fix" it back |
| MediaWiki rejects admin password on container start | Password below MediaWiki policy length / complexity | Use the long default `ChangeMe-Wb-Admin-2026` or set a comparable strong `WIKIBASE_ADMIN_PASS` |

## 11. Future improvements

Follow-ups deferred per the source plan:

- wdqs reindex automation (currently manual — reindex by hand if SPARQL goes stale).
- Switch from username/password bot-login to token-based `Special:BotPasswords` flow for tighter scoping + audit. Bot **account** is already provisioned by bootstrap; only the auth mechanism is pending.
- Wikidata cross-linking: emit `external-id` statements that point at public Q-ids on wikidata.org for shared entities.
- ER `_cleanup_stored_losers` integration — when ER merges two canonical entities, issue `wbcreateredirect` from loser_qid to canon_qid so downstream SPARQL stays valid.
- Multi-tenant separate Wikibase instances per department (today everyone shares one).
