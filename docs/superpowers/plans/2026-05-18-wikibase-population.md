# Self-hosted Wikibase population from kb-llamaindex

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Push canonical entities + typed relations + identifier statements from each ingest into a **self-hosted Wikibase** stack (not the public Wikidata). Wikibase becomes a second source of truth alongside Neo4j — queryable via SPARQL (wdqs) and the Wikibase REST API, useful for cross-team reuse without exposing Neo4j directly.

**Architecture:**
- New Temporal activity `push_wikibase` runs **after** the graph half of `DocumentIngestWorkflow` succeeds, inside the same inner `try/except` so a Wikibase outage downgrades cleanly to `graph_no_wikibase` status (graph still in Neo4j; just the wiki population skipped).
- `src/storage/wikibase.py` wraps `wikibaseintegrator>=0.12` and exposes `push_entities(entities, relations, *, neo4j_store, wb_client, base_class_qids)`.
- QIDs are persisted back to Neo4j on each canonical `:__Entity__` node so re-ingest takes the update path (idempotency).
- Identifier-typed entities (`PhoneNumber`, `Email`, `INN`, `URL`, `Telegram`, …) do **NOT** become Items. They are folded into their owner Item as `datatype: external-id` statements via Properties bootstrapped by `scripts/setup_wikibase.py`.
- Three new docker-compose services: `wikibase` + `wikibase-mysql` + optional `wdqs` (SPARQL).

**Tech Stack:** Python 3.12, `wikibaseintegrator>=0.12,<1.0`, Wikibase Docker (community image), MySQL 8, Blazegraph (wdqs), Temporal worker, Neo4j (existing).

**Spec context:**
- Temporal workflow design: `docs/superpowers/specs/2026-05-15-ingest-temporal-workflow-design.md`.
- Current graph activities: `src/workflow/activities/{inject_canonical,extract_kg,merge_and_resolve,build_property_graph}.py`.
- 24 identifier types: `src/ingestion/identifiers.py:IdentifierType`.
- Original parked plan (overwritten on disk): summary preserved in memory `~/.claude/projects/-Users-a-tsitanov-projects-enterprise-kb/memory/project_kb_llamaindex_wikibase.md` (parked 2026-05-14).

**Session protocol:** Pause after each labelled **Stage** for sync.

**Defaults chosen (flag if you disagree before T1):**

- **Self-hosted Wikibase**, not public Wikidata. (Confirmed in parked-plan summary.)
- **Inline activity, not a child workflow.** Single push step; child WF would add ceremony for no benefit. If you later want to batch pushes across multiple docs, lift it to a child / cron WF — same code, different driver.
- **Default `WIKIBASE_ENABLED=false`** in `WikibaseSettings`. Operator opts in by flipping the env var after `scripts/setup_wikibase.py` has bootstrapped the instance.
- **Best-effort push** — activity wraps push in try/except + reports back a status flag in result. `DocumentIngestWorkflow` adds a new memo state `wikibase_status="ok" | "skipped" | "failed"` but does NOT fail the workflow.
- **All 24 identifier types fold into owner Items as `external-id` statements.** A Person Item gets statements like `(P: Phone, value: "+74951234567")`, `(P: Telegram, value: "@anna_pm")`, etc. — no separate Items for them.
- **No wdqs reindex in this plan.** Bootstrap script does NOT force a SPARQL reindex; operators can do it manually.

---

## Stage 1 — Infra: compose services + dependency

### Task 1: Wikibase + MySQL + wdqs in docker-compose

**Files:**
- Modify: `docker-compose.yml`.
- Modify: `.env.example`.

- [ ] **Step 1: Append three services**

After the existing `neo4j` block (so the order is `… neo4j → wikibase-mysql → wikibase → wdqs …`):

```yaml
  # ── Wikibase: structured-data store (Wikidata-software stack) ───
  # MySQL backs Wikibase's MediaWiki schema.
  wikibase-mysql:
    image: mariadb:11.4
    environment:
      MYSQL_DATABASE: my_wiki
      MYSQL_USER: wikiuser
      MYSQL_PASSWORD: ${WIKIBASE_DB_PASSWORD:-sqlpass}
      MYSQL_ROOT_PASSWORD: ${WIKIBASE_DB_ROOT_PASSWORD:-rootpass}
    volumes:
      - wikibase_mysql_data:/var/lib/mysql
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 10s
      timeout: 5s
      retries: 10
    restart: unless-stopped

  wikibase:
    image: wikibase/wikibase:1.41-bundle
    ports:
      - "${WIKIBASE_HOST_PORT:-8181}:80"
    environment:
      DB_SERVER: wikibase-mysql:3306
      MW_ADMIN_NAME: ${WIKIBASE_ADMIN_USER:-WikibaseAdmin}
      MW_ADMIN_PASS: ${WIKIBASE_ADMIN_PASS:-adminpass}
      MW_ADMIN_EMAIL: admin@example.invalid
      MW_WG_SECRET_KEY: ${WIKIBASE_SECRET_KEY:-deadbeefdeadbeefdeadbeefdeadbeef}
      MW_WG_DEFAULT_SKIN: vector
      DB_USER: wikiuser
      DB_PASS: ${WIKIBASE_DB_PASSWORD:-sqlpass}
      DB_NAME: my_wiki
      WIKIBASE_PINGBACK: "false"
    depends_on:
      wikibase-mysql:
        condition: service_healthy
    volumes:
      - wikibase_images:/var/www/html/images
    healthcheck:
      test: ["CMD", "curl", "-fsS", "http://localhost/wiki/Special:Version", "-o", "/dev/null"]
      interval: 15s
      timeout: 5s
      retries: 10
      start_period: 60s
    restart: unless-stopped

  # ── SPARQL endpoint (optional but recommended).  Not strictly
  # required for ingest — Wikibase REST API is enough for write.
  wdqs:
    image: wikibase/wdqs:0.3.140
    ports:
      - "${WIKIBASE_WDQS_PORT:-8989}:9999"
    environment:
      WIKIBASE_HOST: wikibase
    depends_on:
      wikibase:
        condition: service_healthy
    volumes:
      - wikibase_wdqs_data:/wdqs/data
    restart: unless-stopped
```

Append to the bottom `volumes:` block:

```yaml
volumes:
  # ... existing ...
  wikibase_mysql_data:
  wikibase_images:
  wikibase_wdqs_data:
```

- [ ] **Step 2: `.env.example`**

Append:

```env
# Wikibase
WIKIBASE_ENABLED=false
WIKIBASE_BASE_URL=http://localhost:8181
WIKIBASE_HOST_PORT=8181
WIKIBASE_WDQS_PORT=8989
WIKIBASE_BOT_USER=KbBot
WIKIBASE_BOT_PASSWORD=botpass
WIKIBASE_ADMIN_USER=WikibaseAdmin
WIKIBASE_ADMIN_PASS=adminpass
WIKIBASE_DB_PASSWORD=sqlpass
WIKIBASE_DB_ROOT_PASSWORD=rootpass
# Encryption key — generate 32 hex chars per instance.
WIKIBASE_SECRET_KEY=deadbeefdeadbeefdeadbeefdeadbeef
WIKIBASE_LANGUAGE=ru
WIKIBASE_TIMEOUT_S=30
```

- [ ] **Step 3: Bring up + smoke**

```bash
docker compose -p kb-llamaindex up -d wikibase-mysql wikibase
# Wait for healthcheck (Wikibase install can take 30-60s on first run).
sleep 90
curl -fsS http://localhost:8181/wiki/Special:Version | head -1
# → HTML "MediaWiki ... is up"
```

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml .env.example
git commit -m "feat(infra): wikibase + mysql + wdqs containers"
```

---

### Task 2: `wikibaseintegrator` dependency

**Files:**
- Modify: `pyproject.toml`.

- [ ] **Step 1: Add the dep**

In `[project] dependencies`, next to other storage clients:

```toml
    # Wikibase REST + bot API client.
    "wikibaseintegrator>=0.12,<1.0",
```

- [ ] **Step 2: Sync**

```bash
uv sync --extra dev
uv run python -c "from wikibaseintegrator import WikibaseIntegrator; print('ok')"
```

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "feat: add wikibaseintegrator for Wikibase REST client"
```

---

## Stage 2 — Settings + bootstrap script

### Task 3: `WikibaseSettings` in `src/config.py`

**Files:**
- Modify: `src/config.py`.
- Test: `tests/test_config.py` — defaults + env override.

- [ ] **Step 1: Write failing tests**

In `tests/test_config.py`:

```python
def test_wikibase_settings_defaults():
    from src.config import settings
    w = settings.wikibase
    assert w.enabled is False           # opt-in
    assert w.base_url == "http://localhost:8181"
    assert w.bot_user == "KbBot"
    assert w.language == "ru"
    assert w.timeout_s == 30.0


def test_wikibase_enabled_via_env(monkeypatch):
    from src.config import WikibaseSettings
    monkeypatch.setenv("WIKIBASE_ENABLED", "true")
    monkeypatch.setenv("WIKIBASE_BASE_URL", "http://wb.internal:8181")
    fresh = WikibaseSettings()
    assert fresh.enabled is True
    assert fresh.base_url == "http://wb.internal:8181"
```

Run: ImportError. Good.

- [ ] **Step 2: Implement**

In `src/config.py`, after `LLMCacheSettings` (or another nearby settings class):

```python
class WikibaseSettings(BaseSettings):
    """Self-hosted Wikibase populator settings.

    When ``enabled=True``, ``DocumentIngestWorkflow`` calls
    ``push_wikibase`` after a successful graph build and pushes
    canonical entities + typed relations + identifier statements
    into Wikibase.  Default disabled — operator opts in after
    running ``scripts/setup_wikibase.py`` to bootstrap the bot
    user and the base-class Items.
    """

    model_config = SettingsConfigDict(
        env_prefix="WIKIBASE_", env_file=".env", extra="ignore",
    )

    enabled: bool = False
    base_url: str = "http://localhost:8181"
    bot_user: str = "KbBot"
    bot_password: SecretStr = SecretStr("botpass")
    language: str = "ru"
    timeout_s: float = 30.0
```

Expose on `Settings`:

```python
    @cached_property
    def wikibase(self) -> WikibaseSettings:
        return WikibaseSettings()
```

Add `"WikibaseSettings"` to `__all__`.

- [ ] **Step 3: Run + commit**

```bash
uv run pytest tests/test_config.py -v
git add src/config.py tests/test_config.py
git commit -m "feat(config): WikibaseSettings (default disabled)"
```

---

### Task 4: `scripts/setup_wikibase.py` — bootstrap

Bootstraps:
1. Login as admin → create bot user `KbBot` with bot password.
2. Create base-class Items: `Person`, `Organization`, `Concept`, `Metric`, `Topic`, `Issue`, `Resolution`, `EventOrAction`, `Product`, `Document`. Persist QIDs.
3. Create common Properties:
   - `er_canonical_name` (string)
   - `instance_of` (item)
   - `mention_count` (quantity)
   - One `external-id` property per identifier type (24 of them).
4. Persist QIDs/PIDs into Neo4j as `:WikibaseBaseClass {label, qid}` and `:WikibaseProperty {label, pid, datatype}` nodes — these are the bootstrap caches that `src/storage/wikibase.py` reads on every ingest to avoid round-trip lookups.

**Files:**
- Create: `scripts/setup_wikibase.py` (~200 LOC).
- Modify: `pyproject.toml` — already has `wikibaseintegrator` from Task 2.

- [ ] **Step 1: Script skeleton**

```python
"""One-time Wikibase bootstrap.

Usage::

    uv run python -m scripts.setup_wikibase

Reads connection info from ``WikibaseSettings`` (env-driven).  Creates
the bot user, base-class Items and common Properties.  Idempotent —
re-runs check existence via wbgetentities before creating.

Persists every created QID/PID into Neo4j so the ingest hot path
doesn't repeat lookups.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger
from wikibaseintegrator import WikibaseIntegrator, wbi_config, wbi_login
from wikibaseintegrator.datatypes import ExternalID, Item, Quantity, String

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import settings
from src.graph.store import build_neo4j_graph_store
from src.ingestion.identifiers import IdentifierType


# Base classes mirror the EntityType taxonomy in the spec (R3).
_BASE_CLASSES: list[str] = [
    "Person", "Organization", "Concept", "Metric", "Topic",
    "Issue", "Resolution", "EventOrAction", "Product", "Document",
]

# Common Properties used by every push.
_COMMON_PROPERTIES: list[dict] = [
    {"label": "er_canonical_name", "datatype": "string"},
    {"label": "instance_of",       "datatype": "wikibase-item"},
    {"label": "mention_count",     "datatype": "quantity"},
]

# Identifier-typed Properties — one per ``IdentifierType``.
# Datatype is always ``external-id`` (the value is the canonical
# identifier string, e.g. "+74951234567").
def _identifier_properties() -> list[dict]:
    # Pull the literal at runtime to avoid drift if new types are added.
    from typing import get_args
    types = get_args(IdentifierType)
    return [{"label": t, "datatype": "external-id"} for t in types]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would be created without writing.")
    args = parser.parse_args()

    cfg = settings.wikibase
    wbi_config.config["MEDIAWIKI_API_URL"] = f"{cfg.base_url.rstrip('/')}/w/api.php"
    wbi_config.config["SPARQL_ENDPOINT_URL"] = f"http://localhost:{settings.wikibase.timeout_s or 8989}/proxy/wdqs/bigdata/namespace/wdq/sparql"  # adjust if wdqs port differs
    wbi_config.config["WIKIBASE_URL"] = cfg.base_url

    # Login as admin (for first run) to create the bot user.
    # Subsequent runs login as the bot directly.
    # ... (login flow via wbi_login.Login + wbi_login.Clientlogin) ...

    wbi = WikibaseIntegrator(login=login)

    # 1. Base classes.
    base_qids: dict[str, str] = {}
    for label in _BASE_CLASSES:
        qid = _ensure_item(wbi, label, "base class for KB taxonomy",
                           args.dry_run)
        base_qids[label] = qid

    # 2. Common properties.
    common_pids: dict[str, str] = {}
    for spec in _COMMON_PROPERTIES:
        pid = _ensure_property(wbi, spec["label"], spec["datatype"],
                               args.dry_run)
        common_pids[spec["label"]] = pid

    # 3. Identifier properties.
    ident_pids: dict[str, str] = {}
    for spec in _identifier_properties():
        pid = _ensure_property(wbi, spec["label"], spec["datatype"],
                               args.dry_run)
        ident_pids[spec["label"]] = pid

    # 4. Persist into Neo4j as cache nodes.
    if not args.dry_run:
        _persist_cache(base_qids, common_pids, ident_pids)

    logger.info(
        "wikibase bootstrap done  base={b}  common={c}  identifier={i}",
        b=len(base_qids), c=len(common_pids), i=len(ident_pids),
    )
    return 0


def _ensure_item(wbi, label, description, dry_run) -> str:
    """Return QID for an Item with ``label``; create if absent."""
    # SPARQL `?item rdfs:label "label"@ru` lookup OR fall back to
    # wbsearchentities + filter by exact label.  When found, return
    # qid.  When absent and not dry-run, create via wbi.item.new() +
    # set labels/descriptions + .write().
    ...


def _ensure_property(wbi, label, datatype, dry_run) -> str:
    """Return PID for a Property with ``label``; create if absent."""
    ...


def _persist_cache(base_qids, common_pids, ident_pids) -> None:
    """Upsert ``:WikibaseBaseClass`` and ``:WikibaseProperty`` nodes
    in Neo4j so ingest can skip remote lookups."""
    gs = build_neo4j_graph_store()
    for label, qid in base_qids.items():
        gs.structured_query(
            "MERGE (b:WikibaseBaseClass {label: $label}) SET b.qid = $qid",
            param_map={"label": label, "qid": qid},
        )
    for label, pid in {**common_pids, **ident_pids}.items():
        gs.structured_query(
            "MERGE (p:WikibaseProperty {label: $label}) "
            "SET p.pid = $pid, p.last_seen = datetime()",
            param_map={"label": label, "pid": pid},
        )


if __name__ == "__main__":
    raise SystemExit(main())
```

The skeleton elides the actual `_ensure_item` / `_ensure_property` bodies — fill them with `wbi_helpers.search_entities` + `wbi.item.new() / wbi.property.new()` patterns from the `wikibaseintegrator` README (`Recipes` section).

- [ ] **Step 2: Smoke against live stack**

```bash
docker compose -p kb-llamaindex up -d wikibase
sleep 90
uv run python -m scripts.setup_wikibase --dry-run
# Lists planned creates without writing.
uv run python -m scripts.setup_wikibase
# Real run — creates ~10 base-class Items + ~27 Properties.
```

In Neo4j (`bolt://localhost:7687`), verify:

```cypher
MATCH (b:WikibaseBaseClass) RETURN b.label, b.qid ORDER BY b.label;
MATCH (p:WikibaseProperty) RETURN p.label, p.pid ORDER BY p.label;
```

Both should return rows.

- [ ] **Step 3: Commit**

```bash
git add scripts/setup_wikibase.py
git commit -m "feat(scripts): wikibase bootstrap — base classes + identifier properties"
```

---

**🛑 STAGE 2 GATE.**  Confirm `WikibaseSettings` defaults work + bootstrap script populated ~37 entries (10 base classes + 3 common + 24 identifier properties) in Neo4j cache nodes.

---

## Stage 3 — `src/storage/wikibase.py` — push orchestrator

### Task 5: `AsyncWikibase` + `push_entities`

**Files:**
- Create: `src/storage/wikibase.py` (~350 LOC).
- Test: `tests/test_storage/test_wikibase_push.py` (new).

- [ ] **Step 1: Write failing tests**

```python
"""Wikibase push — covers idempotent insert + update paths.

Uses an in-memory fake Wikibase client (records create/update calls)
so tests don't depend on a live Wikibase.  Live behaviour is
exercised in test_workflow_local.py via the activity smoke.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from llama_index.core.graph_stores.types import EntityNode, Relation

from src.storage.wikibase import push_entities


@pytest.mark.asyncio
async def test_new_entity_gets_qid_and_writes_back_to_neo4j():
    """First-time push creates Item, writes QID into Neo4j entity."""
    e = EntityNode(name="Анна Морозова", label="Person",
                   properties={"description": "PM at Ромашка"})
    rel = Relation(source_id=e.id, target_id="X", label="WORKS_AT",
                   properties={})

    neo4j = MagicMock()
    neo4j.structured_query.return_value = []   # no existing QID

    wb = MagicMock()
    wb.create_item = AsyncMock(return_value="Q123")

    result = await push_entities(
        entities=[e], relations=[rel],
        neo4j_store=neo4j, wb_client=wb,
        base_class_qids={"Person": "Q1"}, property_pids={"instance_of": "P1"},
    )

    wb.create_item.assert_awaited_once()
    assert result["created_items"] == 1
    # QID persisted back to Neo4j on the entity.
    qid_writeback_call = [
        c for c in neo4j.structured_query.call_args_list
        if "wikibase_qid" in str(c)
    ]
    assert qid_writeback_call


@pytest.mark.asyncio
async def test_existing_qid_takes_update_path():
    """Entity with `wikibase_qid` in Neo4j → update, not create."""
    e = EntityNode(name="Анна Морозова", label="Person", properties={})
    neo4j = MagicMock()
    # Simulate Neo4j returning the existing QID for this entity name.
    neo4j.structured_query.return_value = [{"qid": "Q999"}]

    wb = MagicMock()
    wb.update_item = AsyncMock(return_value="Q999")
    wb.create_item = AsyncMock()

    result = await push_entities(
        entities=[e], relations=[],
        neo4j_store=neo4j, wb_client=wb,
        base_class_qids={"Person": "Q1"}, property_pids={"instance_of": "P1"},
    )

    wb.update_item.assert_awaited_once()
    wb.create_item.assert_not_awaited()
    assert result["updated_items"] == 1


@pytest.mark.asyncio
async def test_identifier_label_folded_as_external_id_statement():
    """A PhoneNumber-labelled entity is NOT a separate Item.  Its
    canonical value becomes an external-id statement on the OWNER
    entity it relates to."""
    # ... (test that asserts wb.add_statement called with
    #      datatype=external-id and PID for "PhoneNumber") ...
    pass


@pytest.mark.asyncio
async def test_lazy_relation_property_creation():
    """First time a relation label (e.g. EMPLOYMENT) is seen, push
    creates a new Wikibase Property and caches its PID in Neo4j."""
    # ... (test that `MERGE :WikibaseProperty` is called for the
    #      unseen label, and wb.create_property fires once) ...
    pass
```

- [ ] **Step 2: Implement**

`src/storage/wikibase.py` skeleton:

```python
"""Wikibase push orchestrator.

Reads a batch of merged ``EntityNode`` + ``Relation`` (the output of
``merge_and_resolve``) and projects it into a self-hosted Wikibase
instance:

  * **Items** — one per canonical entity whose ``label`` is in
    ``_NON_IDENTIFIER_LABELS`` (Person, Organization, …).
  * **Statements** — typed via custom Properties:
    - ``instance_of`` → the base-class Item QID.
    - ``er_canonical_name`` → the canonical name.
    - ``mention_count`` → numeric.
    - One ``external-id`` statement per identifier-labelled entity
      that points (via a Relation) at the owner Item.

Identifier-typed entities (``PhoneNumber`` / ``Email`` / ``INN`` /
``URL`` / ``TelegramHandle`` / …) become statements on their owner
Item, NOT separate Items.  Mapping rules:

  1. If a ``Relation`` connects (owner) → (identifier-entity),
     the identifier's ``canonical`` is added as an ``external-id``
     statement on the owner Item under the Property whose label
     matches ``identifier.label``.
  2. If an identifier-entity has no incoming relations (orphan),
     it's dropped silently — there's nothing for an external-id to
     attach to.

QID writeback to Neo4j:
  After a successful create/update, the script writes
  ``e.wikibase_qid = <qid>`` onto the corresponding ``:__Entity__``
  node.  Re-ingest reads this back and takes the update path
  (idempotent).

Lazy Property creation:
  Relation labels are an open set (LightRAG emits ``EMPLOYMENT``,
  ``LEADERSHIP``, ``OWNS``, …).  Before pushing each relation, the
  orchestrator looks up the PID in Neo4j (``:WikibaseProperty``);
  if absent, creates the Property in Wikibase, caches the new PID
  in Neo4j, then proceeds with the statement.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

# Identifier-typed labels — folded into owner Items, not separate Items.
_IDENTIFIER_LABELS: frozenset[str] = frozenset({
    "PhoneNumber", "Email", "INN", "OGRN", "BIC", "SNILS",
    "ContractNumber", "PostalAddress", "DocumentDate", "Amount",
    "URL", "Domain", "TelegramHandle", "VKProfile", "TwitterHandle",
    "InstagramHandle", "LinkedInProfile", "YouTubeChannel",
    "GitHubProfile", "UUID",
    "IMEI", "MACAddress", "LicensePlate", "VIN",
})


async def push_entities(
    entities: "list[Any]",
    relations: "list[Any]",
    *,
    neo4j_store: Any,
    wb_client: Any,
    base_class_qids: dict[str, str],
    property_pids: dict[str, str],
) -> dict[str, int]:
    """Push a batch of entities + relations into Wikibase.

    Returns counts: ``{"created_items", "updated_items",
    "external_id_statements", "relation_statements",
    "new_properties_created"}``.

    Never raises — all errors logged + swallowed; caller (the
    Temporal activity) checks the returned counts and decides what
    to log in the heartbeat.
    """
    # 1. Partition entities by label
    owner_entities = [
        e for e in entities if e.label not in _IDENTIFIER_LABELS
    ]
    identifier_entities = [
        e for e in entities if e.label in _IDENTIFIER_LABELS
    ]

    # 2. Build owner → identifiers mapping via relations
    owner_to_idents: dict[str, list[Any]] = {}
    for rel in relations:
        # rel.source_id / target_id are entity IDs (names in our convention).
        # Identifier is whichever end has a label in _IDENTIFIER_LABELS.
        ...

    # 3. Upsert each owner Item, write its identifier statements.
    counts = {
        "created_items": 0,
        "updated_items": 0,
        "external_id_statements": 0,
        "relation_statements": 0,
        "new_properties_created": 0,
    }
    qid_by_entity_id: dict[str, str] = {}
    for owner in owner_entities:
        existing_qid = _lookup_qid(neo4j_store, owner)
        if existing_qid:
            await wb_client.update_item(
                qid=existing_qid, owner=owner,
                identifiers=owner_to_idents.get(owner.id, []),
                property_pids=property_pids,
            )
            counts["updated_items"] += 1
            qid_by_entity_id[owner.id] = existing_qid
        else:
            qid = await wb_client.create_item(
                owner=owner,
                base_class_qid=base_class_qids.get(owner.label),
                identifiers=owner_to_idents.get(owner.id, []),
                property_pids=property_pids,
            )
            counts["created_items"] += 1
            qid_by_entity_id[owner.id] = qid
            _persist_qid(neo4j_store, owner, qid)
        counts["external_id_statements"] += len(
            owner_to_idents.get(owner.id, [])
        )

    # 4. Owner ↔ owner relations: ensure relation Property exists,
    #    then add the statement.
    for rel in relations:
        if rel.source_id not in qid_by_entity_id:
            continue
        if rel.target_id not in qid_by_entity_id:
            continue
        pid = property_pids.get(rel.label)
        if pid is None:
            pid = await wb_client.create_property(rel.label, "wikibase-item")
            property_pids[rel.label] = pid
            _persist_property(neo4j_store, rel.label, pid)
            counts["new_properties_created"] += 1
        await wb_client.add_statement(
            qid=qid_by_entity_id[rel.source_id],
            pid=pid,
            value=qid_by_entity_id[rel.target_id],
        )
        counts["relation_statements"] += 1

    logger.info("wikibase push done  counts={c}", c=counts)
    return counts


def _lookup_qid(neo4j_store: Any, entity: Any) -> str | None:
    """Read `wikibase_qid` from `:__Entity__` node by name+label."""
    rows = neo4j_store.structured_query(
        "MATCH (e:__Entity__ {name: $name}) "
        "WHERE $label IN labels(e) RETURN e.wikibase_qid AS qid",
        param_map={"name": entity.name, "label": entity.label},
    )
    return rows[0]["qid"] if rows and rows[0].get("qid") else None


def _persist_qid(neo4j_store: Any, entity: Any, qid: str) -> None:
    neo4j_store.structured_query(
        "MATCH (e:__Entity__ {name: $name}) SET e.wikibase_qid = $qid",
        param_map={"name": entity.name, "qid": qid},
    )


def _persist_property(neo4j_store: Any, label: str, pid: str) -> None:
    neo4j_store.structured_query(
        "MERGE (p:WikibaseProperty {label: $label}) "
        "SET p.pid = $pid, p.last_seen = datetime()",
        param_map={"label": label, "pid": pid},
    )
```

Plus a thin async wrapper `AsyncWikibase` around `wikibaseintegrator` that exposes `create_item / update_item / add_statement / create_property` — those are awaited above; concrete impl uses `asyncio.to_thread` since wbi is sync.

- [ ] **Step 3: Run + commit**

```bash
uv run pytest tests/test_storage/test_wikibase_push.py -v
git add src/storage/wikibase.py tests/test_storage/test_wikibase_push.py
git commit -m "feat(storage): wikibase push orchestrator with identifier folding"
```

---

## Stage 4 — Temporal activity + workflow wire-up

### Task 6: `push_wikibase` activity

**Files:**
- Create: `src/workflow/activities/push_wikibase.py`.
- Modify: `src/workflow/activities/__init__.py` (register).
- Modify: `src/workflow/contracts.py` — new `WikibasePushed` contract; extend `IngestResult` with `wikibase_status`.
- Test: `tests/test_workflow/test_push_wikibase.py` (new).

- [ ] **Step 1: Contract + activity**

`src/workflow/contracts.py` additions:

```python
WikibaseStatus = Literal["ok", "skipped", "failed"]


class WikibasePushed(_Frozen):
    status: WikibaseStatus
    created_items: int = 0
    updated_items: int = 0
    external_id_statements: int = 0
    relation_statements: int = 0
    new_properties_created: int = 0


class FinalizeIn(_Frozen):
    # existing fields ...
    wikibase: WikibasePushed | None = None


class IngestResult(_Frozen):
    # existing fields ...
    wikibase_status: WikibaseStatus = "skipped"
```

`src/workflow/activities/push_wikibase.py`:

```python
"""push_wikibase activity.

Reads the merged-entities staging blob, loads the bootstrap caches
(``:WikibaseBaseClass`` and ``:WikibaseProperty`` Neo4j nodes), pushes
into Wikibase via ``src/storage/wikibase.py:push_entities``.

Best-effort: ``WIKIBASE_ENABLED=false`` → status="skipped".
Errors → status="failed" but workflow continues to ``finalize``.
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

        # Load bootstrap caches from Neo4j (populated by
        # `scripts/setup_wikibase.py`).
        base_class_qids = _load_base_classes(graph_store)
        property_pids = _load_properties(graph_store)

        wb_client = AsyncWikibase.from_settings(settings.wikibase)
        activity.heartbeat({"stage": "pushing", "entities": len(entities)})

        counts = await push_entities(
            entities=entities, relations=relations,
            neo4j_store=graph_store, wb_client=wb_client,
            base_class_qids=base_class_qids, property_pids=property_pids,
        )
        activity.heartbeat({"stage": "pushed", **counts})

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
```

Register in `src/workflow/activities/__init__.py` under `LLM_ACTIVITIES` (Wikibase write is IO-bound but isolated; OK to put on main queue or LLM queue — main is fine).

- [ ] **Step 2: Tests**

`tests/test_workflow/test_push_wikibase.py`:

```python
@pytest.mark.asyncio
async def test_disabled_returns_skipped(monkeypatch):
    from src.config import settings
    monkeypatch.setattr(settings.wikibase, "enabled", False, raising=False)
    out = await push_wikibase(_fake_merged())
    assert out.status == "skipped"


@pytest.mark.asyncio
async def test_enabled_pushes_and_reports_counts(monkeypatch):
    # ... mock build_staging_store, build_neo4j_graph_store,
    #     AsyncWikibase; assert push_entities called; result.status == "ok"
    pass


@pytest.mark.asyncio
async def test_error_returns_failed_not_raise(monkeypatch):
    # ... mock push_entities to raise; activity returns
    #     WikibasePushed(status="failed") instead of bubbling
    pass
```

- [ ] **Step 3: Run + commit**

```bash
uv run pytest tests/test_workflow/test_push_wikibase.py -v
git add src/workflow/activities/push_wikibase.py \
        src/workflow/activities/__init__.py \
        src/workflow/contracts.py \
        tests/test_workflow/test_push_wikibase.py
git commit -m "feat(workflow): push_wikibase activity"
```

---

### Task 7: Wire into `DocumentIngestWorkflow`

**Files:**
- Modify: `src/workflow/document_ingest.py`.
- Update: `tests/test_workflow/test_document_ingest_workflow.py`.

- [ ] **Step 1: Insert activity call AFTER successful graph build**

In `src/workflow/document_ingest.py`, inside the inner `try` block, after `build_property_graph`, add the push step.  Note: it has its OWN try/except so a Wikibase failure does NOT downgrade `graph_status` to `vector_only` — the graph IS in Neo4j; we just track a separate `wikibase_status`.

```python
                # graph block — existing build_property_graph call
                built = await workflow.execute_activity(
                    "build_property_graph", merged,
                    ...
                )

                # NEW: Wikibase push — best-effort, separate failure status.
                workflow.upsert_memo({"stage": "push_wikibase"})
                log.info("→ push_wikibase")
                wb = await workflow.execute_activity(
                    "push_wikibase", merged,
                    result_type=WikibasePushed,
                    start_to_close_timeout=timedelta(minutes=15),
                    heartbeat_timeout=timedelta(minutes=2),
                    schedule_to_close_timeout=timedelta(hours=6),
                    retry_policy=_FAST_FOREVER,
                )
                log.info(
                    "← push_wikibase  status=%s  created=%d  updated=%d",
                    wb.status, wb.created_items, wb.updated_items,
                )
            except (ActivityError, ChildWorkflowError) as exc:
                log.warning("graph stage failed, downgrading to vector_only: %s", exc)
                graph_status = "vector_only"
                wb = WikibasePushed(status="skipped")
```

Pass `wb` into `FinalizeIn` and into the final memo:

```python
            workflow.upsert_memo({
                "stage": "finalize",
                "graph_status": graph_status,
                "wikibase_status": wb.status,
                "entities": entities, "relations": relations,
            })
            result = await workflow.execute_activity(
                "finalize",
                FinalizeIn(
                    ctx=ctx, indexed=indexed, graph_status=graph_status,
                    entities=entities, relations=relations,
                    wikibase=wb,
                ),
                ...
            )
```

`finalize` activity writes `wikibase_status` into `IngestResult`.

- [ ] **Step 2: Update finalize**

In `src/workflow/activities/finalize.py`:

```python
return IngestResult(
    doc_id=payload.ctx.doc_id,
    chunk_count=payload.indexed.count,
    graph_status=payload.graph_status,
    entities=payload.entities,
    relations=payload.relations,
    wikibase_status=(
        payload.wikibase.status if payload.wikibase else "skipped"
    ),
)
```

- [ ] **Step 3: Test**

In `tests/test_workflow/test_document_ingest_workflow.py`, extend the happy-path test stubs to include `push_wikibase_stub` returning `WikibasePushed(status="skipped")` and assert it shows up in the final result.

- [ ] **Step 4: Commit**

```bash
git add src/workflow/document_ingest.py \
        src/workflow/activities/finalize.py \
        tests/test_workflow/test_document_ingest_workflow.py
git commit -m "feat(workflow): wire push_wikibase into DocumentIngestWorkflow"
```

---

### Task 8: Update Postgres status check?

Currently `documents.status` CHECK allows: `pending | processing | completed | vector_only | failed`.

Two valid stances:
1. **Don't change PG** — `wikibase_status` lives only on the workflow result + memo; the Postgres `status` stays at `completed` regardless of Wikibase outcome. Operators see Wikibase trouble in Temporal UI / `documents.error` if needed.
2. **Add `wikibase_only` / `wiki_failed` states** — explicit, but adds CHECK constraint changes + migration.

**Default: stance 1** (no PG change).  We have enough observability via memos + IngestResult.

If you disagree, this task adds the CHECK ALTER + migrates `scripts/setup_db.py`.

---

## Stage 5 — Operator runbook + verification

### Task 9: Bootstrap + smoke-test runbook

**Files:**
- Create: `docs/runbook/wikibase.md`.

- [ ] Sections:
  - **Bring-up**: `docker compose up -d wikibase-mysql wikibase wdqs` + first-run wait.
  - **Bootstrap**: `uv run python -m scripts.setup_wikibase` (idempotent).
  - **Enable**: flip `WIKIBASE_ENABLED=true` in `.env`, restart worker.
  - **Smoke verify**: ingest a document, check Wikibase UI for new Items + statements; check Neo4j for `wikibase_qid` on entities; check `WikibaseProperty` cache.
  - **Re-ingest sanity**: same doc → `updated_items > 0, created_items == 0`.
  - **Disable**: `WIKIBASE_ENABLED=false` → push_wikibase returns `skipped` instantly.
  - **Reset**: nuke all `:WikibaseBaseClass` + `:WikibaseProperty` cache nodes in Neo4j AND `docker compose down -v wikibase wikibase-mysql wdqs` — operator's last-resort full reset.

- [ ] Commit:

```bash
git add docs/runbook/wikibase.md
git commit -m "docs(runbook): wikibase bootstrap + smoke + reset"
```

---

**🛑 STAGE 5 GATE.**  End-to-end verification:

1. `docker compose up -d` (full stack).
2. `uv run python -m scripts.setup_wikibase`.
3. `WIKIBASE_ENABLED=true` in env, restart worker.
4. Ingest 3 test docs.
5. Check Neo4j: every canonical entity has `wikibase_qid`. Every relation label seen has a `:WikibaseProperty` cache node.
6. Open Wikibase UI (`http://localhost:8181/wiki/Item:Q123` for one of the QIDs from step 5) — verify labels, aliases, `instance_of`, identifier statements, relation statements all present.
7. Re-ingest one of the same docs. Workflow result: `wikibase_status="ok"`, `created_items=0`, `updated_items>0`.

---

## Open follow-ups (NOT in this plan)

- **wdqs reindex**: SPARQL endpoint stays stale until manual `gradle reindex` on the wdqs container. If we want auto-reindex per push, separate plan.
- **ShEx / EntitySchema** for canonical-entity shape validation. Not blocking but useful for governance.
- **Wikidata cross-linking** — `external-id` properties could carry pointers to Wikidata Q IDs for famous public entities. Different plan.
- **OAuth for bot auth** — currently bot password. OK for local; production deployment may want OAuth via bot owner consumer.
- **Multi-tenant via separate Wikibase instances** — namespacing per tenant. Out of scope here.
- **Delete propagation**: ER `_cleanup_stored_losers` currently `DETACH DELETE`s in Neo4j only. Plan-extension: also issue `wbcreateredirect` from loser_qid to canon_qid in Wikibase. Documented as `wikibase_client` arg on `_cleanup_stored_losers`.

---

## Self-review

**Spec coverage:**
- All five stages cover the original 2026-05-14 parked plan + adapt to current Temporal architecture.
- 24-identifier-type fold via `_IDENTIFIER_LABELS` and identifier Properties from `scripts/setup_wikibase.py`.
- Idempotency via QID writeback on `:__Entity__`.
- Best-effort semantics consistent with existing `vector_only` downgrade pattern.

**Placeholder scan:**
- `scripts/setup_wikibase.py` skeleton has `_ensure_item` / `_ensure_property` bodies elided — the implementer fills them following `wikibaseintegrator` README.  This is intentional; the API is too version-volatile to pin verbatim.
- `src/storage/wikibase.py:push_entities` has `# 2. Build owner → identifiers mapping via relations` left as a comment — that's a ~15-line loop the implementer writes (test cases drive the exact shape).

**Type consistency:**
- `WikibasePushed.status: Literal["ok", "skipped", "failed"]` matches between contract, activity, finalize, IngestResult, runbook.
- `_IDENTIFIER_LABELS` in `wikibase.py` mirrors the literal in `IdentifierType` from `src/ingestion/identifiers.py`.  Drift risk — add a runtime assertion or import the literal directly via `typing.get_args(IdentifierType)`.

**Rollback story:**
- `WIKIBASE_ENABLED=false` → activity returns `skipped` instantly; no Wikibase traffic.
- Failed bootstrap → don't flip `WIKIBASE_ENABLED=true`; ingest continues as today.
- Code rollback per-stage via single-commit revert.

---

## Execution handoff

**Plan complete and saved to `docs/superpowers/plans/2026-05-18-wikibase-population.md`.**

Three execution options:

1. **Subagent-Driven (recommended)** — fresh subagent per task.  Stage 1-2 (infra + bootstrap) is the riskiest because of Wikibase image quirks; once those are validated, Stages 3-5 are mechanical.
2. **Inline Execution** — same session via executing-plans; stage gates between each phase.
3. **Operator-first** — you spin up the Wikibase image locally before the plan starts to confirm the docker setup actually works in your env; then we proceed with code.

Also confirm or override the **five defaults** at the top (self-host Wikibase, inline activity not child WF, default disabled, all 24 identifiers folded, no PG status change).

**Which approach?**
