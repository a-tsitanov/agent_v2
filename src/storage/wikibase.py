"""Wikibase push orchestrator.

Projects merged ``EntityNode`` / ``Relation`` lists from a workflow run
into a self-hosted Wikibase as Items + statements + identifier
external-IDs.  Consumed by the upcoming ``push_wikibase`` Temporal
activity (Task 6) and by the operator-side smoke at
``scripts/smoke_wikibase_push.py``.

Design highlights
-----------------

* Identifier-typed entities (label ∈ :data:`_IDENTIFIER_LABELS`,
  fetched at import time from :data:`IdentifierType` so renames in
  ``src/ingestion/identifiers.py`` propagate automatically) DO NOT
  become Items of their own.  They are folded onto their related
  owner Item as ``external-id`` statements.  The mapping label →
  PID is the cache built by ``scripts/setup_wikibase.py``.

* Owner Items are upserted: if the corresponding ``:__Entity__``
  node in Neo4j carries a ``wikibase_qid`` property already, we
  ``update_item`` (and reuse the QID); otherwise we ``create_item``
  and persist the new QID back into Neo4j so subsequent ingests
  hit the update path (idempotency).

* Owner ↔ owner relations become statements through the relation
  label's Property.  Unknown labels lazy-create an ``item`` Property
  via ``create_property`` and cache the new PID in Neo4j +
  ``property_pids`` in-memory.

* Never raises out of :func:`push_entities` — per-entity / per-
  relation errors are logged + skipped.  The caller (Temporal
  activity) is responsible for surfacing total failures.

The ``wikibaseintegrator`` library is synchronous; we wrap calls in
``asyncio.to_thread`` inside :class:`AsyncWikibase` so the
orchestrator stays in the async path.  Tests mock the whole
``AsyncWikibase`` — no real round-trips.
"""

from __future__ import annotations

import asyncio
import typing
from dataclasses import dataclass
from typing import Any

from loguru import logger

from src.config import WikibaseSettings
from src.ingestion.identifiers import IdentifierType


_IDENTIFIER_LABELS: frozenset[str] = frozenset(typing.get_args(IdentifierType))
"""Labels whose entities are folded as external-id statements rather
than pushed as standalone Wikibase Items.  Materialised at import
time so adding a new ``IdentifierType`` literal propagates here on
the next process start without code changes."""


# ── AsyncWikibase ──────────────────────────────────────────────────


@dataclass
class _Claim:
    """Internal claim shape carried into ``create_item`` / ``update_item``.

    ``datatype`` is one of: ``"wikibase-item"`` (value is a QID
    string), ``"external-id"`` (value is the literal id string),
    ``"string"``, ``"quantity"`` (value is numeric / numeric-string).
    """

    pid: str
    value: str
    datatype: str


class AsyncWikibase:
    """Async-friendly thin wrapper around ``wikibaseintegrator``.

    The underlying SDK is fully synchronous; every method here
    dispatches the actual call via :func:`asyncio.to_thread` so we
    don't block the event loop.  Tests mock the whole class — see
    ``tests/test_storage/test_wikibase_push.py``.
    """

    def __init__(self, wbi: Any, language: str) -> None:
        self._wbi = wbi
        self._language = language

    # -- construction ----------------------------------------------------

    @classmethod
    async def from_settings(cls, cfg: WikibaseSettings) -> AsyncWikibase:
        """Configure the SDK + log in, then return a ready instance.

        Login uses the bot credentials from :class:`WikibaseSettings`.
        Bootstrapping the bot user is the responsibility of
        ``scripts/setup_wikibase.py``.
        """

        def _build() -> Any:
            # Lazy imports — keeps the optional dep out of module
            # import time for callers that don't push to Wikibase.
            from wikibaseintegrator import WikibaseIntegrator, wbi_login
            from wikibaseintegrator.wbi_config import config as wbi_config

            base = cfg.base_url.rstrip("/")
            wbi_config["MEDIAWIKI_API_URL"] = f"{base}/w/api.php"
            wbi_config["MEDIAWIKI_INDEX_URL"] = f"{base}/w/index.php"
            wbi_config["WIKIBASE_URL"] = base
            wbi_config["DEFAULT_LANGUAGE"] = cfg.language
            wbi_config["USER_AGENT"] = "kb-llamaindex/0.1 (push_wikibase)"

            login = wbi_login.Login(
                user=cfg.bot_user,
                password=cfg.bot_password.get_secret_value(),
                mediawiki_api_url=f"{base}/w/api.php",
            )
            return WikibaseIntegrator(login=login)

        wbi = await asyncio.to_thread(_build)
        return cls(wbi=wbi, language=cfg.language)

    # -- public methods --------------------------------------------------

    async def create_item(
        self,
        label: str,
        description: str,
        base_class_qid: str | None,
        claims: list[tuple[str, str, str]],
    ) -> str:
        """Create a new Item and return its QID.

        ``claims`` is a list of ``(pid, value, datatype)`` tuples.
        ``base_class_qid`` is the instance-of target; it's added as
        a separate claim only if ``("instance_of"-PID, qid, "wikibase-item")``
        wasn't already passed in ``claims``.  In practice the caller
        builds the instance_of claim itself via ``property_pids``,
        so this is mostly a typed convenience.
        """
        return await asyncio.to_thread(
            self._create_item_sync, label, description, base_class_qid, claims,
        )

    async def update_item(
        self, qid: str, claims: list[tuple[str, str, str]],
    ) -> None:
        """Add / refresh statements on an existing Item."""
        await asyncio.to_thread(self._update_item_sync, qid, claims)

    async def add_statement(
        self,
        qid: str,
        pid: str,
        value: str,
        datatype: str = "wikibase-item",
    ) -> None:
        """Append a single statement to an existing Item."""
        await asyncio.to_thread(
            self._update_item_sync, qid, [(pid, value, datatype)],
        )

    async def create_property(
        self,
        label: str,
        datatype: str,
        description: str | None = None,
    ) -> str:
        """Create a new Property and return its PID."""
        return await asyncio.to_thread(
            self._create_property_sync, label, datatype, description,
        )

    async def set_aliases(self, qid: str, aliases: list[str]) -> None:
        """Record observed surface forms as aliases on an existing Item.

        The :class:`CanonicalLinker` (``src/graph/canonical_linker.py``)
        keys its exact-alias lookup on these — every surface form we
        observe for an entity becomes a future linking anchor.
        """
        await asyncio.to_thread(self._set_aliases_sync, qid, aliases)

    def _set_aliases_sync(self, qid: str, aliases: list[str]) -> None:
        item = self._wbi.item.get(entity_id=qid)
        # Aliases.set defaults to APPEND_OR_REPLACE (de-duped), so passing
        # the whole list adds them without clobbering existing aliases.
        item.aliases.set(language=self._language, values=aliases)
        item.write()

    # -- sync internals --------------------------------------------------

    def _create_item_sync(
        self,
        label: str,
        description: str,
        base_class_qid: str | None,
        claims: list[tuple[str, str, str]],
    ) -> str:
        item = self._wbi.item.new()
        item.labels.set(language=self._language, value=label)
        if description:
            item.descriptions.set(language=self._language, value=description)
        for pid, value, datatype in claims:
            obj = _build_claim(pid, value, datatype)
            if obj is not None:
                item.claims.add(obj)
        # ``base_class_qid`` is generally already in ``claims`` but
        # we keep this for callers that want the convenience.
        written = item.write()
        return written.id

    def _update_item_sync(
        self, qid: str, claims: list[tuple[str, str, str]],
    ) -> None:
        item = self._wbi.item.get(entity_id=qid)
        for pid, value, datatype in claims:
            obj = _build_claim(pid, value, datatype)
            if obj is not None:
                item.claims.add(obj)
        item.write()

    def _create_property_sync(
        self,
        label: str,
        datatype: str,
        description: str | None,
    ) -> str:
        prop = self._wbi.property.new(datatype=datatype)
        prop.labels.set(language=self._language, value=label)
        if description:
            prop.descriptions.set(language=self._language, value=description)
        written = prop.write()
        return written.id


def _build_claim(pid: str, value: str, datatype: str) -> Any | None:
    """Translate a ``(pid, value, datatype)`` tuple to a wikibaseintegrator
    datatype instance.  Returns None for unsupported datatypes so the
    caller can skip + log.
    """
    try:
        from wikibaseintegrator.datatypes import (
            ExternalID,
            Item as ItemDT,
            Quantity,
            String,
        )
    except ImportError:  # pragma: no cover — optional dep
        logger.warning("wikibaseintegrator not installed; skipping claim")
        return None

    try:
        if datatype == "wikibase-item":
            return ItemDT(prop_nr=pid, value=value)
        if datatype == "external-id":
            return ExternalID(prop_nr=pid, value=value)
        if datatype == "string":
            return String(prop_nr=pid, value=value)
        if datatype == "quantity":
            return Quantity(prop_nr=pid, amount=str(value))
    except Exception as exc:  # noqa: BLE001 — surface in log, skip claim
        logger.warning(
            "wikibase build_claim failed  pid={p}  dt={dt}  err={e}",
            p=pid, dt=datatype, e=exc,
        )
        return None
    logger.warning("wikibase unsupported datatype  dt={dt}  pid={p}", dt=datatype, p=pid)
    return None


# ── neo4j cache helpers ───────────────────────────────────────────


def _lookup_qid_for_entity(neo4j_store: Any, entity: Any) -> str | None:
    """Return the ``wikibase_qid`` stored on the entity's Neo4j node, if any.

    Matches by ``name`` (the same key used elsewhere in this codebase
    for ``:__Entity__`` nodes — see ``src/graph/entity_resolution.py``).
    Returns None on any error so the caller falls back to creating
    a fresh Item.
    """
    try:
        rows = neo4j_store.structured_query(
            """
            MATCH (n:__Entity__ {name: $name})
            WHERE n.wikibase_qid IS NOT NULL
            RETURN n.wikibase_qid AS qid
            LIMIT 1
            """,
            param_map={"name": entity.name},
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "wikibase qid lookup failed  name={n}  err={e}",
            n=entity.name, e=exc,
        )
        return None

    if not rows:
        return None
    row = rows[0]
    qid = row.get("qid") if isinstance(row, dict) else None
    return qid or None


def _persist_qid_for_entity(neo4j_store: Any, entity: Any, qid: str) -> None:
    """Stamp the freshly-created QID onto the entity's Neo4j node."""
    try:
        neo4j_store.structured_query(
            """
            MATCH (n:__Entity__ {name: $name})
            SET n.wikibase_qid = $qid,
                n.wikibase_last_push = datetime()
            """,
            param_map={"name": entity.name, "qid": qid},
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "wikibase qid persist failed  name={n}  qid={q}  err={e}",
            n=entity.name, q=qid, e=exc,
        )


def _persist_property_pid(
    neo4j_store: Any, label: str, pid: str, datatype: str,
) -> None:
    """Upsert a ``:WikibaseProperty`` cache row for a lazy-created Property."""
    try:
        neo4j_store.structured_query(
            """
            MERGE (p:WikibaseProperty {label: $label})
            SET p.pid = $pid, p.datatype = $dt, p.last_seen = datetime()
            """,
            param_map={"label": label, "pid": pid, "dt": datatype},
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "wikibase property cache persist failed  label={l}  err={e}",
            l=label, e=exc,
        )


# ── orchestrator ──────────────────────────────────────────────────


async def push_entities(
    entities: list[Any],
    relations: list[Any],
    *,
    neo4j_store: Any,
    wb_client: AsyncWikibase,
    base_class_qids: dict[str, str],
    property_pids: dict[str, str],
) -> dict[str, int]:
    """Push merged entities + relations into Wikibase.

    Algorithm (high level — see module docstring for rationale):
      1. Partition entities into *owners* (label not in
         ``_IDENTIFIER_LABELS``) and *identifiers*.
      2. Walk relations to build ``owner_id → [identifier_entity, ...]``,
         identifying identifier-bearing edges by exactly-one endpoint
         being an identifier entity.  Other relations are queued as
         owner-owner statements.
      3. For each owner: look up existing QID in Neo4j → update or
         create; collect identifier external-id claims; persist new
         QID back if created.
      4. For each owner-owner relation: lazy-create the relation
         label's Property if needed, then ``add_statement``.

    Returns a counter dict — see module docstring.  Never raises.
    """
    counts = {
        "created_items": 0,
        "updated_items": 0,
        "external_id_statements": 0,
        "relation_statements": 0,
        "new_properties_created": 0,
    }

    # ── 1. Partition entities ────────────────────────────────────
    owner_entities: list[Any] = [
        e for e in entities if getattr(e, "label", None) not in _IDENTIFIER_LABELS
    ]
    identifier_entities: dict[str, Any] = {
        e.id: e for e in entities if getattr(e, "label", None) in _IDENTIFIER_LABELS
    }

    # ── 2. Index relations ───────────────────────────────────────
    owner_to_idents: dict[str, list[Any]] = {}
    owner_owner_rels: list[Any] = []
    for rel in relations:
        src_id = getattr(rel, "source_id", None)
        tgt_id = getattr(rel, "target_id", None)
        src_is_id = src_id in identifier_entities
        tgt_is_id = tgt_id in identifier_entities
        if src_is_id and not tgt_is_id:
            owner_to_idents.setdefault(tgt_id, []).append(
                identifier_entities[src_id]
            )
        elif tgt_is_id and not src_is_id:
            owner_to_idents.setdefault(src_id, []).append(
                identifier_entities[tgt_id]
            )
        elif not src_is_id and not tgt_is_id:
            owner_owner_rels.append(rel)
        # both-ends-identifier → skip (rare; no owner to fold onto)

    # ── 3. Push each owner ───────────────────────────────────────
    qid_by_entity_id: dict[str, str] = {}
    instance_of_pid = property_pids.get("instance_of")
    canonical_name_pid = property_pids.get("er_canonical_name")
    mention_count_pid = property_pids.get("mention_count")

    for owner in owner_entities:
        try:
            ident_claims: list[tuple[str, str, str]] = []
            for ident in owner_to_idents.get(owner.id, []):
                pid = property_pids.get(ident.label)
                if pid is None:
                    logger.warning(
                        "wikibase missing PID for identifier label  label={l}  "
                        "(did bootstrap run after identifiers.py was updated?)",
                        l=ident.label,
                    )
                    continue
                # ``ident.name`` IS the canonical form per
                # ``IdentifierCanonicalizationTransform`` upstream.
                ident_claims.append((pid, ident.name, "external-id"))

            # Common claims (best-effort — skip if the PID is missing
            # from the bootstrap cache; never crash the whole push).
            common_claims: list[tuple[str, str, str]] = []
            base_qid = base_class_qids.get(owner.label)
            if instance_of_pid and base_qid:
                common_claims.append((instance_of_pid, base_qid, "wikibase-item"))
            if canonical_name_pid:
                common_claims.append(
                    (canonical_name_pid, owner.name, "string")
                )
            if mention_count_pid:
                mc = int(
                    (owner.properties or {}).get("mention_count", 1) or 1
                )
                common_claims.append(
                    (mention_count_pid, str(mc), "quantity")
                )

            all_claims = common_claims + ident_claims

            existing_qid = _lookup_qid_for_entity(neo4j_store, owner)
            if existing_qid:
                await wb_client.update_item(qid=existing_qid, claims=all_claims)
                counts["updated_items"] += 1
                qid_by_entity_id[owner.id] = existing_qid
            else:
                description = str(
                    (owner.properties or {}).get("description", "") or ""
                )[:240]
                qid = await wb_client.create_item(
                    label=owner.name,
                    description=description,
                    base_class_qid=base_qid,
                    claims=all_claims,
                )
                counts["created_items"] += 1
                qid_by_entity_id[owner.id] = qid
                _persist_qid_for_entity(neo4j_store, owner, qid)

            # Record observed surface forms as aliases on the owner Item
            # (foundation for canonical entity linking — see
            # ``src/graph/canonical_linker.py``).  Guarded so that
            # entities without ``surface_forms`` behave exactly as before
            # (no extra Wikibase round-trip).
            observed = (owner.properties or {}).get("surface_forms", [])
            extra_aliases = [a for a in observed if a and a != owner.name]
            if extra_aliases:
                try:
                    await wb_client.set_aliases(
                        qid_by_entity_id[owner.id], extra_aliases,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "wikibase alias set failed  name={n}  err={e}",
                        n=owner.name, e=exc,
                    )

            counts["external_id_statements"] += len(ident_claims)
        except Exception as exc:  # noqa: BLE001 — best-effort per owner
            logger.warning(
                "wikibase push owner failed  name={n}  err={e}",
                n=getattr(owner, "name", "?"), e=exc,
            )

    # ── 4. Owner ↔ owner relations ───────────────────────────────
    for rel in owner_owner_rels:
        try:
            src_qid = qid_by_entity_id.get(rel.source_id)
            tgt_qid = qid_by_entity_id.get(rel.target_id)
            if not src_qid or not tgt_qid:
                # Owner failed to push or wasn't in this batch — skip.
                continue
            pid = property_pids.get(rel.label)
            if pid is None:
                pid = await wb_client.create_property(
                    label=rel.label,
                    datatype="wikibase-item",
                    description=(
                        f"Auto-created during ingest for label '{rel.label}'"
                    ),
                )
                property_pids[rel.label] = pid
                _persist_property_pid(
                    neo4j_store, rel.label, pid, "wikibase-item",
                )
                counts["new_properties_created"] += 1
            await wb_client.add_statement(
                qid=src_qid, pid=pid, value=tgt_qid, datatype="wikibase-item",
            )
            counts["relation_statements"] += 1
        except Exception as exc:  # noqa: BLE001 — best-effort per relation
            logger.warning(
                "wikibase push relation failed  label={l}  err={e}",
                l=getattr(rel, "label", "?"), e=exc,
            )

    logger.info("wikibase push done  counts={c}", c=counts)
    return counts


__all__ = [
    "AsyncWikibase",
    "push_entities",
]
