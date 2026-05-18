"""One-time Wikibase bootstrap.

Usage::

    uv run python -m scripts.setup_wikibase
    uv run python -m scripts.setup_wikibase --dry-run
    uv run python -m scripts.setup_wikibase --refresh-cache

Reads connection info from :class:`WikibaseSettings` (env-driven).
Creates the 10 base-class Items and 27 Properties (3 common +
24 identifier-type ``external-id``) and persists their QIDs/PIDs
into Neo4j so the ingest hot path never has to repeat lookups.

Idempotent — re-runs check existence via ``wbsearchentities``
before creating.  ``--dry-run`` reports planned creates without
writing; ``--refresh-cache`` re-pulls existing QIDs/PIDs from
Wikibase into Neo4j cache only.

Auth note:
    A dedicated MediaWiki bot user is the production-correct
    answer, but ``Special:BotPasswords`` requires an interactive
    web flow (CSRF token + form post) that's brittle to mint
    headlessly in v1.  This bootstrap therefore logs in with
    the admin credentials (``WIKIBASE_ADMIN_USER`` /
    ``WIKIBASE_ADMIN_PASS``) read straight from the environment,
    which are already provisioned by ``docker compose`` (T1).
    Production should rotate to bot creds via
    ``Special:BotPasswords`` once the flow is automated.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, get_args

from dotenv import load_dotenv
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Load .env so the optional ``WIKIBASE_ADMIN_*`` bootstrap creds
# (not exposed through ``WikibaseSettings``) are visible via
# :data:`os.environ`.  Must precede the ``src.config`` import so that
# pydantic-settings sees the same values.
load_dotenv()

from src.config import settings  # noqa: E402
from src.graph.store import build_neo4j_graph_store  # noqa: E402
from src.ingestion.identifiers import IdentifierType  # noqa: E402
from src.utils.logging import configure_logging  # noqa: E402


# ── taxonomy ─────────────────────────────────────────────────────────


_BASE_CLASSES: list[tuple[str, str]] = [
    ("Person",        "Person — individual referenced by the KB"),
    ("Organization",  "Organization — company, agency, group"),
    ("Concept",       "Abstract concept or term"),
    ("Metric",        "Measurable metric or KPI"),
    ("Topic",         "Topic discussed in documents"),
    ("Issue",         "Customer issue / problem statement"),
    ("Resolution",    "Resolution applied to an issue"),
    ("EventOrAction", "Event or discrete action"),
    ("Product",       "Product or service"),
    ("Document",      "Document referenced by the KB"),
]

_COMMON_PROPERTIES: list[tuple[str, str]] = [
    ("er_canonical_name", "string"),
    ("instance_of",       "wikibase-item"),
    ("mention_count",     "quantity"),
]


def _identifier_properties() -> list[tuple[str, str]]:
    """One ``external-id`` property per ``IdentifierType`` literal."""
    return [(t, "external-id") for t in get_args(IdentifierType)]


# ── counters for the final summary ──────────────────────────────────


class _Counter:
    """Tally created vs already-existing entities for the summary line."""

    def __init__(self) -> None:
        self.created = 0
        self.existing = 0


# ── wikibase helpers ────────────────────────────────────────────────


def _api_url(base_url: str) -> str:
    return base_url.rstrip("/") + "/w/api.php"


def _configure_wbi(base_url: str, language: str) -> None:
    """Point ``wikibaseintegrator`` at the local Wikibase."""
    from wikibaseintegrator.wbi_config import config as wbi_config

    wbi_config["MEDIAWIKI_API_URL"] = _api_url(base_url)
    wbi_config["MEDIAWIKI_INDEX_URL"] = base_url.rstrip("/") + "/w/index.php"
    wbi_config["WIKIBASE_URL"] = base_url.rstrip("/")
    wbi_config["DEFAULT_LANGUAGE"] = language
    wbi_config["USER_AGENT"] = "kb-llamaindex-bootstrap/0.1 (setup_wikibase.py)"


def _bootstrap_credentials() -> tuple[str, str]:
    """Return ``(user, password)`` to log in with.

    Prefers the admin credentials from the environment so we don't
    fight bot-password creation in v1 — see module docstring.
    TODO: dedicated bot user — for now using admin to bootstrap;
    production should rotate to bot creds via Special:BotPasswords.
    """
    admin_user = os.environ.get("WIKIBASE_ADMIN_USER", "WikibaseAdmin")
    admin_pass = os.environ.get("WIKIBASE_ADMIN_PASS", "")
    if admin_pass:
        return admin_user, admin_pass

    # Fall back to the bot creds from WikibaseSettings.  These may be
    # rejected as too weak by MediaWiki at user-creation time, but
    # if the user already exists they're fine to log in with.
    cfg = settings.wikibase
    return cfg.bot_user, cfg.bot_password.get_secret_value()


def _login(base_url: str) -> Any:
    """Build a wikibaseintegrator ``Login`` object."""
    from wikibaseintegrator import wbi_login

    user, password = _bootstrap_credentials()
    logger.info("wikibase login  user={u}", u=user)
    return wbi_login.Login(
        user=user,
        password=password,
        mediawiki_api_url=_api_url(base_url),
    )


def _find_entity_by_label(
    label: str,
    *,
    language: str,
    search_type: str,
) -> str | None:
    """Return the ID of an entity whose label matches ``label`` exactly.

    Talks to ``wbsearchentities`` directly via
    :func:`mediawiki_api_call_helper` instead of going through
    :func:`wbi_helpers.search_entities` — the latter blows up on a
    ``KeyError: 'label'`` in 0.12.15 when the Wikibase API omits the
    ``label`` echo (the local Wikibase image does this even when the
    label is in fact set; see ``match.text`` for the actual matched
    string).  ``strict_language=True`` so we don't match a same-string
    label that only exists in a fallback language.
    """
    from wikibaseintegrator import wbi_helpers

    params = {
        "action": "wbsearchentities",
        "search": label,
        "language": language,
        "strictlanguage": 1,
        "type": search_type,
        "limit": 20,
        "format": "json",
    }
    try:
        payload = wbi_helpers.mediawiki_api_call_helper(
            data=params, allow_anonymous=True,
        )
    except Exception as exc:  # noqa: BLE001 — surface API errors loudly
        logger.error(
            "wbsearchentities failed  label={l}  type={t}  err={e}",
            l=label, t=search_type, e=exc,
        )
        raise

    for hit in payload.get("search", []):
        match_info = hit.get("match") or {}
        match_text = match_info.get("text")
        match_lang = match_info.get("language")
        match_type = match_info.get("type")
        # Only treat a hit as authoritative if the API says the actual
        # label (not alias) in the requested language was the match.
        if (
            match_type == "label"
            and match_lang == language
            and match_text == label
        ):
            return hit["id"]
    return None


def _ensure_item(
    wbi: Any,
    label: str,
    description: str,
    language: str,
    *,
    dry_run: bool,
    refresh_only: bool,
    counter: _Counter,
) -> str | None:
    """Return QID for an Item with ``label``; create if absent."""
    existing = _find_entity_by_label(
        label, language=language, search_type="item",
    )
    if existing:
        logger.info("item exists  label={l}  qid={q}", l=label, q=existing)
        counter.existing += 1
        return existing

    if dry_run or refresh_only:
        logger.info(
            "item would-create  label={l}  desc={d}",
            l=label, d=description,
        )
        counter.created += 1
        return None

    item = wbi.item.new()
    item.labels.set(language=language, value=label)
    item.descriptions.set(language=language, value=description)
    written = item.write()
    qid = written.id
    logger.info("item created  label={l}  qid={q}", l=label, q=qid)
    counter.created += 1
    return qid


def _ensure_property(
    wbi: Any,
    label: str,
    datatype: str,
    language: str,
    *,
    dry_run: bool,
    refresh_only: bool,
    counter: _Counter,
) -> str | None:
    """Return PID for a Property with ``label`` + ``datatype``."""
    existing = _find_entity_by_label(
        label, language=language, search_type="property",
    )
    if existing:
        logger.info(
            "property exists  label={l}  pid={p}  dt={d}",
            l=label, p=existing, d=datatype,
        )
        counter.existing += 1
        return existing

    if dry_run or refresh_only:
        logger.info(
            "property would-create  label={l}  dt={d}",
            l=label, d=datatype,
        )
        counter.created += 1
        return None

    prop = wbi.property.new(datatype=datatype)
    prop.labels.set(language=language, value=label)
    written = prop.write()
    pid = written.id
    logger.info(
        "property created  label={l}  pid={p}  dt={d}",
        l=label, p=pid, d=datatype,
    )
    counter.created += 1
    return pid


# ── neo4j cache ─────────────────────────────────────────────────────


def _persist_cache(
    base_qids: dict[str, str],
    property_pids: dict[str, tuple[str, str]],
) -> None:
    """Upsert ``:WikibaseBaseClass`` and ``:WikibaseProperty`` nodes."""
    gs = build_neo4j_graph_store()
    for label, qid in base_qids.items():
        if not qid:
            continue
        gs.structured_query(
            "MERGE (b:WikibaseBaseClass {label: $label}) "
            "SET b.qid = $qid, b.last_seen = datetime()",
            param_map={"label": label, "qid": qid},
        )
    for label, (pid, datatype) in property_pids.items():
        if not pid:
            continue
        gs.structured_query(
            "MERGE (p:WikibaseProperty {label: $label}) "
            "SET p.pid = $pid, p.datatype = $dt, "
            "    p.last_seen = datetime()",
            param_map={"label": label, "pid": pid, "dt": datatype},
        )


# ── entry point ─────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report planned creates without writing to Wikibase or Neo4j.",
    )
    parser.add_argument(
        "--refresh-cache", action="store_true",
        help="Refresh Neo4j cache from existing Wikibase entities; "
             "do not create anything new.",
    )
    args = parser.parse_args()

    configure_logging(level=settings.api.log_level, json_output=False)
    cfg = settings.wikibase
    logger.info(
        "wikibase bootstrap  base_url={u}  language={l}",
        u=cfg.base_url, l=cfg.language,
    )

    _configure_wbi(cfg.base_url, cfg.language)

    # WikibaseIntegrator is needed only when we may write — search
    # works against the public API without login.  We still log in
    # eagerly so credential issues surface up front rather than after
    # 10 base classes have been "searched".
    wbi = None
    if not args.dry_run:
        from wikibaseintegrator import WikibaseIntegrator

        login = _login(cfg.base_url)
        wbi = WikibaseIntegrator(login=login)

    counter = _Counter()

    # 1. Base-class Items
    base_qids: dict[str, str] = {}
    for label, description in _BASE_CLASSES:
        qid = _ensure_item(
            wbi, label, description, cfg.language,
            dry_run=args.dry_run, refresh_only=args.refresh_cache,
            counter=counter,
        )
        if qid:
            base_qids[label] = qid

    # 2-3. Common + identifier-type Properties
    property_pids: dict[str, tuple[str, str]] = {}
    for label, datatype in _COMMON_PROPERTIES + _identifier_properties():
        pid = _ensure_property(
            wbi, label, datatype, cfg.language,
            dry_run=args.dry_run, refresh_only=args.refresh_cache,
            counter=counter,
        )
        if pid:
            property_pids[label] = (pid, datatype)

    # 4. Persist into Neo4j cache (skip when dry-run)
    if not args.dry_run:
        _persist_cache(base_qids, property_pids)

    logger.info(
        "wikibase bootstrap done  base={b}  common={c}  identifier={i}  "
        "(created={cr}, existing={ex})",
        b=len(_BASE_CLASSES),
        c=len(_COMMON_PROPERTIES),
        i=len(_identifier_properties()),
        cr=counter.created,
        ex=counter.existing,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
