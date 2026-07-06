"""TG → ingest harness: enqueue Telegram messages via POST /api/v1/ingest
(which uploads to MinIO + publishes to the rabbit queue).

Two modes:

* **Sync (default, no --channels).** Runs CONTINUOUSLY while started:
  discovers all dialogs of the account, keeps channels + groups (personal
  chats are ALWAYS excluded), catches up on new messages every
  --poll-interval seconds. Restart-safe: per-dialog ``last_id`` lives in
  --state (JSON), so stopping/starting NEVER re-ingests old messages —
  the API does NOT dedup (every POST mints a new doc_id), dedup lives here.
  A never-seen dialog is bootstrapped with its newest --bootstrap-limit
  messages (0 = full history). --once does a single catch-up round and exits.
  ``--folders "Имя1,Имя2"`` scopes the sync to the account's Telegram
  FOLDERS (dialog filters), matched by name case-insensitively: explicitly
  included/pinned chats count, excluded ones are dropped, and the folder's
  «все группы»/«все каналы» category flags are honored; folders are
  re-read every round, so editing a folder in the TG client re-scopes the
  sync without restart. Личные чаты не синкаются даже из папки.

* **Backfill (--channels @a,@b).** Legacy one-shot: last --limit messages
  per named channel, no state, may create duplicates if repeated.

DATE SEMANTICS: ``document_date`` sent to /ingest = the message's ORIGINAL
POST date (``msg.date``, UTC) — so doc-date filters and the analytics
time axis reflect when the post was written, NOT when it was ingested
(edge ``created_at`` = ingest time is a separate axis used by
whats_changed/volatility).

Runbook:
  1. docker compose -f docker-compose.prod.yml up -d   # rabbit+consumer+api included
  2. TG_API_ID=… TG_API_HASH=… uv run python -m scripts.tg_ingest \
       --api-key <API_KEYS from .env> [--once] [--poll-interval 60] \
       [--bootstrap-limit 100] [--state .tg_ingest.state.json] [--queue <name>]

TG_API_ID / TG_API_HASH come from https://my.telegram.org. First run does an
interactive Telethon login (phone + code) and writes the session file.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

from loguru import logger


def _message_to_doc(msg: Any, channel: str) -> tuple[str, str, str] | None:
    """Map a Telethon message → (filename, text, document_date), or None if empty."""
    text = (getattr(msg, "message", None) or "").strip()
    if not text:
        return None
    filename = f"tg_{channel.lstrip('@')}_{msg.id}.txt"
    document_date = msg.date.date().isoformat()
    return filename, text, document_date


async def post_ingest(
    http: Any,
    api_base: str,
    api_key: str,
    filename: str,
    text: str,
    document_date: str,
    queue: str | None,
) -> bool:
    """POST one document to /api/v1/ingest (multipart). True on 2xx; fail-soft."""
    data: dict[str, str] = {"document_date": document_date}
    if queue:
        data["queue"] = queue
    try:
        resp = await http.post(
            f"{api_base}/api/v1/ingest",
            headers={"X-API-Key": api_key},
            files={"file": (filename, text.encode("utf-8"), "text/plain")},
            data=data,
        )
        return 200 <= resp.status_code < 300
    except Exception as exc:
        logger.warning("post_ingest failed file={f}: {e}", f=filename, e=exc)
        return False


async def read_and_enqueue(
    tg_client: Any,
    http: Any,
    *,
    channels: list[str],
    limit: int,
    api_base: str,
    api_key: str,
    queue: str | None,
) -> Counter:
    """Backfill: read last-`limit` messages per channel (oldest→newest) and enqueue."""
    tally: Counter = Counter()
    for channel in channels:
        async for msg in tg_client.iter_messages(channel, limit=limit, reverse=True):
            doc = _message_to_doc(msg, channel)
            if doc is None:
                tally["skipped"] += 1
                continue
            filename, text, document_date = doc
            ok = await post_ingest(http, api_base, api_key, filename, text, document_date, queue)
            tally["sent" if ok else "failed"] += 1
    logger.info("tg_ingest tally: {t}", t=dict(tally))
    return tally


# ── continuous sync mode (channels + groups, restart-safe dedup) ─────


def select_dialogs(dialogs: list[Any]) -> list[Any]:
    """Keep channels and groups (incl. megagroups); DROP personal chats.

    A megagroup has both ``is_group`` and ``is_channel`` set — kept once.
    ``is_user`` (личка / бот-диалог) is always excluded by design."""
    return [
        d for d in dialogs
        if not getattr(d, "is_user", False)
        and (getattr(d, "is_group", False) or getattr(d, "is_channel", False))
    ]


def dialog_slug(dialog: Any) -> str:
    """Stable human-ish id for filenames: @username when the dialog has
    one, else the numeric dialog id (negative for channels/groups)."""
    username = getattr(getattr(dialog, "entity", None), "username", None)
    return username if username else str(dialog.id)


# ── Telegram folders (dialog filters) as sync scope ─────────────────


def _filter_title(folder: Any) -> str:
    """Folder title across TL layers: plain str in old ones,
    TextWithEntities(.text) in new ones; '' for DialogFilterDefault."""
    t = getattr(folder, "title", "")
    return getattr(t, "text", t) or ""


def resolve_folders(
    folders: list[Any], names: list[str], *, peer_id: Callable[[Any], int],
) -> tuple[dict, list[str]]:
    """Merge the wanted folders (matched by title, case-insensitive) into a
    single selection spec.

    Returns ``({include_ids, exclude_ids, groups, broadcasts}, missing)``:
    explicit peers (include + pinned) union'ed across folders, excluded
    peers likewise, plus the two category flags a folder may carry instead
    of explicit peers («все группы» / «все каналы»).  ``exclude_muted`` /
    ``exclude_read`` / archive flags are intentionally ignored — the sync
    cares about membership, not the client-side unread cosmetics."""
    wanted = {n.strip().casefold(): n.strip() for n in names if n.strip()}
    spec = {
        "include_ids": set(), "exclude_ids": set(),
        "groups": False, "broadcasts": False,
    }
    found: set[str] = set()
    for f in folders:
        title = _filter_title(f).strip()
        if title.casefold() not in wanted:
            continue
        found.add(title.casefold())
        for p in [*getattr(f, "include_peers", []), *getattr(f, "pinned_peers", [])]:
            spec["include_ids"].add(peer_id(p))
        for p in getattr(f, "exclude_peers", []):
            spec["exclude_ids"].add(peer_id(p))
        spec["groups"] = spec["groups"] or bool(getattr(f, "groups", False))
        spec["broadcasts"] = spec["broadcasts"] or bool(getattr(f, "broadcasts", False))
    missing = [orig for key, orig in wanted.items() if key not in found]
    return spec, missing


def dialog_in_folders(dialog: Any, spec: dict) -> bool:
    """Membership check against a resolve_folders() spec. Explicit include
    beats exclude; category flags cover peers a folder holds implicitly."""
    did = dialog.id
    if did in spec["include_ids"]:
        return True
    if did in spec["exclude_ids"]:
        return False
    if spec["groups"] and getattr(dialog, "is_group", False):
        return True
    if spec["broadcasts"] and getattr(dialog, "is_channel", False) \
            and not getattr(dialog, "is_group", False):
        return True
    return False


def load_state(path: Path | str) -> dict:
    """Read the sync-state JSON ({dialog_id: {last_id, title}}); {} if absent."""
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("tg_ingest: state file {p} unreadable ({e}) — starting empty", p=p, e=exc)
        return {}


def save_state(path: Path | str, state: dict) -> None:
    """Atomic-ish write (tmp + replace) so a Ctrl-C can't truncate the state."""
    p = Path(path)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(p)


async def sync_round(
    tg_client: Any,
    http: Any,
    *,
    dialogs: list[Any],
    state: dict,
    save: Callable[[], None],
    api_base: str,
    api_key: str,
    queue: str | None,
    bootstrap_limit: int,
) -> Counter:
    """One catch-up pass over ``dialogs``.

    Per dialog: fetch messages NEWER than ``state[id].last_id`` (or the
    newest ``bootstrap_limit`` for a never-seen dialog), enqueue in
    chronological order, advance ``last_id`` + ``save()`` after EVERY
    message — so an interrupt loses at most the in-flight one, and a
    restart never re-sends. On a POST failure the dialog's round stops
    WITHOUT advancing (the message is retried next round); empty messages
    advance the cursor (nothing to send — never retry them)."""
    tally: Counter = Counter()
    for dialog in dialogs:
        key = str(dialog.id)
        slug = dialog_slug(dialog)
        entry = state.setdefault(
            key, {"last_id": 0, "title": getattr(dialog, "title", "") or slug},
        )
        last_id = int(entry.get("last_id", 0) or 0)
        try:
            if last_id <= 0 and bootstrap_limit:
                fetched = [
                    m async for m in tg_client.iter_messages(
                        dialog.entity, limit=bootstrap_limit,
                    )
                ]
            else:
                fetched = [
                    m async for m in tg_client.iter_messages(
                        dialog.entity, min_id=last_id, reverse=True,
                    )
                ]
        except Exception as exc:
            logger.warning("tg_ingest: fetch failed for {s}: {e}", s=slug, e=exc)
            tally["dialog_errors"] += 1
            continue
        for msg in sorted(fetched, key=lambda m: m.id):
            doc = _message_to_doc(msg, slug)
            if doc is None:
                tally["skipped"] += 1
                entry["last_id"] = max(int(entry["last_id"]), msg.id)
                save()
                continue
            filename, text, document_date = doc
            ok = await post_ingest(
                http, api_base, api_key, filename, text, document_date, queue,
            )
            if not ok:
                tally["failed"] += 1
                break  # retry from this message next round
            tally["sent"] += 1
            entry["last_id"] = max(int(entry["last_id"]), msg.id)
            save()
    return tally


def main() -> int:
    import argparse
    import asyncio
    import os

    p = argparse.ArgumentParser(
        description="Sync TG channels+groups into the ingest queue "
        "(default: continuous, restart-safe; --channels = legacy one-shot backfill).",
    )
    p.add_argument("--channels", default=None, help="legacy backfill: comma-separated, e.g. @a,@b")
    p.add_argument("--limit", type=int, default=50, help="backfill: messages per channel")
    p.add_argument("--queue", default=None, help="target ingest queue (rabbitmq backend)")
    p.add_argument("--api-base", default="http://localhost:8000")
    p.add_argument("--api-key", default=os.environ.get("KB_API_KEY", "dev-local-key"))
    p.add_argument("--session", default=".tg_ingest.session")
    # sync-mode knobs
    p.add_argument("--state", default=".tg_ingest.state.json", help="sync: per-dialog last_id JSON")
    p.add_argument("--poll-interval", type=float, default=60.0, help="sync: seconds between rounds")
    p.add_argument(
        "--bootstrap-limit", type=int, default=100,
        help="sync: newest N messages for a never-seen dialog (0 = full history)",
    )
    p.add_argument("--once", action="store_true", help="sync: single catch-up round, then exit")
    p.add_argument(
        "--folders", default=None,
        help="sync: comma-separated TG folder names — only dialogs from these "
        "folders are synced (default: all channels+groups of the account)",
    )
    args = p.parse_args()

    api_id = int(os.environ["TG_API_ID"])
    api_hash = os.environ["TG_API_HASH"]

    async def _run_backfill() -> None:
        import httpx
        from telethon import TelegramClient

        channels = [c.strip() for c in args.channels.split(",") if c.strip()]
        async with (
            TelegramClient(args.session, api_id, api_hash) as tg,
            httpx.AsyncClient(timeout=30.0) as http,
        ):
            await read_and_enqueue(
                tg,
                http,
                channels=channels,
                limit=args.limit,
                api_base=args.api_base,
                api_key=args.api_key,
                queue=args.queue,
            )

    async def _run_sync() -> None:
        import httpx
        from telethon import TelegramClient
        from telethon import utils as tg_utils
        from telethon.tl import functions

        folder_names = [
            n for n in (args.folders or "").split(",") if n.strip()
        ]
        state = load_state(args.state)
        async with (
            TelegramClient(args.session, api_id, api_hash) as tg,
            httpx.AsyncClient(timeout=30.0) as http,
        ):
            missing_warned = False
            while True:
                dialogs = select_dialogs([d async for d in tg.iter_dialogs()])
                if folder_names:
                    res = await tg(functions.messages.GetDialogFiltersRequest())
                    filters = getattr(res, "filters", res)  # obj in new layers, list in old
                    spec, missing = resolve_folders(
                        filters, folder_names, peer_id=tg_utils.get_peer_id,
                    )
                    if missing and not missing_warned:
                        logger.warning(
                            "tg_ingest: folders not found: {m} (have: {have})",
                            m=missing,
                            have=[t for f in filters if (t := _filter_title(f).strip())],
                        )
                        missing_warned = True
                    dialogs = [d for d in dialogs if dialog_in_folders(d, spec)]
                tally = await sync_round(
                    tg,
                    http,
                    dialogs=dialogs,
                    state=state,
                    save=lambda: save_state(args.state, state),
                    api_base=args.api_base,
                    api_key=args.api_key,
                    queue=args.queue,
                    bootstrap_limit=args.bootstrap_limit,
                )
                logger.info(
                    "tg_ingest sync round: dialogs={d} tally={t}",
                    d=len(dialogs), t=dict(tally),
                )
                if args.once:
                    break
                await asyncio.sleep(args.poll_interval)

    try:
        asyncio.run(_run_backfill() if args.channels else _run_sync())
    except KeyboardInterrupt:
        logger.info("tg_ingest: stopped by user (state is saved per-message)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
