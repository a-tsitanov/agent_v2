"""Deterministic identifier extraction + canonicalization.

Pre-LLM stage of the retrieval pipeline.  Detects business, digital
and device identifiers in raw document text via regex / lib-based
parsers and returns each match with a canonical form suitable for
use as an ``entity_name`` in Neo4j.

Why deterministic + canonical:
  Two documents may write the same phone as ``+7 (495) 123-45-67`` and
  ``8 495 1234567``.  If we let the LLM extract both verbatim, Neo4j
  ends up with two separate nodes — graph dedup breaks.  We pre-canon
  to E.164 (``+74951234567``) so identical entities collapse to one
  node regardless of source formatting.

Currently 19 types across three groups (see ``IdentifierType``):

* **Business / financial** — ``PhoneNumber``, ``Email``, ``INN``,
  ``OGRN``, ``BIC``, ``SNILS``, ``ContractNumber``,
  ``PostalAddress``, ``DocumentDate``, ``Amount``.
* **Digital identity** — ``URL``, ``Domain``, ``TelegramHandle``,
  ``VKProfile``, ``UUID``.
* **Device / hardware** — ``IMEI`` (Luhn), ``MACAddress``,
  ``LicensePlate`` (RU), ``VIN`` (mod-11 checksum).

The output of ``extract_identifiers()`` is consumed by
``IdentifierCanonicalizationTransform`` in ``pipeline.py``, which:
  1. Calls ``inject_canonical_entities`` to upsert one canonical
     ``EntityNode`` per ``(entity_type, canonical)`` pair into Neo4j —
     guarantees the canonical node exists before LLM extraction.
  2. Appends a ``Канонические идентификаторы:`` block to the chunk
     text so the LLM uses canonical forms when building relationships
     (the system prompt teaches the LLM this protocol).

When two detectors match overlapping spans (e.g. ``URL`` and
``VKProfile`` both matching ``https://vk.com/user``), the higher
priority specialised type wins via ``_resolve_overlaps``.

``postal`` (libpostal Python bindings) is imported optionally — when
the C library isn't installed locally we fall back to a rule-based
address normalizer.  Phone numbers and dates use pure-Python libs
(``phonenumbers``, ``dateparser``) which install without system deps.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

import dateparser
import phonenumbers

try:  # libpostal is heavy and optional — fall back to rule-based
    from postal.parser import parse_address as _libpostal_parse  # type: ignore[import-not-found]

    _HAS_LIBPOSTAL = True
except ImportError:  # pragma: no cover — depends on system libpostal-dev
    _libpostal_parse = None  # type: ignore[assignment]
    _HAS_LIBPOSTAL = False


IdentifierType = Literal[
    # Business / financial
    "PhoneNumber",
    "Email",
    "INN",
    "OGRN",
    "BIC",
    "SNILS",
    "ContractNumber",
    "PostalAddress",
    "DocumentDate",
    "Amount",
    # Digital identity
    "URL",
    "Domain",
    "TelegramHandle",
    "VKProfile",
    "UUID",
    # Device / hardware
    "IMEI",
    "MACAddress",
    "LicensePlate",
    "VIN",
]


# Priority for overlap resolution.  When two detectors produce
# overlapping matches, the one with the higher priority wins and
# the lower-priority match is dropped.  Used by ``_resolve_overlaps``.
# Rule of thumb: specialised types > generic types.
_PRIORITY: dict[str, int] = {
    "PhoneNumber": 100,
    "Email": 100,
    "INN": 100,
    "OGRN": 100,
    "BIC": 100,
    "SNILS": 100,
    "ContractNumber": 90,
    "PostalAddress": 90,
    "DocumentDate": 90,
    "Amount": 90,
    "IMEI": 95,
    "MACAddress": 95,
    "LicensePlate": 95,
    "VIN": 95,
    "UUID": 95,
    "TelegramHandle": 80,
    "VKProfile": 80,
    "URL": 50,
    "Domain": 10,
}


@dataclass(frozen=True)
class NormalizedIdentifier:
    """One identifier match with both verbatim and canonical forms.

    ``span`` is character offsets in the source text — Stage C uses it
    to build ``Канонические идентификаторы:`` blocks aligned with the
    LLM's view of the document.
    """

    entity_type: IdentifierType
    canonical: str
    original: str
    span: tuple[int, int]


# ── PhoneNumber ──────────────────────────────────────────────────────


def _extract_phones(text: str) -> list[NormalizedIdentifier]:
    """E.164 via google's libphonenumber port. RU as default region."""
    out: list[NormalizedIdentifier] = []
    for match in phonenumbers.PhoneNumberMatcher(text, "RU"):
        canonical = phonenumbers.format_number(
            match.number, phonenumbers.PhoneNumberFormat.E164
        )
        out.append(
            NormalizedIdentifier(
                entity_type="PhoneNumber",
                canonical=canonical,
                original=match.raw_string,
                span=(match.start, match.end),
            )
        )
    return out


# ── Email ────────────────────────────────────────────────────────────

_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")


def _extract_emails(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _EMAIL_RE.finditer(text):
        out.append(
            NormalizedIdentifier(
                entity_type="Email",
                canonical=m.group(0).lower(),
                original=m.group(0),
                span=m.span(),
            )
        )
    return out


# ── INN (10 or 12 digits with checksum) ──────────────────────────────

_INN_RE = re.compile(r"(?<!\d)(\d{10}|\d{12})(?!\d)")


def _check_inn_10(d: str) -> bool:
    coeffs = (2, 4, 10, 3, 5, 9, 4, 6, 8)
    s = sum(int(d[i]) * coeffs[i] for i in range(9))
    return (s % 11) % 10 == int(d[9])


def _check_inn_12(d: str) -> bool:
    c1 = (7, 2, 4, 10, 3, 5, 9, 4, 6, 8)
    c2 = (3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8, 0)
    n11 = (sum(int(d[i]) * c1[i] for i in range(10)) % 11) % 10
    n12 = (sum(int(d[i]) * c2[i] for i in range(11)) % 11) % 10
    return n11 == int(d[10]) and n12 == int(d[11])


def _extract_inns(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _INN_RE.finditer(text):
        d = m.group(1)
        valid = (len(d) == 10 and _check_inn_10(d)) or (
            len(d) == 12 and _check_inn_12(d)
        )
        if valid:
            out.append(
                NormalizedIdentifier(
                    entity_type="INN",
                    canonical=d,
                    original=d,
                    span=m.span(1),
                )
            )
    return out


# ── OGRN (13 or 15 digits with checksum) ─────────────────────────────

_OGRN_RE = re.compile(r"(?<!\d)(\d{13}|\d{15})(?!\d)")


def _check_ogrn_13(d: str) -> bool:
    return int(d[12]) == int(d[:12]) % 11 % 10


def _check_ogrn_15(d: str) -> bool:
    return int(d[14]) == int(d[:14]) % 13 % 10


def _extract_ogrn(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _OGRN_RE.finditer(text):
        d = m.group(1)
        valid = (len(d) == 13 and _check_ogrn_13(d)) or (
            len(d) == 15 and _check_ogrn_15(d)
        )
        if valid:
            out.append(
                NormalizedIdentifier(
                    entity_type="OGRN",
                    canonical=d,
                    original=d,
                    span=m.span(1),
                )
            )
    return out


# ── BIC (РФ: 9 digits, the first two are 04) ────────────────────────

_BIC_RE = re.compile(r"\b04\d{7}\b")


def _extract_bic(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _BIC_RE.finditer(text):
        out.append(
            NormalizedIdentifier(
                entity_type="BIC",
                canonical=m.group(0),
                original=m.group(0),
                span=m.span(),
            )
        )
    return out


# ── ContractNumber ───────────────────────────────────────────────────
#
# Heuristic: ``№`` (Russian marker) or literal ``No.``/``N.`` followed
# by an alphanumeric token of 2-30 chars.
#
# Constraints to avoid false positives on plain prose:
#   * NO case-insensitive flag — body-text "no symptoms" / "no warranties"
#     would otherwise match and pull arbitrary capitalised words.
#   * The captured token MUST contain at least one digit (real contract
#     IDs always do — pure alpha tokens are section headings,
#     not contract refs).

_CONTRACT_RE = re.compile(
    r"(?:№|\bNo\.|\bN\.)\s*([A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9./\-]{1,29})",
)


def _canonicalize_contract(raw: str) -> str:
    """Uppercase + strip whitespace; preserve / - . separators."""
    return raw.upper().replace(" ", "")


def _extract_contracts(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _CONTRACT_RE.finditer(text):
        original = m.group(1)
        if not any(ch.isdigit() for ch in original):
            continue  # alpha-only tokens are not contract numbers
        out.append(
            NormalizedIdentifier(
                entity_type="ContractNumber",
                canonical=_canonicalize_contract(original),
                original=original,
                span=m.span(1),
            )
        )
    return out


# ── DocumentDate ─────────────────────────────────────────────────────

_DATE_DMY_RE = re.compile(
    # Require a 4-digit year — filters out software-version triplets
    # like ``1.4.10`` (would otherwise parse as ``2010-04-01``).  Two-
    # digit years are rare in formal Russian business documents; the
    # eval golden set assumes 4-digit only.
    r"(?<!\d)(\d{1,2}[./\-]\d{1,2}[./\-]\d{4})(?!\d)"
)
_DATE_ISO_RE = re.compile(
    r"(?<!\d)(\d{4}-\d{2}-\d{2})(?!\d)"
)
_DATE_VERBAL_RE = re.compile(
    r"\b\d{1,2}\s+(?:янв(?:ар(?:я|ь)|\.)?|фев(?:рал(?:я|ь)|\.)?|"
    r"мар(?:та|т)|апр(?:ел(?:я|ь)|\.)?|мая|май|июн(?:я|ь)|июл(?:я|ь)|"
    r"авг(?:уста|уст|\.)?|сент(?:ябр(?:я|ь)|\.)?|окт(?:ябр(?:я|ь)|\.)?|"
    r"ноя(?:бр(?:я|ь)|\.)?|дек(?:абр(?:я|ь)|\.)?)"
    r"\s+\d{4}(?:\s*(?:г\.?|года))?\b",
    re.IGNORECASE,
)


def _extract_dates(text: str) -> list[NormalizedIdentifier]:
    """Three flavours: ISO (cheap strptime), DMY-numeric, RU verbal.

    ``dateparser`` with ``DATE_ORDER=DMY`` mis-parses ISO YMD strings;
    we strptime ISO directly to side-step that.
    """
    from datetime import datetime

    out: list[NormalizedIdentifier] = []
    seen: set[tuple[int, int]] = set()

    for m in _DATE_ISO_RE.finditer(text):
        span = m.span()
        try:
            parsed = datetime.strptime(m.group(1), "%Y-%m-%d")
        except ValueError:
            continue
        seen.add(span)
        out.append(
            NormalizedIdentifier(
                entity_type="DocumentDate",
                canonical=parsed.strftime("%Y-%m-%d"),
                original=m.group(0),
                span=span,
            )
        )

    for regex, dmy in ((_DATE_DMY_RE, True), (_DATE_VERBAL_RE, False)):
        for m in regex.finditer(text):
            span = m.span()
            if span in seen:
                continue
            try:
                parsed_dt = dateparser.parse(
                    m.group(0),
                    languages=["ru"],
                    settings={"DATE_ORDER": "DMY"} if dmy else None,
                )
            except Exception:  # noqa: BLE001 — dateparser quirks
                continue
            if not parsed_dt:
                continue
            seen.add(span)
            out.append(
                NormalizedIdentifier(
                    entity_type="DocumentDate",
                    canonical=parsed_dt.strftime("%Y-%m-%d"),
                    original=m.group(0),
                    span=span,
                )
            )
    return out


# ── Amount (Russian rubles, RUB, ₽) ──────────────────────────────────

_AMOUNT_RE = re.compile(
    r"(\d[\d\s ]*(?:[.,]\d{1,2})?)\s*"
    r"(?:(млн|тыс|млрд)\.?\s*)?"
    r"(?:руб(?:лей|ля|\.)?|РУБ|RUB|₽)",
    re.IGNORECASE,
)
_AMOUNT_MULT = {"тыс": 1_000, "млн": 1_000_000, "млрд": 1_000_000_000}


def _extract_amounts(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _AMOUNT_RE.finditer(text):
        raw_num = (
            m.group(1)
            .replace(" ", "")
            .replace(" ", "")
            .replace(",", ".")
        )
        mult_word = (m.group(2) or "").lower()
        try:
            value = float(raw_num)
        except ValueError:
            continue
        if mult_word in _AMOUNT_MULT:
            value *= _AMOUNT_MULT[mult_word]
        out.append(
            NormalizedIdentifier(
                entity_type="Amount",
                canonical=f"{value:.2f} RUB",
                original=m.group(0),
                span=m.span(),
            )
        )
    return out


# ── PostalAddress ────────────────────────────────────────────────────
#
# Detect: 6-digit postal code anchors an address.  Window forward up to
# 200 chars (or to first newline) to capture city/street/house tokens.
# Skip windows that don't contain at least one street/city marker —
# avoids matching random 6-digit numbers (e.g. order numbers).

_POSTAL_CODE_RE = re.compile(r"(?<!\d)(\d{6})(?!\d)")
_ADDR_WINDOW = 200
_ADDR_MARKER_RE = re.compile(
    r"\b(г\.|город|Москв|Санкт-Петербург|Питер|ул\.|улица|пр-кт|пер\.|"
    r"проспект|переулок|обл\.|область|край|респ\.|республика|"
    r"д\.|дом\s)",
    re.IGNORECASE,
)

# Rule-based abbreviation expansion for the rule-only fallback path.
# Lowercased, stripped of dots/commas, normalized whitespace.
_ABBR_EXPANSIONS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bг\.\s*", re.IGNORECASE), ""),
    (re.compile(r"\bгород\s+", re.IGNORECASE), ""),
    (re.compile(r"\bул\.\s*", re.IGNORECASE), "ул "),
    (re.compile(r"\bулица\s+", re.IGNORECASE), "ул "),
    (re.compile(r"\bпр-кт\.?\s*", re.IGNORECASE), "пр "),
    (re.compile(r"\bпроспект\s+", re.IGNORECASE), "пр "),
    (re.compile(r"\bпер\.\s*", re.IGNORECASE), "пер "),
    (re.compile(r"\bпереулок\s+", re.IGNORECASE), "пер "),
    (re.compile(r"\bд\.\s*", re.IGNORECASE), ""),
    (re.compile(r"\bдом\s+", re.IGNORECASE), ""),
    (re.compile(r"\bстр\.\s*", re.IGNORECASE), "стр "),
    (re.compile(r"\bстроение\s+", re.IGNORECASE), "стр "),
    (re.compile(r"\bкорп\.\s*", re.IGNORECASE), "к "),
    (re.compile(r"\bкорпус\s+", re.IGNORECASE), "к "),
    (re.compile(r"\bкв\.\s*", re.IGNORECASE), "кв "),
    (re.compile(r"\bквартира\s+", re.IGNORECASE), "кв "),
    (re.compile(r"\bобл\.\s*", re.IGNORECASE), "обл "),
    (re.compile(r"\bобласть\s+", re.IGNORECASE), "обл "),
    (re.compile(r"\bр-н\.?\s*", re.IGNORECASE), "р-н "),
    (re.compile(r"\bрайон\s+", re.IGNORECASE), "р-н "),
)


def _normalize_address_rule(raw: str) -> str:
    """Lowercase + abbreviation expansion + whitespace cleanup."""
    s = raw.lower()
    for pattern, repl in _ABBR_EXPANSIONS:
        s = pattern.sub(repl, s)
    s = re.sub(r"\s+", " ", s).strip(" ,.;")
    s = re.sub(r"\s*,\s*", ", ", s)
    return s


def _normalize_address(raw: str) -> str:
    """libpostal-based parse → structured fields → canonical assembly.

    Falls back to ``_normalize_address_rule`` when libpostal is missing
    or raises.  When libpostal IS available it transliterates Russian
    addresses by default — we use ``parse_address`` (structured) rather
    than ``expand_address`` (returns transliterated alternatives) to
    keep the canonical in Russian script.
    """
    if not _HAS_LIBPOSTAL or _libpostal_parse is None:
        return _normalize_address_rule(raw)
    try:
        parsed = _libpostal_parse(raw)
        fields: dict[str, str] = {label: value for value, label in parsed}
        parts: list[str] = []
        for label in ("postcode", "city", "road", "house_number", "unit"):
            v = fields.get(label)
            if v:
                parts.append(v.lower())
        if not parts:
            return _normalize_address_rule(raw)
        return ", ".join(parts)
    except Exception:  # noqa: BLE001 — libpostal C errors are opaque
        return _normalize_address_rule(raw)


_ADDR_TERMINATORS: tuple[str, ...] = ("\n", ")", " и ", "; ", " — ")


def _truncate_address_window(window: str) -> str:
    """Stop at the earliest natural address terminator.

    Without this the 200-char window past the postcode swallows trailing
    clauses (``...д. 76, стр. 1) и АО «Промсервис»``) and pollutes the
    canonical.  Terminators chosen empirically from real Russian
    contracts.
    """
    cuts = [window.find(t) for t in _ADDR_TERMINATORS]
    cuts = [c for c in cuts if c >= 0]
    if cuts:
        return window[: min(cuts)]
    return window


def _extract_addresses(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    seen_spans: set[tuple[int, int]] = set()
    for m in _POSTAL_CODE_RE.finditer(text):
        start = m.start()
        end = min(len(text), m.end() + _ADDR_WINDOW)
        window = _truncate_address_window(text[start:end])
        end = start + len(window)
        if not _ADDR_MARKER_RE.search(window):
            continue
        span = (start, end)
        if span in seen_spans:
            continue
        seen_spans.add(span)
        cleaned = window.strip().rstrip(",.;")
        out.append(
            NormalizedIdentifier(
                entity_type="PostalAddress",
                canonical=_normalize_address(cleaned),
                original=cleaned,
                span=span,
            )
        )
    return out


# ── URL / Domain ─────────────────────────────────────────────────────

# Stops at whitespace, quotes, angle-brackets and a final punctuation
# trailer (handled separately) so we don't pull `.`, `)`, etc. into the
# canonical URL.
_URL_RE = re.compile(
    r"https?://[^\s<>\"']+",
    re.IGNORECASE,
)
_URL_TRAIL_RE = re.compile(r"[\.,;:!?\)\]\}>]+$")

_DOMAIN_RE = re.compile(
    # subdomain.example.co.uk (1+ labels, last label 2-24 letters)
    r"\b(?!-)(?:[a-z0-9-]{1,63}\.)+[a-z]{2,24}\b",
    re.IGNORECASE,
)
# Common, real TLDs we trust without an SLD whitelist.  Anything else
# falls through (so `payment.dec` isn't mistaken for a domain).  Add
# more here if real corpus produces false negatives.
_DOMAIN_TLD_ALLOW: frozenset[str] = frozenset({
    "com", "net", "org", "io", "ai", "dev", "co", "uk", "de", "fr",
    "ru", "su", "by", "ua", "kz", "uz", "am", "az", "ge", "kg", "tj",
    "tm", "md", "rs", "pl", "cz", "sk", "lt", "lv", "ee", "fi", "se",
    "no", "dk", "nl", "be", "at", "ch", "es", "pt", "it", "gr", "ie",
    "edu", "gov", "mil", "info", "biz", "name", "pro", "tv", "me",
    "app", "tech", "cloud", "online", "site", "store", "shop", "blog",
    "team", "ws", "tg", "us", "ca", "cn", "jp", "kr", "in", "br",
    "tr", "id", "th", "vn", "mx", "ar", "cl", "ng", "za", "il", "ae",
    "sa", "eu", "su", "xyz",
})


def _normalize_url(raw: str) -> str:
    """Lower-case scheme + host, strip trailing slash, no trailing punct."""
    raw = _URL_TRAIL_RE.sub("", raw)
    # Lower-case scheme + host while preserving path/query/fragment.
    m = re.match(r"^(https?)://([^/?#]+)(.*)$", raw, re.IGNORECASE)
    if not m:
        return raw
    scheme, host, rest = m.group(1).lower(), m.group(2).lower(), m.group(3)
    if rest in ("", "/"):
        rest = ""
    return f"{scheme}://{host}{rest}"


def _extract_urls(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _URL_RE.finditer(text):
        raw = m.group(0)
        cleaned = _URL_TRAIL_RE.sub("", raw)
        start, end = m.span()
        end = start + len(cleaned)
        out.append(
            NormalizedIdentifier(
                entity_type="URL",
                canonical=_normalize_url(cleaned),
                original=cleaned,
                span=(start, end),
            )
        )
    return out


def _extract_domains(text: str) -> list[NormalizedIdentifier]:
    """Bare domains (no protocol). URL / Email / social detectors
    have higher priority so their full URL forms win on overlap."""
    out: list[NormalizedIdentifier] = []
    for m in _DOMAIN_RE.finditer(text):
        candidate = m.group(0)
        tld = candidate.rsplit(".", 1)[1].lower()
        if tld not in _DOMAIN_TLD_ALLOW:
            continue
        out.append(
            NormalizedIdentifier(
                entity_type="Domain",
                canonical=candidate.lower(),
                original=candidate,
                span=m.span(),
            )
        )
    return out


# ── Social handles ───────────────────────────────────────────────────

# `@username` (4-32 chars), `t.me/username`, `telegram.me/username`,
# optionally with `https://` prefix.  Username rules from Telegram:
# letters, digits, underscores; must start with a letter.
_TELEGRAM_USER = r"[A-Za-z][A-Za-z0-9_]{3,31}"
_TELEGRAM_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/(?P<user>" + _TELEGRAM_USER + r")"
    r"|(?<![A-Za-z0-9._])@(?P<at>" + _TELEGRAM_USER + r")",
    re.IGNORECASE,
)

_VK_RE = re.compile(
    r"(?:https?://)?(?:m\.|new\.)?vk(?:\.com|\.ru|ontakte\.ru)"
    # Path: VK usernames are letters/digits/underscores; `id12345` style
    # is also allowed.  Trailing dots/commas are punctuation, not URL.
    r"/(?P<path>[A-Za-z0-9_\-]+)",
    re.IGNORECASE,
)


def _extract_telegram(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _TELEGRAM_RE.finditer(text):
        username = (m.group("user") or m.group("at") or "").lower()
        if not username:
            continue
        out.append(
            NormalizedIdentifier(
                entity_type="TelegramHandle",
                canonical=f"@{username}",
                original=m.group(0),
                span=m.span(),
            )
        )
    return out


def _extract_vk(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _VK_RE.finditer(text):
        path = m.group("path")
        if not path or path.lower() in {"id", "www", "feed", "im"}:
            continue
        out.append(
            NormalizedIdentifier(
                entity_type="VKProfile",
                canonical=f"vk.com/{path.lower()}",
                original=m.group(0),
                span=m.span(),
            )
        )
    return out


# ── UUID ─────────────────────────────────────────────────────────────

_UUID_RE = re.compile(
    r"\b[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\b",
    re.IGNORECASE,
)


def _extract_uuids(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _UUID_RE.finditer(text):
        out.append(
            NormalizedIdentifier(
                entity_type="UUID",
                canonical=m.group(0).lower(),
                original=m.group(0),
                span=m.span(),
            )
        )
    return out


# ── IMEI ─────────────────────────────────────────────────────────────

_IMEI_RE = re.compile(r"\b\d{15}\b")


def _luhn_ok(digits: str) -> bool:
    """Standard Luhn checksum (IMEI / credit card)."""
    s = 0
    for i, ch in enumerate(reversed(digits)):
        n = int(ch)
        if i % 2 == 1:
            n *= 2
            if n > 9:
                n -= 9
        s += n
    return s % 10 == 0


def _extract_imei(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _IMEI_RE.finditer(text):
        digits = m.group(0)
        if not _luhn_ok(digits):
            continue
        out.append(
            NormalizedIdentifier(
                entity_type="IMEI",
                canonical=digits,
                original=digits,
                span=m.span(),
            )
        )
    return out


# ── MAC address ──────────────────────────────────────────────────────

_MAC_RE = re.compile(
    r"\b(?:[0-9A-Fa-f]{2}[:\-]){5}[0-9A-Fa-f]{2}\b",
)


def _extract_mac(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _MAC_RE.finditer(text):
        raw = m.group(0)
        canonical = raw.lower().replace("-", ":")
        out.append(
            NormalizedIdentifier(
                entity_type="MACAddress",
                canonical=canonical,
                original=raw,
                span=m.span(),
            )
        )
    return out


# ── SNILS ────────────────────────────────────────────────────────────

# Formatted form only — bare 11-digit numbers are too ambiguous
# (collide with OGRN/etc.).  Acceptable: ``123-456-789 01`` or
# ``123-456-789-01``.
_SNILS_RE = re.compile(r"\b\d{3}-\d{3}-\d{3}[\s\-]\d{2}\b")


def _snils_checksum_ok(digits: str) -> bool:
    """11-digit SNILS: first 9 are the id, last 2 are the checksum.

    Sum digit[i] * (9 - i) for i in 0..8, then mod 101 with special
    cases for 100 / 101 (both → ``00``).
    """
    if len(digits) != 11:
        return False
    body, check = digits[:9], digits[9:]
    s = sum(int(d) * (9 - i) for i, d in enumerate(body))
    if s < 100:
        expected = f"{s:02d}"
    elif s in (100, 101):
        expected = "00"
    else:
        rem = s % 101
        expected = "00" if rem in (100, 101) else f"{rem:02d}"
    return check == expected


def _extract_snils(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _SNILS_RE.finditer(text):
        raw = m.group(0)
        digits = re.sub(r"\D", "", raw)
        if not _snils_checksum_ok(digits):
            continue
        out.append(
            NormalizedIdentifier(
                entity_type="SNILS",
                canonical=digits,
                original=raw,
                span=m.span(),
            )
        )
    return out


# ── Russian license plate ────────────────────────────────────────────

# Russian car plates use a 12-letter Cyrillic subset that visually
# matches Latin look-alikes.  Format: X NNN XX RR (1+3+2+2-3).  Spaces
# / non-breaking spaces are optional between letter and digit groups.
_RU_PLATE_LETTERS = "АВЕКМНОРСТУХ"
_RU_PLATE_RE = re.compile(
    rf"\b[{_RU_PLATE_LETTERS}]\d{{3}}[{_RU_PLATE_LETTERS}]{{2}}[\s ]?\d{{2,3}}\b",
)


def _extract_license_plates(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _RU_PLATE_RE.finditer(text):
        raw = m.group(0)
        # Canonical: no internal whitespace, upper-case.
        canonical = re.sub(r"[\s ]+", "", raw).upper()
        out.append(
            NormalizedIdentifier(
                entity_type="LicensePlate",
                canonical=canonical,
                original=raw,
                span=m.span(),
            )
        )
    return out


# ── VIN ──────────────────────────────────────────────────────────────

# 17 chars, no I/O/Q.  Letters + digits.
_VIN_RE = re.compile(r"\b[A-HJ-NPR-Z0-9]{17}\b")

_VIN_WEIGHTS = (8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2)
_VIN_TRANSLIT = {
    **{str(d): d for d in range(10)},
    "A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6, "G": 7, "H": 8,
    "J": 1, "K": 2, "L": 3, "M": 4, "N": 5,
    "P": 7, "R": 9,
    "S": 2, "T": 3, "U": 4, "V": 5, "W": 6, "X": 7, "Y": 8, "Z": 9,
}


def _vin_checksum_ok(vin: str) -> bool:
    if len(vin) != 17:
        return False
    vin = vin.upper()
    total = 0
    for ch, w in zip(vin, _VIN_WEIGHTS):
        v = _VIN_TRANSLIT.get(ch)
        if v is None:
            return False
        total += v * w
    rem = total % 11
    expected = "X" if rem == 10 else str(rem)
    return vin[8] == expected


def _extract_vins(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _VIN_RE.finditer(text):
        candidate = m.group(0).upper()
        if not _vin_checksum_ok(candidate):
            continue
        out.append(
            NormalizedIdentifier(
                entity_type="VIN",
                canonical=candidate,
                original=m.group(0),
                span=m.span(),
            )
        )
    return out


# ── Overlap resolution ───────────────────────────────────────────────


def _spans_overlap(a: tuple[int, int], b: tuple[int, int]) -> bool:
    return not (a[1] <= b[0] or b[1] <= a[0])


def _resolve_overlaps(
    matches: list[NormalizedIdentifier],
) -> list[NormalizedIdentifier]:
    """Drop lower-priority matches whose span overlaps an already
    accepted higher-priority match.

    Resolution order: priority desc, then span start asc, then wider
    span first (so ``https://vk.com/u`` beats the inner ``vk.com``).
    """
    ranked = sorted(
        matches,
        key=lambda m: (
            -_PRIORITY.get(m.entity_type, 0),
            m.span[0],
            -(m.span[1] - m.span[0]),
        ),
    )
    accepted: list[NormalizedIdentifier] = []
    for m in ranked:
        if any(_spans_overlap(m.span, kept.span) for kept in accepted):
            continue
        accepted.append(m)
    accepted.sort(key=lambda m: m.span)
    return accepted


# ── public aggregator ────────────────────────────────────────────────


def extract_identifiers(text: str) -> list[NormalizedIdentifier]:
    """Run every detector on ``text``; return matches sorted by span.

    Multiple occurrences of the same canonical form ARE returned (each
    with its own span). Deduplication for graph injection is the
    integration layer's responsibility (Stage C).

    Overlap policy: when two detectors match overlapping spans (e.g.
    ``URL`` and ``VKProfile`` both matching ``https://vk.com/user``),
    the higher-priority specialised type wins via
    ``_resolve_overlaps``.
    """
    if not text:
        return []
    found: list[NormalizedIdentifier] = []
    # Business / financial
    found.extend(_extract_phones(text))
    found.extend(_extract_emails(text))
    found.extend(_extract_inns(text))
    found.extend(_extract_ogrn(text))
    found.extend(_extract_bic(text))
    found.extend(_extract_snils(text))
    found.extend(_extract_contracts(text))
    found.extend(_extract_dates(text))
    found.extend(_extract_amounts(text))
    found.extend(_extract_addresses(text))
    # Digital identity
    found.extend(_extract_urls(text))
    found.extend(_extract_domains(text))
    found.extend(_extract_telegram(text))
    found.extend(_extract_vk(text))
    found.extend(_extract_uuids(text))
    # Device / hardware
    found.extend(_extract_imei(text))
    found.extend(_extract_mac(text))
    found.extend(_extract_license_plates(text))
    found.extend(_extract_vins(text))
    return _resolve_overlaps(found)


# ── Stage-C helpers: payload + augment block builders ───────────────


def dedupe_by_canonical(
    idents: list[NormalizedIdentifier],
) -> list[NormalizedIdentifier]:
    """Keep first occurrence per (entity_type, canonical) pair.

    Preserves source order so the first textual mention wins for
    ``original``/``span`` fields used in descriptions.
    """
    seen: set[tuple[str, str]] = set()
    out: list[NormalizedIdentifier] = []
    for ident in idents:
        key = (ident.entity_type, ident.canonical)
        if key in seen:
            continue
        seen.add(key)
        out.append(ident)
    return out


def build_custom_kg_payload(
    idents: list[NormalizedIdentifier],
    *,
    doc_id: str,
    file_path: str,
    text: str = "",
    snippet_window: int = 80,
) -> dict:
    """Assemble a ``rag.ainsert_custom_kg`` payload from identifier matches.

    One entity per (entity_type, canonical) — duplicates within the doc
    collapse to a single node, but ``ainsert_custom_kg`` is itself
    idempotent across documents (descriptions accumulate when the same
    canonical is inserted from multiple ``source_id``).

    ``description`` includes the verbatim original form and a small
    surrounding snippet so the node carries provenance once it lands in
    Neo4j — useful both for the LLM (when it later relates entities
    via ``ainsert(text)``) and for human auditing.
    """
    entities: list[dict] = []
    for ident in dedupe_by_canonical(idents):
        snippet = ""
        if text:
            start = max(0, ident.span[0] - snippet_window)
            end = min(len(text), ident.span[1] + snippet_window)
            snippet = text[start:end].replace("\n", " ").strip()
        if ident.original != ident.canonical:
            desc = (
                f"{ident.entity_type} извлечён из документа {doc_id}; "
                f"канонический вид: {ident.canonical}; в тексте: "
                f"«{ident.original}»."
            )
        else:
            desc = (
                f"{ident.entity_type} извлечён из документа {doc_id}; "
                f"в тексте: «{ident.original}»."
            )
        if snippet:
            desc += f" Контекст: «…{snippet}…»."
        entities.append(
            {
                "entity_name": ident.canonical,
                "entity_type": ident.entity_type,
                "description": desc,
                "source_id": doc_id,
                "file_path": file_path,
            }
        )
    return {
        "chunks": [],
        "entities": entities,
        "relationships": [],
    }


_AUGMENT_HEADER = (
    "\n\n---\n"
    "Канонические идентификаторы (используй ИМЕННО ТАКУЮ форму в "
    "entity_name):\n"
)


def build_augment_block(idents: list[NormalizedIdentifier]) -> str:
    """Format the canonical-identifiers block appended to document text.

    Produces empty string when there are no identifiers — the caller
    should skip appending altogether in that case.
    """
    deduped = dedupe_by_canonical(idents)
    if not deduped:
        return ""
    lines: list[str] = []
    for ident in deduped:
        if ident.original != ident.canonical:
            lines.append(
                f"- {ident.entity_type}: {ident.canonical} "
                f"(в тексте: «{ident.original}»)"
            )
        else:
            lines.append(f"- {ident.entity_type}: {ident.canonical}")
    return _AUGMENT_HEADER + "\n".join(lines) + "\n"


__all__ = [
    "IdentifierType",
    "NormalizedIdentifier",
    "build_augment_block",
    "build_custom_kg_payload",
    "dedupe_by_canonical",
    "extract_identifiers",
]
