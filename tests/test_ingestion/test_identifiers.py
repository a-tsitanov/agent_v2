"""Unit tests for ``src/ingestion/identifiers.py``.

Coverage goals:
  * One happy-path case per identifier type.
  * Edge cases that previously broke real documents (mixed separators,
    unicode quotes, NBSP, RU month names).
  * Validation: invalid INN/OGRN checksums must NOT be returned —
    that filter is the only thing keeping random 10/13-digit numbers
    out of the graph.
  * Integration: a realistic Russian contract excerpt yields the full
    set of canonical identifiers in span order.

Tests do not require libpostal — when it isn't installed, the address
normalizer falls back to the rule layer (which these tests exercise).
"""

from __future__ import annotations

import pytest

from src.ingestion.identifiers import (
    NormalizedIdentifier,
    build_augment_block,
    build_custom_kg_payload,
    dedupe_by_canonical,
    extract_identifiers,
)


# Real-world reference values used across tests.
SBER_INN = "7707083893"           # ИНН Сбербанка (10 digits, valid checksum)
GAZPROM_INN = "7736050003"        # ИНН Газпрома (10 digits, valid checksum)
INDIV_INN = "500400123402"        # 12-digit INN (computed valid checksum)
SBER_OGRN = "1027700132195"       # ОГРН Сбербанка (13, valid)
IP_OGRN = "304500116000157"       # ОГРНИП (15, valid)
SBER_BIC = "044525225"            # БИК Сбербанк ОПЕРУ Москва


def _by_type(
    items: list[NormalizedIdentifier],
    t: str,
) -> list[NormalizedIdentifier]:
    return [x for x in items if x.entity_type == t]


# ── PhoneNumber ──────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected_canonical",
    [
        ("+7 (495) 234-56-78", "+74952345678"),
        ("+7 495 234 56 78", "+74952345678"),
        ("8 (495) 234-56-78", "+74952345678"),
        ("+7-495-234-56-78", "+74952345678"),
    ],
)
def test_phone_e164(raw: str, expected_canonical: str) -> None:
    found = _by_type(extract_identifiers(f"Контактный телефон: {raw}"), "PhoneNumber")
    assert len(found) == 1
    assert found[0].canonical == expected_canonical
    assert found[0].original.strip().startswith(("+", "8"))


def test_phone_no_match_when_no_phone_in_text() -> None:
    found = _by_type(
        extract_identifiers("В документе нет ни одного телефона."),
        "PhoneNumber",
    )
    assert found == []


# ── Email ────────────────────────────────────────────────────────────


def test_email_lowercased_canonical() -> None:
    text = "Контакт: I.Ivanov@SEVTECH.ru, копия — Bob@Example.COM"
    emails = _by_type(extract_identifiers(text), "Email")
    canonicals = sorted(e.canonical for e in emails)
    assert canonicals == ["bob@example.com", "i.ivanov@sevtech.ru"]
    # original case preserved
    assert emails[0].original == "I.Ivanov@SEVTECH.ru"


# ── INN ──────────────────────────────────────────────────────────────


def test_inn_10_valid_checksum_extracted() -> None:
    text = f"ООО «Тест» (ИНН {SBER_INN}) поставщик."
    inns = _by_type(extract_identifiers(text), "INN")
    assert len(inns) == 1
    assert inns[0].canonical == SBER_INN


def test_inn_10_invalid_checksum_rejected() -> None:
    # 7707083894 — last digit changed to break checksum
    text = "ИНН 7707083894 — неправильная контрольная сумма."
    inns = _by_type(extract_identifiers(text), "INN")
    assert inns == []


def test_inn_12_valid_extracted() -> None:
    text = f"ИП Иванов И.И., ИНН {INDIV_INN}"
    inns = _by_type(extract_identifiers(text), "INN")
    assert len(inns) == 1
    assert inns[0].canonical == INDIV_INN


def test_inn_does_not_match_inside_longer_digit_run() -> None:
    # 14-digit number — neither 10 nor 12 — must not match
    text = "Code 12345678901234 some text."
    inns = _by_type(extract_identifiers(text), "INN")
    assert inns == []


# ── OGRN ─────────────────────────────────────────────────────────────


def test_ogrn_13_valid_extracted() -> None:
    text = f"ПАО Сбербанк, ОГРН {SBER_OGRN}, действует на основании устава."
    ogrns = _by_type(extract_identifiers(text), "OGRN")
    assert len(ogrns) == 1
    assert ogrns[0].canonical == SBER_OGRN


def test_ogrn_15_valid_extracted() -> None:
    text = f"ИП Петров П.П., ОГРНИП {IP_OGRN}"
    ogrns = _by_type(extract_identifiers(text), "OGRN")
    assert len(ogrns) == 1
    assert ogrns[0].canonical == IP_OGRN


def test_ogrn_invalid_checksum_rejected() -> None:
    text = "ОГРН 1027700132190 — последняя цифра неверна."
    ogrns = _by_type(extract_identifiers(text), "OGRN")
    assert ogrns == []


# ── BIC ──────────────────────────────────────────────────────────────


def test_bic_extracted() -> None:
    text = f"Банк получателя: ПАО Сбербанк, БИК {SBER_BIC}"
    bics = _by_type(extract_identifiers(text), "BIC")
    assert len(bics) == 1
    assert bics[0].canonical == SBER_BIC


def test_bic_does_not_match_non_04_prefix() -> None:
    # Russian BICs start with 04 — 12-prefixed 9-digit must not match
    text = "Random number 123456789 — not a BIC."
    bics = _by_type(extract_identifiers(text), "BIC")
    assert bics == []


# ── ContractNumber ───────────────────────────────────────────────────


def test_contract_with_no_marker_extracted_uppercase() -> None:
    text = "Договор поставки № дп-2024/178-К от 15.03.2024."
    contracts = _by_type(extract_identifiers(text), "ContractNumber")
    assert len(contracts) == 1
    assert contracts[0].canonical == "ДП-2024/178-К"
    assert contracts[0].original == "дп-2024/178-К"


def test_contract_no_marker_no_match() -> None:
    # Without №/No prefix nothing should match the contract pattern
    text = "Some random ABC-123 token without a contract marker."
    contracts = _by_type(extract_identifiers(text), "ContractNumber")
    assert contracts == []


def test_contract_english_negation_no_false_positive() -> None:
    """Body-text 'no symptoms' / 'no warranties' must not match —
    the regex earlier ran with IGNORECASE and pulled section
    headings out of medical/legal prose as contract numbers."""
    text = (
        "The patient reports no symptoms. There are NO WARRANTIES "
        "of merchantability. No radiation has been administered."
    )
    contracts = _by_type(extract_identifiers(text), "ContractNumber")
    assert contracts == []


def test_contract_alpha_only_token_rejected() -> None:
    """`No. SYMPTOMS` should not match — captured token has no digit."""
    text = "No. SYMPTOMS noted in the consult note."
    contracts = _by_type(extract_identifiers(text), "ContractNumber")
    assert contracts == []


def test_contract_english_marker_with_digit() -> None:
    """Legit `No. 17-K` style references must still extract."""
    text = "See contract No. 17-K dated 2024-03-15."
    contracts = _by_type(extract_identifiers(text), "ContractNumber")
    assert len(contracts) == 1
    assert contracts[0].canonical == "17-K"


# ── DocumentDate ─────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected_iso",
    [
        ("15.03.2024", "2024-03-15"),
        ("15/03/2024", "2024-03-15"),
        ("15-03-2024", "2024-03-15"),
        ("2024-03-15", "2024-03-15"),
    ],
)
def test_date_numeric(raw: str, expected_iso: str) -> None:
    text = f"Дата подписания: {raw}."
    dates = _by_type(extract_identifiers(text), "DocumentDate")
    assert len(dates) == 1
    assert dates[0].canonical == expected_iso


def test_date_verbal_russian() -> None:
    text = "Заключён 15 марта 2024 года в Москве."
    dates = _by_type(extract_identifiers(text), "DocumentDate")
    assert len(dates) >= 1
    assert any(d.canonical == "2024-03-15" for d in dates)


# ── Amount ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected_canonical",
    [
        ("4 250 000,00 руб.", "4250000.00 RUB"),
        ("1500000 руб", "1500000.00 RUB"),
        ("4,25 млн руб", "4250000.00 RUB"),
        ("125 тыс. руб.", "125000.00 RUB"),
        ("99,99 ₽", "99.99 RUB"),
    ],
)
def test_amount(raw: str, expected_canonical: str) -> None:
    text = f"Сумма договора: {raw}."
    amounts = _by_type(extract_identifiers(text), "Amount")
    assert len(amounts) >= 1
    assert any(a.canonical == expected_canonical for a in amounts)


def test_amount_no_match_without_currency() -> None:
    text = "Просто число 4 250 000 без валюты."
    amounts = _by_type(extract_identifiers(text), "Amount")
    assert amounts == []


# ── PostalAddress ────────────────────────────────────────────────────


def test_postal_address_extracted_with_postcode_and_marker() -> None:
    text = (
        "Юридический адрес: 127015, г. Москва, "
        "ул. Бутырская, д. 76, стр. 1."
    )
    addrs = _by_type(extract_identifiers(text), "PostalAddress")
    assert len(addrs) == 1
    canonical = addrs[0].canonical
    # Rule layer outputs lowercased, abbreviation-expanded form
    assert "127015" in canonical
    assert "москва" in canonical.lower() or "ул бутырская" in canonical


def test_postal_code_alone_not_extracted_without_marker() -> None:
    # 6-digit number with no city/street markers nearby — not an address
    text = "Заказ № 127015 от поставщика на сумму 100 руб."
    addrs = _by_type(extract_identifiers(text), "PostalAddress")
    assert addrs == []


# ── integration ──────────────────────────────────────────────────────


def test_integration_full_contract_excerpt() -> None:
    text = (
        f"Договор поставки № ДП-2024/178-К от 15.03.2024 заключён между "
        f"ООО «Северные технологии» (ИНН {SBER_INN}, ОГРН {SBER_OGRN}, "
        f"юр. адрес: 127015, г. Москва, ул. Бутырская, д. 76, стр. 1) "
        f"и АО «Промсервис».\n"
        f"Контактное лицо: Иванов Иван Петрович, "
        f"телефон +7 (495) 234-56-78, e-mail: i.ivanov@sevtech.ru. "
        f"Сумма договора: 4 250 000,00 руб. "
        f"Банк получателя: ПАО Сбербанк, БИК {SBER_BIC}."
    )

    found = extract_identifiers(text)
    by_type: dict[str, list[NormalizedIdentifier]] = {}
    for f in found:
        by_type.setdefault(f.entity_type, []).append(f)

    canonicals = {t: [x.canonical for x in lst] for t, lst in by_type.items()}

    # Sanity: every type we emitted shows up at least once with the
    # right canonical
    assert "ДП-2024/178-К" in canonicals.get("ContractNumber", [])
    assert "2024-03-15" in canonicals.get("DocumentDate", [])
    assert SBER_INN in canonicals.get("INN", [])
    assert SBER_OGRN in canonicals.get("OGRN", [])
    assert "+74952345678" in canonicals.get("PhoneNumber", [])
    assert "i.ivanov@sevtech.ru" in canonicals.get("Email", [])
    assert "4250000.00 RUB" in canonicals.get("Amount", [])
    assert SBER_BIC in canonicals.get("BIC", [])
    assert any(
        "127015" in c for c in canonicals.get("PostalAddress", [])
    )

    # spans must be sorted (sanity for Stage C augment-block assembly)
    spans = [f.span for f in found]
    assert spans == sorted(spans)


def test_empty_text_returns_empty() -> None:
    assert extract_identifiers("") == []


def test_text_without_identifiers_returns_empty() -> None:
    text = "Это просто абзац без каких-либо структурных данных."
    assert extract_identifiers(text) == []


# ── Stage-C helpers ──────────────────────────────────────────────────


def test_dedupe_by_canonical_keeps_first_occurrence() -> None:
    a = NormalizedIdentifier("PhoneNumber", "+74952345678", "+7 495 234 56 78", (0, 18))
    b = NormalizedIdentifier("PhoneNumber", "+74952345678", "8 495 234 56 78", (50, 65))
    c = NormalizedIdentifier("Email", "x@y.ru", "x@y.ru", (70, 76))
    out = dedupe_by_canonical([a, b, c])
    assert len(out) == 2
    # first occurrence kept
    assert out[0] is a
    assert out[1] is c


def test_build_custom_kg_payload_structure() -> None:
    text = "Тел: +7 495 234 56 78, e-mail: x@y.ru."
    idents = extract_identifiers(text)
    assert idents
    payload = build_custom_kg_payload(
        idents, doc_id="doc-42", file_path="/docs/x.txt", text=text,
    )
    assert payload["chunks"] == []
    assert payload["relationships"] == []
    types = {e["entity_type"] for e in payload["entities"]}
    assert "PhoneNumber" in types
    assert "Email" in types
    for ent in payload["entities"]:
        assert ent["source_id"] == "doc-42"
        assert ent["file_path"] == "/docs/x.txt"
        assert ent["entity_name"]
        assert ent["description"]
        # description mentions doc id + an original or canonical form
        assert "doc-42" in ent["description"]


def test_build_custom_kg_payload_empty_when_no_idents() -> None:
    payload = build_custom_kg_payload([], doc_id="d", file_path="f")
    assert payload == {"chunks": [], "entities": [], "relationships": []}


def test_build_custom_kg_payload_dedupes_within_doc() -> None:
    a = NormalizedIdentifier("INN", "7707083893", "7707083893", (10, 20))
    b = NormalizedIdentifier("INN", "7707083893", "7707083893", (50, 60))  # dup
    payload = build_custom_kg_payload([a, b], doc_id="d", file_path="f")
    assert len(payload["entities"]) == 1


def test_build_augment_block_format() -> None:
    idents = [
        NormalizedIdentifier("PhoneNumber", "+74952345678", "+7 495 234 56 78", (0, 18)),
        NormalizedIdentifier("INN", "7707083893", "7707083893", (20, 30)),
    ]
    block = build_augment_block(idents)
    assert "Канонические идентификаторы" in block
    assert "+74952345678" in block
    assert "7707083893" in block
    # original differs from canonical → annotation present
    assert "в тексте: «+7 495 234 56 78»" in block
    # original equals canonical → no annotation noise
    assert "(в тексте: «7707083893»)" not in block


def test_build_augment_block_empty_input() -> None:
    assert build_augment_block([]) == ""


# ── URL / Domain ─────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,canonical",
    [
        ("https://example.com/", "https://example.com"),
        ("HTTPS://EXAMPLE.COM/Path?q=1", "https://example.com/Path?q=1"),
        ("http://foo.bar.dev/abc.", "http://foo.bar.dev/abc"),
    ],
)
def test_url_canonicalises_scheme_host_and_trailing_slash(raw, canonical) -> None:
    found = _by_type(extract_identifiers(f"Ссылка: {raw}"), "URL")
    assert len(found) == 1
    assert found[0].canonical == canonical


def test_bare_domain_picked_when_no_protocol() -> None:
    found = _by_type(
        extract_identifiers("Сайт example.com и Resource example.invalid."),
        "Domain",
    )
    # Only `example.com` — `.invalid` isn't in our TLD allow-list.
    assert [x.canonical for x in found] == ["example.com"]


def test_url_supersedes_inner_domain() -> None:
    out = extract_identifiers("Открой https://example.com/x")
    # Domain regex matches `example.com` inside the URL; overlap
    # resolver must drop it.
    assert all(x.entity_type != "Domain" for x in out)


# ── Social handles ───────────────────────────────────────────────────


def test_telegram_at_handle() -> None:
    found = _by_type(extract_identifiers("Контакт @ivan_dev для связи"), "TelegramHandle")
    assert len(found) == 1
    assert found[0].canonical == "@ivan_dev"


def test_telegram_t_me_link_canonicalises_to_at() -> None:
    found = _by_type(extract_identifiers("Telegram: t.me/Anna_PM"), "TelegramHandle")
    assert len(found) == 1
    assert found[0].canonical == "@anna_pm"


def test_email_local_does_not_match_telegram_handle() -> None:
    # `@example.com` inside `user@example.com` should NOT be a
    # Telegram handle — Email's wider span (priority 100) wins.
    out = extract_identifiers("Письмо ivan@example.com")
    assert any(x.entity_type == "Email" for x in out)
    assert all(x.entity_type != "TelegramHandle" for x in out)


def test_vk_profile_url_and_short() -> None:
    out = extract_identifiers("Профили: vk.com/anna_pm и https://m.vk.com/id12345")
    vks = _by_type(out, "VKProfile")
    assert sorted(x.canonical for x in vks) == [
        "vk.com/anna_pm", "vk.com/id12345",
    ]
    # URL detector should NOT also produce a match for the
    # `https://m.vk.com/id12345` — VKProfile has higher priority.
    assert all(x.entity_type != "URL" for x in out)


# ── Twitter / X ──────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,canonical",
    [
        ("https://twitter.com/elonmusk", "@elonmusk"),
        ("https://x.com/jack", "@jack"),
        ("twitter.com/Anna_PM", "@anna_pm"),
        ("Anna в твиттере @anna_dev", "@anna_dev"),
        ("Twitter: @jdoe", "@jdoe"),
    ],
)
def test_twitter_handle(raw, canonical) -> None:
    found = _by_type(extract_identifiers(raw), "TwitterHandle")
    assert len(found) == 1
    assert found[0].canonical == canonical


def test_twitter_bare_at_without_context_falls_to_telegram() -> None:
    out = extract_identifiers("Бот @bare_at без контекста")
    assert all(x.entity_type != "TwitterHandle" for x in out)
    assert any(
        x.entity_type == "TelegramHandle" and x.canonical == "@bare_at"
        for x in out
    )


# ── Instagram ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,canonical",
    [
        ("https://instagram.com/anna_pm/", "@anna_pm"),
        ("instagram.com/john.doe", "@john.doe"),
        ("инсте @photo_anna", "@photo_anna"),
        ("Insta: @official_ig", "@official_ig"),
    ],
)
def test_instagram_handle(raw, canonical) -> None:
    found = _by_type(extract_identifiers(raw), "InstagramHandle")
    assert len(found) == 1
    assert found[0].canonical == canonical


# ── LinkedIn ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,canonical",
    [
        ("https://www.linkedin.com/in/john-doe-12345/",
         "linkedin.com/in/john-doe-12345"),
        ("linkedin.com/company/acme", "linkedin.com/company/acme"),
        ("https://ru.linkedin.com/in/Ivan-Petrov", "linkedin.com/in/ivan-petrov"),
    ],
)
def test_linkedin_profile(raw, canonical) -> None:
    found = _by_type(extract_identifiers(raw), "LinkedInProfile")
    assert len(found) == 1
    assert found[0].canonical == canonical


# ── YouTube ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,canonical",
    [
        ("youtube.com/@mkbhd", "youtube.com/@mkbhd"),
        ("https://www.youtube.com/channel/UCBJycsmduvYEL83R_U4JriQ",
         "youtube.com/channel/UCBJycsmduvYEL83R_U4JriQ"),
        ("youtube.com/c/google", "youtube.com/c/google"),
        ("youtube.com/user/legacy_name", "youtube.com/user/legacy_name"),
    ],
)
def test_youtube_channel(raw, canonical) -> None:
    found = _by_type(extract_identifiers(raw), "YouTubeChannel")
    assert len(found) == 1
    assert found[0].canonical == canonical


# ── GitHub ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,canonical",
    [
        ("github.com/octocat", "github.com/octocat"),
        ("https://github.com/anthropics/claude-code",
         "github.com/anthropics/claude-code"),
        ("https://www.github.com/torvalds/linux",
         "github.com/torvalds/linux"),
    ],
)
def test_github_profile(raw, canonical) -> None:
    found = _by_type(extract_identifiers(raw), "GitHubProfile")
    assert len(found) == 1
    assert found[0].canonical == canonical


def test_github_reserved_path_not_extracted() -> None:
    # `github.com/marketplace` / `/topics` / `/settings` etc. are
    # site sections, not user profiles.
    out = extract_identifiers("Смотри github.com/marketplace/category/ai")
    assert all(x.entity_type != "GitHubProfile" for x in out)


# ── UUID ─────────────────────────────────────────────────────────────


def test_uuid_canonicalises_to_lowercase() -> None:
    found = _by_type(
        extract_identifiers("ID: 550E8400-E29B-41D4-A716-446655440000"),
        "UUID",
    )
    assert len(found) == 1
    assert found[0].canonical == "550e8400-e29b-41d4-a716-446655440000"


# ── IMEI ─────────────────────────────────────────────────────────────


def test_imei_valid_luhn_extracted() -> None:
    # 356938035643809 has a valid Luhn checksum (sample from spec).
    found = _by_type(extract_identifiers("IMEI 356938035643809"), "IMEI")
    assert [x.canonical for x in found] == ["356938035643809"]


def test_imei_invalid_luhn_rejected() -> None:
    # Last digit flipped → fails Luhn → must NOT be returned.
    found = _by_type(extract_identifiers("Нет IMEI 356938035643800"), "IMEI")
    assert found == []


# ── MAC address ──────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,canonical",
    [
        ("00:1A:2B:3C:4D:5E", "00:1a:2b:3c:4d:5e"),
        ("aa-bb-cc-dd-ee-ff", "aa:bb:cc:dd:ee:ff"),
    ],
)
def test_mac_address_canonicalises(raw, canonical) -> None:
    found = _by_type(extract_identifiers(f"MAC: {raw}"), "MACAddress")
    assert len(found) == 1
    assert found[0].canonical == canonical


# ── SNILS ────────────────────────────────────────────────────────────


def test_snils_valid_checksum_extracted() -> None:
    # 112-233-445 95 has the documented valid SNILS checksum.
    found = _by_type(extract_identifiers("СНИЛС: 112-233-445 95"), "SNILS")
    assert [x.canonical for x in found] == ["11223344595"]


def test_snils_bare_11_digit_not_extracted() -> None:
    # No dashes → ambiguous (OGRN territory) → SNILS detector skips it.
    found = _by_type(extract_identifiers("ОГРН 11223344595"), "SNILS")
    assert found == []


def test_snils_invalid_checksum_rejected() -> None:
    found = _by_type(extract_identifiers("СНИЛС: 112-233-445 00"), "SNILS")
    assert found == []


# ── Russian license plate ────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,canonical",
    [
        ("А123ВЕ77", "А123ВЕ77"),
        ("М001ММ 199", "М001ММ199"),
    ],
)
def test_license_plate(raw, canonical) -> None:
    found = _by_type(extract_identifiers(f"Номер {raw}"), "LicensePlate")
    assert len(found) == 1
    assert found[0].canonical == canonical


def test_license_plate_latin_lookalike_rejected() -> None:
    # Same shape but with Latin letters — RU pattern won't match,
    # and there's no plate-context keyword to trigger the generic
    # detector either.  Must NOT be returned as a plate.
    found = _by_type(extract_identifiers("A123BE77"), "LicensePlate")
    assert found == []


# ── Generic (non-RU) license plates: context-anchored ────────────────


@pytest.mark.parametrize(
    "context_phrase,raw,canonical",
    [
        ("license plate", "ABC1234", "ABC1234"),
        ("plate number", "ABC-1234", "ABC1234"),
        ("registration plate", "AB12 CDE", "AB12CDE"),
        ("vehicle reg. number", "B-MK 1234", "BMK1234"),
        ("car plate", "1ABC234", "1ABC234"),
        ("гос. номер", "AB12CDE", "AB12CDE"),
        ("номер автомобиля", "AB12CDE", "AB12CDE"),
    ],
)
def test_generic_license_plate_with_context(
    context_phrase, raw, canonical,
) -> None:
    found = _by_type(
        extract_identifiers(f"{context_phrase}: {raw}"),
        "LicensePlate",
    )
    assert len(found) == 1
    assert found[0].canonical == canonical


def test_generic_plate_without_context_is_not_extracted() -> None:
    # No keyword nearby — must NOT be picked up as a plate, even
    # though the shape itself matches.
    found = _by_type(
        extract_identifiers("Order ABC1234 paid in full."),
        "LicensePlate",
    )
    assert found == []


def test_generic_plate_alongside_ru_plate_in_same_text() -> None:
    text = (
        "RU side: гос. номер А123ВЕ77. "
        "US side: license plate ABC1234."
    )
    canonicals = sorted(
        x.canonical for x in _by_type(extract_identifiers(text), "LicensePlate")
    )
    assert canonicals == ["ABC1234", "А123ВЕ77"]


# ── VIN ──────────────────────────────────────────────────────────────


def test_vin_valid_checksum_extracted() -> None:
    # `1M8GDM9AXKP042788` — classic VIN with valid mod-11 checksum.
    found = _by_type(extract_identifiers("VIN: 1M8GDM9AXKP042788"), "VIN")
    assert [x.canonical for x in found] == ["1M8GDM9AXKP042788"]


def test_vin_invalid_checksum_rejected() -> None:
    # First digit flipped 1→2 (weight 8 at pos 0) shifts the mod-11
    # sum away from the expected ``X`` at position 8.
    found = _by_type(extract_identifiers("VIN: 2M8GDM9AXKP042788"), "VIN")
    assert found == []


# ── Integration: a single corpus produces every new type ─────────────


def test_kitchen_sink_extracts_all_new_types() -> None:
    text = (
        "Подпись: ivan@example.com, t.me/ivan_dev. "
        "Сайт https://shop.example.com/cart. "
        "Резервный домен example.ru. "
        "Профиль ВК vk.com/anna_pm. "
        "Телефон в IoT-шлюзе IMEI 356938035643809, MAC 00:1A:2B:3C:4D:5E. "
        "Автомобиль А123ВЕ77 VIN 1M8GDM9AXKP042788. "
        "СНИЛС 112-233-445 95. "
        "ID: 550e8400-e29b-41d4-a716-446655440000."
    )
    out = extract_identifiers(text)
    types = {x.entity_type for x in out}
    assert {
        "Email", "TelegramHandle", "URL", "Domain", "VKProfile",
        "IMEI", "MACAddress", "LicensePlate", "VIN", "SNILS", "UUID",
    } <= types
