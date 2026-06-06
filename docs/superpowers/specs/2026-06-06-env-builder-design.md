# Interactive `.env` Builder — Design

**Date:** 2026-06-06
**Status:** Draft (pending review)
**Goal:** An interactive script that builds a correct `.env` from `.env.example`, preserving every comment and section, written alongside in the repo root — with secret generation, cross-field validation, and safe merge of an existing `.env`.

---

## 1. Motivation

`.env.example` (244 lines) is the source of truth for every setting: section
headers (`# ── X ──`), a comment block per variable, and `KEY=VALUE` defaults.
Today setup means hand-copying it to `.env` and editing ~60 vars, several of
which are secrets (`OPENAI_API_KEY`, DB/MinIO/Wikibase/Grafana passwords,
`WIKIBASE_SECRET_KEY` "generate 32 hex", `API_KEYS`) and several of which have
cross-field constraints (`MILVUS_DIM` must equal `LITELLM_EMBEDDING_DIM`; the
`LLM_POOL` f49a83c sizing rule; Temporal caps ≥ pool lane ceilings). A guided
builder removes the foot-guns and keeps the resulting `.env` self-documenting.

---

## 2. Locked decisions (from brainstorming)

| # | Decision |
|---|----------|
| D1 | **Prompting model = section-by-section with skip.** Per `# ── X ──` section: keep-defaults / configure / quit. |
| D2 | **Extras included:** secret generation, cross-field validation, merge-with-existing-`.env`. **Excluded:** profiles/presets, connectivity preflight (YAGNI). |
| D3 | **Approach A:** self-contained stdlib-only `scripts/make_env.py`; injectable input for testability; matches `scripts/setup_db.py` convention. No new deps. |
| D4 | **Structure-preserving:** output `.env` is byte-for-byte identical to `.env.example` except for the values on `KEY=` lines. |

---

## 3. Architecture

Single module **`scripts/make_env.py`** + tests. Pure, testable units around a
thin interactive shell.

### Data model (structure preservation)

`parse_example(text) -> list[Line]` where `Line` is one of:

- `Comment(text)` — a comment line; emitted verbatim.
- `Blank()` — an empty line; emitted verbatim.
- `Section(title, raw)` — a `# ── Title ──` header (a marked Comment that also
  starts a group).
- `KV(key, example_val, comment_lines, section)` — a `KEY=VALUE` line plus the
  contiguous comment block directly above it (shown when prompting).

Commented-out optional vars (`# LITELLM_ROLE_TIERS=`, `# LLM_POOL_LANE_CAPS=`)
parse as `Comment`, NOT `KV` — they stay commented out and are not prompted.

### Components (one responsibility each)

- `parse_example(text) -> list[Line]`
- `parse_env(text) -> dict[str, str]` — simple `KEY=VALUE` parse (no
  interpolation) for the merge default-source.
- `render(lines: list[Line], values: dict[str, str]) -> str` — re-emit verbatim;
  `KV` lines become `f"{key}={values[key]}"`.
- `is_secret(key) -> bool` — name heuristic: contains `PASSWORD`, `PASS`,
  `SECRET`, `_KEY`, `API_KEY`, `API_KEYS`, `ACCESS_KEY`.
- `gen_secret(key) -> str` — see §5.
- `validate(values) -> list[Issue]` — cross-field checks (§6); `Issue(level, msg)`
  with `level ∈ {ERROR, WARN}`.
- `run_interactive(lines, values, *, input_fn, getpass_fn) -> dict[str,str]` —
  the section-skip loop (§4); input fns injected so tests drive it without a TTY.
- `main(argv)` — CLI wiring (§7).

---

## 4. Interaction flow

```
(optional) merge: if .env exists -> "take its values as defaults? [Y/n]"
for each Section in order:
    print section title + its vars with current defaults
    ask: "[Enter] keep defaults  [e] configure  [q] quit"
    if 'e':
        for each KV in section:
            show KV.comment_lines + "KEY [default=<val>]: "
            input:
              empty Enter -> keep default
              text        -> new value
              (secret only) 'g' -> gen_secret(key)
```

Default source per `KV` (priority): (1) existing `.env` value if merge enabled
and present, else (2) `example_val`. Secret inputs use `getpass_fn` (no echo);
empty Enter keeps the default (local-dev defaults like `minioadmin` are fine).

---

## 5. Secret generation

`is_secret(key)` → offer `g`:

- `WIKIBASE_SECRET_KEY` → exactly 32 hex chars (`secrets.token_hex(16)`) — the
  example comment mandates this.
- `*_PASSWORD` / `*_PASS` → `secrets.token_urlsafe(24)`; this comfortably
  exceeds the documented minimums (Wikibase admin ≥10, bot ≥8).
- `API_KEYS` / `LITELLM_API_KEY` → a `sk-` prefix + `secrets.token_urlsafe(32)`.
- other secrets → `secrets.token_urlsafe(24)`.

Generation is opt-in per field; empty Enter still keeps the default.

---

## 6. Cross-field validation

`validate(values) -> list[Issue]`, run after collection, before write:

1. **ERROR** if `MILVUS_DIM != LITELLM_EMBEDDING_DIM` (example requires equality).
2. **ERROR** if the `LLM_POOL` sizing rule breaks:
   `lane_caps["extraction"] > LLM_POOL_TIER_SMALL_TOTAL - LLM_POOL_JUDGE_FLOOR`.
   Source `lane_caps` from `LLM_POOL_LANE_CAPS` if set, else the documented
   default map (extraction 18 / judge 14 / …).
3. **WARN** if `TEMPORAL_LLM_ACTIVITY_CONCURRENCY < extraction ceiling` or
   `TEMPORAL_MERGE_ACTIVITY_CONCURRENCY < judge ceiling` (pool won't bind first).
4. **ERROR** if `OPENAI_API_KEY` is empty while `LITELLM_MODEL_SMALL` or
   `LITELLM_MODEL_LARGE` looks like an OpenAI model (`gpt-` prefix).

On any ERROR: print the list, then offer "fix (return to that field) / write
anyway (`--force`) / cancel". WARN-only: print and allow write.

---

## 7. CLI

`argparse`:

- `uv run python -m scripts.make_env` — interactive (default).
- `--example PATH` (default `.env.example`), `--out PATH` (default `.env`).
- `--non-interactive` — no prompts; copy example defaults + generate any empty
  secrets; validators still run (errors abort unless `--force`). For CI / first
  boot.
- `--force` — write despite ERROR-level validation.
- `--no-merge` — ignore an existing `.env`.

Non-TTY guard: if interactive mode and `not sys.stdin.isatty()`, exit with a
hint to use `--non-interactive`.

### Write + backup

Output written to `--out` (default `.env`, repo root, alongside `.env.example`).
If the target exists, back it up to `.env.bak` (overwriting any previous bak)
before writing.

---

## 8. Testing

`tests/test_scripts/test_make_env.py`:

1. **Round-trip:** `render(parse_example(EX), example_defaults)` equals `EX`
   (comments / sections / order identical; values unchanged).
2. **Value substitution:** an overridden value lands on the right `KEY=` line;
   surrounding comments intact.
3. **`gen_secret`:** `WIKIBASE_SECRET_KEY` is 32 hex; passwords meet min lengths;
   assert length/charset (not determinism).
4. **Validators:** table-driven — dim-mismatch / pool-rule / temporal-caps /
   openai-key → expected `Issue` levels.
5. **Merge:** existing `.env` value beats the example default; `.env.bak` created.
6. **Interactive (injected `input_fn`/`getpass_fn`):** "configure section X, Enter
   elsewhere" → correct values; a secret taken via `g` is generated.

---

## 9. Out of scope (YAGNI)

Profiles/presets; connectivity preflight; un-commenting the optional commented
vars; non-`KEY=VALUE` `.env` syntaxes (export lines, multiline values) — the
project's `.env.example` uses only simple `KEY=VALUE`.
