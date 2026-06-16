# Deploy redesign — Batch 2 (single source of truth) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

> **Project git gate:** commits allowed (allow-rule set); do NOT `git push`.

**Goal:** Make `config.py` the single source of truth for env: introspect it to generate an exhaustive `.env.reference`, fail CI on drift, and expose prod-correct defaults.

**Architecture:** A new introspection helper enumerates every `BaseSettings` field → env var. `make_env --reference` generates `.env.reference` from it; `make_env --check` regenerates in-memory and diffs the committed file (exit 1 on drift) + lists config vars missing from `.env.example`. Prod-wrong defaults get exposed in `.env.prod.example` + the compose anchor.

**Tech Stack:** Python, pydantic-settings v2 introspection (`model_fields`, `model_config`).

Spec: `docs/superpowers/specs/2026-06-16-deploy-env-redesign-design.md` (Components 1-full, 6).

---

## File Structure
- `scripts/make_env.py` — add `iter_app_env_vars()`, `build_reference()`, `--reference`/`--check` modes.
- `.env.reference` — NEW generated artifact (committed; the exhaustive env catalog).
- `.env.prod.example`, `docker-compose.prod.yml` — prod-correct defaults (Component 6).
- Tests: `tests/test_scripts/test_make_env.py`.

---

## Task 1: Introspect config.py → env var catalog

**Files:** Modify `scripts/make_env.py`; Test `tests/test_scripts/test_make_env.py`.

- [ ] **Step 1: Write failing test** — add to `tests/test_scripts/test_make_env.py`:

```python
from scripts.make_env import iter_app_env_vars


def test_iter_app_env_vars_covers_known_fields():
    rows = iter_app_env_vars()
    envs = {r.env for r in rows}
    # representative fields across several settings classes
    assert "MILVUS_DIM" in envs
    assert "LLM_POOL_N" in envs
    assert "TEMPORAL_LLM_ACTIVITY_CONCURRENCY" in envs
    assert "WIKI_ENABLED" in envs
    assert "HF_OFFLINE" in envs          # validation_alias path (no prefix)
    # secrets are flagged + emitted with an empty default (never a real secret)
    by = {r.env: r for r in rows}
    assert by["NEO4J_PASSWORD"].secret is True
    assert by["NEO4J_PASSWORD"].default == ""
    # removed dead fields are absent
    assert "AGENT_TOP_K" not in envs
    assert "API_HOST" not in envs
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_scripts/test_make_env.py::test_iter_app_env_vars_covers_known_fields -q`
Expected: FAIL (`iter_app_env_vars` undefined).

- [ ] **Step 3: Implement `iter_app_env_vars()` in `scripts/make_env.py`**

Add near the top (after imports). It introspects every `BaseSettings` subclass defined in `src.config`:

```python
@dataclass
class EnvVar:
    env: str
    default: str       # rendered default ("" for secrets / None / undefined)
    secret: bool
    group: str         # settings class name (for grouping)


def _render_default(value) -> str:
    """Render a field default as an env string (JSON for list/dict)."""
    import json
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def iter_app_env_vars() -> list[EnvVar]:
    """Enumerate every env var the app reads, from config.py settings classes.

    The authoritative catalog: env_prefix + UPPER(field), or the field's
    explicit ``validation_alias`` (e.g. HFSettings).  Secrets are flagged and
    emitted with an empty default (never a real secret value).
    """
    import importlib
    import inspect
    from pydantic import SecretStr
    from pydantic_settings import BaseSettings
    from pydantic_core import PydanticUndefined

    cfg = importlib.import_module("src.config")
    rows: list[EnvVar] = []
    seen: set[str] = set()
    for name, cls in vars(cfg).items():
        if not (inspect.isclass(cls) and issubclass(cls, BaseSettings)):
            continue
        if cls is BaseSettings or name == "Settings":
            continue
        prefix = cls.model_config.get("env_prefix", "") or ""
        for fname, fld in cls.model_fields.items():
            alias = getattr(fld, "validation_alias", None)
            env = (alias if isinstance(alias, str) else (prefix + fname)).upper()
            if env in seen:
                continue
            seen.add(env)
            # resolve default (default_factory when default is undefined)
            if fld.default is not PydanticUndefined and fld.default is not None:
                raw = fld.default
            elif fld.default_factory is not None:  # type: ignore[truthy-function]
                raw = fld.default_factory()
            else:
                raw = None
            is_sec = isinstance(raw, SecretStr) or is_secret(env)
            default = "" if isinstance(raw, SecretStr) else _render_default(raw)
            if is_sec:
                default = ""  # never emit a secret default
            rows.append(EnvVar(env=env, default=default, secret=is_sec, group=name))
    rows.sort(key=lambda r: (r.group, r.env))
    return rows
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_scripts/test_make_env.py::test_iter_app_env_vars_covers_known_fields -q`
Expected: PASS. Also smoke it: `.venv/bin/python -c "from scripts.make_env import iter_app_env_vars; rs=iter_app_env_vars(); print(len(rs),'env vars'); [print(r.group, r.env, repr(r.default), 'SECRET' if r.secret else '') for r in rs[:5]]"`

- [ ] **Step 5: Commit**

```bash
git add scripts/make_env.py tests/test_scripts/test_make_env.py
git commit -m "feat(make_env): introspect config.py into an env var catalog"
```

---

## Task 2: `--reference` generation + `--check` drift guard

**Files:** Modify `scripts/make_env.py`; Test `tests/test_scripts/test_make_env.py`.

- [ ] **Step 1: Write failing tests** — add:

```python
from scripts.make_env import build_reference


def test_build_reference_groups_and_marks_secrets():
    text = build_reference()
    assert "# Generated from src/config.py" in text
    assert "MILVUS_DIM=1536" in text
    # secret lines emitted with empty value + a marker comment
    assert "NEO4J_PASSWORD=" in text
    assert "# secret" in text.lower()
    # grouped by settings class (a section header per class)
    assert "MilvusSettings" in text


def test_reference_is_deterministic():
    assert build_reference() == build_reference()
```

- [ ] **Step 2: Run to verify fail**

Run: `.venv/bin/python -m pytest tests/test_scripts/test_make_env.py -k "build_reference or reference_is_det" -q`
Expected: FAIL (`build_reference` undefined).

- [ ] **Step 3: Implement `build_reference()` + wire `--reference`/`--check` in `make_env.py`**

```python
_REFERENCE_HEADER = (
    "# Generated from src/config.py by `python -m scripts.make_env --reference`.\n"
    "# DO NOT EDIT BY HAND. Exhaustive catalog of every app env var.\n"
    "# Secrets show an empty value (set them yourself).\n"
)


def build_reference() -> str:
    """Render the exhaustive .env.reference from the config.py catalog."""
    rows = iter_app_env_vars()
    out = [_REFERENCE_HEADER]
    group = None
    for r in rows:
        if r.group != group:
            group = r.group
            out.append(f"\n# ── {group} ──")
        suffix = "   # secret" if r.secret else ""
        out.append(f"{r.env}={r.default}{suffix}")
    return "\n".join(out) + "\n"
```

Add to `main()`'s argparse + dispatch (BEFORE the example-parsing flow, since these modes don't need `.env.example`):
```python
    p.add_argument("--reference", action="store_true",
                   help="(re)generate .env.reference from config.py and exit")
    p.add_argument("--check", action="store_true",
                   help="verify .env.reference is current + .env.example coverage; exit 1 on drift")
```
Then near the top of `main()` (after `args = p.parse_args(argv)`):
```python
    ref_path = Path(".env.reference")
    if args.reference:
        ref_path.write_text(build_reference())
        print(f"wrote {ref_path}")
        return 0
    if args.check:
        problems = []
        current = build_reference()
        on_disk = ref_path.read_text() if ref_path.exists() else ""
        if current != on_disk:
            problems.append(".env.reference is stale — run `make_env --reference`")
        # coverage: every config env var should appear in .env.example
        example_keys = {ln.key for ln in parse_example(Path(args.example).read_text())
                        if isinstance(ln, KV)}
        catalog = {r.env for r in iter_app_env_vars()}
        missing = sorted(catalog - example_keys)
        if missing:
            problems.append(".env.example missing app vars: " + ", ".join(missing))
        for pr in problems:
            print(f"  [DRIFT] {pr}")
        if problems:
            return 1
        print("env check: OK")
        return 0
```

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest tests/test_scripts/test_make_env.py -k "reference" -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/make_env.py tests/test_scripts/test_make_env.py
git commit -m "feat(make_env): --reference generator + --check drift/coverage guard"
```

---

## Task 3: Generate + commit `.env.reference`

**Files:** Create `.env.reference`.

- [ ] **Step 1: Generate**

Run: `.venv/bin/python -m scripts.make_env --reference && head -20 .env.reference && wc -l .env.reference`
Expected: writes `.env.reference`; head shows the header + first grouped section.

- [ ] **Step 2: Verify --check passes for the reference** (coverage may still report .env.example gaps — that's expected/informational for now; the reference-current check must pass)

Run: `.venv/bin/python -m scripts.make_env --check; echo "exit=$?"`
If it exits 1 ONLY due to `.env.example missing app vars`, that's acceptable for this batch (documenting all ~40 in .env.example is out of scope) — adjust the check so the **coverage gap is a WARNING (printed) but only the stale-reference condition fails (exit 1)**. Edit `--check`: keep `problems` for the stale-reference case (exit 1), print coverage `missing` as `[INFO]` lines without failing. Re-run until `--check` exits 0 with the freshly generated reference.

- [ ] **Step 3: Commit**

```bash
git add .env.reference scripts/make_env.py
git commit -m "docs(env): add generated .env.reference (exhaustive env catalog); check fails only on stale reference"
```

---

## Task 4: Expose prod-correct defaults (Component 6)

**Files:** Modify `.env.prod.example`, `docker-compose.prod.yml` (the `x-app-env` anchor).

- [ ] **Step 1: Add to the `x-app-env` anchor in `docker-compose.prod.yml`** (under the API block):
```yaml
  API_ENV: ${API_ENV:-production}
  API_LOG_JSON: ${API_LOG_JSON:-true}
  API_CORS_ORIGINS: ${API_CORS_ORIGINS:?set the allowed origin(s) for prod, e.g. https://app.example.com}
  MINIO_SECURE: ${MINIO_SECURE:-false}
```
(`API_CORS_ORIGINS` uses `:?` so prod fails fast rather than shipping `*`. If that's too strict for the team, use a concrete default instead — note the choice.)

- [ ] **Step 2: Add the wikibase-mysql root password + grafana admin user to the anchor/services**

In `docker-compose.prod.yml`: the `wikibase-mysql` service already reads `${WIKIBASE_DB_ROOT_PASSWORD:-rootpass}` — confirm it's there (it is). Ensure `grafana` service passes `GF_SECURITY_ADMIN_USER: ${GRAFANA_ADMIN_USER:-admin}` (add if missing). These are service env (not app anchor).

- [ ] **Step 3: Document them in `.env.prod.example`** — add a "Prod hardening" block:
```
# ── Prod hardening (override the unsafe dev defaults) ──
API_ENV=production
API_LOG_JSON=true
API_CORS_ORIGINS=https://change-me.example.com
MINIO_SECURE=false
WIKIBASE_DB_ROOT_PASSWORD=change-me-wb-root
GRAFANA_ADMIN_USER=admin
```

- [ ] **Step 4: Validate prod compose**

Run: `API_CORS_ORIGINS=https://x LITELLM_BASE_URL=x docker compose -f docker-compose.prod.yml config >/dev/null && echo prod-ok`
Expected: `prod-ok`. (If `API_CORS_ORIGINS` `:?` makes it fail without the var, that proves the fail-fast works — confirm it passes WITH the var set.)

- [ ] **Step 5: Commit**

```bash
git add docker-compose.prod.yml .env.prod.example
git commit -m "feat(deploy): expose prod-correct defaults (API_ENV/CORS/LOG_JSON/MINIO_SECURE/grafana/wb-root)"
```

---

## Task 5: Verification sweep

- [ ] **Step 1: Tests + ruff**

Run: `.venv/bin/python -m pytest tests/test_scripts/test_make_env.py -q && ruff check --select F scripts/make_env.py`
Expected: all pass; ruff clean.

- [ ] **Step 2: Round-trip drift guard works**

Run: `.venv/bin/python -m scripts.make_env --check; echo exit=$?` → exit 0 (reference current).
Then temporarily add a throwaway field to a settings class in config.py, run `--check` → exit 1 ("stale"); revert.

- [ ] **Step 3: Final commit** (if anything left)

```bash
git add -A && git commit -m "chore: deploy batch 2 cleanup"
```

---

## Self-Review notes (author)
- **Spec coverage:** Component 1-full (config.py source of truth → catalog + reference + drift check) → Tasks 1-3; Component 6 (prod defaults) → Task 4. Generation of `.env.example` itself is intentionally NOT done (keeps the curated commented template; `.env.reference` is the exhaustive machine doc; `--check` guards drift).
- **Scope note:** full `.env.example` coverage enforcement (adding all ~40 undocumented vars) is deferred — `--check` reports them as INFO, fails only on a stale `.env.reference`. Tighten to hard-fail in a follow-up if CI gating is wanted.
- **Type consistency:** `EnvVar(env, default, secret, group)` used in `iter_app_env_vars`, `build_reference`, and tests. `--reference`/`--check` flags consistent.
- **Forward-decl:** REDIS_*/LLM_CACHE_ENABLED have no config field, so they won't appear in `.env.reference` (correct — they're not app config yet); they remain only in the prod anchor as documented forward-decls.
