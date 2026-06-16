# Deploy redesign — Batch 1 (correctness) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Project git gate:** commits are allowed (allow-rule for `git commit` is set); do NOT `git push`. Each "Commit" step may run.

**Goal:** Make a fresh deploy correct out of the box: one canonical embedding profile (1536/OpenAI), prod auto-runs `setup_db`, a fail-fast preflight at boot, and `setup_wikibase` that doesn't need the host docker socket.

**Architecture:** config.py becomes the canonical 1536 profile; a compose `init` one-shot service runs `setup_db` before api/worker start; `Settings.preflight()` runs at API/worker startup; wikibase bot creation moves to a compose one-shot service (no `docker exec` from the app).

**Tech Stack:** Python, pydantic-settings, Docker Compose v2, Temporal, FastAPI.

Spec: `docs/superpowers/specs/2026-06-16-deploy-env-redesign-design.md`

---

## File Structure
- `src/config.py` — canonical dim/model defaults; new `Settings.preflight()`.
- `src/api/main.py` — call preflight in lifespan.
- `src/workflow/worker.py` — call preflight at worker start.
- `scripts/setup_wikibase.py` — drop the host `docker exec` bot step (schema-only + clear message).
- `docker-compose.yml`, `docker-compose.prod.yml` — `init` one-shot (setup_db) + `wiki-bootstrap` one-shot (bot); `depends_on` wiring.
- `.env.example`, `.env.prod.example` — align to 1536/OpenAI canon + commented Ollama block.
- Tests: `tests/test_config/test_settings.py`, `tests/test_config/test_preflight.py` (new), `tests/test_scripts/test_setup_wikibase.py` (new/adjust).

---

## Task 1: Lock the canonical embedding profile (1536 / OpenAI)

**Files:**
- Modify: `src/config.py:82` (`dim`), `src/config.py:167` (`embedding_model`)
- Modify: `.env.example`, `.env.prod.example`
- Test: `tests/test_config/test_settings.py`

- [ ] **Step 1: Write the failing test** — add to `tests/test_config/test_settings.py`:

```python
def test_canonical_embedding_profile_defaults():
    """Canonical profile is OpenAI text-embedding-3-small / 1536 (spec B)."""
    from src.config import MilvusSettings, LiteLLMSettings
    assert MilvusSettings().dim == 1536
    assert LiteLLMSettings().embedding_model == "text-embedding-3-small"
```

- [ ] **Step 2: Run to verify it fails**

Run: `mv .env .env.bak 2>/dev/null; pytest tests/test_config/test_settings.py::test_canonical_embedding_profile_defaults -q; mv .env.bak .env 2>/dev/null`
Expected: FAIL (dim is 768, model is nomic-embed-text). (The `.env` is moved aside so local overrides don't mask defaults; always restored.)

- [ ] **Step 3: Implement — `src/config.py`**

Line 82: `    dim: int = 768` → `    dim: int = 1536`
Line 167: `    embedding_model: str = "nomic-embed-text"` → `    embedding_model: str = "text-embedding-3-small"`

- [ ] **Step 4: Run to verify it passes**

Run: `mv .env .env.bak 2>/dev/null; pytest tests/test_config/test_settings.py -q; mv .env.bak .env 2>/dev/null`
Expected: PASS (all config defaults tests green on defaults).

- [ ] **Step 5: Align templates to the canon**

In `.env.example`: set `LITELLM_MODEL_SMALL=gpt-4o-mini`, `LITELLM_MODEL_LARGE=gpt-4o-mini`, `LITELLM_EMBEDDING_MODEL=text-embedding-3-small`, `MILVUS_DIM=1536`. Immediately below them add a commented opt-in block:
```
# --- Local Ollama profile (opt-in; uncomment to run without OpenAI) ---
# LITELLM_MODEL_SMALL=gemma4:e4b
# LITELLM_MODEL_LARGE=gemma4:e4b
# LITELLM_EMBEDDING_MODEL=nomic-embed-text
# MILVUS_DIM=768
```
In `.env.prod.example`: confirm `MILVUS_DIM=1536` and the model lines already match the canon (they do after the cleanup) — add the same commented Ollama block for completeness.

Verify: `grep -nE "MILVUS_DIM|EMBEDDING_MODEL|MODEL_SMALL" .env.example .env.prod.example`

- [ ] **Step 6: Commit**

```bash
git add src/config.py .env.example .env.prod.example tests/test_config/test_settings.py
git commit -m "feat(config): canonical 1536/text-embedding-3-small profile; Ollama as opt-in"
```

---

## Task 2: Fail-fast preflight at boot

**Files:**
- Modify: `src/config.py` (add `Settings.preflight`)
- Modify: `src/api/main.py:42-43`, `src/workflow/worker.py:200-201`
- Test: `tests/test_config/test_preflight.py` (new)

- [ ] **Step 1: Write the failing test** — create `tests/test_config/test_preflight.py`:

```python
"""Boot-time preflight: actionable problems instead of mid-request stack traces."""
from __future__ import annotations

from unittest.mock import MagicMock

from src.config import Settings


def _settings(**over):
    s = MagicMock(spec=Settings)
    # minimal shape the preflight reads
    s.api = MagicMock(env=over.get("env", "production"), keys=over.get("keys", "real-key-123"))
    s.neo4j = MagicMock(password=MagicMock(get_secret_value=lambda: over.get("neo4j", "realpass")))
    s.postgres = MagicMock(password=MagicMock(get_secret_value=lambda: over.get("pg", "realpass")))
    s.minio = MagicMock(access_key=MagicMock(get_secret_value=lambda: over.get("minio", "realkey")))
    s.llm_pool = MagicMock(n=over.get("n", 8))
    s.temporal = MagicMock(
        llm_activity_concurrency=over.get("llm_cap", 18),
        merge_activity_concurrency=over.get("merge_cap", 14),
    )
    s.wikibase = MagicMock(enabled=over.get("wb", False))
    s.wiki = MagicMock(enabled=over.get("wiki", False))
    return s


def test_preflight_clean_prod_has_no_problems():
    assert Settings.preflight(_settings()) == []


def test_preflight_flags_placeholder_secret_in_prod():
    problems = Settings.preflight(_settings(keys="dev-local-key"))
    assert any("API_KEYS" in p for p in problems)


def test_preflight_flags_temporal_cap_below_pool_n():
    problems = Settings.preflight(_settings(n=20, llm_cap=18))
    assert any("LLM_POOL_N" in p for p in problems)


def test_preflight_dev_allows_placeholders():
    # dev env: placeholder secrets are not hard problems
    assert Settings.preflight(_settings(env="development", keys="dev-local-key")) == []
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_config/test_preflight.py -q`
Expected: FAIL (`Settings.preflight` undefined).

- [ ] **Step 3: Implement — add `preflight` to `Settings` in `src/config.py`**

Add as a `@staticmethod` on the `Settings` class (so it can take any settings-shaped object; production code calls `settings.preflight()` via an instance — define it to accept `self`-or-passed). Use this exact method:

```python
    _PLACEHOLDER_SECRETS = {
        "dev-local-key", "changeme", "change-me", "postgres", "minioadmin",
        "changemebot", "botpass", "sk-litellm-stub",
    }

    @staticmethod
    def preflight(s: "Settings") -> list[str]:
        """Return a list of actionable config problems (empty == OK).

        Hard problems only matter in production (``API_ENV=production``);
        in dev they are advisory.  Callers decide whether to exit.
        """
        problems: list[str] = []
        prod = s.api.env == "production"

        if prod:
            checks = {
                "API_KEYS": s.api.keys,
                "NEO4J_PASSWORD": s.neo4j.password.get_secret_value(),
                "POSTGRES_PASSWORD": s.postgres.password.get_secret_value(),
                "MINIO_ACCESS_KEY": s.minio.access_key.get_secret_value(),
            }
            for name, val in checks.items():
                if val in Settings._PLACEHOLDER_SECRETS:
                    problems.append(
                        f"{name} is a placeholder default ({val!r}); set a real "
                        f"secret in production.")

        n = s.llm_pool.n
        if s.temporal.llm_activity_concurrency < n:
            problems.append(
                f"TEMPORAL_LLM_ACTIVITY_CONCURRENCY "
                f"({s.temporal.llm_activity_concurrency}) < LLM_POOL_N ({n}); "
                f"the Temporal cap must be >= N so the pool is the throttle.")
        if s.temporal.merge_activity_concurrency < n:
            problems.append(
                f"TEMPORAL_MERGE_ACTIVITY_CONCURRENCY "
                f"({s.temporal.merge_activity_concurrency}) < LLM_POOL_N ({n}).")

        if s.wiki.enabled or s.wikibase.enabled:
            bot_pw = s.wikibase.bot_password.get_secret_value()
            if len(bot_pw) < 8:
                problems.append(
                    "WIKIBASE_BOT_PASSWORD must be >= 8 chars when wiki/wikibase "
                    "is enabled (setup_wikibase refuses to provision the bot).")
        return problems
```
(Note: the wiki check reads `s.wikibase.bot_password`; the test's wiki/wb default False so it's not exercised there — that's fine.)

- [ ] **Step 4: Run to verify it passes**

Run: `pytest tests/test_config/test_preflight.py -q`
Expected: PASS (4 tests).

- [ ] **Step 5: Wire into API startup — `src/api/main.py`**

In `lifespan` (after the litellm validator call at line 43), add:
```python
    from src.config import settings as _settings
    problems = _settings.preflight(_settings)
    if problems:
        msg = "preflight found config problems:\n  - " + "\n  - ".join(problems)
        if _settings.api.env == "production":
            raise RuntimeError(msg)
        logger.warning(msg)
```
(`logger` is already imported in main.py; if not, import loguru `from loguru import logger`.)

- [ ] **Step 6: Wire into worker startup — `src/workflow/worker.py`**

In `_run_one` (after the litellm validator at line 201), add the same block but only for the first group to avoid N duplicate logs — gate on `group == "main"`:
```python
    if group == "main":
        from src.config import settings as _settings
        problems = _settings.preflight(_settings)
        if problems:
            msg = "preflight: " + "; ".join(problems)
            if _settings.api.env == "production":
                raise RuntimeError(msg)
            logger.warning(msg)
```

- [ ] **Step 7: Run + import sanity**

Run: `pytest tests/test_config -q && python -c "import src.api.main, src.workflow.worker; print('OK')"`
Expected: PASS + `OK`.

- [ ] **Step 8: Commit**

```bash
git add src/config.py src/api/main.py src/workflow/worker.py tests/test_config/test_preflight.py
git commit -m "feat(config): fail-fast preflight at API/worker startup"
```

---

## Task 3: `init` one-shot service runs setup_db automatically

**Files:**
- Modify: `docker-compose.yml`, `docker-compose.prod.yml`

CONTEXT: `python -m scripts.setup_db` is idempotent (creates Postgres tables + MinIO bucket + Temporal search-attrs). Today it's a manual step; prod never runs it. Add an init one-shot.

- [ ] **Step 1: Add the `init` service to `docker-compose.prod.yml`**

Add (a service that reuses the app build + the `x-app-env` anchor):
```yaml
  init:
    <<: [*app-build]
    command: ["python", "-m", "scripts.setup_db"]
    environment: *app-env
    restart: "no"
    depends_on:
      postgres: {condition: service_healthy}
      milvus: {condition: service_healthy}
      minio: {condition: service_healthy}
      temporal: {condition: service_started}
```
Then make `api` and `worker` wait for it: add to BOTH their `depends_on`:
```yaml
      init: {condition: service_completed_successfully}
```

- [ ] **Step 2: Add the `init` service to `docker-compose.yml` (dev)**

Dev runs the app on the host, so api/worker aren't compose services — but the init service is still useful (`docker compose up -d init` initializes schemas without remembering the script). Add, reusing the dev app build context (mirror how dev runs scripts; if dev has no app image build, use the same image the prod build uses by adding a minimal build or skip api/worker depends). Concretely add:
```yaml
  init:
    build: {context: ., dockerfile: Dockerfile}
    command: ["python", "-m", "scripts.setup_db"]
    env_file: [.env]
    restart: "no"
    depends_on:
      postgres: {condition: service_healthy}
      milvus: {condition: service_healthy}
      minio: {condition: service_healthy}
      temporal: {condition: service_started}
    profiles: ["init"]
```
(Gate dev `init` behind a `profiles: ["init"]` so the default `docker compose up -d` for the lightweight dev stack doesn't force an app image build; run via `docker compose --profile init up init`. In prod `init` has no profile — it always runs.)

- [ ] **Step 3: Validate compose config**

Run:
```bash
docker compose -f docker-compose.yml config >/dev/null && echo dev-ok
LITELLM_BASE_URL=x docker compose -f docker-compose.prod.yml config >/dev/null && echo prod-ok
```
Expected: `dev-ok` / `prod-ok`. Confirm the rendered prod `api`/`worker` show `init` under depends_on with `service_completed_successfully`:
`LITELLM_BASE_URL=x docker compose -f docker-compose.prod.yml config | grep -A6 "depends_on" | grep -i init`

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml docker-compose.prod.yml
git commit -m "feat(deploy): init one-shot service runs setup_db before api/worker"
```

---

## Task 4: `setup_wikibase` without the host docker socket

**Files:**
- Modify: `scripts/setup_wikibase.py` (drop the `docker exec` bot step)
- Modify: `docker-compose.yml`, `docker-compose.prod.yml` (add `wiki-bootstrap` one-shot)
- Test: `tests/test_scripts/test_setup_wikibase.py` (new)

CONTEXT: `scripts/setup_wikibase.py` currently (a) creates the bot via `docker exec <wikibase> php maintenance/run.php createAndPromote` (needs host docker — fails in containers), and (b) bootstraps schema (Items/Properties) over the MediaWiki API (works anywhere). Move (a) to a compose one-shot that runs the maintenance command inside the wikibase image itself; keep (b) in the script.

- [ ] **Step 1: Add `wiki-bootstrap` one-shot to BOTH composes** (prod under the existing `wikibase` profile; dev likewise)

```yaml
  wiki-bootstrap:
    image: wikibase/wikibase-bundle:1.41.1-wmde.20
    profiles: ["wikibase"]
    command:
      - "php"
      - "/var/www/html/maintenance/run.php"
      - "createAndPromote"
      - "--bot"
      - "--force"
      - "${WIKIBASE_BOT_USER:-KbBot}"
      - "${WIKIBASE_BOT_PASSWORD:-changemebot}"
    restart: "no"
    depends_on:
      wikibase: {condition: service_healthy}
```
(It shares the wikibase image so the maintenance script + DB config are present; it connects to the same MariaDB via the baked LocalSettings. No host docker needed.)

- [ ] **Step 2: Drop the docker-exec bot step from `scripts/setup_wikibase.py`**

In `main()` (around line 422) the call is `_ensure_bot_user(cfg, dry_run=args.dry_run)`. Remove that call and the `_ensure_bot_user` + `_wikibase_container_name` functions (lines ~97-177). Replace the removed call site with a one-line log:
```python
    logger.info(
        "bot user provisioning is handled by the `wiki-bootstrap` compose "
        "one-shot (docker compose --profile wikibase run --rm wiki-bootstrap); "
        "this script now only bootstraps the schema over the API.")
```
Remove now-unused imports (`shutil`, `subprocess` if only used there — verify with ruff). Keep the `_MIN_PASSWORD_LEN` check moved into preflight (already covered in Task 2) — delete the local one if it's now unused, or leave the constant if other code references it (grep first).

- [ ] **Step 3: Write the test** — create `tests/test_scripts/test_setup_wikibase.py`:

```python
"""setup_wikibase no longer needs the host docker CLI (container-runnable)."""
import scripts.setup_wikibase as sw


def test_no_docker_exec_dependency():
    src = (sw.__file__)
    text = open(src, encoding="utf-8").read()
    # the host-docker bot path is gone
    assert "_ensure_bot_user" not in text
    assert "docker" not in text.lower() or "wiki-bootstrap" in text
```

- [ ] **Step 4: Run**

Run: `pytest tests/test_scripts/test_setup_wikibase.py -q && ruff check --select F401,F841 scripts/setup_wikibase.py`
Expected: PASS + ruff clean (no unused imports left).

- [ ] **Step 5: Validate composes**

Run:
```bash
docker compose -f docker-compose.yml config >/dev/null && echo dev-ok
LITELLM_BASE_URL=x docker compose -f docker-compose.prod.yml config >/dev/null && echo prod-ok
```
Expected: both ok; `wiki-bootstrap` present under the `wikibase` profile.

- [ ] **Step 6: Commit**

```bash
git add scripts/setup_wikibase.py docker-compose.yml docker-compose.prod.yml tests/test_scripts/test_setup_wikibase.py
git commit -m "feat(deploy): wikibase bot via compose one-shot; setup_wikibase no longer needs host docker"
```

---

## Task 5: Verification sweep + live check

- [ ] **Step 1: Static + unit**

Run: `ruff check src scripts tests | tail; mv .env .env.bak; pytest tests/test_config tests/test_scripts -q; mv .env.bak .env`
Expected: touched tests pass on defaults; ruff clean on changed files.

- [ ] **Step 2: Live — init service initializes a fresh schema** (stack is up)

Run: `docker compose --profile init up init` (dev) — expect it to run setup_db and exit 0. Check Postgres `documents` table + MinIO bucket exist.

- [ ] **Step 3: Live — preflight fires** Temporarily set `API_ENV=production` + a placeholder secret in a throwaway env and import the app; confirm it raises with the actionable message. Revert.

- [ ] **Step 4: Live — wiki-bootstrap creates the bot** (if wikibase profile up)

Run: `docker compose --profile wikibase run --rm wiki-bootstrap` — expect `Password set. done.`; verify `KbBot` is in group `bot` (DB query from earlier).

- [ ] **Step 5: Final commit** (only if steps left changes)

```bash
git add -A && git commit -m "chore: deploy redesign batch 1 cleanup"
```

---

## Self-Review notes (author)

- **Spec coverage:** Component 1 (canon dim) → Task 1; Component 2 (preflight) → Task 2; Component 3a (init/setup_db) → Task 3; Component 3b (wikibase no socket) → Task 4. Components 1-full (generator), 4 (make up), 5 (platform/prefetch), 6 (prod defaults), 7 (docs) are Batch 2/3 — NOT in this plan by design.
- **Type consistency:** `Settings.preflight(s)` signature used identically in config.py, main.py, worker.py, and tests. `wiki-bootstrap` / `init` service names used consistently across composes + verification.
- **Known env caveat:** config-default tests are run with `.env` moved aside (local `.env` has overrides) — every such step moves it back.
