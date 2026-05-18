# Project-side Redis cache for LLM responses

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cache LLM responses **inside our process** — between `build_llm()` and the LiteLLM proxy — so Temporal activity retries don't re-pay LLM calls.  Zero ops coordination with the prod LiteLLM deployment.  Cache backend: our own Redis container.

**Architecture:** A `CachedLLM` class subclassing LlamaIndex `OpenAILike`.  It hashes ``(model, messages, temperature, seed, tools, response_format)`` → SHA-256 key, looks it up in Redis (`GET kb-llamaindex:llm:<prompt_ver>:<model>:<hash>`), serves the cached `ChatResponse` on hit, otherwise calls `super().achat(...)`, stores the result, returns it.  Errors and streaming bypass the cache entirely.  Falls through to upstream silently if Redis is down.

**Tech Stack:** Python 3.12, redis-py (`redis[asyncio]`), LlamaIndex `OpenAILike`, Pydantic v2 settings.  Redis 7 in docker-compose.

**Spec context:** Workflow durability story in
`docs/superpowers/specs/2026-05-15-ingest-temporal-workflow-design.md` §7.
LLM factory: `src/retrieval/llm.py`.  Activity retry policy: forever
with `schedule_to_close` ceiling in
`src/workflow/document_ingest.py`.

**Session protocol:** Pause after each labelled **Stage** for sync.

**Defaults chosen (flag if you disagree before T1):**

- **Wrap `achat` first, then `astructured_predict`.**  Inventory of
  the codebase shows the two methods that matter:
  `achat` (extract_kg, merge summary, ER judge, translator,
  reflective synthesis) and `astructured_predict` (judge).
  `achat_with_tools` (ReAct agent) is also async but its inputs
  carry user-typed queries — caching gives marginal benefit at the
  agent layer, and the workflow retry case (which is what we want
  to solve) doesn't touch it.  Phase agent caching to Stage 4.
- **Redis namespace** prefix `kb-llamaindex:llm:`.  Configurable
  via `LLM_CACHE_NAMESPACE`.
- **TTL** 24h default.  Configurable via `LLM_CACHE_TTL_S`.
- **Negative caching disabled** — errors are NEVER stored.  Retry
  is what we WANT to recover from.
- **Streaming bypasses the cache** — `stream_chat` returns
  generators we can't roundtrip cleanly through Redis.  Project
  doesn't use streaming on the activity hot path.
- **Cache OFF by default in tests** — Redis isn't a unit-test dep.
  `LLM_CACHE_ENABLED=true` to opt-in; tests + CI keep the
  current uncached path.

---

## Stage 1 — Redis infra + minimal `CachedLLM` (achat only)

### Task 1: Add `redis` service + dev/local config

**Files:**
- Modify: `docker-compose.yml`.
- Modify: `.env.example`.

- [ ] **Step 1: Append the service**

```yaml
  # ── Cache: project-side Redis for LLM response cache ───────────
  # We own this Redis (not the prod LiteLLM's cache).  Keeps the
  # cache lifecycle decoupled from the upstream LiteLLM deployment.
  redis:
    image: redis:7-alpine
    command:
      - redis-server
      - --maxmemory
      - "${REDIS_MAXMEMORY:-512mb}"
      - --maxmemory-policy
      - allkeys-lru
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 3s
      retries: 5
    restart: unless-stopped
```

  Append `redis_data:` to the `volumes:` block.

- [ ] **Step 2: `.env.example`**

```env
# Project-side Redis for LLM response cache.  Owned by us; separate
# from any prod-LiteLLM cache backend.  Toggle off via
# LLM_CACHE_ENABLED=false to bypass entirely.
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_MAXMEMORY=512mb
LLM_CACHE_ENABLED=true
LLM_CACHE_TTL_S=86400
LLM_CACHE_NAMESPACE=kb-llamaindex:llm
# Bump when prompt templates change to invalidate stale entries.
# Treated as part of the cache key so cache transitions without a
# manual flush.
LLM_PROMPT_VERSION=v1
```

- [ ] **Step 3: Bring up + smoke**

```bash
docker compose -p kb-llamaindex up -d redis
docker compose -p kb-llamaindex exec redis redis-cli ping     # → PONG
```

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml .env.example
git commit -m "feat(infra): redis container for project-side LLM cache"
```

---

### Task 2: `LLMCacheSettings` in `src/config.py`

**Files:**
- Modify: `src/config.py`.
- Test: extend `tests/test_config.py`.

- [ ] **Step 1: Write the test**

```python
def test_llm_cache_settings_defaults():
    from src.config import settings
    s = settings.llm_cache
    assert s.enabled is True
    assert s.ttl_s == 86400
    assert s.namespace == "kb-llamaindex:llm"
    assert s.prompt_version == "v1"
    assert s.redis_host == "localhost"
    assert s.redis_port == 6379

def test_llm_cache_disabled_via_env(monkeypatch):
    monkeypatch.setenv("LLM_CACHE_ENABLED", "false")
    from importlib import reload
    import src.config as cfg
    reload(cfg)
    assert cfg.settings.llm_cache.enabled is False
```

Expected: ImportError on `settings.llm_cache`.  Good.

- [ ] **Step 2: Add the settings class**

In `src/config.py`, after `LiteLLMSettings`:

```python
class LLMCacheSettings(BaseSettings):
    """Project-side Redis cache for LLM responses.

    Sits between ``build_llm()`` and the LiteLLM proxy.  Keys are
    SHA-256 of ``(model, messages, params, prompt_version)``.  Cache
    Down → bypass silently, never fails the calling activity.
    """

    model_config = SettingsConfigDict(env_prefix="LLM_CACHE_", extra="ignore")

    enabled: bool = True
    ttl_s: int = 86400
    namespace: str = "kb-llamaindex:llm"
    # Treated as part of the cache key.  Bump when a prompt template
    # changes so stale responses get evicted naturally without
    # needing to FLUSHDB.
    prompt_version: str = "v1"
    # Redis connection — same env vars regardless of dev / prod.
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: SecretStr | None = None
    # Soft circuit breaker: how long to wait for a Redis op before
    # giving up and going to upstream.  Keeps us from doubling
    # request latency when Redis is slow.
    op_timeout_s: float = 0.250
```

And expose it from `Settings`:

```python
    @cached_property
    def llm_cache(self) -> LLMCacheSettings:
        return LLMCacheSettings()
```

Add `"LLMCacheSettings"` to `__all__`.

- [ ] **Step 3: Run + commit**

```bash
uv run pytest tests/test_config.py -v
git add src/config.py tests/test_config.py
git commit -m "feat(config): LLMCacheSettings (Redis-backed response cache)"
```

---

### Task 3: Add `redis[asyncio]` dependency

**Files:**
- Modify: `pyproject.toml`.

- [ ] **Step 1: Append to `dependencies`**

```toml
    # Project-side LLM response cache.
    "redis>=5.1,<6",
```

- [ ] **Step 2: Sync + commit**

```bash
uv sync --extra dev
git add pyproject.toml uv.lock
git commit -m "feat: add redis-py for LLM response cache"
```

---

### Task 4: `CachedLLM` class (achat only)

**Files:**
- Create: `src/retrieval/llm_cache.py`.
- Test: `tests/test_retrieval/test_llm_cache.py` (new).

- [ ] **Step 1: Write the failing tests first**

```python
"""Project-side LLM response cache.

Covers four behaviours:
  * cache miss: upstream called, response stored.
  * cache hit: upstream NOT called, stored response returned.
  * Redis down: bypasses transparently, upstream called.
  * cache disabled: never touches Redis, upstream called.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class _FakeRedis:
    """Minimal in-memory redis async client stand-in."""

    def __init__(self):
        self.store: dict[str, bytes] = {}
        self.gets = 0
        self.sets = 0

    async def get(self, key):
        self.gets += 1
        return self.store.get(key)

    async def set(self, key, value, ex=None):
        self.sets += 1
        self.store[key] = value
        return True

    async def ping(self):
        return True


@pytest.mark.asyncio
async def test_cache_miss_calls_upstream_and_stores(monkeypatch):
    from llama_index.core.base.llms.types import ChatMessage, ChatResponse, MessageRole
    from src.retrieval.llm_cache import CachedLLM, _make_key

    fake = _FakeRedis()
    upstream_resp = ChatResponse(message=ChatMessage(
        role=MessageRole.ASSISTANT, content="hello",
    ))

    with patch("src.retrieval.llm_cache._connect_redis", return_value=fake):
        client = CachedLLM(model="m", api_base="x", api_key="y")
        client._achat_upstream = AsyncMock(return_value=upstream_resp)

        out = await client.achat([
            ChatMessage(role=MessageRole.USER, content="ping"),
        ])

    assert out.message.content == "hello"
    client._achat_upstream.assert_awaited_once()
    assert fake.sets == 1
    assert fake.gets == 1


@pytest.mark.asyncio
async def test_cache_hit_skips_upstream(monkeypatch):
    """Same input → second call returns cached value without
    invoking upstream."""
    from llama_index.core.base.llms.types import ChatMessage, ChatResponse, MessageRole
    from src.retrieval.llm_cache import CachedLLM

    fake = _FakeRedis()
    upstream_resp = ChatResponse(message=ChatMessage(
        role=MessageRole.ASSISTANT, content="cached-hi",
    ))

    with patch("src.retrieval.llm_cache._connect_redis", return_value=fake):
        client = CachedLLM(model="m", api_base="x", api_key="y")
        client._achat_upstream = AsyncMock(return_value=upstream_resp)

        msgs = [ChatMessage(role=MessageRole.USER, content="ping")]
        await client.achat(msgs)        # warms cache
        client._achat_upstream.reset_mock()
        second = await client.achat(msgs)

    assert second.message.content == "cached-hi"
    client._achat_upstream.assert_not_called()
    assert fake.sets == 1


@pytest.mark.asyncio
async def test_upstream_error_not_cached():
    from llama_index.core.base.llms.types import ChatMessage, MessageRole
    from src.retrieval.llm_cache import CachedLLM

    fake = _FakeRedis()
    with patch("src.retrieval.llm_cache._connect_redis", return_value=fake):
        client = CachedLLM(model="m", api_base="x", api_key="y")
        client._achat_upstream = AsyncMock(
            side_effect=RuntimeError("upstream 503"),
        )
        with pytest.raises(RuntimeError):
            await client.achat([
                ChatMessage(role=MessageRole.USER, content="boom"),
            ])
    assert fake.sets == 0


@pytest.mark.asyncio
async def test_redis_down_falls_through_to_upstream():
    from llama_index.core.base.llms.types import ChatMessage, ChatResponse, MessageRole
    from src.retrieval.llm_cache import CachedLLM

    class _BrokenRedis:
        async def get(self, *a, **kw):
            raise ConnectionError("nope")
        async def set(self, *a, **kw):
            raise ConnectionError("nope")
        async def ping(self):
            raise ConnectionError("nope")

    upstream_resp = ChatResponse(message=ChatMessage(
        role=MessageRole.ASSISTANT, content="direct",
    ))
    with patch(
        "src.retrieval.llm_cache._connect_redis", return_value=_BrokenRedis(),
    ):
        client = CachedLLM(model="m", api_base="x", api_key="y")
        client._achat_upstream = AsyncMock(return_value=upstream_resp)
        out = await client.achat([
            ChatMessage(role=MessageRole.USER, content="ping"),
        ])
    assert out.message.content == "direct"


@pytest.mark.asyncio
async def test_cache_disabled_setting_bypasses_redis(monkeypatch):
    from llama_index.core.base.llms.types import ChatMessage, ChatResponse, MessageRole
    from src.config import settings
    from src.retrieval.llm_cache import CachedLLM

    monkeypatch.setattr(settings.llm_cache, "enabled", False, raising=False)
    fake = _FakeRedis()
    upstream_resp = ChatResponse(message=ChatMessage(
        role=MessageRole.ASSISTANT, content="direct",
    ))
    with patch("src.retrieval.llm_cache._connect_redis", return_value=fake):
        client = CachedLLM(model="m", api_base="x", api_key="y")
        client._achat_upstream = AsyncMock(return_value=upstream_resp)
        await client.achat([
            ChatMessage(role=MessageRole.USER, content="ping"),
        ])
    assert fake.gets == 0
    assert fake.sets == 0
```

Run: ImportError on `src.retrieval.llm_cache`.  Good.

- [ ] **Step 2: Implement `CachedLLM`**

`src/retrieval/llm_cache.py`:

```python
"""Project-side LLM response cache.

Wraps ``llama_index.llms.openai_like.OpenAILike`` and intercepts
``achat`` (Stage 1).  Stage 2 / 4 extend to ``astructured_predict``
and tool-calling.

Cache key components (in deterministic JSON order):
  - prompt_version (env-controlled invalidation lever)
  - model name
  - messages (role + content + name + tool_calls)
  - temperature
  - additional_kwargs (seed, response_format, …)
SHA-256 → first 32 hex chars suffix.
``{namespace}:{prompt_version}:{model}:{hash}``.

Failure modes are non-fatal:
  * Redis down / slow → bypass to upstream, log a warning.
  * Upstream raises → propagate exception, DO NOT cache.
  * Cache disabled in settings → never touches Redis.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any, Optional, Sequence

import redis.asyncio as aioredis
from llama_index.core.base.llms.types import ChatMessage, ChatResponse
from llama_index.llms.openai_like import OpenAILike
from loguru import logger

from src.config import settings


_REDIS_CLIENT: Optional["aioredis.Redis"] = None


def _connect_redis() -> "aioredis.Redis":
    """Lazy singleton.  Connect once per process; reused across all
    `CachedLLM` instances and roles."""
    global _REDIS_CLIENT
    if _REDIS_CLIENT is None:
        cfg = settings.llm_cache
        kwargs: dict[str, Any] = {
            "host": cfg.redis_host,
            "port": cfg.redis_port,
            "db": cfg.redis_db,
            "socket_timeout": cfg.op_timeout_s,
            "socket_connect_timeout": cfg.op_timeout_s,
            "decode_responses": False,
        }
        if cfg.redis_password is not None:
            kwargs["password"] = cfg.redis_password.get_secret_value()
        _REDIS_CLIENT = aioredis.Redis(**kwargs)
    return _REDIS_CLIENT


def _serialise_messages(messages: Sequence[ChatMessage]) -> list[dict]:
    """Canonical JSON for the messages list — ordered, type-safe."""
    out: list[dict] = []
    for m in messages:
        out.append({
            "role": m.role.value if hasattr(m.role, "value") else str(m.role),
            "content": m.content or "",
            # additional_kwargs may carry tool_calls, name, function args
            "extra": _canonicalise(m.additional_kwargs or {}),
        })
    return out


def _canonicalise(obj: Any) -> Any:
    """Recursive sort-keys for stable hashing.  Handles dict, list,
    tuple, primitive values."""
    if isinstance(obj, dict):
        return {k: _canonicalise(v) for k, v in sorted(obj.items())}
    if isinstance(obj, (list, tuple)):
        return [_canonicalise(x) for x in obj]
    return obj


def _make_key(*, model: str, messages: Sequence[ChatMessage], extras: dict) -> str:
    cfg = settings.llm_cache
    payload = {
        "v": cfg.prompt_version,
        "model": model,
        "messages": _serialise_messages(messages),
        "extras": _canonicalise(extras),
    }
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    h = hashlib.sha256(blob).hexdigest()[:32]
    return f"{cfg.namespace}:{cfg.prompt_version}:{model}:{h}"


class CachedLLM(OpenAILike):
    """`OpenAILike` with a Redis read-through cache around `achat`."""

    async def _achat_upstream(self, messages, **kwargs):
        """Indirection point so tests can patch upstream cleanly."""
        return await super().achat(messages, **kwargs)

    async def achat(self, messages, **kwargs):
        cfg = settings.llm_cache
        if not cfg.enabled or kwargs.get("stream"):
            return await self._achat_upstream(messages, **kwargs)

        extras = {
            "temperature": getattr(self, "temperature", 0.0),
            "additional_kwargs": _canonicalise(self.additional_kwargs or {}),
            # function calling: tools / response format
            "kwargs": _canonicalise(
                {k: v for k, v in kwargs.items() if k != "stream"},
            ),
        }
        key = _make_key(
            model=self.model, messages=messages, extras=extras,
        )

        # GET
        try:
            r = _connect_redis()
            raw = await asyncio.wait_for(r.get(key), timeout=cfg.op_timeout_s)
            if raw is not None:
                logger.debug("llm cache HIT  key={k}", k=key[-8:])
                return ChatResponse.parse_raw(raw)
        except Exception as exc:  # noqa: BLE001
            logger.warning("llm cache GET fail: {e}", e=exc)

        # MISS: call upstream
        resp = await self._achat_upstream(messages, **kwargs)

        # SET (best effort, never blocks longer than op_timeout_s)
        try:
            r = _connect_redis()
            await asyncio.wait_for(
                r.set(key, resp.json(), ex=cfg.ttl_s),
                timeout=cfg.op_timeout_s,
            )
            logger.debug("llm cache SET  key={k}", k=key[-8:])
        except Exception as exc:  # noqa: BLE001
            logger.warning("llm cache SET fail: {e}", e=exc)

        return resp
```

- [ ] **Step 3: Run tests + commit**

```bash
uv run pytest tests/test_retrieval/test_llm_cache.py -v
git add src/retrieval/llm_cache.py tests/test_retrieval/test_llm_cache.py
git commit -m "feat(llm-cache): CachedLLM with Redis read-through (achat)"
```

---

### Task 5: Wire `CachedLLM` into `build_llm()`

**Files:**
- Modify: `src/retrieval/llm.py`.
- Update tests: any test that mocks `OpenAILike` constructor needs the new path.

- [ ] **Step 1: Write the failing test**

```python
def test_build_llm_returns_cached_when_enabled(monkeypatch):
    from src.config import settings
    from src.retrieval.llm import build_llm
    from src.retrieval.llm_cache import CachedLLM

    monkeypatch.setattr(settings.llm_cache, "enabled", True, raising=False)
    llm = build_llm()
    assert isinstance(llm, CachedLLM)


def test_build_llm_returns_plain_when_disabled(monkeypatch):
    from src.config import settings
    from src.retrieval.llm import build_llm
    from src.retrieval.llm_cache import CachedLLM
    from llama_index.llms.openai_like import OpenAILike

    monkeypatch.setattr(settings.llm_cache, "enabled", False, raising=False)
    llm = build_llm()
    assert isinstance(llm, OpenAILike)
    assert not isinstance(llm, CachedLLM)
```

- [ ] **Step 2: Update `build_llm()`**

In `src/retrieval/llm.py`:

```python
from src.retrieval.llm_cache import CachedLLM
from llama_index.llms.openai_like import OpenAILike


def build_llm() -> LLM:
    cfg = settings.litellm
    function_calling = os.environ.get(
        "LITELLM_FUNCTION_CALLING", "true"
    ).lower() not in {"0", "false", "no"}

    Klass = CachedLLM if settings.llm_cache.enabled else OpenAILike

    return Klass(
        model=cfg.llm_model,
        api_base=cfg.base_url,
        api_key=cfg.api_key.get_secret_value(),
        timeout=cfg.timeout_s,
        max_retries=cfg.max_retries,
        is_chat_model=True,
        is_function_calling_model=function_calling,
        # Cache friendliness — same prompt must hit cache reliably.
        temperature=0.0,
        additional_kwargs={"seed": 0},
    )
```

- [ ] **Step 3: Run + commit**

```bash
uv run pytest tests/test_retrieval/ -v
uv run pytest -q --ignore=tests/test_workflow/test_workflow_local.py
git add src/retrieval/llm.py tests/test_retrieval/
git commit -m "feat(llm): build_llm returns CachedLLM when LLM_CACHE_ENABLED=true"
```

---

### Task 6: Live smoke — measure the win

**Files:** none.

- [ ] **Step 1: Cold cache ingest**

```bash
docker compose -p kb-llamaindex up -d redis
docker compose -p kb-llamaindex exec redis redis-cli FLUSHDB    # clean slate
# Ingest a fixture doc.  Note merge_and_resolve wall-clock in Temporal UI.
```

- [ ] **Step 2: Warm cache ingest (same content)**

```bash
# Re-ingest same content with a fresh doc_id.  Same prompts → cache hits.
docker compose -p kb-llamaindex exec redis redis-cli DBSIZE     # should be > 0
# Watch merge_and_resolve wall-clock — should drop to single-digit seconds.
docker compose -p kb-llamaindex exec redis redis-cli MONITOR | head -20
# Confirms GETs return values, not (nil).
```

- [ ] **Step 3: Retry test**

```bash
# Submit an ingest.  Mid-extract, kill the worker:
pkill -f "src.workflow.worker"
# Wait 5 s, restart:
uv run python -m src.workflow.worker > /tmp/wf-retry.log 2>&1 &
# Temporal retries the activity.  LLM calls already done → cache hits → fast.
```

Bring back the before/after numbers.

---

**🛑 STAGE 1 GATE.**  Done when:
- Redis container healthy.
- `tests/test_retrieval/test_llm_cache.py` green (5 tests).
- Live measure shows cache hit > 80% on identical re-ingest.
- Retry latency drops materially.

---

## Stage 2 — Wrap `astructured_predict`

Why: ER judge and `LLMJudge` use structured predict.  Without
wrapping them, retries on `merge_and_resolve` still pay the judge
calls twice.

### Task 7: Extend `CachedLLM` to cover `astructured_predict`

**Files:**
- Modify: `src/retrieval/llm_cache.py`.
- Test: add cases to `test_llm_cache.py`.

- [ ] **Step 1: Write the failing test**

```python
@pytest.mark.asyncio
async def test_astructured_predict_caches_response():
    from pydantic import BaseModel
    from src.retrieval.llm_cache import CachedLLM

    class Out(BaseModel):
        verdict: bool

    fake = _FakeRedis()
    with patch("src.retrieval.llm_cache._connect_redis", return_value=fake):
        client = CachedLLM(model="m", api_base="x", api_key="y")
        client._astructured_upstream = AsyncMock(return_value=Out(verdict=True))
        msgs = "Is the sky blue?"
        first = await client.astructured_predict(Out, msgs)
        client._astructured_upstream.reset_mock()
        second = await client.astructured_predict(Out, msgs)

    assert first.verdict is True
    assert second.verdict is True
    client._astructured_upstream.assert_not_called()
```

- [ ] **Step 2: Implement**

In `src/retrieval/llm_cache.py`, add:

```python
async def _astructured_upstream(self, *args, **kwargs):
    return await super().astructured_predict(*args, **kwargs)


async def astructured_predict(self, output_cls, prompt, **kwargs):
    cfg = settings.llm_cache
    if not cfg.enabled:
        return await self._astructured_upstream(output_cls, prompt, **kwargs)

    extras = {
        "temperature": getattr(self, "temperature", 0.0),
        "additional_kwargs": _canonicalise(self.additional_kwargs or {}),
        "output_cls": output_cls.__name__,
        "kwargs": _canonicalise({k: v for k, v in kwargs.items()}),
    }
    # `prompt` may be a string or a PromptTemplate; render to str
    # for hashing.
    rendered = prompt if isinstance(prompt, str) else str(prompt)
    pseudo_msgs = [ChatMessage(role="user", content=rendered)]
    key = _make_key(model=self.model, messages=pseudo_msgs, extras=extras)
    key += ":structured"     # avoid collisions with achat keys

    try:
        r = _connect_redis()
        raw = await asyncio.wait_for(r.get(key), timeout=cfg.op_timeout_s)
        if raw is not None:
            return output_cls.model_validate_json(raw)
    except Exception as exc:  # noqa: BLE001
        logger.warning("llm cache GET (structured) fail: {e}", e=exc)

    resp = await self._astructured_upstream(output_cls, prompt, **kwargs)

    try:
        r = _connect_redis()
        await asyncio.wait_for(
            r.set(key, resp.model_dump_json(), ex=cfg.ttl_s),
            timeout=cfg.op_timeout_s,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("llm cache SET (structured) fail: {e}", e=exc)
    return resp
```

- [ ] **Step 3: Commit**

```bash
git add src/retrieval/llm_cache.py tests/test_retrieval/test_llm_cache.py
git commit -m "feat(llm-cache): wrap astructured_predict"
```

---

## Stage 3 — Prompt-version sentinel for invalidation hygiene

### Task 8: Wire `LLM_PROMPT_VERSION` into all template renderers

Right now `LLM_PROMPT_VERSION` is already part of the cache key
(via `LLMCacheSettings.prompt_version`).  This task just makes
sure the team knows to bump it when prompts change.

**Files:**
- Modify: `docs/MODELS.md` (or wherever model/prompt docs live) —
  one paragraph explaining the bump-on-prompt-change rule.
- Modify: prompt template modules — add a `PROMPT_TEMPLATE_VERSION`
  constant in the header so prompt edits are visually paired with a
  version bump in the same file.

- [ ] T8.1: Audit prompt files:
  - `src/graph/lightrag_prompts.py`
  - `src/graph/merge.py` (SUMMARIZE_ENTITY_DESCRIPTIONS)
  - `src/graph/entity_resolution.py` (judge prompts)
  - `src/ingestion/translate_transform.py`
  - `src/retrieval/judge.py`, `src/retrieval/reflective_synth.py`,
    `src/retrieval/react_agent.py`

  In each, document at the top: "Bump LLM_PROMPT_VERSION when
  editing any of the templates below."

- [ ] T8.2: Smoke test — change a prompt template; check that
  unchanged `LLM_PROMPT_VERSION` keeps serving stale.  Bump it;
  cache misses + warms with the new template.  Document the
  expected behaviour in the runbook.

- [ ] T8.3: Commit.

```bash
git add docs/ src/graph/ src/ingestion/translate_transform.py \
        src/retrieval/judge.py src/retrieval/reflective_synth.py
git commit -m "docs(llm-cache): prompt-version bump procedure"
```

---

## Stage 4 (optional) — Observability + agent layer

### Task 9: Per-activity hit-rate counter

Patch each activity to read `r.info("stats")` or use a per-process
counter incremented inside `_connect_redis` wrapper, then emit
`activity.heartbeat({"llm_cache_hits": N, "llm_cache_misses": M})`.

### Task 10: Wrap `achat_with_tools` (ReAct agent)

Lower priority than activity stages — re-runs of agent flows are
rare and we usually don't retry them.

### Task 11: Runbook for cache flush + warm

`docs/runbook/llm-cache.md`:

```bash
# Flush everything in our namespace
docker compose -p kb-llamaindex exec redis redis-cli --scan \
    --pattern 'kb-llamaindex:llm:*' | \
    xargs -n 1 docker compose -p kb-llamaindex exec -T redis redis-cli del

# Stats
docker compose -p kb-llamaindex exec redis redis-cli INFO stats

# Warm a doc explicitly: re-ingest the doc through /api/v1/ingest.
# The cache will fill on first activity completion.
```

---

## Gotchas

1. **Prompts must be deterministic.**  Any `datetime.now()` /
   `uuid.uuid4()` injected into a prompt body causes every call to
   miss the cache.  Run `grep -rn "datetime\.now\|uuid\." src/graph
   src/ingestion src/retrieval/judge.py src/retrieval/reflective_synth.py`
   before Stage 1 and either remove or freeze them.
2. **Tool-call results vary.**  When LLM returns
   `{tool_calls: [...]}`, the JSON includes generated tool-call IDs
   in some providers.  These rarely affect downstream logic, but
   the cached response replays the same IDs — that's almost always
   fine.  If it breaks, strip IDs from cache values.
3. **Pydantic version drift.**  `ChatResponse.parse_raw` /
   `model_validate_json` API differs across LlamaIndex versions.
   The test suite catches this; pin the LlamaIndex version when
   landing.
4. **Stampede on cold cache.**  N parallel identical prompts all
   miss together → all N call upstream.  Acceptable for first
   attempt; the retry use case (what we care about) already has
   a warmed cache.
5. **Redis password rotation.**  If prod Redis enforces password
   auth, `LLM_CACHE_REDIS_PASSWORD` env var handles it.  Rotations
   require restarting the API + worker processes.
6. **PII in cache.**  Prompts contain document content.  Redis
   here is project-owned and runs in the same network as the rest
   of the stack.  If the deployment crosses a trust boundary, add
   `requirepass` and use `ssl: true` in the Redis client config.

---

## Self-review

**Spec coverage:**
- Stage 1: infra + ``achat`` wrap — covers ~90% of LLM volume.
- Stage 2: ``astructured_predict`` — covers ER + judge.
- Stage 3: prompt-version invalidation hygiene.
- Stage 4: observability + agent layer (lower priority).

**Placeholder scan:** All code blocks are runnable; CLI snippets
include expected outputs.

**Type consistency:**
- `CachedLLM` extends `OpenAILike`; `build_llm()` returns the
  same `LLM` ABC regardless of branch.
- `_make_key` signature is stable between `achat` and
  `astructured_predict` call sites; the `:structured` suffix in
  Task 7 prevents collisions.

**Rollback story:**
- Toggle `LLM_CACHE_ENABLED=false` in env → `build_llm()` returns
  plain `OpenAILike`.  Zero code change.
- Stop the Redis container → `_connect_redis()` ConnectionError
  → cache falls through to upstream silently (covered by
  `test_redis_down_falls_through_to_upstream`).

---

## Execution handoff

**Plan complete and saved to
`docs/superpowers/plans/2026-05-18-redis-llm-cache.md`.**

Three execution options:

1. **Subagent-Driven (recommended)** — fresh subagent per task.
   Stage 1 is 6 small tasks (~1 day's work).
2. **Inline Execution** — executing-plans skill, batches with
   gates between stages.
3. **Stage-1-only** — ship the achat wrap (covers ~90% of use
   cases); defer structured / observability.

Also confirm or override the four defaults from the top of the
plan (wrap order: achat → astructured; 24h TTL; namespace
`kb-llamaindex:llm`; cache OFF in tests).

**Which approach?**
