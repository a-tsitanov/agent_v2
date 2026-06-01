# Search: Drift-bug fix + selfrag-наследие + аудит мёртвого кода — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Закрыть `AttributeError: 'dict' object has no attribute 'drift_mode'` в drift-поиске регрессионным тестом + защитой, и убрать мёртвое наследие отрезанного selfrag/ReAct-слоя (R7b cutover), не трогая активные пути.

**Architecture:** Поиск после Plan #2 (R2–R7b) — это Temporal-воркфлоу: `SearchOrchestratorWorkflow` (local, plan-execute), `GlobalSearchWorkflow` (global map-reduce), `DriftSearchWorkflow`/`AutoSearchWorkflow` (router_wf). Единственная HTTP-поверхность — `src/api/routes/search_v2.py` → `/api/v1/search/{local,global,drift,auto}`. selfrag/agent/ReAct-эндпоинты и `SearchWorkflow` удалены в `f6cb02d`; рефлексивный синтез остался кодом-сиротой.

**Tech Stack:** Python 3.12, Temporal (`temporalio` 1.27.2, `temporalio.contrib.pydantic.pydantic_data_converter`), Pydantic v2, FastAPI, pytest + `temporalio.testing.WorkflowEnvironment`, `uv`.

---

## Контекст расследования (прочитать перед началом)

Три вопроса из задачи и что выяснено по коду:

**1. Куда делся selfrag и кто наследник.**
selfrag (Self-RAG: ReAct-цикл + рефлексивный синтез с ре-ретривом) удалён в коммите `f6cb02d`
(`refactor(search)!: remove legacy SearchWorkflow + /search,/agent,/selfrag … (Plan #2 R7b, BREAKING)`).
Функциональный наследник — **`SearchOrchestratorWorkflow`** (`/api/v1/search/local`): детерминированный
plan → параллельный per-subquery retrieve → merge/dedup → coverage-gate → rerank → один синтез на large-tier.
Способность рефлексивного синтеза (`reflective_synthesize`) физически жива в `src/retrieval/reflective_synth.py`
и в ветке `if params.mode == "selfrag":` внутри `synthesize_answer`, но **недостижима**: ни один путь не
выставляет `mode="selfrag"` (оркестратор передаёт `"simple"`). Это код-сирота — см. Phase 3 (решение).

**2. Drift-баг (`'dict' object has no attribute 'drift_mode'`).**
Падение в `src/workflow/search/global_wf.py:141`: `mode = "drift" if params.drift_mode else "global"`.
Гипотеза «`args=[...]` ломает десериализацию» **опровергнута эмпирически**:
- В том же `DriftSearchWorkflow.run` аргументы тоже приходят через `args=[local_params, global_params]`, и
  `local_params.query` (router_wf.py:74) и `global_params.model_copy(...)` (router_wf.py:88) **работают** — то
  есть до падения объекты были нормальными моделями, а не dict. Будь `args=[...]` причиной, упало бы раньше.
- Прогон точного пути через конвертер на диске показал, что `params` корректно восстанавливается в
  `GlobalSearchParams` **во всех** конвертерах (и pydantic, и дефолтном) на `temporalio` 1.27.2:
  `pc.from_payloads(pc.to_payloads([gp, [node]]), [GlobalSearchParams, list[SerializedNode] | None])` →
  `arg[0] = GlobalSearchParams`. Воспроизвести баг из текущего исходника **не удалось**.
- `typing.get_type_hints` и `temporalio.common._type_hints_from_func` для `GlobalSearchWorkflow.run` дают
  `arg_types=[GlobalSearchParams, list[SerializedNode] | None]` — корректно.

Вывод: текущий код на диске корректен. Наиболее вероятная причина наблюдаемой ошибки — **окружение**:
устаревший запущенный воркер (не перезапущен после R7a/R7b или собран до того, как `pydantic_data_converter`
попал в `worker.py:98`), либо иная версия `temporalio` в деплое, где дефолтный конвертер для Pydantic v2
возвращает `dict`. **Дыра в тестах подтверждает риск**: `tests/test_workflow/test_search_global.py` и
`test_search_router.py` проверяют только чистые хелперы (`rank_summaries`, `dispatch_for_route`); ни один тест
не гоняет воркфлоу через реальный `WorkflowEnvironment` с конвертером — путь декодирования аргументов
(ровно там, где живёт баг) не покрыт вообще.

План поэтому: (Phase 1) операционно подтвердить/перезапустить воркер; закрыть дыру регрессионным тестом
через настоящий `WorkflowEnvironment`; добавить дешёвую defense-in-depth коэрцию dict→model.
(Phase 2) почистить мёртвый код по аудиту. (Phase 3) принять решение по рефлексивному синтезу.

**3. Аудит старого кода (полный список — в Phase 2).**
Мёртвые модули: `src/retrieval/agent.py`, `judge.py`, `react_agent.py`, `reflective_synth.py`,
`query_engine.py` (импортируются только из тестов). Не подключённые: `hybrid.py`, `reranker.py`.
Сироты-конфиги: `LLMRole` `distill`/`coverage`; `AgentSettings.{max_rounds, max_iterations, max_refinements,
distill_enabled, distill_min_chars, max_coverage_checks}`. Дубли: `_deduplicate_nodes`/`_node_to_citation` в
`agent.py` против канонических в `_common.py`. Мёртвая обёртка `atomic_tools.deduplicate_sources`.

---

## File Structure

**Phase 1 (drift):**
- Modify: `src/workflow/search/global_wf.py` — defense-in-depth коэрция `params`/`drift_seed` в начале `run`.
- Create: `tests/test_workflow/test_search_drift_roundtrip.py` — регрессионный e2e через `WorkflowEnvironment`.
- Read-only: `src/workflow/worker.py`, `src/workflow/client.py` (проверка конвертера; правок не требуется).

**Phase 2 (cleanup):**
- Delete: `src/retrieval/{agent.py,judge.py,react_agent.py,reflective_synth.py,query_engine.py}` (после Phase 3).
- Modify: `src/retrieval/atomic_tools.py` (убрать `deduplicate_sources`), `src/config.py` (убрать сироты-роли/поля),
  `src/di/providers.py` (убрать инстанцирование `LLMJudge`), `src/models/search.py` (убрать сироты-поля ответа),
  `src/workflow/activities/synthesize_answer.py` (убрать `selfrag`-ветку — зависит от Phase 3).
- Modify: соответствующие `tests/test_retrieval/*` (удалить тесты мёртвых модулей).

**Phase 3 (decision):**
- Doc-only либо restore-path — см. задачу.

---

## PHASE 1 — Drift-баг: подтвердить, покрыть, защитить

### Task 1: Операционно подтвердить причину (running worker)

**Files:** нет правок кода — диагностика окружения.

- [ ] **Step 1: Узнать версию temporalio в активном окружении воркера**

Run (на машине/в контейнере, где крутится воркер, НЕ в dev-shell):
```bash
uv run python -c "import temporalio; print('temporalio', temporalio.__version__)"
```
Expected: `temporalio 1.27.2`. Если версия ниже и/или отличается от dev — это кандидат в корневую причину
(старый дефолтный конвертер возвращает dict для Pydantic v2).

- [ ] **Step 2: Подтвердить, что воркер собран с pydantic-конвертером и актуальным кодом**

Run:
```bash
git -C /Users/a.tsitanov/projects/kb-llamaindex log -1 --format='%H %s' -- src/workflow/worker.py
grep -n "pydantic_data_converter" src/workflow/worker.py src/workflow/client.py
```
Expected: `worker.py:98` и `client.py` оба передают `data_converter=pydantic_data_converter`. Если запущенный
процесс стартовал до этого коммита — он устарел.

- [ ] **Step 3: Перезапустить воркер на текущем коде и повторить drift-запрос**

Перезапустить процесс `uv run python -m src.workflow.worker`, затем:
```bash
curl -s -X POST localhost:8000/api/v1/search/drift \
  -H "Authorization: Bearer $API_KEY" -H 'Content-Type: application/json' \
  -d '{"query":"тест drift","top_k":5}' | head -c 400
```
Expected: HTTP 200 без `AttributeError`. Зафиксировать результат (прошло/нет) — он определяет, был ли баг
чисто окружения. Регрессионный тест (Task 2) ставится в любом случае.

- [ ] **Step 4: Зафиксировать версию temporalio как пол**

Открыть `pyproject.toml`, убедиться, что у `temporalio` нижняя граница `>=1.27` (а не `*`/слишком низкая).
Если ниже — поднять нижнюю границу до `>=1.27,<2`, чтобы дефолтный-конвертер-возвращает-dict не вернулся.

- [ ] **Step 5: Commit (если правился pyproject)**

```bash
git add pyproject.toml uv.lock
git commit -m "build(deps): pin temporalio>=1.27 (pydantic v2 payload decode)"
```

---

### Task 2: Регрессионный тест — drift через настоящий WorkflowEnvironment

Закрывает дыру: гоняет точную падавшую сигнатуру `args=[GlobalSearchParams(drift_mode=True),
list[SerializedNode]]` внутри реального воркера с `pydantic_data_converter`. Если баг реален в любом
окружении — тест падает с тем самым `AttributeError`; на текущем коде — проходит и навсегда стережёт путь.

**Files:**
- Test: `tests/test_workflow/test_search_drift_roundtrip.py` (создать)

- [ ] **Step 1: Написать падающий/сторожевой тест**

Create `tests/test_workflow/test_search_drift_roundtrip.py`:
```python
"""Regression: drift path decodes GlobalSearchParams (not dict) inside a
real Temporal worker with the pydantic data converter.

Guards the exact call that raised
``AttributeError: 'dict' object has no attribute 'drift_mode'`` —
``GlobalSearchWorkflow.run(args=[GlobalSearchParams(drift_mode=True),
list[SerializedNode]])`` — which the pure-helper tests never exercised.
"""

import pytest
from temporalio import activity
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from src.config import settings
from src.workflow.contracts import (
    GlobalSearchParams,
    MapCommunitiesParams,
    MapCommunitiesResult,
    MapPartialParams,
    MapPartialResult,
    SerializedNode,
    SynthesizeParams,
    SynthesizeResult,
)
from src.workflow.search.global_wf import GlobalSearchWorkflow


@activity.defn(name="map_communities")
async def _map_communities(p: MapCommunitiesParams) -> MapCommunitiesResult:
    return MapCommunitiesResult(communities=[])


@activity.defn(name="map_community_partial")
async def _map_community_partial(p: MapPartialParams) -> MapPartialResult:
    return MapPartialResult(community_id=p.community_id, partial="", score=0.0)


@activity.defn(name="synthesize_answer")
async def _synthesize_answer(p: SynthesizeParams) -> SynthesizeResult:
    # Echo the mode back through the answer so the test can assert the
    # workflow read params.drift_mode correctly (mode flows into outcome).
    return SynthesizeResult(text=f"ok:{p.mode}", refinement_rounds=0)


@pytest.mark.asyncio
async def test_global_drift_decodes_params_not_dict(monkeypatch):
    # synthesize_answer is pinned to large_task_queue inside the workflow;
    # point it at the single test queue so one worker hosts everything.
    monkeypatch.setattr(settings.temporal, "large_task_queue", "drift-test-q")

    async with await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter,
    ) as env:
        async with Worker(
            env.client,
            task_queue="drift-test-q",
            workflows=[GlobalSearchWorkflow],
            activities=[_map_communities, _map_community_partial, _synthesize_answer],
        ):
            outcome = await env.client.execute_workflow(
                GlobalSearchWorkflow.run,
                args=[
                    GlobalSearchParams(query="q", drift_mode=True),
                    [SerializedNode(chunk_id="c1", text="seed", score=0.9)],
                ],
                id="drift-rt-1",
                task_queue="drift-test-q",
            )

    assert outcome.mode == "drift"          # would be "global" if drift_mode lost
    assert outcome.answer == "ok:simple"    # REDUCE synth uses mode="simple"
```

- [ ] **Step 2: Запустить — убедиться, что тест валиден (на текущем коде проходит)**

Run: `uv run pytest tests/test_workflow/test_search_drift_roundtrip.py -v`
Expected: PASS на текущем диск-коде (подтверждает, что воркфлоу читает `params.drift_mode` и помечает
`mode="drift"`). Если в окружении воспроизводится баг — здесь будет тот самый `AttributeError`, и тест
служит точкой воспроизведения для Phase-1/Task-1.

> Примечание: если `outcome.answer` не равен `"ok:simple"` (REDUCE мог звать синтез с другим `mode`),
> поправить ожидание под фактический `mode`, который `build_reduce_call` кладёт в `SynthesizeParams`
> (см. `src/workflow/search/global_wf.py` ниже строки 213) — но `outcome.mode == "drift"` менять нельзя,
> это и есть проверка бага.

- [ ] **Step 3: Commit**

```bash
git add tests/test_workflow/test_search_drift_roundtrip.py
git commit -m "test(search): e2e drift roundtrip guards GlobalSearchParams decode (drift_mode regression)"
```

---

### Task 3: Defense-in-depth — коэрция dict→model на входе GlobalSearchWorkflow.run

Дешёвая страховка независимо от версии конвертера в любом будущем деплое: если `params`/`drift_seed`
всё-таки придут как `dict`/`list[dict]`, привести их к моделям до первого обращения к атрибуту. Симптом
не маскирует корневую причину (она операционная и закрыта Task 1), но делает воркфлоу устойчивым к
конвертер-фолбэку — рекомендованный паттерн defense-in-depth поверх найденной причины.

**Files:**
- Modify: `src/workflow/search/global_wf.py:133-141`

- [ ] **Step 1: Написать падающий unit-тест на коэрцию (без Temporal)**

Добавить в `tests/test_workflow/test_search_global.py`:
```python
def test_coerce_params_accepts_dict():
    from src.workflow.search.global_wf import _coerce_global_params
    from src.workflow.contracts import GlobalSearchParams

    out = _coerce_global_params({"query": "q", "drift_mode": True})
    assert isinstance(out, GlobalSearchParams)
    assert out.drift_mode is True
    # passthrough for already-typed input
    typed = GlobalSearchParams(query="q")
    assert _coerce_global_params(typed) is typed
```

- [ ] **Step 2: Запустить — убедиться, что падает (нет функции)**

Run: `uv run pytest tests/test_workflow/test_search_global.py::test_coerce_params_accepts_dict -v`
Expected: FAIL — `ImportError: cannot import name '_coerce_global_params'`.

- [ ] **Step 3: Реализовать хелпер и применить в run**

В `src/workflow/search/global_wf.py`, рядом с прочими pure-хелперами (после импортов, до класса) добавить:
```python
def _coerce_global_params(params: GlobalSearchParams | dict) -> GlobalSearchParams:
    """Belt-and-suspenders: a misconfigured data converter (or an older
    temporalio) can hand workflow args back as plain ``dict`` instead of
    the typed model.  Coerce so ``params.drift_mode`` never raises."""
    if isinstance(params, dict):
        return GlobalSearchParams(**params)
    return params
```
В методе `run` заменить начало тела:
```python
        log = workflow.logger
        t_start = workflow.now()
        mode: SearchMode = "drift" if params.drift_mode else "global"
```
на:
```python
        log = workflow.logger
        t_start = workflow.now()
        params = _coerce_global_params(params)
        if drift_seed and isinstance(drift_seed[0], dict):
            drift_seed = [SerializedNode(**n) for n in drift_seed]
        mode: SearchMode = "drift" if params.drift_mode else "global"
```

- [ ] **Step 4: Запустить оба теста**

Run: `uv run pytest tests/test_workflow/test_search_global.py::test_coerce_params_accepts_dict tests/test_workflow/test_search_drift_roundtrip.py -v`
Expected: PASS оба.

- [ ] **Step 5: Commit**

```bash
git add src/workflow/search/global_wf.py tests/test_workflow/test_search_global.py
git commit -m "fix(search): coerce dict->GlobalSearchParams in global_wf.run (drift_mode AttributeError defense)"
```

---

### Task 3b: Synthesize-timeout hardening — ограничить контекст синтеза (вкл. fail-open rerank)

Корневая цепочка живого инцидента: rerank упал → orchestrator fail-open отдаёт в синтез **весь**
неранжированный merged-пул (не top-N) → большой контекст → `synthesize_answer` не укладывается в
`start_to_close=5min` → StartToClose timeout → ретраи → падение. Синтез НИКОГДА не должен получать
неограниченный пул. Ограничиваем пул перед синтезом независимо от исхода rerank.

**Files:**
- Modify: `src/workflow/search/orchestrator.py` (pure helper `cap_synth_sources` + применить на fallback)
- Test: `tests/test_workflow/test_search_orchestrator.py`
- Doc: приложение про несостыковку таймаутов (300s Temporal vs 900s LiteLLM) — фикс таймаутов отдельно,
  после замера реальной латентности large-модели.

- [ ] **Step 1: Падающий тест на pure-хелпер**

Добавить в `tests/test_workflow/test_search_orchestrator.py`:
```python
def test_cap_synth_sources_bounds_pool():
    from src.workflow.search.orchestrator import cap_synth_sources
    from src.workflow.contracts import SerializedNode

    pool = [SerializedNode(chunk_id=str(i), text="t") for i in range(20)]
    out = cap_synth_sources(pool, 5)
    assert len(out) == 5
    assert [n.chunk_id for n in out] == ["0", "1", "2", "3", "4"]
    # top_n<=0 → return as-is (defensive: never silently empty the context)
    assert cap_synth_sources(pool, 0) == pool
```

- [ ] **Step 2: Запустить — убедиться, что падает**

Run: `uv run pytest tests/test_workflow/test_search_orchestrator.py::test_cap_synth_sources_bounds_pool -v`
Expected: FAIL — `ImportError: cannot import name 'cap_synth_sources'`.

- [ ] **Step 3: Реализовать хелпер и применить**

В `src/workflow/search/orchestrator.py` рядом с `build_synthesize_call` добавить:
```python
def cap_synth_sources(
    sources: list[SerializedNode], top_n: int,
) -> list[SerializedNode]:
    """Bound the pool fed to synthesis.  Rerank already trims to top_n;
    on the FAIL-OPEN fallback the workflow passes the raw merged pool —
    cap it here too so a flaky reranker can never blow up the synthesis
    prompt (and the 5-min start_to_close).  ``top_n<=0`` ⇒ no cap."""
    if top_n <= 0:
        return sources
    return sources[:top_n]
```
В методе `run`, в except-ветке rerank (orchestrator.py:259-262) после warning добавить:
```python
            synth_sources = cap_synth_sources(
                merged, settings.temporal.rerank_top_n,
            )
```

- [ ] **Step 4: Прогнать тесты оркестратора**

Run: `uv run pytest tests/test_workflow/test_search_orchestrator.py -v`
Expected: PASS (новый + существующие).

- [ ] **Step 5: Commit**

```bash
git add src/workflow/search/orchestrator.py tests/test_workflow/test_search_orchestrator.py
git commit -m "fix(search): cap synthesis context on fail-open rerank (synthesize timeout hardening)"
```

> Таймауты (300s Temporal vs 900s LiteLLM) — отдельная настройка: либо поднять `synthesize start_to_close`
> под реальную латентность large-модели, либо снизить LiteLLM `timeout_s`, чтобы падать быстро с ошибкой LLM,
> а не от Temporal. Требует замера `curl`-латентности large-модели — не делаем вслепую в этом таске.

---

## PHASE 2 — Аудит: удаление мёртвого кода (только после Phase 3 по рефлексивному синтезу)

> Перед каждым удалением — подтвердить отсутствие импортов из `src/` (не из `tests/`):
> `grep -rn "import <name>\|from .*<module> import" src/`. Удаляем только то, что тянут лишь тесты.
> Соответствует принципу из памяти: opt-in, без слепых замен; тесты держим зелёными.

### Task 4: Удалить мёртвую обёртку `deduplicate_sources`

**Files:**
- Modify: `src/retrieval/atomic_tools.py` (удалить функцию `deduplicate_sources`, ~line 508)

- [ ] **Step 1: Подтвердить, что не используется**

Run: `grep -rn "deduplicate_sources" src/`
Expected: единственная строка — определение в `atomic_tools.py`. Если есть другие — НЕ удалять, остановиться.

- [ ] **Step 2: Удалить функцию**

Удалить тело `def deduplicate_sources(...)` в `src/retrieval/atomic_tools.py`.

- [ ] **Step 3: Прогнать тесты retrieval**

Run: `uv run pytest tests/test_retrieval -q`
Expected: PASS (или отсутствие новых падений относительно базлайна).

- [ ] **Step 4: Commit**

```bash
git add src/retrieval/atomic_tools.py
git commit -m "refactor(retrieval): drop dead deduplicate_sources wrapper"
```

### Task 5: Убрать сироты-поля конфигурации

**Files:**
- Modify: `src/config.py` — `LLMRole` (убрать `"distill"`, `"coverage"`), `_DEFAULT_ROLE_TIERS`,
  `AgentSettings` поля `max_rounds, max_iterations, max_refinements, distill_enabled, distill_min_chars,
  max_coverage_checks`.

- [ ] **Step 1: Подтвердить, что каждое поле не читается в src/**

Run:
```bash
for n in distill coverage max_rounds max_iterations max_refinements distill_enabled distill_min_chars max_coverage_checks; do
  echo "== $n =="; grep -rn "$n" src/ | grep -v "config.py"; done
```
Expected: для удаляемых — пусто (только определения в `config.py`). Внимание: `max_refinements` ЕСТЬ в
`OrchestratorParams`/`GlobalSearchParams`/`SynthesizeParams` — это НЕ то же поле; удаляем только
`AgentSettings.max_refinements`, если grep подтверждает, что его никто не читает. `coverage` как роль
удаляем, но `coverage_check_enabled`/`max_coverage_rounds` ОСТАВЛЯЕМ (используются).

- [ ] **Step 2: Удалить подтверждённые сироты-поля и значения ролей**

Внести правки в `src/config.py`: убрать `"distill", "coverage"` из `LLMRole` и из `_DEFAULT_ROLE_TIERS`;
удалить перечисленные подтверждённые поля `AgentSettings`.

- [ ] **Step 3: Прогнать config-тесты + импорт-смоук**

Run: `uv run pytest tests/test_config -q && uv run python -c "from src.config import settings; print('ok')"`
Expected: PASS и `ok`.

- [ ] **Step 4: Commit**

```bash
git add src/config.py
git commit -m "refactor(config): drop orphaned distill/coverage roles + dead AgentSettings knobs"
```

### Task 6: Удалить мёртвые модули retrieval (judge / agent / react_agent / query_engine)

> `reflective_synth.py` — отдельно, зависит от Phase 3.

**Files:**
- Delete: `src/retrieval/judge.py`, `src/retrieval/agent.py`, `src/retrieval/react_agent.py`,
  `src/retrieval/query_engine.py`
- Modify: `src/di/providers.py` (убрать инстанцирование `LLMJudge`, ~line 98)
- Delete: соответствующие `tests/test_retrieval/test_*` для этих модулей

- [ ] **Step 1: Подтвердить, что в src/ нет живых импортов**

Run:
```bash
grep -rn "retrieval.judge\|retrieval.agent\b\|react_agent\|query_engine\|LLMJudge\|agentic_search\|agentic_react_search" src/ | grep -v "reflective_synth"
```
Expected: только `src/di/providers.py` (LLMJudge). Если что-то ещё — остановиться и пересмотреть.

- [ ] **Step 2: Убрать LLMJudge из DI**

В `src/di/providers.py` удалить импорт `LLMJudge` и его регистрацию/инстанцирование (~line 98) и любые
геттеры `judge`, на которые никто не ссылается (проверить `grep -rn "\.judge" src/`).

- [ ] **Step 3: Удалить модули и их тесты**

```bash
git rm src/retrieval/judge.py src/retrieval/agent.py src/retrieval/react_agent.py src/retrieval/query_engine.py
git rm tests/test_retrieval/test_agent*.py tests/test_retrieval/test_judge*.py \
       tests/test_retrieval/test_react*.py tests/test_retrieval/test_*query_engine*.py 2>/dev/null || true
```
(перед `git rm` тестов — `ls tests/test_retrieval/` и удалить ровно те файлы, что покрывают эти модули.)

- [ ] **Step 4: Полный прогон + импорт-смоук воркера/апи**

Run:
```bash
uv run python -c "import src.workflow.worker, src.api.main; print('imports ok')"
uv run pytest tests/test_retrieval tests/test_di tests/test_api -q
```
Expected: `imports ok` и PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(retrieval)!: remove dead judge/agent/react_agent/query_engine (post-R7b cutover)"
```

### Task 7: Зафиксировать статус hybrid.py (НЕ трогать reranker.py)

> ВАЖНАЯ ПОПРАВКА К АУДИТУ: `src/retrieval/reranker.py` **активен** — его тянет
> `src/workflow/_search_deps.py:178 get_reranker()` ← активити `rerank_sources` ← `SearchOrchestratorWorkflow`
> (orchestrator.py:241). `build_reranker()` вызывает `configure_hf()` и грузит `BAAI/bge-reranker-v2-m3` через
> sentence-transformers/HF-кэш. Сабагент-аудитор пропустил indirection через `get_reranker` — reranker.py
> УДАЛЯТЬ НЕЛЬЗЯ. Не подключён только `hybrid.py` (BM25+dense RRF).

**Files:** нет правок кода — пометка.

- [ ] **Step 1: Зафиксировать статус**

Подтвердить, что `hybrid.py` тянут только тесты: `grep -rn "retrieval.hybrid\|build_hybrid\|build_bm25" src/`
(ожидать пусто). Внести в `docs/SEARCH.md`: «`retrieval/hybrid.py` — не подключён; кандидат под бенч-эксперимент.
`retrieval/reranker.py` — активен через `_search_deps.get_reranker` (rerank_sources), грузит BGE из HF-кэша».
Решение об интеграции `hybrid.py` — отдельный эксперимент через `tests/eval/`.

- [ ] **Step 2: Commit (docs)**

```bash
git add docs/SEARCH.md
git commit -m "docs(search): mark hybrid unwired; note reranker is active via get_reranker"
```

---

## PHASE 3 — Рефлексивный синтез (наследие selfrag): осознанное решение

### Task 8: Решить — удалить или вернуть в строй рефлексивный синтез

`reflective_synthesize` + ветка `mode=="selfrag"` в `synthesize_answer` живы, но недостижимы. Это вопрос
продукта, не механики: рефлексивный синтез (self-critique + ре-ретрив по `[NEED:...]`) — это качество
ответа, которое старый selfrag давал, а новый детерминированный оркестратор не делает.

**Files:** зависит от решения — либо удаление (см. ниже), либо новый opt-in путь.

- [ ] **Step 1: Поднять решение пользователю (две опции)**

  - **(A) Удалить наследие**: убрать `src/retrieval/reflective_synth.py`, ветку `if params.mode ==
    "selfrag"` в `src/workflow/activities/synthesize_answer.py`, значения `"agent"/"selfrag"` из `SearchMode`
    (`contracts.py:30`) и сироты-поля ответа в `src/models/search.py` (`ReflectiveAnswerDetail`,
    `agentic_rounds`, `agentic_round_stats`, `agentic_step_stats`, `answer_detail`, если не используются
    активным путём — проверить grep). Чистый код, минус возможность.
  - **(B) Вернуть как opt-in флаг** (память: «opt-in swaps, never blind replacement; benchmark before
    adopting»): добавить `OrchestratorParams.reflective: bool = False`; когда `True`, оркестратор передаёт
    `mode="selfrag"` в `synthesize_answer`; замерить на `tests/eval/golden_qa` против дефолта, прежде чем
    включать. Сохраняет качество-фичу, не ломая дефолт.

  Это развилка для `AskUserQuestion` при исполнении плана — НЕ выбирать молча.

- [ ] **Step 2 (если A): удалить и прогнать**

Удалить перечисленное, затем `grep -rn "selfrag\|reflective\|ReflectiveAnswer\|answer_detail" src/` →
ожидать пусто; `uv run pytest -q` → PASS; обновить `README.md`/`docs/SEARCH.md`, где ещё упомянут `/selfrag`.
Commit: `refactor(search)!: remove unreachable reflective-synth (selfrag) remnants`.

- [ ] **Step 2 (если B): реализовать opt-in + бенч**

TDD: тест на `OrchestratorParams.reflective=True` → `synthesize_answer` зовётся с `mode="selfrag"`;
прогон `tests/eval/run_answer_eval.py` на `golden_qa` для сравнения. Commit:
`feat(search): opt-in reflective synthesis flag (benchmarked, default off)`.

- [ ] **Step 3: Обновить документацию о наследнике selfrag**

В `docs/SEARCH.md` добавить раздел «selfrag → наследник»: selfrag удалён в `f6cb02d`; местный детерминированный
наследник — `SearchOrchestratorWorkflow` (`/search/local`); рефлексивный синтез — статус по итогу Step 1.
Поправить устаревшие упоминания `/selfrag` в `README.md`.

---

## PHASE 4 — Offline HF-модели: плоский local_dir вместо blobs+symlinks

**Проблема.** `scripts/download_models.py` качает GLiNER и BGE-reranker **в кэш** (`GLiNER.from_pretrained` /
`SentenceTransformer` пишут в `HF_HOME`/`SENTENCE_TRANSFORMERS_HOME`/`TRANSFORMERS_CACHE`). Кэш HF имеет
штатный content-addressed layout: `models--org--name/blobs/<sha>` (реальные файлы) + `snapshots/<rev>/file →
../../blobs/<sha>` (симлинки). Это НЕ зависит от версии пакета (hub 0.36.2 / hf-xet 1.5.0 /
sentence-transformers 5.1.2 — у всех так; hf-xet меняет только транспорт, не layout). Боль реальна для
air-gapped: при копировании кэша (`scp`/`docker COPY`/`tar` без dereference) симлинки рвутся → offline-загрузка
падает «модель не найдена».

**Решение.** `huggingface_hub.snapshot_download(repo_id, local_dir=...)` (в hub ≥0.23, значит и 0.36.2) пишет
ПЛОСКИЕ реальные файлы без blobs/симлинков (остаётся лишь служебный `.cache/huggingface/` — игнор/удалить),
такую папку тривиально переносить. Грузить модели по пути с `local_files_only=True`.

**Files:**
- Modify: `scripts/download_models.py` (добавить `--local-dir` режим через `snapshot_download`)
- Test: `tests/test_scripts/test_download_models.py` (создать/дополнить)
- Modify (опц., Step 5): `src/retrieval/reranker.py`, `src/graph/gliner_extract.py` — приём локального пути
- Modify: `docs/MODELS.md` (offline-раздел: local_dir-вариант)

### Task 9: Режим `--local-dir` в download_models.py

- [ ] **Step 1: Падающий тест (snapshot_download зовётся с local_dir)**

Create/добавить в `tests/test_scripts/test_download_models.py`:
```python
def test_snapshot_writes_flat_local_dir(monkeypatch, tmp_path):
    import scripts.download_models as dm

    calls = {}
    def fake_snapshot_download(*, repo_id, local_dir, **kw):
        calls["repo_id"] = repo_id
        calls["local_dir"] = local_dir
        return local_dir
    monkeypatch.setattr(dm, "snapshot_download", fake_snapshot_download, raising=False)

    dm._snapshot("BAAI/bge-reranker-v2-m3", str(tmp_path))
    assert calls["repo_id"] == "BAAI/bge-reranker-v2-m3"
    # flat dest = <local_dir>/<repo-leaf>, no blobs/snapshots involved
    assert calls["local_dir"].endswith("/bge-reranker-v2-m3")
```

- [ ] **Step 2: Запустить — убедиться, что падает**

Run: `uv run pytest tests/test_scripts/test_download_models.py::test_snapshot_writes_flat_local_dir -v`
Expected: FAIL — `AttributeError: module 'scripts.download_models' has no attribute '_snapshot'`.

- [ ] **Step 3: Реализовать `_snapshot` + флаг `--local-dir`**

В `scripts/download_models.py` добавить импорт и хелпер:
```python
from huggingface_hub import snapshot_download  # noqa: E402  (рядом с прочими импортами)


def _snapshot(repo_id: str, local_dir: str) -> None:  # pragma: no cover
    """Download a repo into a FLAT local dir (real files, no blobs/symlinks)."""
    dest = Path(local_dir) / repo_id.split("/")[-1]
    snapshot_download(repo_id=repo_id, local_dir=str(dest))
    logger.info("download_models: {r} -> {d}", r=repo_id, d=dest)
```
В `build_arg_parser` добавить:
```python
    parser.add_argument(
        "--local-dir",
        default=None,
        help="Write FLAT real files here (no blobs/symlinks) instead of the HF cache.",
    )
```
В `main`, перед кэш-веткой, развилка:
```python
    if args.local_dir:
        if want_gliner:
            _snapshot(gliner_model, args.local_dir)
        if want_reranker:
            _snapshot(rerank_model, args.local_dir)
        logger.info(
            "download_models: flat models in {d}. Point HF_RERANK_MODEL / "
            "gliner_model at <dir>/<model-leaf> and load with local_files_only=True.",
            d=args.local_dir,
        )
        return 0
    # else: existing cache-based path (_force_online + from_pretrained)
```

- [ ] **Step 4: Запустить тест**

Run: `uv run pytest tests/test_scripts/test_download_models.py::test_snapshot_writes_flat_local_dir -v`
Expected: PASS.

- [ ] **Step 5 (опц.): грузить по пути с local_files_only**

Если `settings.hf.rerank_model` / `settings.ingestion.gliner_model` — это локальный путь (содержит `/` и
существует как директория), грузить с `local_files_only=True`:
- `src/retrieval/reranker.py:build_reranker` → `SentenceTransformerRerank(model=resolved_model, ...)` уже
  принимает путь; добавить, что при offline передаётся `local_files_only=True` (через `configure_hf` env уже
  выставлен `HF_HUB_OFFLINE=1`, так что отдельный аргумент не обязателен — но путь к плоской папке работает).
- GLiNER: `GLiNER.from_pretrained(path, local_files_only=settings.hf.offline)`.
Тест: подсунуть tmp-папку как модель, замокать загрузчик, проверить, что зовётся с путём.

- [ ] **Step 6: Документация + commit**

Обновить `docs/MODELS.md` (offline-раздел): добавить рецепт
`python -m scripts.download_models --local-dir /data/models` + перенос плоской папки + `HF_RERANK_MODEL=/data/models/bge-reranker-v2-m3`.
```bash
git add scripts/download_models.py tests/test_scripts/test_download_models.py docs/MODELS.md src/retrieval/reranker.py src/graph/gliner_extract.py
git commit -m "feat(models): --local-dir flat HF download (no blobs/symlinks) for portable offline cache"
```

---

## Приложение: диагностика «synthesize_answer завис»

Control-flow оркестратора (`orchestrator.py`): `plan → retrieve(children) → merge → coverage-gate →
rerank_sources (FAIL-OPEN, очередь kb-search-small) → synthesize_answer (PINNED на kb-search-large)`.
Ключевые факты:
- `rerank_sources` обёрнут в `try/except` (orchestrator.py:240-262): ошибка rerank **НЕ блокирует** синтез —
  логируется warning и идём дальше на merged-пуле. Но перед этим FAST_RETRY = 3 попытки (до ~30s бэкофф) ×
  `start_to_close=3min` → до ~9-12 мин «как будто висит», ПОКА не дойдёт до синтеза.
- `rerank_sources` грузит BGE через `get_reranker → build_reranker → configure_hf` (HF-кэш). Если кэш-симлинки
  битые / offline-env не выставлен → rerank падает ИЛИ **виснет на загрузке модели** (попытка достучаться до
  Хаба). Это прямая связь с Phase 4 / вашим offline-вопросом.
- `synthesize_answer` пиннится на `large_task_queue="kb-search-large"`, который обслуживает ТОЛЬКО
  `large_worker` с `large_activity_concurrency=2`. Несколько задач → одновременно только 2 синтеза, остальные
  ждут в `Scheduled`. Если large-воркер не поднят / имя очереди не совпадает / большой LLM (LiteLLM) висит —
  активити стоит в `Scheduled` или `Started`-без-`Completed`.

**Как проверить, что synthesize_answer реально выполняется (по порядку):**

1. **Узнать фазу воркфлоу — это сразу разводит «rerank» vs «synthesize»:**
   ```bash
   temporal workflow query --workflow-id search-local-<id> --type get_state
   ```
   `phase=="rerank"` → висит rerank (НЕ синтез) — смотрите HF-модель. `phase=="synthesize"` → дошли до синтеза.

2. **Event history / pending activities (Temporal UI или CLI):**
   ```bash
   temporal workflow describe --workflow-id search-local-<id>
   ```
   В Pending Activities у `synthesize_answer`:
   - есть `ScheduledTime`, нет `LastStartedTime` → **на kb-search-large нет поллера** (large-воркер не запущен /
     не то имя очереди) ИЛИ оба слота заняты. Это самая частая причина «зависания».
   - есть `LastStartedTime`, растёт `Attempt`, в `lastFailure` — ошибка → активити падает и ретраит (смотрите текст).
   - `LastStartedTime` есть, фейлов нет, но `LastHeartbeatTime` старый → застряли ВНУТРИ `synthesizer.asynthesize`
     (большой LLM/LiteLLM не отвечает). synthesize_answer шлёт heartbeat `{stage: plain_synth}` — по нему видно.

3. **Есть ли поллеры на large-очереди:**
   ```bash
   temporal task-queue describe --task-queue kb-search-large
   ```
   Пустой список pollers → `large_worker` не обслуживает очередь → синтез будет вечно в `Scheduled`.
   Сверьте, что воркер логнул при старте `temporal worker  large_queue=kb-search-large  large_concurrency=2`
   и что `TEMPORAL_LARGE_TASK_QUEUE` в API и в воркере совпадают.

4. **Метрики Prometheus воркера:** `temporal_activity_schedule_to_start_latency{task_queue="kb-search-large"}`
   растёт без предела → нет поллера/насыщение. `temporal_worker_task_slots_available{task_queue="kb-search-large"}==0`
   → оба слота заняты (head-of-line: первый синтез висит на большом LLM, остальные ждут).

5. **Проверить сам большой LLM:** synthesize использует `get_synthesis_synthesizer()` (large tier,
   `build_synthesis_llm`). Дёрнуть LiteLLM-прокси на large-модель напрямую — если висит там, синтез будет
   стоять до `start_to_close=5min` × 3 ретрая.

**Вывод по «связано ли с rerank»:** скорее НЕТ как блокиратор (rerank fail-open), но ДА как симптом общей
причины — HF-модель reranker и порядок фаз. Чаще всего реальная картина одна из: (а) воркфлоу ещё в фазе
`rerank` (ретраи/зависшая загрузка BGE — лечится Phase 4 offline), либо (б) синтез стоит в `Scheduled` на
kb-search-large из-за непод­нятого large-воркера / насыщения concurrency=2 / зависшего большого LLM. Шаги 1–3
однозначно разводят эти случаи.

## Self-Review

**Spec coverage:**
- «куда делся selfrag + наследник» → Контекст-расследования п.1 + Task 8/Step 3 (док).
- «drift AttributeError» → Phase 1 (Task 1 причина, Task 2 регресс-тест, Task 3 защита).
- «аудит старого кода» → Контекст п.3 + Phase 2 (Tasks 4–7) + Phase 3 (Task 8).
- «план по работе» → этот документ.

**Placeholder scan:** код тестов и правок приведён целиком; сигнатуры (`_coerce_global_params`,
`GlobalSearchParams`, `SerializedNode`, активити-стабы) согласованы с `src/workflow/contracts.py`.

**Type consistency:** `_coerce_global_params` определён в Task 3 и используется там же; имена полей контрактов
(`drift_mode`, `community_id`, `partial`, `score`, `chunk_id`, `text`) сверены с `contracts.py`.

**Открытые развилки (требуют `AskUserQuestion` при исполнении):** Task 8 Step 1 (A vs B).
