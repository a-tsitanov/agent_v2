# Changeset: search context-control + completeness

Portable bundle of the agent-loop improvements that follow the
tool-calling fixes (see `../search-agent-tools`). Baseline = commit
`14e1c2b`; final = `25663ac`. Three commits, three concerns:

1. **Observation distillation** (`496baef`) — bound reasoning-context
   growth on large corpora.
2. **Advisory relevance** (`6ff51b3`) — never drop sources, no fact loss.
3. **Pre-submit coverage check** (`25663ac`) — answer completeness.

## Problem → fix

### 1. Context overflow
Every reasoning step re-sends the full message history; big observations
(`read_full_document` ≤20k chars, `graph_search`, `get_chunks_by_doc_id`)
accumulate over up to 8 iterations and silently overflow the model's
context window (ollama then truncates the *front* — losing the system
prompt).

- New `distill_observation` activity: for observations larger than
  `distill_min_chars`, one LLM call compresses them to query-relevant
  facts + a relevance verdict. The distilled text goes into the agent's
  history; a hard `observation_max_chars` cap backstops it.

### 2. Fact loss risk
The relevance verdict initially gated the accumulator (dropping
"irrelevant" sources). A weak local model can misgrade and lose a fact.

- Relevance is now **advisory only**: full `NodeWithScore` sources
  ALWAYS reach `synthesize_answer`. Relevance just shapes the agent's
  history note ("dead end") and step stats.

### 3. Incompleteness
The agent self-decides when to `submit_answer`; a weak model stops early
and never checks whether multi-part questions are fully covered.

- New `coverage_check` activity: on `submit_answer`, an LLM judges
  whether the gathered evidence covers the whole question. If a gap is
  named, it's injected into the history and one more retrieval round
  runs. Bounded by `max_coverage_checks` (default 1) + `max_iterations`;
  fail-open so a flaky check never traps the agent.

## Files
- `src/workflow/activities/distill_observation.py` — new activity.
- `src/workflow/activities/coverage_check.py` — new activity.
- `src/workflow/activities/__init__.py` — register both in SEARCH_ACTIVITIES.
- `src/workflow/contracts.py` — DistillParams/Result, CoverageParams/Result,
  SearchParams knobs, AgenticStepStatDict.relevance.
- `src/workflow/search_workflow.py` — distillation step + advisory gate +
  coverage gate in the ReAct loop.
- `src/config.py` — AgentSettings knobs (distill_*, observation_max_chars,
  coverage_check_enabled, max_coverage_checks).
- `src/api/routes/{agent,selfrag}.py`, `src/mcp/search_server.py` —
  resolve knobs from AgentSettings at submit time → SearchParams
  (replay-safe; workflow never reads env at runtime).

## Config knobs (env, prefix `AGENT_`)
```
AGENT_DISTILL_ENABLED=true
AGENT_DISTILL_MIN_CHARS=1500
AGENT_OBSERVATION_MAX_CHARS=6000
AGENT_COVERAGE_CHECK_ENABLED=true
AGENT_MAX_COVERAGE_CHECKS=1
```

## How to reproduce elsewhere
```bash
# apply on a checkout at/near baseline 14e1c2b:
git apply full.patch
# or preserve commits:
git am commits/*.patch
# or copy files verbatim:
rsync -a files/ /path/to/kb-llamaindex/
```

## Verifying against local Ollama + LiteLLM
`tests/` holds standalone scripts (no Temporal/Milvus/Neo4j needed — they
run the activities via `temporalio.testing.ActivityEnvironment`).
```bash
LITELLM_MASTER_KEY=sk-litellm-stub \
  uvx --from 'litellm[proxy]' litellm \
  --config tests/litellm_ollama.yaml --port 4000 &

uv run python tests/test_distill.py    # 8155→57 chars; off-topic=irrelevant
uv run python tests/test_coverage.py   # partial→gap; full→complete
```

## Contents
- `full.patch` — single diff `14e1c2b..25663ac`.
- `commits/` — `git format-patch` series for `git am`.
- `files/` — full copies of every changed file at its repo path.
- `tests/` — standalone ollama/litellm validation scripts + config.
