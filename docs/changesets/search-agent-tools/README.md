# Changeset: search agent tool-calling fixes

Portable bundle of the agent function-calling fixes for `kb-llamaindex`.
Covers three related fixes to the Temporal-backed `SearchWorkflow` ReAct
loop. Baseline = commit `362d784`; final = `682d57d`.

## What was broken

1. **No parameter schemas.** The reasoning step built stub tools from
   `_never_called(**kw)`, so the LLM saw tools with no parameters and
   emitted empty/garbage args → `dispatch(**kwargs)` blew up.
2. **LLM never picked graph tools.** Descriptions framed `vector_search`
   as the default, so entity/relation questions still went to vector
   search.
3. **Malformed reasoning history.** The loop appended only `role="tool"`
   observations with no preceding `assistant` tool_call turn — an
   invalid OpenAI function-calling sequence. The model couldn't track
   what it had already called and kept falling back to the generic tool.

## The fixes (by file)

- `src/retrieval/atomic_tools.py`
  - `TOOL_FUNCTIONS` registry + `build_tool_schema(name)`: derives the
    real param schema from each tool's keyword-only args (the injected
    DI dependency is the positional arg before `*`, so it's excluded).
  - Prescriptive `TOOL_DESCRIPTIONS` that steer graph vs vector choice.
- `src/workflow/activities/agent_reasoning.py`
  - `_stub_tool` passes `fn_schema=build_tool_schema(name) or _NoArgs`
    (empty schema for the no-arg `submit_answer` terminator).
- `src/workflow/contracts.py`
  - `SerializedToolCall` (id/name/arguments) + `tool_calls` field on
    `SerializedMessage`.
- `src/workflow/_search_serde.py`
  - Build/parse OpenAI-shaped `tool_calls` in `additional_kwargs`.
- `src/workflow/search_workflow.py`
  - Tool-routing rules added to `_SYSTEM_PROMPT`.
  - Append the assistant tool_call turn (stable `call_id`,
    replay-safe) before each TOOL observation.
- `docker/litellm_config.yaml`
  - Local Ollama `gemma4:e4b` entry (test infra).

## How to reproduce elsewhere

### Option A — apply the patch (same repo, different checkout)
```bash
cd /path/to/kb-llamaindex      # at or near baseline 362d784
git apply /path/to/changeset/full.patch
# or, to preserve commits + authorship:
git am /path/to/changeset/commits/*.patch
```

### Option B — copy files verbatim
`files/` mirrors the repo layout. Copy each over the originals:
```bash
rsync -a /path/to/changeset/files/ /path/to/kb-llamaindex/
```

## Verifying against local Ollama + LiteLLM

`tests/` holds the standalone scripts used to validate this (no
Temporal/Milvus/Neo4j needed — only the LLM endpoint).

```bash
# 1. ollama running on :11434 with the model (here gemma4:e4b)
# 2. start a litellm proxy → ollama:
LITELLM_MASTER_KEY=sk-litellm-stub \
  uvx --from 'litellm[proxy]' litellm \
  --config tests/litellm_ollama.yaml --port 4000 &

# 3. run from the kb-llamaindex repo root:
uv run python tests/test_routing.py      # routing discrimination
uv run python tests/test_agent_tools.py  # 2-turn history acceptance
```

Expected `test_routing.py` output (per question type):
```
[relation  ] -> graph_search       {'query': 'ООО Ромашка'}
[identifier] -> find_entity_by_id  {'name': '+79161234567'}
[everything] -> graph_search       {'query': 'Иванов Иван Иванович'}
[topical   ] -> vector_search      {'query': '...', 'top_k': 3}
```

## Contents
- `full.patch` — single diff `362d784..682d57d` (all three fixes).
- `commits/` — `git format-patch` series for `git am`.
- `files/` — full copies of every changed file at its repo path.
- `tests/` — standalone validation scripts + litellm ollama config.
