# Hermes ↔ kb-llamaindex RAG integration — design (Approach A: foundation)

Date: 2026-06-03
Status: approved (brainstorming) → ready for implementation plan
Scope: **Approach A only** — wire the existing RAG into Hermes Agent and encode
the four "pillars" in a single skill. Learnability (Approach C: feedback capture
+ ShareGPT export) is explicitly **out of scope** and tracked as a later spec.

## Problem & goal

The RAG (`kb-llamaindex`) is currently stateless and consumed as MCP tools or
FastAPI endpoints — a one-shot "ask → answer" surface. The goal is interactive,
memory-aware, problem-oriented usage by integrating it into **Hermes Agent**
(Nous Research, Feb 2026): a persistent server-based agent that already provides
the infrastructure for the four things we want:

| Pillar (user's words)            | Where it lives                                   |
|----------------------------------|--------------------------------------------------|
| Многоходовый диалог с уточнениями | Hermes conversation loop (native)                |
| Память о пользователе/домене      | Hermes persistent memory in `~/.hermes/` (native)|
| Шаблоны ответов по типу задачи    | The `knowledge-base` skill (this spec)           |
| Обучаемость на истории            | Hermes skill-gen + ShareGPT/Atropos (later spec) |

**Key insight:** we do not build memory, a conversation loop, or a learning
system — Hermes carries those natively. The value of this work is **not the
wire** (registering an MCP server is a few lines of YAML) but **what the skill
teaches Hermes about how to use this knowledge base well**.

**Principle:** the integration is **purely additive**. We do not change the
behaviour of the existing MCP servers or FastAPI routes — we only add a way to
consume them and a skill that orients their use. (Per project rule: opt-in,
never blind replacement.)

## Relevant facts about the two systems

### Hermes Agent (the consumer)
- Persistent server agent; memory stored locally in `~/.hermes/`.
- **MCP client**: supports both stdio (command/args/env) and remote HTTP/SSE
  (`url` + `headers`) servers in `~/.hermes/config.yaml` under `mcp_servers`.
  Auto-discovers tools at startup; a checklist toggles which land in
  `mcp_servers.<name>.tools.include`.
- Tool naming: discovered tools are prefixed `mcp_<server_name>_<tool_name>`
  (e.g. `vector_search` from server `kbtools` → `mcp_kbtools_vector_search`).
- **Skills**: `SKILL.md` files, agentskills.io open standard (same format as the
  superpowers skills). Markdown body + YAML frontmatter (`name`, `description`).
  Loaded on relevance via `description`.
- Native learning: auto-documents solved problems as skills; exports
  conversations to ShareGPT; RL via Atropos. (Out of scope here.)

### kb-llamaindex (the provider) — already exposes what Hermes consumes
- **MCP-2** (`src/mcp/tools_server.py`): 6 atomic retrieval tools, stdio + SSE.
  Run: `uv run python -m src.mcp.tools_server --transport sse --host 0.0.0.0 --port 9002`.
  Tools (current, verified against source):
  - `vector_search(query, top_k=10)` → `{"sources": [...]}` (BM25+dense+RRF hybrid)
  - `graph_search(query, depth=2)` → entities + relations from the KG
  - `find_entity_by_id(name, entity_type=None)` → exact canonical-name lookup
    (E.164 phone, INN, email, SNILS, OGRN, …)
  - `find_neighbours(entity_name, hops=1)` → N-hop walk around an entity
  - `get_chunks_by_doc_id(doc_id, limit=50, offset=0)` → ordered chunks of one doc
  - `read_full_document(doc_id, max_chars=20000)` → raw original file text
  - GPU/LLM safety: the `BoundedLLM` semaphore (`settings.agent.llm_max_concurrent`)
    gates every LLM call; concurrent clients serialise behind it automatically.
- **MCP-1** (`src/mcp/search_server.py`): single `kb_search(query)` tool, stdio +
  SSE, runs `SearchOrchestratorWorkflow` (plan-execute-synthesize, local mode)
  on Temporal queue `kb-search-small`. Heavyweight one-shot.
  Run: `uv run python -m src.mcp.search_server --transport sse --host 0.0.0.0 --port 9001`.
- Auth: `KB_MCP_REQUIRE_AUTH` env + API key (`KB_API_KEY`).
- Stack the SSE service needs reachable: Milvus, Neo4j, Postgres, LiteLLM proxy.

## Architecture

### 1. Topology & transport

Run MCP-2 (and MCP-1) as **long-lived SSE services** alongside the existing
worker/API (same network, same stack) and connect Hermes via `url`. **Not** as
Hermes-spawned stdio subprocesses — a stdio subprocess would need the full
kb-llamaindex stack reachable from the Hermes host, which is fragile.

Tool surface exposed to Hermes:
- **MCP-2 (primary)** — the 6 atomic tools drive the interactive loop that Hermes
  itself orchestrates. Become `mcp_kbtools_*` in Hermes.
- **MCP-1 `kb_search` (escape hatch)** — for hard multi-hop questions where the
  atomic tools aren't enough in 2–3 steps. The skill teaches Hermes when to
  escalate. Becomes `mcp_kbsearch_kb_search`.

`~/.hermes/config.yaml`:
```yaml
mcp_servers:
  kbtools:
    url: "http://<kb-host>:9002/sse"
    headers:
      Authorization: "Bearer ${KB_API_KEY}"
    tools:
      include: [vector_search, graph_search, find_entity_by_id,
                find_neighbours, get_chunks_by_doc_id, read_full_document]
      prompts: false
      resources: false
  kbsearch:
    url: "http://<kb-host>:9001/sse"
    headers:
      Authorization: "Bearer ${KB_API_KEY}"
    tools:
      include: [kb_search]
```

**Auth flow:** kb-side `KB_MCP_REQUIRE_AUTH` / API-key ↔ Hermes
`headers.Authorization`. **Risk to verify in the plan:** confirm the FastMCP SSE
transport actually reads the `Authorization` header and enforces it; if not, add
a small middleware/`_shared.py` change.

### 2. The `knowledge-base` skill (the heart)

A single `SKILL.md` installed into Hermes (`~/.hermes/skills/` or via Skills
Hub). The body carries all four pillars — it is the "operating manual" for the
KB that Hermes loads when `description` matches.

Frontmatter:
```yaml
---
name: knowledge-base
description: Use when the user asks about company/domain knowledge, documents,
  entities (people, orgs, phones, INN/OGRN/SNILS), or anything likely stored in
  the internal knowledge base. Routes queries to kb-llamaindex retrieval tools.
---
```

Body — four blocks:

**① Problem orientation (tool-selection decision tree).** Not "always
vector_search" — choose by question type:
- Known exact identifier (phone / INN / email / SNILS / OGRN) → `find_entity_by_id`.
- Relationship / "who is connected to X" / "X's surroundings" → `find_neighbours`
  / `graph_search`.
- Factual / semantic question → `vector_search`, then `get_chunks_by_doc_id` for
  surrounding context when a hit needs more.
- Need full text (tables, short docs) → `read_full_document`.
- Hard multi-hop question not solvable in 2–3 atomic steps → escalate to `kb_search`.
- Anchor: the local Wikibase is the canonical identity source; on name conflicts,
  trust the canonical name from the graph.

**② Response templates (by task type).** RAG already returns
`sources`/`citations`/`uncertainties`; templates formalise them:
- *Factual answer* — claim + `[doc_id]` citations + explicit "Uncertainties"
  block when RAG returns any.
- *Entity dossier* — canonical name, type, key attributes, relations (from graph),
  sources.
- *"What do we know about X"* — grouped summary across vector + graph, with links.
- Answer language = question language (note: RAG synthesis is currently
  Russian-leaning — a nuance to watch).

**③ Memory conventions (`~/.hermes/`).** The skill instructs Hermes to:
- *Record*: user domain/role, recurring entities and important `doc_id`s,
  preferred answer format.
- *Inject*: on a new query, enrich it with remembered context (entity names,
  typical department) before calling a tool — compensating for the stateless RAG.

**④ Multi-turn (reformulation).** Because the RAG is stateless, the skill
requires Hermes to build a **self-contained** query from the conversation history
(resolve "he/she/it/there") before each tool call — never send a raw follow-up.

Out of skill (YAGNI): on-the-fly skill generation and ShareGPT export — that is
Hermes-native learnability, deferred to Approach C.

### 3. RAG-side changes (minimal)

1. **Verify SSE auth-header passthrough.** Confirm FastMCP SSE reads
   `Authorization: Bearer` and enforces `KB_MCP_REQUIRE_AUTH`/API-key. If not, a
   small `_shared.py`/middleware change. (Only possible code change; conditional.)
2. **SSE service ops:** document the prod launch commands for `tools_server` /
   `search_server --transport sse` (alongside the worker). No `docker-compose`
   change in scope A if run directly on the host.
3. **No** new endpoints, tables, or session fields. State/memory/learning live in
   Hermes.

### 4. Verification & acceptance

Aligns with the project rule "benchmark before adopting; extend `tests/eval/`":
1. **Wire smoke test:** Hermes starts, discovers the 6+1 tools (visible as
   `mcp_kbtools_*` / `mcp_kbsearch_kb_search`); a manual call to each tool returns
   the expected JSON.
2. **Scenario eval set** (new artifact under `tests/eval/`, e.g.
   `eval/hermes_scenarios.md` or `.py`): 5–8 golden interactive scenarios — one
   per decision-tree branch + a multi-turn follow-up + an entity dossier. Run
   through Hermes; manual/semi-automated grading: correct tool chosen? template
   applied? reformulation worked?
3. **Existing MCP test regression** (`tests/test_mcp/`) — must stay green
   (integration is additive).
4. **Docs artifact:** new runbook `docs/runbook/hermes.md` (how to start the SSE
   service, the `~/.hermes/config.yaml` block, where the skill lives) + a link
   from `docs/runbook/mcp.md`.

## Deliverables

1. `docs/runbook/hermes.md` — operator guide (SSE service launch, Hermes config,
   skill install) + cross-link from `docs/runbook/mcp.md`.
2. `SKILL.md` for the `knowledge-base` skill (kept in-repo, e.g.
   `integrations/hermes/knowledge-base/SKILL.md`, installable into Hermes).
3. Example `~/.hermes/config.yaml` snippet (in the runbook).
4. (Conditional) SSE auth-header passthrough fix in `src/mcp/_shared.py`.
5. `tests/eval/hermes_scenarios.*` — golden interactive scenario set.

## Out of scope (future work)

- **Approach C — learnability hooks:** `kb_record_feedback` MCP tool + a
  trajectory table + ShareGPT/Atropos export of KB interactions. Separate spec.
- **Approach B — stateful companion service** inside kb-llamaindex. Rejected:
  duplicates Hermes-native memory/loop; violates YAGNI.
- `docker-compose` integration of the SSE services (ops follow-up).
- Changing RAG synthesis language behaviour.

## Open risks to resolve during planning

1. FastMCP SSE `Authorization` header passthrough/enforcement (see §3.1).
2. Whether Hermes' HTTP MCP client speaks SSE specifically or expects streamable
   HTTP — confirm the `url` form (`/sse`) matches what Hermes expects.
3. RAG synthesis is Russian-leaning; if Hermes serves non-Russian users, the
   template's "answer language = question language" rule may conflict with
   `kb_search` output (atomic tools return raw chunks, so less affected).
