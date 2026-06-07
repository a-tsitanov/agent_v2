# Conversation history (multi-turn search) — design

**Status:** spec for review. **Track:** GraphRAG/LightRAG-parity #4. Unblocks the Hermes-agent integration (multi-turn) — see [[hermes_integration]].

## Problem

`/api/v1/search/*` is **stateless**: `SearchRequest` carries only `query` + `top_k` (`src/models/search.py:15-36`), and the flow (`SearchRequest → _local_params → OrchestratorParams → plan_subquestions → retrieve → synthesize`) never sees prior turns. So follow-ups fail: "а что по его цене?" / "сравни с предыдущим" have no referent — the planner decomposes a dangling pronoun and retrieval misses.

## Goal

Multi-turn search: a follow-up question is answered in the context of the conversation. Opt-in, back-compatible (empty history → today's behaviour byte-for-byte), and **client-managed state** (no server-side sessions — the caller, e.g. the Hermes agent, owns the transcript and passes it in; matches our stateless durable-workflow design).

## Approach — contextualize, don't dump

The robust pattern (used by GraphRAG local search and standard RAG) is **query contextualisation**: rewrite the follow-up into a standalone, self-contained question using the recent history, BEFORE planning/retrieval — rather than dumping raw history into every retrieval prompt (which pollutes retrieval and inflates tokens).

```
"а что по его цене?"  +  history[…"расскажи про Продукт X"…]
        └─► contextualize_query (small LLM) ─► "Какая цена у Продукта X?"
                                                      └─► plan → retrieve → synthesize  (unchanged)
```

- Runs **only when history is non-empty** — zero extra cost / zero behaviour change for single-shot queries.
- One small-tier LLM call; output replaces `query` everywhere downstream (planner, retrieval, synthesis), so the rest of the pipeline is untouched.
- History is **bounded** (last `history_max_turns`, e.g. 6, and `history_max_chars`) for prompt size + replay determinism.
- Optional v1.1: also pass the (bounded) history to the synthesis prompt so the answer can speak referentially ("as mentioned, …"). v1 keeps synthesis on the contextualised query only.

### Components touched

| File | Change |
|---|---|
| `src/models/search.py` | `ConversationTurn {role: "user"\|"assistant", content: str}`; `SearchRequest.history: list[ConversationTurn] = []` |
| `src/workflow/contracts.py` | `OrchestratorParams` / `GlobalSearchParams` gain `history: list[ConversationTurnDict]`; new `ContextualizeParams`/`ContextualizeResult` |
| `src/api/routes/search_v2.py` | `_local_params` / `_global_params` thread `req.history` through |
| `src/workflow/search/activities/contextualize.py` (new) | `contextualize_query` activity — small LLM, bounded history, fail-open to raw query |
| `src/workflow/search/orchestrator.py` + `global_wf.py` | run `contextualize_query` first **iff** history non-empty; feed result into `PlanParams.query` |
| `src/config.py` | `AgentSettings`: `conversation_history_enabled` (default True), `history_max_turns`, `history_max_chars` |

### Data flow

```
SearchRequest(query, history)  ─►  Orchestrator/Global workflow
   if history & enabled:
       q' = contextualize_query(query, bounded(history))   # 1 small-LLM call
   else:
       q' = query
   plan_subquestions(q') → retrieve → synthesize           # all unchanged
```

### Error handling / back-compat

- Empty history (or flag off) → skip contextualisation entirely; pipeline identical to today.
- Contextualise LLM error/timeout → fail-open to the raw `query` (log + proceed). Never blocks the search.
- Determinism: history is bounded + passed in params (no runtime env / clock reads), so workflow replay is safe.

### Testing

- Unit: `_bound_history` caps turns/chars; the contextualise prompt builder includes history + query; with a mock LLM, a pronoun follow-up → standalone question; empty history → activity is skipped (no LLM call) and `q' == query`; LLM failure → `q' == query`.
- Route: `SearchRequest` accepts/round-trips `history`; `_local_params`/`_global_params` carry it into params.
- Live smoke (optional): two-turn session via `/search/local` — turn 2 ("а что по цене?") returns an answer grounded in turn 1's entity.

## Resolved decisions (2026-06-07)

1. **Sessions:** **client-managed** history (caller/Hermes owns the transcript; server stays stateless). No server-side session store.
2. **Usage:** **contextualise-only** in v1 (rewrite follow-up → standalone query; do not feed raw history into synthesis). Revisit synthesis-history later if answers feel disjointed.
3. **`history_max_turns`:** **6** (3 user + 3 assistant), plus `history_max_chars` ceiling.
