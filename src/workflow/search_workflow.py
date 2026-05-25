"""``SearchWorkflow`` — Temporal-backed search session.

Replaces the in-process ReAct loop (``agentic_react_search``) and
reflective synth (``reflective_synthesize``) wrapper for all three
search-endpoint modes:

* ``mode="simple"``   — one vector_search → synthesize
* ``mode="agent"``    — ReAct loop → synthesize
* ``mode="selfrag"``  — ReAct loop → reflective_synthesize

Three activities (``agent_reasoning_step``, ``tool_execution``,
``synthesize_answer``) keep the LLM-bound work on its own task queue
``kb-search-llm`` so concurrent search sessions serialise against the
configurable cap (``TEMPORAL_SEARCH_ACTIVITY_CONCURRENCY``) without
fighting ingest's queue.

The workflow exposes a ``get_state`` query so MCP-1 (Stage 3) can
poll for progress and emit MCP ``notifications/progress`` messages
back to the client.
"""

from __future__ import annotations

import json
import time
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from src.retrieval.atomic_tools import TOOL_DESCRIPTIONS
    from src.workflow.contracts import (
        AgentDecision,
        AgenticStepStatDict,
        DistillParams,
        DistillResult,
        ReasoningParams,
        SearchOutcome,
        SearchParams,
        SerializedMessage,
        SerializedToolCall,
        SynthesizeParams,
        SynthesizeResult,
        ToolCallParams,
        ToolCallResult,
        ToolSpec,
    )


# Three retries, short backoff — search latency matters; we don't
# want forever-retry like ingest.  Application-level failures (tool
# not found, LLM 4xx) are non-retryable and bubble up.
_FAST_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
)


_SYSTEM_PROMPT = """\
You are a research agent answering questions over a corpus that
mixes analytical reports, email correspondence, and support-call
transcripts.  User questions arrive in Russian; the knowledge
graph (entities, descriptions, relations) is normalised to
Russian, while raw chunk text may be in any source language.

You have access to tools to look things up.  Your job:
1. Read the user's question.
2. Decide which tool to call to gather missing information.
3. Repeat until you have enough.
4. Call `submit_answer` to finalize.

Tool selection — choose deliberately, do NOT default to vector_search:
- The question names a concrete identifier (phone, INN, email, exact
  name) → call `find_entity_by_id` first.
- The question is about a specific person/organization/topic, or asks
  how things are connected ("кто связан с", "связь между X и Y",
  "чьи", "с кем") → call `graph_search`.
- "расскажи всё про X" once X is anchored → `find_neighbours`.
- `vector_search` is only for open-ended topical questions with no
  named entity, or to gather supporting passages AFTER the graph has
  surfaced entity names.
- Good pattern: anchor on entities with the graph first, then use
  `vector_search` for surrounding text — not the other way around.

Rules:
- Do NOT answer from prior knowledge — only from tool observations.
- Do NOT call `submit_answer` until you have at least one
  successful retrieval result.
- If you call the same tool with the same arguments twice and got
  the same result, stop retrying — submit what you have.
- Keep tool queries focused: one specific question per call.
- Tool queries may use Russian terms — the graph is Russian.  When
  querying for source-language strings (proper names, identifiers),
  preserve them verbatim.
- Final answer goes through a separate synthesizer that writes
  in Russian; you don't need to translate the tool outputs yourself.
"""


def _build_tool_specs() -> list[ToolSpec]:
    """Tool menu shown to the LLM in agent_reasoning_step.

    Pulled from ``atomic_tools.TOOL_DESCRIPTIONS`` plus the
    ``submit_answer`` terminator — single source of truth.
    """
    specs = [
        ToolSpec(name=name, description=desc)
        for name, desc in TOOL_DESCRIPTIONS.items()
    ]
    specs.append(ToolSpec(
        name="submit_answer",
        description=(
            "Finalize the answer using the sources gathered so far. "
            "Call this when you have enough information to answer the "
            "user's question."
        ),
    ))
    return specs


@workflow.defn
class SearchWorkflow:
    """One search session — durable, cancellable, queue-capped."""

    def __init__(self) -> None:
        self._state: dict[str, Any] = {
            "phase": "init",
            "steps_completed": 0,
            "last_tool": "",
            "n_sources": 0,
            "mode": "agent",
        }

    @workflow.query
    def get_state(self) -> dict:
        """Progress snapshot for MCP-1 progress streaming."""
        return dict(self._state)

    @workflow.run
    async def run(self, params: SearchParams) -> SearchOutcome:
        log = workflow.logger
        t_start = workflow.now()
        self._state["mode"] = params.mode
        log.info(
            "search_workflow start  mode=%s  query=%s",
            params.mode, params.query[:80],
        )

        accumulated: list = []   # list[SerializedNode]
        step_stats: list[AgenticStepStatDict] = []

        if params.mode == "simple":
            # ── simple: 1 vector_search → synthesize ────────────────
            self._state["phase"] = "vector_search"
            tool_result: ToolCallResult = await workflow.execute_activity(
                "tool_execution",
                ToolCallParams(
                    tool_name="vector_search",
                    tool_kwargs={"query": params.query},
                ),
                result_type=ToolCallResult,
                start_to_close_timeout=timedelta(minutes=2),
                schedule_to_close_timeout=timedelta(minutes=10),
                retry_policy=_FAST_RETRY,
            )
            accumulated.extend(tool_result.sources_added)
            self._state["n_sources"] = len(accumulated)
            step_stats.append(AgenticStepStatDict(
                step=1, tool_name="vector_search",
                tool_args={"query": params.query},
                observation_summary=tool_result.observation[:200],
            ))
        else:
            # ── agent / selfrag: ReAct loop ─────────────────────────
            tool_specs = _build_tool_specs()
            messages: list[SerializedMessage] = [
                SerializedMessage(role="system", content=_SYSTEM_PROMPT),
                SerializedMessage(role="user", content=params.query),
            ]
            last_sig: str = ""
            repeat_count = 0

            for step_i in range(1, params.max_iterations + 1):
                self._state["phase"] = f"reasoning_step_{step_i}"
                decision: AgentDecision = await workflow.execute_activity(
                    "agent_reasoning_step",
                    ReasoningParams(messages=messages, tools=tool_specs),
                    result_type=AgentDecision,
                    start_to_close_timeout=timedelta(minutes=2),
                    schedule_to_close_timeout=timedelta(minutes=10),
                    retry_policy=_FAST_RETRY,
                )

                if decision.finished_no_call:
                    log.info("search_workflow  step=%d no tool call → exit",
                             step_i)
                    break
                if decision.tool_name == "submit_answer":
                    log.info("search_workflow  step=%d submit_answer", step_i)
                    break

                sig = f"{decision.tool_name}:{decision.tool_kwargs}"
                if sig == last_sig:
                    repeat_count += 1
                else:
                    repeat_count = 0
                last_sig = sig

                # Stable id linking the assistant tool-call turn to the
                # TOOL observation below.  Fall back to a deterministic
                # step-derived id when the LLM didn't supply one (keeps
                # the workflow replay-safe — no uuid/random).
                call_id = decision.tool_call_id or f"call_{step_i}"

                self._state["phase"] = f"tool_execution_{step_i}"
                self._state["last_tool"] = decision.tool_name
                tool_params = ToolCallParams(
                    tool_name=decision.tool_name,
                    tool_kwargs=decision.tool_kwargs,
                    accumulated_sources=(
                        accumulated if decision.tool_name == "filter_by_metadata"
                        else []
                    ),
                )
                tool_result = await workflow.execute_activity(
                    "tool_execution",
                    tool_params,
                    result_type=ToolCallResult,
                    start_to_close_timeout=timedelta(minutes=3),
                    schedule_to_close_timeout=timedelta(minutes=10),
                    retry_policy=_FAST_RETRY,
                )
                # Distil large observations: compress to query-relevant
                # facts + grade relevance.  Keeps reasoning context bounded
                # on big corpora; full sources still feed synthesis.
                relevance = "relevant"
                hist_observation = tool_result.observation
                if (
                    params.distill_enabled
                    and not tool_result.error
                    and len(tool_result.observation)
                    > params.distill_min_chars
                ):
                    self._state["phase"] = f"distill_{step_i}"
                    distilled: DistillResult = await workflow.execute_activity(
                        "distill_observation",
                        DistillParams(
                            query=params.query,
                            tool_name=decision.tool_name,
                            observation=tool_result.observation,
                        ),
                        result_type=DistillResult,
                        start_to_close_timeout=timedelta(minutes=2),
                        schedule_to_close_timeout=timedelta(minutes=10),
                        retry_policy=_FAST_RETRY,
                    )
                    relevance = distilled.relevance
                    hist_observation = distilled.distilled
                # Hard cap — backstop even when distillation is off or the
                # distilled text is still long.
                hist_observation = hist_observation[
                    : params.observation_max_chars
                ]

                # Gate the accumulator on relevance (CRAG-lite): clearly
                # irrelevant results don't pollute the synthesizer context.
                if tool_result.sources_added and relevance != "irrelevant":
                    accumulated.extend(tool_result.sources_added)
                    # Dedup by chunk_id — same chunk may surface from
                    # multiple tools (vector + graph_search returning
                    # related text).
                    seen: set[str] = set()
                    deduped = []
                    for n in accumulated:
                        if n.chunk_id in seen:
                            continue
                        seen.add(n.chunk_id)
                        deduped.append(n)
                    accumulated = deduped

                self._state["n_sources"] = len(accumulated)
                self._state["steps_completed"] = step_i

                step_stats.append(AgenticStepStatDict(
                    step=step_i,
                    tool_name=decision.tool_name,
                    tool_args=decision.tool_kwargs,
                    observation_summary=hist_observation[:200],
                    relevance=relevance,
                ))

                # Repeat-call guard: after recording the third identical
                # tool call we exit — the agent isn't gaining anything
                # by retrying.  Synth runs over what we have.
                if repeat_count >= 2:
                    log.info(
                        "search_workflow  repeat-call guard tripped → exit",
                    )
                    break

                # Record the assistant tool-call turn, THEN its TOOL
                # observation — a valid function-calling pairing.  Without
                # the assistant turn the next reasoning step sees orphan
                # tool messages, can't tell what it already called, and
                # keeps falling back to the most generic tool.
                messages.append(SerializedMessage(
                    role="assistant",
                    content=decision.raw_text,
                    tool_calls=[SerializedToolCall(
                        id=call_id,
                        name=decision.tool_name,
                        arguments=json.dumps(
                            decision.tool_kwargs, ensure_ascii=False,
                        ),
                    )],
                ))
                messages.append(SerializedMessage(
                    role="tool",
                    content=hist_observation,
                    tool_call_id=call_id,
                    name=decision.tool_name,
                ))

        # ── synthesis ───────────────────────────────────────────────
        self._state["phase"] = "synthesize"
        synth: SynthesizeResult = await workflow.execute_activity(
            "synthesize_answer",
            SynthesizeParams(
                query=params.query,
                mode=params.mode,
                accumulated=accumulated,
                max_refinements=params.max_refinements,
            ),
            result_type=SynthesizeResult,
            start_to_close_timeout=timedelta(minutes=5),
            schedule_to_close_timeout=timedelta(minutes=15),
            retry_policy=_FAST_RETRY,
        )

        self._state["phase"] = "done"
        latency_ms = int(
            (workflow.now() - t_start).total_seconds() * 1000,
        )
        return SearchOutcome(
            query=params.query,
            mode=params.mode,
            answer=synth.text,
            sources=accumulated,
            step_stats=step_stats,
            citations=list(synth.citations),
            uncertainties=list(synth.uncertainties),
            refinement_rounds=synth.refinement_rounds,
            latency_ms=latency_ms,
        )
