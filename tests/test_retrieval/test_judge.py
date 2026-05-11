"""Unit tests for `LLMJudge` — both structured and text paths.

Stubs the LLM so we don't depend on a live Ollama / LiteLLM.
"""

from __future__ import annotations

import json

import pytest
from llama_index.core.base.llms.types import ChatMessage, ChatResponse

from src.models.search import JudgeOutput
from src.retrieval.judge import LLMJudge


# ── stubs ────────────────────────────────────────────────────────────


class _StructuredLLM:
    """Returns a pre-built `JudgeOutput` from `astructured_predict`."""

    def __init__(self, output: JudgeOutput) -> None:
        self.output = output
        self.calls: list[dict] = []

    async def astructured_predict(self, output_cls, prompt, **kwargs):
        self.calls.append(kwargs)
        return self.output


class _StructuredFailLLM:
    """`astructured_predict` raises — should trigger text fallback."""

    def __init__(self, text_payload: dict) -> None:
        self._text_payload = text_payload

    async def astructured_predict(self, output_cls, prompt, **kwargs):
        raise RuntimeError("structured_predict failed")

    async def achat(self, messages):
        return ChatResponse(
            message=ChatMessage(
                role="assistant", content=json.dumps(self._text_payload),
            )
        )


class _TextLLM:
    """Plain `achat` returning a content string."""

    def __init__(self, content: str) -> None:
        self.content = content

    async def achat(self, messages):
        return ChatResponse(
            message=ChatMessage(role="assistant", content=self.content)
        )


class _RaisingLLM:
    async def achat(self, messages):
        raise RuntimeError("LLM down")


# ── structured path ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_via_structured_happy_path() -> None:
    out = JudgeOutput(
        sufficient=False,
        follow_up_query="find more about X",
        reason="missing key detail",
    )
    judge = LLMJudge(_StructuredLLM(out))  # type: ignore[arg-type]

    result = await judge.via_structured("q", [])

    assert result["sufficient"] is False
    assert result["follow_up_query"] == "find more about X"
    assert "missing key detail" in result["reason"]


@pytest.mark.asyncio
async def test_via_structured_falls_back_to_text_on_failure() -> None:
    fallback = {"sufficient": True, "follow_up_query": "", "reason": "from text"}
    judge = LLMJudge(_StructuredFailLLM(fallback))  # type: ignore[arg-type]

    result = await judge.via_structured("q", [])

    # structured raised → text path used → parsed fallback dict
    assert result["sufficient"] is True
    assert result["reason"] == "from text"


# ── text path (preserves existing contract) ─────────────────────────


@pytest.mark.asyncio
async def test_text_path_parses_plain_json() -> None:
    payload = json.dumps({"sufficient": False, "follow_up_query": "x", "reason": "y"})
    judge = LLMJudge(_TextLLM(payload))  # type: ignore[arg-type]

    out = await judge("q", [])
    assert out == {"sufficient": False, "follow_up_query": "x", "reason": "y"}


@pytest.mark.asyncio
async def test_text_path_strips_markdown_fence() -> None:
    content = "```json\n" + json.dumps({"sufficient": True}) + "\n```"
    judge = LLMJudge(_TextLLM(content))  # type: ignore[arg-type]
    out = await judge("q", [])
    assert out["sufficient"] is True


@pytest.mark.asyncio
async def test_text_path_invalid_json_defaults_to_sufficient() -> None:
    judge = LLMJudge(_TextLLM("not json {[}"))  # type: ignore[arg-type]
    out = await judge("q", [])
    assert out["sufficient"] is True


@pytest.mark.asyncio
async def test_text_path_llm_exception_defaults_to_sufficient() -> None:
    judge = LLMJudge(_RaisingLLM())  # type: ignore[arg-type]
    out = await judge("q", [])
    assert out["sufficient"] is True
    assert "LLM down" in out["reason"]
