"""Tests for `src/retrieval/_common.py`."""

from __future__ import annotations

from src.retrieval._common import strip_thinking


def test_strips_simple_think_block() -> None:
    text = "<think>internal reasoning</think>The real answer."
    assert strip_thinking(text) == "The real answer."


def test_strips_multiline_think_block() -> None:
    text = (
        "<think>\nLine 1\nLine 2\nrehearsing (a, b, c)\n</think>\n"
        "(real_subj, real_pred, real_obj)"
    )
    out = strip_thinking(text)
    assert "(a, b, c)" not in out
    assert "(real_subj, real_pred, real_obj)" in out


def test_strips_case_insensitive() -> None:
    assert strip_thinking("<THINK>x</THINK>y") == "y"
    assert strip_thinking("<Thinking>x</Thinking>y") == "y"


def test_strips_multiple_blocks() -> None:
    text = "<think>a</think>X<think>b</think>Y"
    assert strip_thinking(text) == "XY"


def test_unclosed_think_truncates_to_block() -> None:
    """qwen3 sometimes emits an open <think> when generation truncates.
    Everything from the open tag to EOF should be dropped — otherwise
    the parser eats the entire prompt response as thinking-debris."""
    text = "valid prefix\n<think>unclosed chain-of-thought blah blah"
    assert strip_thinking(text) == "valid prefix"


def test_empty_input_passthrough() -> None:
    assert strip_thinking("") == ""
    assert strip_thinking("plain text") == "plain text"


def test_does_not_strip_unrelated_xml() -> None:
    """Tags that aren't <think>/<thinking> stay put."""
    text = "<answer>x</answer><foo>y</foo>"
    assert strip_thinking(text) == "<answer>x</answer><foo>y</foo>"


def test_strips_with_attributes_on_tag() -> None:
    """Some emit `<think lang="en">...</think>` — handle that."""
    text = '<think lang="ru">мысли</think>actual'
    assert strip_thinking(text) == "actual"
