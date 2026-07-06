"""Unit tests for the input document classifier (Track 2).

Deterministic rule layer is pure + thoroughly tested; the LLM layer is
mocked.  Fail-soft contract: any error / uncertainty → INGEST."""

from __future__ import annotations

import pytest

from src.ingestion.classifier import apply_rules

_EXT = ["exe", "png", "zip"]


def _rules(name, size):
    return apply_rules(
        name, size, max_size_mb=1.0, min_size_bytes=1, skip_extensions=_EXT,
    )


def test_rules_pass_normal_document():
    v = _rules("report.pdf", 5000)
    assert v.skip is False


def test_rules_skip_blocked_extension():
    v = _rules("photo.PNG", 5000)  # case-insensitive
    assert v.skip is True
    assert "png" in v.reason.lower()


def test_rules_skip_empty_file():
    v = _rules("empty.txt", 0)
    assert v.skip is True
    assert "small" in v.reason.lower()


def test_rules_skip_oversized_file():
    v = _rules("huge.txt", 2 * 1024 * 1024)  # > 1 MB cap
    assert v.skip is True
    assert "large" in v.reason.lower()


def test_rules_no_extension_is_allowed():
    v = _rules("Makefile", 500)
    assert v.skip is False


# ── classify_document activity (force / rules / llm) ────────────────

import importlib
import types
from unittest.mock import AsyncMock, MagicMock, patch

from src.ingestion.classifier import LLMVerdict
from src.workflow.contracts import ClassifyIn, Ctx

# `from src.workflow.activities import classify_document` resolves to the
# FUNCTION (re-exported in the package __init__), shadowing the submodule.
# import_module returns the real module object for patch.object targeting.
mod = importlib.import_module("src.workflow.activities.classify_document")


def _cfg(**over):
    base = dict(max_size_mb=1.0, min_size_bytes=1, skip_extensions=["exe", "png"],
                preview_chars=100, llm_enabled=True)
    base.update(over)
    return types.SimpleNamespace(**base)


def _ctx(path):
    return Ctx(doc_id="d1", local_path=str(path), cleanup_dir=None, workflow_run_id="r")


@pytest.mark.asyncio
async def test_activity_force_bypasses_rules(tmp_path):
    f = tmp_path / "photo.png"  # would be blocked by rules
    f.write_text("x")
    with patch.object(mod, "settings", types.SimpleNamespace(classifier=_cfg())):
        out = await mod.classify_document(ClassifyIn(ctx=_ctx(f), force=True))
    assert out.ingest is True
    assert out.reason == "forced"


@pytest.mark.asyncio
async def test_activity_rules_skip_does_not_call_llm(tmp_path):
    f = tmp_path / "photo.png"
    f.write_text("x")
    with patch.object(mod, "settings", types.SimpleNamespace(classifier=_cfg())), patch.object(mod, "classify_with_llm", new=AsyncMock()) as llm:
        out = await mod.classify_document(ClassifyIn(ctx=_ctx(f), force=False))
    assert out.ingest is False
    assert "png" in out.reason.lower()
    llm.assert_not_awaited()


@pytest.mark.asyncio
async def test_activity_rules_pass_llm_disabled_ingests(tmp_path):
    f = tmp_path / "doc.txt"
    f.write_text("hello")
    with patch.object(mod, "settings",
                      types.SimpleNamespace(classifier=_cfg(llm_enabled=False))):
        out = await mod.classify_document(ClassifyIn(ctx=_ctx(f), force=False))
    assert out.ingest is True


@pytest.mark.asyncio
async def test_activity_llm_skip(tmp_path):
    f = tmp_path / "doc.txt"
    f.write_text("garbage dump")
    with patch.object(mod, "settings", types.SimpleNamespace(classifier=_cfg())), patch.object(mod, "classify_with_llm", new=AsyncMock(return_value=LLMVerdict(ingest=False, reason="junk"))), patch(
        "src.retrieval.llm_pool.get_llm_pool", return_value=MagicMock(),
    ):
        out = await mod.classify_document(ClassifyIn(ctx=_ctx(f), force=False))
    assert out.ingest is False
    assert out.reason == "junk"
