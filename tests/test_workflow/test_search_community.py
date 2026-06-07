"""Unit tests for the offline community-build activities + workflow
helpers (Search R6).

The community activities are plain async fns over stubbed deps (no live
Temporal) — same pattern as the other search-activity tests.  We cover:

  * ``summarize_community_activity`` produces a summary via a mock small
    LLM and issues the ``:Community.summary`` MERGE; fail-safe on error,
  * the pure ``build_summarize_specs`` workflow helper turns detected
    communities into batchable summarize params (idempotent re-run).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.workflow.contracts import (
    CommunityRef,
    DetectCommunitiesResult,
    SummarizeCommunityParams,
)
from src.workflow.search.activities import community as community_mod
from src.workflow.search.activities.community import summarize_community_activity
from src.workflow.search.community_wf import build_summarize_specs


class _FakeStore:
    def __init__(self, *, member_rows=None, child_rows=None, raise_on=None):
        self._member_rows = member_rows or []
        self._child_rows = child_rows or []
        self._raise_on = raise_on
        self.calls: list[tuple[str, dict]] = []

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map or {}))
        if self._raise_on and self._raise_on in cypher:
            raise RuntimeError("boom")
        if "PARENT_OF" in cypher:
            return self._child_rows
        if "RETURN" in cypher and "description" in cypher:
            return self._member_rows
        return []


class _FakeEmbed:
    """Returns a fixed-length vector; records the embedded text."""

    def __init__(self, dim=4):
        self.dim = dim
        self.seen_text: str | None = None

    async def aget_text_embedding(self, text):
        self.seen_text = text
        return [0.1] * self.dim


def _report_json(title="Группа", summary="Сводка.", findings=None):
    import json

    return json.dumps(
        {
            "title": title,
            "summary": summary,
            "findings": findings
            if findings is not None
            else [{"statement": "Вывод.", "importance": 80}],
        },
        ensure_ascii=False,
    )


class _FakeLLM:
    """Records the prompt; returns a canned structured-report JSON."""

    def __init__(self, text=None):
        self.text = text if text is not None else _report_json()
        self.seen_prompt: str | None = None

    async def acomplete(self, prompt, **_kw):
        self.seen_prompt = prompt
        return MagicMock(text=self.text)


@pytest.fixture(autouse=True)
def _stub_activity_ctx(monkeypatch):
    mock = MagicMock()
    mock.heartbeat = MagicMock()
    mock.logger = MagicMock()
    monkeypatch.setattr(community_mod, "activity", mock)
    # Default embed stub so the activity never builds a real embedding
    # model in unit tests (individual tests may override).
    monkeypatch.setattr(community_mod, "_get_embed_model", lambda: _FakeEmbed())


@pytest.mark.asyncio
async def test_summarize_produces_report_and_persists(monkeypatch):
    store = _FakeStore(member_rows=[
        {"name": "Ромашка", "description": "Поставщик материалов."},
        {"name": "СтройИнвест", "description": "Подрядчик."},
    ])
    llm = _FakeLLM(_report_json(
        title="Строительные компании",
        summary="Группа строительных компаний.",
        findings=[{"statement": "Тесно связаны контрактами.", "importance": 90}],
    ))
    embed = _FakeEmbed()
    monkeypatch.setattr(community_mod, "_get_store", lambda: store)
    monkeypatch.setattr(community_mod, "_get_summary_llm", lambda: llm)
    monkeypatch.setattr(community_mod, "_get_embed_model", lambda: embed)

    out = await summarize_community_activity(SummarizeCommunityParams(
        community_id="2", level=0, members=["Ромашка", "СтройИнвест"],
    ))

    assert out.community_id == "2"
    assert out.summary == "Группа строительных компаний."
    assert out.persisted is True
    # The member names made it into the prompt.
    assert "Ромашка" in (llm.seen_prompt or "")
    # title + summary were embedded.
    assert "Строительные компании" in (embed.seen_text or "")
    assert "Группа строительных компаний." in (embed.seen_text or "")
    # The report MERGE was issued with report/title/summary/report_vec.
    write = next(
        (pm for c, pm in store.calls if "report" in c and "MERGE" in c), None
    )
    assert write is not None
    assert write["community_id"] == "2"
    assert write["title"] == "Строительные компании"
    assert write["summary"] == "Группа строительных компаний."
    assert isinstance(write["report_vec"], list)
    import json as _json
    parsed = _json.loads(write["report"])
    assert parsed["findings"][0]["importance"] == 90


@pytest.mark.asyncio
async def test_summarize_level_gt0_uses_child_reports(monkeypatch):
    store = _FakeStore(
        member_rows=[{"name": "X", "description": "y"}],
        child_rows=[
            {"title": "Дочернее A", "summary": "Сводка A."},
            {"title": "Дочернее B", "summary": "Сводка B."},
        ],
    )
    llm = _FakeLLM()
    monkeypatch.setattr(community_mod, "_get_store", lambda: store)
    monkeypatch.setattr(community_mod, "_get_summary_llm", lambda: llm)

    out = await summarize_community_activity(SummarizeCommunityParams(
        community_id="7", level=1, members=["X", "Z"],
    ))

    assert out.persisted is True
    # Child-report context (not member context) drove the prompt.
    assert "Дочернее A" in (llm.seen_prompt or "")
    assert "Дочерние сообщества" in (llm.seen_prompt or "")
    # The PARENT_OF child-report query was issued.
    assert any("PARENT_OF" in c for c, _ in store.calls)


@pytest.mark.asyncio
async def test_summarize_level_gt0_falls_back_to_members(monkeypatch):
    # No child reports yet → must fall back to member context.
    store = _FakeStore(
        member_rows=[{"name": "Альфа", "description": "Описание."}],
        child_rows=[],
    )
    llm = _FakeLLM()
    monkeypatch.setattr(community_mod, "_get_store", lambda: store)
    monkeypatch.setattr(community_mod, "_get_summary_llm", lambda: llm)

    await summarize_community_activity(SummarizeCommunityParams(
        community_id="7", level=1, members=["Альфа"],
    ))

    assert "Альфа" in (llm.seen_prompt or "")
    assert "Сущности сообщества" in (llm.seen_prompt or "")


@pytest.mark.asyncio
async def test_summarize_persists_without_vec_on_embed_failure(monkeypatch):
    store = _FakeStore(member_rows=[{"name": "A", "description": "x"}])
    monkeypatch.setattr(community_mod, "_get_store", lambda: store)
    monkeypatch.setattr(community_mod, "_get_summary_llm", lambda: _FakeLLM())

    class _BoomEmbed:
        async def aget_text_embedding(self, _t):
            raise RuntimeError("embed down")

    monkeypatch.setattr(community_mod, "_get_embed_model", lambda: _BoomEmbed())

    out = await summarize_community_activity(SummarizeCommunityParams(
        community_id="3", level=0, members=["A", "B"],
    ))
    assert out.persisted is True
    write = next(
        (pm for c, pm in store.calls if "report" in c and "MERGE" in c), None
    )
    assert write is not None
    assert write["report_vec"] is None


@pytest.mark.asyncio
async def test_summarize_failsafe_on_llm_error(monkeypatch):
    store = _FakeStore(member_rows=[{"name": "A", "description": "x"}])

    class _BoomLLM:
        async def acomplete(self, *_a, **_k):
            raise RuntimeError("llm down")

    monkeypatch.setattr(community_mod, "_get_store", lambda: store)
    monkeypatch.setattr(community_mod, "_get_summary_llm", lambda: _BoomLLM())

    out = await summarize_community_activity(SummarizeCommunityParams(
        community_id="1", members=["A", "B"],
    ))
    assert out.summary == ""
    assert out.persisted is False


@pytest.mark.asyncio
async def test_summarize_empty_members_skips_llm(monkeypatch):
    called = {"llm": False}

    def _llm():
        called["llm"] = True
        raise AssertionError("must not build LLM for empty community")

    monkeypatch.setattr(community_mod, "_get_store", lambda: _FakeStore())
    monkeypatch.setattr(community_mod, "_get_summary_llm", _llm)

    out = await summarize_community_activity(
        SummarizeCommunityParams(community_id="1", members=[]),
    )
    assert out.persisted is False
    assert called["llm"] is False


# ── pure workflow helper: detected → batchable summarize specs ──────


def test_build_summarize_specs_maps_each_community():
    detect = DetectCommunitiesResult(communities=[
        CommunityRef(community_id="1", level=0, members=["A", "B", "C"]),
        CommunityRef(community_id="2", level=0, members=["D", "E", "F"]),
    ])
    specs = build_summarize_specs(detect)
    assert [s.community_id for s in specs] == ["1", "2"]
    assert all(isinstance(s, SummarizeCommunityParams) for s in specs)
    assert specs[0].members == ["A", "B", "C"]


def test_build_summarize_specs_empty():
    assert build_summarize_specs(DetectCommunitiesResult(communities=[])) == []


# ── pure helper: structured-report parser ───────────────────────────


def test_parse_report_valid_json():
    text = (
        '{"title": "T", "summary": "S", '
        '"findings": [{"statement": "f1", "importance": 70}]}'
    )
    out = community_mod._parse_report(text)
    assert out["title"] == "T"
    assert out["summary"] == "S"
    assert out["findings"] == [{"statement": "f1", "importance": 70}]


def test_parse_report_strips_code_fence_and_prose():
    text = "Вот отчёт:\n```json\n" + _report_json("X", "Y", []) + "\n```\nготово"
    out = community_mod._parse_report(text)
    assert out["title"] == "X"
    assert out["summary"] == "Y"


def test_parse_report_garbage_falls_back_to_raw():
    out = community_mod._parse_report("just some prose, no json here")
    assert out["title"] == ""
    assert out["summary"] == "just some prose, no json here"
    assert out["findings"] == []


def test_parse_report_empty_string():
    out = community_mod._parse_report("")
    assert out == {"title": "", "summary": "", "findings": []}


def test_parse_report_tolerates_bad_finding_shapes():
    text = (
        '{"title": 5, "summary": "S", "findings": '
        '["not a dict", {"importance": 9}, {"statement": "ok", "importance": "x"}]}'
    )
    out = community_mod._parse_report(text)
    # non-str title → "", missing/blank statement dropped, non-int importance → 0
    assert out["title"] == ""
    assert out["summary"] == "S"
    assert out["findings"] == [{"statement": "ok", "importance": 0}]


def test_write_report_cypher_shape():
    cy = community_mod._WRITE_REPORT_CYPHER
    assert "MERGE (c:Community {id: $community_id, level: $level})" in cy
    for field in ("c.report = $report", "c.title = $title",
                  "c.summary = $summary", "c.report_vec = $report_vec",
                  "c.summarized_at = timestamp()"):
        assert field in cy


def test_child_reports_cypher_shape():
    cy = community_mod._CHILD_REPORTS_CYPHER
    assert "-[:PARENT_OF]->(child:Community)" in cy
    assert "child.report IS NOT NULL" in cy
    assert "ORDER BY child.member_count DESC" in cy


def test_project_cypher_is_undirected_for_leiden():
    # Leiden rejects a directed graph ("works only with undirected graphs").
    # The projection MUST mark all relationship types undirected.
    from src.graph.communities import _project_cypher

    cypher = _project_cypher("g-test")
    assert "undirectedRelationshipTypes: ['*']" in cypher
    assert "gds.graph.project(" in cypher
