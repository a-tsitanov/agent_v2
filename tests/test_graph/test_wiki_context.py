from unittest.mock import MagicMock

import pytest

import src.graph.wiki_context as wiki_context
from src.graph.wiki_context import (
    EntityContext,
    read_citations,
    read_entity_subgraph,
    read_source_docs,
    subgraph_hash,
)


def _ctx(**kw):
    base = dict(
        name="ООО Альфа", label="Organization", description="A supplier.",
        wikibase_qid="Q5", page_title="ООО Альфа",
        relations=[
            ("заключила договор", "out", "Договор № 17-К", "Document", "signed"),
            ("контакт", "out", "+74951234567", "PhoneNumber", ""),
        ],
    )
    base.update(kw)
    return EntityContext(**base)


@pytest.fixture
def fake_ops(monkeypatch):
    """Monkeypatch build_wiki_graph_ops in wiki_context's own namespace and
    return the fake ops instance the module will receive."""
    ops = MagicMock()
    build = MagicMock(return_value=ops)
    monkeypatch.setattr(wiki_context, "build_wiki_graph_ops", build)
    return ops, build


def test_hash_is_deterministic_and_order_independent():
    a = _ctx()
    b = _ctx(relations=list(reversed(_ctx().relations)))  # reordered
    assert subgraph_hash(a) == subgraph_hash(b)


def test_hash_changes_when_a_relation_changes():
    a = _ctx()
    b = _ctx(relations=_ctx().relations + [("платит", "out", "X", "Amount", "")])
    assert subgraph_hash(a) != subgraph_hash(b)


def test_hash_changes_on_description_change():
    assert subgraph_hash(_ctx()) != subgraph_hash(_ctx(description="changed"))


def test_hash_ignores_qid_and_page_title():
    assert subgraph_hash(_ctx(wikibase_qid="Q9", page_title="other")) == \
           subgraph_hash(_ctx())


def test_read_entity_subgraph_routes_through_seam_and_builds_context(fake_ops):
    ops, build = fake_ops
    store = MagicMock()
    ops.read_subgraph.return_value = [{
        "name": "ООО Альфа", "label": "Organization",
        "description": "A supplier.", "qid": "Q5", "page_title": "ООО Альфа",
        "relations": [
            {"rl": "заключила договор", "dir": "out",
             "nn": "Договор № 17-К", "nl": "Document", "rd": "signed"},
        ],
    }]

    ctx = read_entity_subgraph(store, "ООО Альфа")

    build.assert_called_once_with(store)
    ops.read_subgraph.assert_called_once_with("ООО Альфа", 30)
    assert ctx.name == "ООО Альфа" and ctx.wikibase_qid == "Q5"
    assert ctx.relations == [
        ("заключила договор", "out", "Договор № 17-К", "Document", "signed")]
    # page_title falls back to name when the stored prop is empty
    ops.read_subgraph.return_value[0]["page_title"] = ""
    assert read_entity_subgraph(store, "ООО Альфа").page_title == "ООО Альфа"


def test_read_entity_subgraph_raises_on_empty_rows(fake_ops):
    ops, _build = fake_ops
    ops.read_subgraph.return_value = []
    with pytest.raises(ValueError, match="entity not found: 'Ghost'"):
        read_entity_subgraph(MagicMock(), "Ghost")


def test_read_entity_subgraph_passes_max_relations(fake_ops):
    ops, _build = fake_ops
    ops.read_subgraph.return_value = [{
        "name": "X", "label": "Organization", "description": "",
        "qid": "", "page_title": "", "relations": [],
    }]
    read_entity_subgraph(MagicMock(), "X", max_relations=7)
    ops.read_subgraph.assert_called_once_with("X", 7)


def test_read_citations_routes_through_seam_and_returns_text_docid_pairs(fake_ops):
    ops, build = fake_ops
    store = MagicMock()
    ops.read_citations.return_value = [
        {"text": "ООО Альфа заключила…", "doc_id": "d1"},
        {"text": "…контакт +7495…", "doc_id": "d2"},
    ]

    cites = read_citations(store, "ООО Альфа", k=5)

    build.assert_called_once_with(store)
    ops.read_citations.assert_called_once_with("ООО Альфа", 5)
    assert cites == [("ООО Альфа заключила…", "d1"), ("…контакт +7495…", "d2")]


def test_hash_folds_source_doc_ids():
    a = subgraph_hash(_ctx(), source_doc_ids=["d1", "d2"])
    b = subgraph_hash(_ctx(), source_doc_ids=["d1"])
    assert a != b
    # order-independent
    assert subgraph_hash(_ctx(), source_doc_ids=["d2", "d1"]) == a
    # default (no docs) stays backward-compatible and stable
    assert subgraph_hash(_ctx()) == subgraph_hash(_ctx(), source_doc_ids=[])


def test_read_source_docs_routes_through_seam_and_returns_result(fake_ops):
    ops, build = fake_ops
    store = MagicMock()
    ops.read_source_docs.return_value = ["d1", "d2"]

    result = read_source_docs(store, "X")

    assert result == ["d1", "d2"]
    build.assert_called_once_with(store)
    ops.read_source_docs.assert_called_once_with("X")
