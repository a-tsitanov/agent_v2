from src.graph.wiki_context import EntityContext, subgraph_hash


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


from unittest.mock import MagicMock
from src.graph.wiki_context import read_entity_subgraph, read_citations


def test_read_entity_subgraph_builds_context():
    store = MagicMock()
    store.structured_query.return_value = [{
        "name": "ООО Альфа", "label": "Organization",
        "description": "A supplier.", "qid": "Q5", "page_title": "ООО Альфа",
        "relations": [
            {"rl": "заключила договор", "dir": "out",
             "nn": "Договор № 17-К", "nl": "Document", "rd": "signed"},
        ],
    }]
    ctx = read_entity_subgraph(store, "ООО Альфа")
    assert ctx.name == "ООО Альфа" and ctx.wikibase_qid == "Q5"
    assert ctx.relations == [
        ("заключила договор", "out", "Договор № 17-К", "Document", "signed")]
    # page_title falls back to name when the stored prop is empty
    store.structured_query.return_value[0]["page_title"] = ""
    assert read_entity_subgraph(store, "ООО Альфа").page_title == "ООО Альфа"


def test_read_citations_returns_text_docid_pairs():
    store = MagicMock()
    store.structured_query.return_value = [
        {"text": "ООО Альфа заключила…", "doc_id": "d1"},
        {"text": "…контакт +7495…", "doc_id": "d2"},
    ]
    cites = read_citations(store, "ООО Альфа", k=5)
    assert cites == [("ООО Альфа заключила…", "d1"), ("…контакт +7495…", "d2")]
